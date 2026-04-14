"""
Parse task — PURE DATA PROCESSING.

This task has ZERO Telegram API calls.
All user communication is handled exclusively by deliver_task via delivery.py.

Responsibilities:
  1. Idempotency guard: skip if job already terminal (completed/failed)
  2. Mark job as 'processing'
  3. Run Telethon channel parser
  4. Save results to DB
  5. Consume credit (success) or refund credit (failure)
  6. Enqueue deliver_task for user notification — exactly once per path

Idempotency guard (Step 1)
──────────────────────────
`acks_late=True` means Celery re-queues the task if the worker crashes after
executing but before ACKing the message.  Without the guard, a completed job
could be re-parsed: Step 2 would flip status back to 'processing', Step 3
would re-run the Telethon parse, and Step 6 would enqueue a second
deliver_task — bypassing the deliver_task idempotency guard because
result_delivered is already True from the first run, so the second
deliver_task would be silently dropped but the parse results would be doubled.

The guard reads the job row before any mutation.  If status is already
'completed' or 'failed' it logs and returns immediately — no parse, no enqueue.

Error handling order mirrors stale cleanup in parse_orchestrator:
  1. update_job_status → 'failed' FIRST  (job is finalized before credit is released)
  2. refund_credit() SECOND              (credit returned only after job is closed)
  3. deliver_task.delay("failure")       (user is notified after state is final)
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

from telegram_bot.db.engine import get_sync_session
from telegram_bot.db.models import ParseJob
import telegram_bot.db.repository_sync as repo
from telegram_bot.parser.client import make_worker_client, get_worker_session_path
from telegram_bot.parser.channel_parser import parse_channel
from telegram_bot.services.limits_sync import consume_credit, refund_credit
from telegram_bot.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="telegram_bot.tasks.parse_task.run_parse_job",
    max_retries=0,          # failures are handled explicitly, not via Celery retry
    soft_time_limit=300,    # raise SoftTimeLimitExceeded after 5 min
    time_limit=360,         # hard kill after 6 min
    acks_late=True,
)
def run_parse_job(
    self,
    job_id: int,
    chat_id: int,
    channel_username: str,
    post_limit: int,
) -> None:
    """
    Main Celery task for channel parsing.

    parse_task performs NO Telegram Bot API calls.
    On both success and failure it enqueues deliver_task for user notification.
    """
    logger.info(
        "run_parse_job: START  job_id=%d celery_request_id=%s "
        "channel=%r post_limit=%d chat_id=%d",
        job_id, self.request.id, channel_username, post_limit, chat_id,
    )

    # ── Step 1: idempotency guard + mark as processing ────────────────────────
    # Reads current status before any mutation.  If the job is already terminal
    # (completed or failed), this is an acks_late re-delivery after a worker
    # crash — skip entirely.  This prevents double-parse and double-enqueue.
    with get_sync_session() as session:
        job = repo.get_job(session, job_id)
        if job is None:
            logger.error(
                "run_parse_job: ABORT  job_id=%d not found in DB  "
                "celery_request_id=%s",
                job_id, self.request.id,
            )
            return
        if job.status in ("completed", "failed"):
            logger.warning(
                "run_parse_job: SKIP  job_id=%d status=%r already terminal  "
                "celery_request_id=%s — acks_late re-delivery or duplicate task",
                job_id, job.status, self.request.id,
            )
            return
        logger.info(
            "run_parse_job: marking processing  job_id=%d celery_request_id=%s",
            job_id, self.request.id,
        )
        repo.update_job_status(
            session,
            job_id,
            "processing",
            processing_started_at=datetime.now(timezone.utc),
        )

    try:
        # Step 2: parse via Telethon.
        # make_worker_client() returns a fresh disconnected client every time.
        # _parse() connects it, runs the parse, and disconnects — all within
        # this single asyncio.run() call and its event loop.
        worker_session = get_worker_session_path()
        session_exists = os.path.exists(worker_session + ".session")
        logger.info(
            "run_parse_job: worker session_file=%r  file_exists=%s  "
            "job_id=%d celery_request_id=%s",
            worker_session + ".session", session_exists, job_id, self.request.id,
        )
        result = asyncio.run(_parse(channel_username, post_limit))

        # Step 3: save results to DB
        with get_sync_session() as session:
            repo.save_post_results(session, job_id, result.posts)

        # Step 4a: consume credit (success path)
        with get_sync_session() as session:
            consume_credit(session, job_id)
            repo.complete_job(session, job_id)

        logger.info(
            "run_parse_job: DONE  job_id=%d posts=%d channel=%r  "
            "celery_request_id=%s",
            job_id, len(result.posts), channel_username, self.request.id,
        )

        # Step 5: enqueue delivery (separate task — can retry independently)
        from telegram_bot.tasks.deliver_task import deliver_result  # noqa: PLC0415
        logger.info(
            "run_parse_job: ENQUEUEING deliver_task  "
            "job_id=%d chat_id=%d outcome=success celery_request_id=%s",
            job_id, chat_id, self.request.id,
        )
        deliver_result.delay(job_id, chat_id, "success")

    except Exception as exc:
        logger.exception(
            "run_parse_job: FAILED  job_id=%d channel=%r  celery_request_id=%s  error=%s",
            job_id, channel_username, self.request.id, exc,
        )

        # Error handling order: finalize job state BEFORE releasing credit.
        # This prevents a window where credit is back but job still appears active.

        # Step 4b-i: finalize job state first
        with get_sync_session() as session:
            repo.update_job_status(session, job_id, "failed", str(exc))

        # Step 4b-ii: return credit atomically (idempotent — safe if stale cleanup
        # already ran refund_credit on this job)
        with get_sync_session() as session:
            refunded = refund_credit(session, job_id)
            if refunded:
                logger.info(
                    "run_parse_job: credit refunded  job_id=%d celery_request_id=%s",
                    job_id, self.request.id,
                )

        # Step 5: notify user of failure
        from telegram_bot.tasks.deliver_task import deliver_result  # noqa: PLC0415
        logger.info(
            "run_parse_job: ENQUEUEING deliver_task  "
            "job_id=%d chat_id=%d outcome=failure celery_request_id=%s",
            job_id, chat_id, self.request.id,
        )
        deliver_result.delay(job_id, chat_id, "failure")


async def _parse(channel_username: str, post_limit: int):
    """
    Create a fresh Telethon client, connect, parse, disconnect — all within
    this coroutine so the client never outlives the event loop it was born in.

    A new client is created on every call: make_worker_client() is a factory,
    not a singleton. This is the only way to avoid:
      "The asyncio event loop must not change after connection"
    which occurs when a connected client is reused across asyncio.run() calls
    (each call creates a new event loop).
    """
    client = make_worker_client()
    try:
        await client.connect()

        # ── Authorization self-check ──────────────────────────────────────────
        session_file = getattr(client.session, "filename", "unknown")
        authorized = await client.is_user_authorized()
        logger.info(
            "_parse: connected — session_file=%r  authorized=%s",
            session_file,
            authorized,
        )
        if authorized:
            me = await client.get_me()
            logger.info(
                "_parse: identity — id=%s username=@%s name=%s",
                me.id,
                me.username,
                me.first_name,
            )
        else:
            logger.error(
                "_parse: session %r is NOT authorized — "
                "ResolveUsernameRequest WILL fail. "
                "Run: python -m telegram_bot.scripts.init_session session_worker_0",
                session_file,
            )
        # ─────────────────────────────────────────────────────────────────────

        return await parse_channel(client, channel_username, post_limit)
    finally:
        if client.is_connected():
            await client.disconnect()

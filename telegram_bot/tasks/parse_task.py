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

Analytics integration
─────────────────────
Each successful guard pass opens one parsing_sessions row and one
parsing_attempts row in the admin dashboard tables (separate from parse_jobs).
All analytics writes are wrapped in try/except — if they fail, parsing continues
unaffected.  The parse_jobs business flow is never interrupted by analytics.
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone

from telegram_bot.db.engine import get_sync_session
from telegram_bot.db.models import ParseJob
import telegram_bot.db.repository_sync as repo
import telegram_bot.services.analytics as analytics
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
    #
    # _bot_user_id is captured here while the session is open so it can be
    # passed to analytics after the session closes (job object becomes detached).
    _bot_user_id: int | None = None
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
        _bot_user_id = job.bot_user_id  # capture before session closes

    # ── Analytics: open session row + first attempt ───────────────────────────
    # Both IDs start as None — all downstream analytics calls guard on is not None,
    # so a failure here does not affect the parsing flow at all.
    _analytics_session_id: int | None = None
    _analytics_attempt_id: int | None = None
    _task_start_mono = time.monotonic()  # used for duration_ms in both paths

    try:
        with get_sync_session() as asess:
            # Resolve username for dashboard display (best-effort).
            _bot_user = repo.get_bot_user(asess, _bot_user_id)
            _username = (_bot_user.username if _bot_user else None) or ""

            # One parsing_sessions row per parse_jobs row.
            # parse_job_id stored in options so the dashboard can cross-reference.
            _analytics_session_id = analytics.create_parsing_session(
                asess,
                telegram_user_id=_bot_user_id,
                username=_username,
                channel=channel_username,
                post_limit=post_limit,
                options={"parse_job_id": job_id, "chat_id": chat_id},
            )
            # Increment attempts_count before starting the attempt row so the
            # count is always ≥ 1 when the attempt row is visible.
            analytics.increment_session_attempts(asess, _analytics_session_id)

            # One parsing_attempts row per Celery execution (attempt_number=1
            # here because max_retries=0; structure ready for future retry paths).
            _analytics_attempt_id = analytics.start_parsing_attempt(
                asess,
                session_id=_analytics_session_id,
                attempt_number=self.request.retries + 1,
                celery_task_id=self.request.id or "",
            )
    except Exception as _ana_exc:
        logger.warning(
            "run_parse_job: analytics open failed — continuing  "
            "job_id=%d  error=%s",
            job_id, _ana_exc,
        )

    # ── Main parsing flow ─────────────────────────────────────────────────────
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

        # ── Analytics: close attempt + session (success path) ─────────────────
        _duration_ms = int((time.monotonic() - _task_start_mono) * 1000)
        try:
            if _analytics_attempt_id is not None:
                with get_sync_session() as asess:
                    analytics.complete_parsing_attempt(
                        asess,
                        attempt_id=_analytics_attempt_id,
                        status="success",
                        duration_ms=_duration_ms,
                    )
            if _analytics_session_id is not None:
                with get_sync_session() as asess:
                    analytics.complete_parsing_session(
                        asess,
                        session_id=_analytics_session_id,
                        status="success",
                        duration_ms=_duration_ms,
                        result_rows=len(result.posts),
                    )
        except Exception as _ana_exc:
            logger.warning(
                "run_parse_job: analytics close (success) failed — continuing  "
                "job_id=%d  error=%s",
                job_id, _ana_exc,
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

        # ── Analytics: close attempt + session (failure path) ─────────────────
        _duration_ms = int((time.monotonic() - _task_start_mono) * 1000)
        try:
            # Use the exception type as a short machine-readable error code.
            # Truncate the message to 500 chars so it fits cleanly in the DB column.
            _error_code = type(exc).__name__
            _error_msg = str(exc)[:500]

            if _analytics_attempt_id is not None:
                with get_sync_session() as asess:
                    analytics.complete_parsing_attempt(
                        asess,
                        attempt_id=_analytics_attempt_id,
                        status="failed",
                        duration_ms=_duration_ms,
                        error_code=_error_code,
                        error_message=_error_msg,
                    )
            if _analytics_session_id is not None:
                with get_sync_session() as asess:
                    analytics.complete_parsing_session(
                        asess,
                        session_id=_analytics_session_id,
                        status="failed",
                        duration_ms=_duration_ms,
                        error_code=_error_code,
                        error_message=_error_msg,
                    )
        except Exception as _ana_exc:
            logger.warning(
                "run_parse_job: analytics close (failure) failed — continuing  "
                "job_id=%d  error=%s",
                job_id, _ana_exc,
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

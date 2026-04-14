"""
Deliver task — PURE USER COMMUNICATION.

This task has no parsing logic and does not touch credits or job status.
Results are already in DB when this task runs.

Separated from parse_task so that:
  - Delivery can be retried on Telegram API errors without re-running the parse
  - A Telegram outage does not count as a parsing failure
  - User can (in future) request re-delivery of any completed job

Retry policy: up to 3 attempts with exponential backoff (10s, 20s, 40s).

Retry-safety:
  Every call to delivery.notify_success / notify_failure reloads all data
  from the DB and builds a fresh CSV.  No stream object or ORM reference is
  shared between attempts — each retry is a clean start.
"""

import logging
import traceback

from sqlalchemy import update

from telegram_bot.db.engine import get_sync_session
from telegram_bot.db.models import ParseJob
import telegram_bot.db.repository_sync as repo
from telegram_bot.services import delivery
from telegram_bot.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    name="telegram_bot.tasks.deliver_task.deliver_result",
    max_retries=3,
    default_retry_delay=10,      # first retry after 10s; doubles each retry
    soft_time_limit=60,
    time_limit=90,
    acks_late=True,
)
def deliver_result(self, job_id: int, chat_id: int, outcome: str) -> None:
    """
    Send parse result or failure message to the user.

    Args:
        job_id:  ParseJob primary key (results are loaded from DB here)
        chat_id: Telegram chat ID to send messages to
        outcome: "success" | "failure"

    On Telegram API error: retries up to 3× with exponential backoff.
    Credit status and job status are NOT modified here — they were finalised
    by parse_task before this task was enqueued.

    Idempotency guard: mark_result_delivered_if_not_yet() atomically claims
    the delivery slot before any Telegram API call.  If the slot is already
    taken (Celery retry after a successful send, or duplicate task), this
    task exits immediately without re-sending.

    Every retry builds the CSV fresh from DB — no stale / closed BytesIO.
    """
    attempt = self.request.retries + 1
    max_attempts = self.max_retries + 1

    logger.info(
        "deliver_result: START  job=%s outcome=%s chat=%s attempt=%d/%d",
        job_id, outcome, chat_id, attempt, max_attempts,
    )

    # ── Idempotency guard ─────────────────────────────────────────────────────
    # Atomically claim the delivery slot.  Only ONE invocation wins; all
    # subsequent calls (retries or duplicates) see result_delivered=True and
    # exit without touching Telegram.
    logger.info(
        "deliver_result: CLAIMING slot  job=%s outcome=%s attempt=%d/%d",
        job_id, outcome, attempt, max_attempts,
    )
    with get_sync_session() as session:
        claimed = repo.mark_result_delivered_if_not_yet(session, job_id)

    if not claimed:
        logger.warning(
            "deliver_result: SKIP  job=%s already delivered — duplicate or "
            "retry after a successful send (outcome=%s, attempt=%d/%d)",
            job_id, outcome, attempt, max_attempts,
        )
        return

    logger.info(
        "deliver_result: SLOT CLAIMED  job=%s outcome=%s — calling notify",
        job_id, outcome,
    )
    # ─────────────────────────────────────────────────────────────────────────

    try:
        if outcome == "success":
            logger.info(
                "deliver_result: calling notify_success  job=%s chat=%s",
                job_id, chat_id,
            )
            delivery.notify_success(job_id, chat_id)
        else:
            logger.info(
                "deliver_result: calling notify_failure  job=%s chat=%s",
                job_id, chat_id,
            )
            delivery.notify_failure(job_id, chat_id)

        logger.info(
            "deliver_result: OK  job=%s outcome=%s chat=%s attempt=%d/%d",
            job_id, outcome, chat_id, attempt, max_attempts,
        )

    except Exception as exc:
        # Delivery failed AFTER we claimed the slot.
        # Reset the flag so the next retry can re-claim and re-attempt.
        # This is the ONLY place we reset result_delivered.
        logger.warning(
            "deliver_result: FAIL  job=%s outcome=%s chat=%s "
            "attempt=%d/%d  error=%s\n%s",
            job_id, outcome, chat_id,
            attempt, max_attempts,
            exc,
            traceback.format_exc(),
        )

        # Reset delivery slot so the retry gets a fresh start
        try:
            with get_sync_session() as session:
                session.execute(
                    update(ParseJob)
                    .where(ParseJob.id == job_id)
                    .values(result_delivered=False, delivered_at=None)
                )
                session.commit()
            logger.info(
                "deliver_result: slot reset for retry — job=%s attempt=%d/%d",
                job_id, attempt, max_attempts,
            )
        except Exception as reset_exc:
            # Log but do not suppress — still schedule the retry
            logger.error(
                "deliver_result: failed to reset delivery slot for job=%s: %s",
                job_id, reset_exc,
            )

        # Exponential backoff: 10s → 20s → 40s
        raise self.retry(exc=exc, countdown=10 * (2 ** self.request.retries))

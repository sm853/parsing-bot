"""
Deliver task — PURE USER COMMUNICATION.

This task has no parsing logic and does not touch credits or job status.
Results are already in DB when this task runs.

Separated from parse_task so that:
  - Delivery can be retried on Telegram API errors without re-running the parse
  - A Telegram outage does not count as a parsing failure

Retry policy: up to 3 attempts with exponential backoff (10s, 20s, 40s).

Idempotency model (two layers)
──────────────────────────────
Layer 1 — job-level guard (this file):
  result_delivered=True means ALL steps completed.  Task exits immediately
  without calling delivery.py at all.

Layer 2 — per-step guard (delivery.py / _send_step):
  Each step (summary, document, keyboard) has its own DB boolean.
  Atomic claim before send + reset on failure ensures each message is sent
  at most once, even if deliver_task is retried after a partial delivery.

On retry after partial failure:
  result_delivered is still False (only set after all steps complete).
  The task re-enters delivery.notify_success/notify_failure, which skips
  completed steps and re-attempts only the failed one.
  No message is ever sent twice.
"""

import logging
import traceback

from telegram_bot.db.engine import get_sync_session
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
        job_id:  ParseJob primary key
        chat_id: Telegram chat ID
        outcome: "success" | "failure"

    Guards:
      result_delivered=True  → all steps done, skip entirely
      per-step flags in DB   → resume from first incomplete step on retry
    """
    attempt = self.request.retries + 1
    max_attempts = self.max_retries + 1

    logger.info(
        "deliver_result: START  job=%s outcome=%s chat=%s attempt=%d/%d",
        job_id, outcome, chat_id, attempt, max_attempts,
    )

    # ── Layer 1 guard: skip if already fully delivered ────────────────────────
    with get_sync_session() as session:
        job = repo.get_job(session, job_id)
        if job is None:
            logger.error("deliver_result: ABORT  job=%s not found in DB", job_id)
            return
        if job.result_delivered:
            logger.warning(
                "deliver_result: SKIP  job=%s already fully delivered  "
                "outcome=%s attempt=%d/%d",
                job_id, outcome, attempt, max_attempts,
            )
            return
    # ─────────────────────────────────────────────────────────────────────────

    try:
        if outcome == "success":
            delivery.notify_success(job_id, chat_id)
        else:
            delivery.notify_failure(job_id, chat_id)

        # All steps completed — mark the whole delivery done.
        with get_sync_session() as session:
            repo.mark_result_delivered_if_not_yet(session, job_id)

        logger.info(
            "deliver_result: DONE  job=%s outcome=%s chat=%s attempt=%d/%d",
            job_id, outcome, chat_id, attempt, max_attempts,
        )

    except Exception as exc:
        # delivery._send_step already reset the failed step's flag.
        # result_delivered stays False — the retry will re-enter and resume
        # from the first incomplete step without re-sending earlier ones.
        logger.warning(
            "deliver_result: FAIL  job=%s outcome=%s chat=%s "
            "attempt=%d/%d  error=%s\n%s",
            job_id, outcome, chat_id,
            attempt, max_attempts,
            exc,
            traceback.format_exc(),
        )
        raise self.retry(exc=exc, countdown=10 * (2 ** self.request.retries))

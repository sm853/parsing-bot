"""
Parsing orchestration — business logic layer.

Handlers import ONLY this module. No direct DB or Celery imports in handlers.

Responsibilities:
  1. Stale job cleanup (processing AND pending) before any new job attempt
  2. Idempotency guard (one active job per user)
  3. Credit availability check
  4. Credit reservation
  5. Job creation — race-safe via IntegrityError catch
  6. Celery task dispatch (only when a fresh job was created)

Race-condition strategy
───────────────────────
Layer 1 — handler (asyncio): module-level _in_flight set in parsing_flow.py
  eliminates duplicate processing within the same bot process before any DB work.

Layer 2 — DB unique index: the partial unique index `one_active_job_per_user`
  on parse_jobs(bot_user_id) WHERE status IN ('pending','processing')
  is the last hard guard. Two concurrent requests (different processes, or a
  rare asyncio scheduling edge case) that both slip past layer 1 will race at
  the INSERT. The loser gets IntegrityError. We catch it here, refund the
  credit that was reserved for the loser, and return the winner's job with
  created_new=False so the handler shows the correct "already running" message.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

import telegram_bot.db.repository_async as repo
from telegram_bot.config import settings
from telegram_bot.db.models import ParseJob
from telegram_bot.services import limits

logger = logging.getLogger(__name__)


@dataclass
class StartParsingResult:
    """
    Structured return value from start_parsing().

    Exactly one of these is true in any given return:
      - created_new=True, job set       → fresh job dispatched
      - created_new=False, job set      → existing active job returned (idempotency)
      - no_credits=True, job=None       → user has 0 available_credits

    Handlers use this to show the right message without re-querying the DB.
    """
    job: Optional[ParseJob]
    created_new: bool       # True → job was just created and dispatched
    no_credits: bool        # True → no credits; caller should show payment prompt


async def start_parsing(
    session: AsyncSession,
    bot_user_id: int,
    channel_username: str,
    channel_title: Optional[str],
    post_limit: int,
    chat_id: int,
) -> StartParsingResult:
    """
    Full orchestration entry point called by the handler on confirmation.

    Returns a StartParsingResult. See the dataclass docstring for the
    three possible outcomes.

    Flow:
      1. Clean up stale jobs (both processing and pending timeouts)
      2. Check for existing active job (idempotency — return it if found)
      3. Check available_credits
      4. reserve_credit()  ← atomic UPDATE; credit is decremented here
      5. create_parse_job() inside try/except IntegrityError
         → IntegrityError means race was lost: refund credit, return existing job
      6. Dispatch Celery task (only reaches this point when created_new=True)
    """
    logger.info(
        "start_parsing: ENTER  user=%s channel=%r post_limit=%d chat_id=%d",
        bot_user_id, channel_username, post_limit, chat_id,
    )

    # ── Step 1: clean up stale jobs ───────────────────────────────────────────
    await _cleanup_stale_jobs(session, bot_user_id)

    # ── Step 2: idempotency check ─────────────────────────────────────────────
    existing = await repo.get_active_job(session, bot_user_id)
    if existing is not None:
        logger.info(
            "start_parsing: EXISTING JOB  user=%s job_id=%s status=%s channel=%r"
            " → returning created_new=False (no Celery dispatch)",
            bot_user_id, existing.id, existing.status, existing.channel_username,
        )
        return StartParsingResult(job=existing, created_new=False, no_credits=False)

    # ── Step 3: credit check ──────────────────────────────────────────────────
    reserved = await limits.reserve_credit(session, bot_user_id)
    if not reserved:
        logger.info(
            "start_parsing: NO CREDITS  user=%s → returning no_credits=True",
            bot_user_id,
        )
        return StartParsingResult(job=None, created_new=False, no_credits=True)

    logger.info(
        "start_parsing: credit reserved for user=%s — attempting job creation",
        bot_user_id,
    )

    # ── Step 4: create job record — race-safe ─────────────────────────────────
    # The partial unique index `one_active_job_per_user` prevents two concurrent
    # pending/processing jobs for the same user.  If two requests both passed
    # steps 1-3 (both found no active job, both reserved credit), the loser's
    # INSERT will raise IntegrityError.  We catch it, restore the credit, and
    # return the winner's job.
    try:
        job = await repo.create_parse_job(
            session,
            bot_user_id=bot_user_id,
            channel_username=channel_username,
            channel_title=channel_title,
            post_limit=post_limit,
        )
    except IntegrityError:
        # Race lost: another concurrent request won the INSERT.
        # The session is now in a rolled-back state; start a fresh transaction
        # to restore the credit we decremented and to fetch the winner's job.
        await session.rollback()

        logger.warning(
            "start_parsing: RACE LOST  user=%s — IntegrityError on INSERT "
            "(unique constraint `one_active_job_per_user`). "
            "Restoring reserved credit and returning winner's job.",
            bot_user_id,
        )

        # Restore the credit: the reservation was committed before the INSERT
        # attempt, so rollback() above did NOT undo it.  We increment directly.
        await limits.restore_reserved_credit(session, bot_user_id)

        # Fetch the winning job to return to the handler
        winner = await repo.get_active_job(session, bot_user_id)
        logger.info(
            "start_parsing: RACE LOST resolved — winner job_id=%s for user=%s",
            winner.id if winner else None,
            bot_user_id,
        )
        return StartParsingResult(job=winner, created_new=False, no_credits=False)

    logger.info(
        "start_parsing: JOB INSERTED  job_id=%d user=%s channel=%r",
        job.id, bot_user_id, channel_username,
    )

    # ── Step 5: dispatch to Celery ────────────────────────────────────────────
    # Only reachable when THIS request created the job (created_new=True).
    # Lazy import avoids circular dependency.
    from telegram_bot.tasks.parse_task import run_parse_job  # noqa: PLC0415

    logger.info(
        "start_parsing: DISPATCHING  job_id=%d channel=%r post_limit=%d "
        "chat_id=%d user=%s",
        job.id, channel_username, post_limit, chat_id, bot_user_id,
    )
    celery_result = run_parse_job.delay(job.id, chat_id, channel_username, post_limit)

    # Persist the Celery task ID for observability / debugging
    await repo.update_job_status(session, job.id, "pending", celery_task_id=celery_result.id)
    await session.refresh(job)

    logger.info(
        "start_parsing: DONE  job_id=%d celery_task_id=%s user=%s channel=%r x%d"
        " → created_new=True",
        job.id, celery_result.id, bot_user_id, channel_username, post_limit,
    )
    return StartParsingResult(job=job, created_new=True, no_credits=False)


async def get_job_status(
    session: AsyncSession, bot_user_id: int
) -> Optional[ParseJob]:
    """Return the active job for status display, or None if idle."""
    return await repo.get_active_job(session, bot_user_id)


# ── Internal helpers ───────────────────────────────────────────────────────────

async def _cleanup_stale_jobs(session: AsyncSession, user_id: int) -> None:
    """
    Compensating operation: marks stale jobs as failed and refunds credits.

    Covers two stale scenarios:
      - status='processing' AND processing_started_at older than STALE_PROCESSING_MINUTES
        (Celery worker died mid-parse or hit the timeout)
      - status='pending' AND created_at older than STALE_PENDING_MINUTES
        (Celery never picked up the task — broker issue or worker not running)

    Order is intentional:
      1. update_job_status → 'failed' FIRST  (job is finalized in DB)
      2. refund_credit() SECOND              (credit returned only after job is closed)
    This prevents a window where the job still appears 'active' but credit is back.

    Both operations are idempotent:
      - update_job_status on an already-failed job is a no-op
      - refund_credit uses conditional UPDATE (WHERE credit_status='reserved') so
        concurrent or repeated calls never double-credit the user.
    """
    stale_jobs = await repo.get_stale_jobs(
        session,
        user_id,
        processing_threshold=settings.STALE_PROCESSING_MINUTES,
        pending_threshold=settings.STALE_PENDING_MINUTES,
    )

    for job in stale_jobs:
        logger.warning(
            "start_parsing: stale job cleanup — job_id=%s status=%s user=%s",
            job.id, job.status, user_id,
        )
        await repo.update_job_status(session, job.id, "failed", "Timed out")
        refunded = await limits.refund_credit(session, job.id)
        if refunded:
            logger.info(
                "start_parsing: refunded credit for stale job_id=%s", job.id
            )

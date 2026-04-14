"""
Credit lifecycle management.

All credits live in BotUser.available_credits — unified field for both
free (initial) and paid (Stars) credits. No distinction at storage level.

Credit state machine per job:
  reserved  → decremented when job is created
  consumed  → finalized after successful parse (no balance change — already decremented)
  refunded  → credit restored after failure (idempotent via conditional DB UPDATE)

Async variants (this file) are used by the bot process.
Sync variants live in telegram_bot/services/limits_sync.py, used by Celery workers.
"""

from sqlalchemy.ext.asyncio import AsyncSession

import telegram_bot.db.repository_async as repo


async def reserve_credit(session: AsyncSession, user_id: int) -> bool:
    """
    Atomically decrement available_credits by 1.
    Returns True if successful, False if the user has no credits left.
    Single UPDATE statement — race-condition safe.
    """
    return await repo.decrement_credits(session, user_id)


async def consume_credit(session: AsyncSession, job_id: int) -> None:
    """
    Mark credit as consumed after a successful parse.
    No balance change (credit was already decremented at reserve time).
    """
    await repo.set_credit_status(session, job_id, "consumed")


async def refund_credit(session: AsyncSession, job_id: int) -> bool:
    """
    IDEMPOTENT. Return credit on parse failure or stale job cleanup.

    Delegates to repo.conditional_refund_credit which executes both
    UPDATE statements inside a single DB transaction:
      1. parse_jobs.credit_status: 'reserved' → 'refunded'  (conditional)
      2. bot_users.available_credits += 1                    (only if step 1 ran)

    Returns True if refund was applied, False if credit was already
    consumed or previously refunded.

    Safe to call from stale cleanup AND parse_task error handler on the
    same job_id — only the first call applies.
    """
    return await repo.conditional_refund_credit(session, job_id)


async def restore_reserved_credit(session: AsyncSession, user_id: int) -> None:
    """
    Restore a credit that was reserved by reserve_credit() but whose job
    INSERT failed due to a concurrent unique-constraint violation (race lost).

    This is distinct from refund_credit(job_id): no job was ever committed,
    so there is no job row to refund against. We simply increment the balance.

    Called exclusively from parse_orchestrator.start_parsing() when it catches
    IntegrityError from repo.create_parse_job().
    """
    await repo.increment_credits(session, user_id)


async def grant_paid_credits(session: AsyncSession, user_id: int, count: int) -> None:
    """Add credits after a Telegram Stars payment."""
    await repo.increment_credits(session, user_id, count)


async def get_available_credits(session: AsyncSession, user_id: int) -> int:
    """Return the current credit balance for display purposes."""
    user = await repo.get_bot_user(session, user_id)
    return user.available_credits if user else 0

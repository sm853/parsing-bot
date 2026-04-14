"""
Async repository — used exclusively by the bot process.
All functions accept an AsyncSession and return ORM objects or primitives.
No business logic here: pure DB I/O only.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from telegram_bot.db.models import BotUser, CommenterResult, ParseJob, PostResult
from telegram_bot.parser.channel_parser import PostData


# ── BotUser ────────────────────────────────────────────────────────────────────

async def upsert_bot_user(
    session: AsyncSession,
    telegram_id: int,
    username: Optional[str],
    first_name: Optional[str],
) -> BotUser:
    """Insert new user or update username/first_name if already exists."""
    user = await session.get(BotUser, telegram_id)
    if user is None:
        from telegram_bot.config import settings
        user = BotUser(
            telegram_id=telegram_id,
            username=username,
            first_name=first_name,
            available_credits=settings.INITIAL_CREDITS,
        )
        session.add(user)
    else:
        user.username = username
        user.first_name = first_name
    await session.commit()
    await session.refresh(user)
    return user


async def get_bot_user(session: AsyncSession, telegram_id: int) -> Optional[BotUser]:
    return await session.get(BotUser, telegram_id)


# ── ParseJob ──────────────────────────────────────────────────────────────────

async def get_active_job(session: AsyncSession, user_id: int) -> Optional[ParseJob]:
    """Return job with status pending or processing for this user, if any."""
    result = await session.execute(
        select(ParseJob).where(
            and_(
                ParseJob.bot_user_id == user_id,
                ParseJob.status.in_(["pending", "processing"]),
            )
        )
    )
    return result.scalar_one_or_none()


async def get_stale_jobs(
    session: AsyncSession,
    user_id: int,
    processing_threshold: int,
    pending_threshold: int,
) -> list[ParseJob]:
    """
    Return jobs that are considered stale:
      - status='processing' AND processing_started_at older than processing_threshold minutes
      - status='pending'    AND created_at older than pending_threshold minutes
    """
    now = datetime.now(timezone.utc)
    proc_cutoff = now - timedelta(minutes=processing_threshold)
    pend_cutoff = now - timedelta(minutes=pending_threshold)

    result = await session.execute(
        select(ParseJob).where(
            and_(
                ParseJob.bot_user_id == user_id,
                (
                    (ParseJob.status == "processing") & (ParseJob.processing_started_at < proc_cutoff)
                    | (ParseJob.status == "pending") & (ParseJob.created_at < pend_cutoff)
                ),
            )
        )
    )
    return list(result.scalars().all())


async def create_parse_job(
    session: AsyncSession,
    bot_user_id: int,
    channel_username: str,
    channel_title: Optional[str],
    post_limit: int,
) -> ParseJob:
    job = ParseJob(
        bot_user_id=bot_user_id,
        channel_username=channel_username,
        channel_title=channel_title,
        post_limit=post_limit,
        status="pending",
        credit_status="reserved",
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return job


async def update_job_status(
    session: AsyncSession,
    job_id: int,
    status: str,
    error_message: Optional[str] = None,
    processing_started_at: Optional[datetime] = None,
    celery_task_id: Optional[str] = None,
) -> None:
    values: dict = {"status": status}
    if error_message is not None:
        values["error_message"] = error_message
    if processing_started_at is not None:
        values["processing_started_at"] = processing_started_at
    if celery_task_id is not None:
        values["celery_task_id"] = celery_task_id
    await session.execute(update(ParseJob).where(ParseJob.id == job_id).values(**values))
    await session.commit()


async def complete_job(session: AsyncSession, job_id: int) -> None:
    await session.execute(
        update(ParseJob)
        .where(ParseJob.id == job_id)
        .values(status="completed", completed_at=datetime.now(timezone.utc))
    )
    # Increment total_parses_done on BotUser
    job = await session.get(ParseJob, job_id)
    if job:
        await session.execute(
            update(BotUser)
            .where(BotUser.telegram_id == job.bot_user_id)
            .values(total_parses_done=BotUser.total_parses_done + 1)
        )
    await session.commit()


async def save_post_results(
    session: AsyncSession,
    job_id: int,
    posts: list[PostData],
) -> None:
    """Bulk-insert PostResult + CommenterResult rows for a completed job."""
    for p in posts:
        post_row = PostResult(
            job_id=job_id,
            post_id=p.post_id,
            post_link=p.link,
            post_text=p.text,
            media_type=p.media_type,
            extracted_links=json.dumps(p.extracted_links),
            views=p.views,
            reactions_count=p.reactions_count,
            comments_count=p.comments_count,
        )
        session.add(post_row)
        await session.flush()  # get post_row.id
        for username in p.commenters:
            session.add(CommenterResult(post_result_id=post_row.id, username=username))
    await session.commit()


# ── Credit helpers (called only from limits.py) ───────────────────────────────

async def decrement_credits(session: AsyncSession, user_id: int) -> bool:
    """
    Atomically decrements available_credits by 1 if > 0.
    Returns True if decremented, False if already 0.
    Single UPDATE statement — no separate SELECT needed.
    """
    result = await session.execute(
        update(BotUser)
        .where(and_(BotUser.telegram_id == user_id, BotUser.available_credits > 0))
        .values(available_credits=BotUser.available_credits - 1)
        .returning(BotUser.telegram_id)
    )
    row_id = result.scalar_one_or_none()
    await session.commit()
    return row_id is not None


async def increment_credits(session: AsyncSession, user_id: int, count: int = 1) -> None:
    await session.execute(
        update(BotUser)
        .where(BotUser.telegram_id == user_id)
        .values(available_credits=BotUser.available_credits + count)
    )
    await session.commit()


async def set_credit_status(session: AsyncSession, job_id: int, status: str) -> None:
    """Set credit_status on a job (used for 'consumed' transition)."""
    await session.execute(
        update(ParseJob).where(ParseJob.id == job_id).values(credit_status=status)
    )
    await session.commit()


async def mark_result_delivered_if_not_yet(
    session: AsyncSession, job_id: int
) -> bool:
    """
    IDEMPOTENT delivery guard.

    Atomically flips result_delivered False → True and records delivered_at.
    Returns True  if this call claimed the delivery slot (caller should send).
    Returns False if delivery was already recorded (caller must skip silently).

    Safe to call multiple times from retried deliver_task invocations — only
    the first call returns True.
    """
    from datetime import datetime, timezone  # noqa: PLC0415

    result = await session.execute(
        update(ParseJob)
        .where(and_(ParseJob.id == job_id, ParseJob.result_delivered.is_(False)))
        .values(result_delivered=True, delivered_at=datetime.now(timezone.utc))
        .returning(ParseJob.id)
    )
    # Consume the cursor BEFORE commit — calling commit() with an open cursor
    # raises "cannot commit transaction - SQL statements in progress" on SQLite.
    claimed_id = result.scalar_one_or_none()
    await session.commit()
    return claimed_id is not None


async def conditional_refund_credit(session: AsyncSession, job_id: int) -> bool:
    """
    IDEMPOTENT + TRANSACTIONAL refund guard.

    Executes both updates inside a single transaction:
      1. UPDATE parse_jobs SET credit_status='refunded'
         WHERE id=:job_id AND credit_status='reserved'   ← double-refund guard
      2. If rowcount == 1: UPDATE bot_users SET available_credits += 1

    Returns True if the refund was applied, False if skipped
    (credit was already consumed or previously refunded).

    Safe to call multiple times on the same job_id — only the first call applies.
    """
    # Step 1: conditional job update
    result = await session.execute(
        update(ParseJob)
        .where(and_(ParseJob.id == job_id, ParseJob.credit_status == "reserved"))
        .values(credit_status="refunded")
        .returning(ParseJob.bot_user_id)
    )
    bot_user_id = result.scalar_one_or_none()

    if bot_user_id is None:
        # Guard blocked: credit was already consumed or refunded — skip silently
        await session.rollback()
        return False

    # Step 2: restore credit to user
    await session.execute(
        update(BotUser)
        .where(BotUser.telegram_id == bot_user_id)
        .values(available_credits=BotUser.available_credits + 1)
    )
    await session.commit()
    return True

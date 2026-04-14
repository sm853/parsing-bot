"""
Sync repository — used exclusively by Celery workers.
Identical function signatures to repository_async but uses synchronous SQLAlchemy Session.

Worker NEVER imports repository_async. Bot NEVER imports repository_sync.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, update
from sqlalchemy.orm import Session

from telegram_bot.db.models import BotUser, CommenterResult, ParseJob, PostResult
from telegram_bot.parser.channel_parser import PostData


# ── BotUser ────────────────────────────────────────────────────────────────────

def get_bot_user(session: Session, telegram_id: int) -> Optional[BotUser]:
    return session.get(BotUser, telegram_id)


# ── ParseJob ──────────────────────────────────────────────────────────────────

def get_job(session: Session, job_id: int) -> Optional[ParseJob]:
    """Load a single ParseJob by primary key."""
    return session.get(ParseJob, job_id)


def get_active_job(session: Session, user_id: int) -> Optional[ParseJob]:
    return (
        session.query(ParseJob)
        .filter(
            ParseJob.bot_user_id == user_id,
            ParseJob.status.in_(["pending", "processing"]),
        )
        .first()
    )


def update_job_status(
    session: Session,
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
    session.execute(update(ParseJob).where(ParseJob.id == job_id).values(**values))
    session.commit()


def complete_job(session: Session, job_id: int) -> None:
    session.execute(
        update(ParseJob)
        .where(ParseJob.id == job_id)
        .values(status="completed", completed_at=datetime.now(timezone.utc))
    )
    job = session.get(ParseJob, job_id)
    if job:
        session.execute(
            update(BotUser)
            .where(BotUser.telegram_id == job.bot_user_id)
            .values(total_parses_done=BotUser.total_parses_done + 1)
        )
    session.commit()


def save_post_results(session: Session, job_id: int, posts: list[PostData]) -> None:
    """Bulk-insert PostResult + CommenterResult rows."""
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
        session.flush()
        for username in p.commenters:
            session.add(CommenterResult(post_result_id=post_row.id, username=username))
    session.commit()


def get_job_with_posts(session: Session, job_id: int) -> Optional[ParseJob]:
    """Load job with eagerly loaded posts + commenters for report building."""
    from sqlalchemy.orm import joinedload
    return (
        session.query(ParseJob)
        .options(
            joinedload(ParseJob.posts).joinedload(PostResult.commenters)
        )
        .filter(ParseJob.id == job_id)
        .first()
    )


# ── Credit helpers (called only from limits.py) ───────────────────────────────

def decrement_credits(session: Session, user_id: int) -> bool:
    """
    Atomically decrements available_credits by 1 if > 0.
    Returns True if decremented, False if already 0.
    """
    result = session.execute(
        update(BotUser)
        .where(and_(BotUser.telegram_id == user_id, BotUser.available_credits > 0))
        .values(available_credits=BotUser.available_credits - 1)
        .returning(BotUser.telegram_id)
    )
    row_id = result.scalar_one_or_none()
    session.commit()
    return row_id is not None


def increment_credits(session: Session, user_id: int, count: int = 1) -> None:
    session.execute(
        update(BotUser)
        .where(BotUser.telegram_id == user_id)
        .values(available_credits=BotUser.available_credits + count)
    )
    session.commit()


def set_credit_status(session: Session, job_id: int, status: str) -> None:
    """Set credit_status on a job (used for 'consumed' transition)."""
    session.execute(
        update(ParseJob).where(ParseJob.id == job_id).values(credit_status=status)
    )
    session.commit()


def mark_result_delivered_if_not_yet(session: Session, job_id: int) -> bool:
    """
    IDEMPOTENT delivery guard (sync version for Celery workers).

    Atomically flips result_delivered False → True and records delivered_at.
    Returns True  if this call claimed the delivery slot (caller should send).
    Returns False if delivery was already recorded (caller must skip silently).

    Safe to call multiple times from retried deliver_task invocations — only
    the first call returns True.
    """
    from datetime import datetime, timezone  # noqa: PLC0415

    result = session.execute(
        update(ParseJob)
        .where(and_(ParseJob.id == job_id, ParseJob.result_delivered.is_(False)))
        .values(result_delivered=True, delivered_at=datetime.now(timezone.utc))
        .returning(ParseJob.id)
    )
    # Consume the cursor BEFORE commit — calling commit() with an open cursor
    # raises "cannot commit transaction - SQL statements in progress" on SQLite.
    claimed_id = result.scalar_one_or_none()
    session.commit()
    return claimed_id is not None


def conditional_refund_credit(session: Session, job_id: int) -> bool:
    """
    IDEMPOTENT + TRANSACTIONAL refund guard.

    Single transaction containing both UPDATEs:
      1. UPDATE parse_jobs SET credit_status='refunded'
         WHERE id=:job_id AND credit_status='reserved'   ← double-refund guard
      2. If rowcount == 1: UPDATE bot_users SET available_credits += 1

    Returns True if refund was applied, False if skipped
    (credit already consumed or previously refunded).

    Safe to call from stale cleanup AND parse_task exception handler on the
    same job_id — only the first call applies, the second is a no-op.
    """
    # Step 1: conditional job update
    result = session.execute(
        update(ParseJob)
        .where(and_(ParseJob.id == job_id, ParseJob.credit_status == "reserved"))
        .values(credit_status="refunded")
        .returning(ParseJob.bot_user_id)
    )
    bot_user_id = result.scalar_one_or_none()

    if bot_user_id is None:
        # Guard blocked: already consumed or refunded — skip silently
        session.rollback()
        return False

    # Step 2: restore credit
    session.execute(
        update(BotUser)
        .where(BotUser.telegram_id == bot_user_id)
        .values(available_credits=BotUser.available_credits + 1)
    )
    session.commit()
    return True

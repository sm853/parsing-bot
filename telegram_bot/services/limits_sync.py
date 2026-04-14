"""
Sync credit lifecycle — used exclusively by Celery workers.
Identical contract to limits.py but operates on a synchronous SQLAlchemy Session.
"""

from sqlalchemy.orm import Session

import telegram_bot.db.repository_sync as repo


def consume_credit(session: Session, job_id: int) -> None:
    """Mark credit as consumed after a successful parse."""
    repo.set_credit_status(session, job_id, "consumed")


def refund_credit(session: Session, job_id: int) -> bool:
    """
    IDEMPOTENT. Return credit on parse failure.
    See limits.py for full contract.
    """
    return repo.conditional_refund_credit(session, job_id)

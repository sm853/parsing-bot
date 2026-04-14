"""
Bot-specific ORM models — new tables only.
The existing backend tables (users, channels, posts, comments) are untouched.
"""

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, ForeignKey, Index,
    Integer, String, Text, func, text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class BotUser(Base):
    """Telegram user identity and credit balance."""

    __tablename__ = "bot_users"

    # Use Telegram user_id directly as PK — stable, no lookup needed
    telegram_id = Column(BigInteger, primary_key=True)
    username = Column(String(255), nullable=True)       # @handle, can be None or change
    first_name = Column(String(255), nullable=True)

    # Unified credit counter: free (initial) + paid (Stars) credits combined.
    # No distinction at storage level — reserve/consume/refund all use this field.
    available_credits = Column(Integer, default=5, nullable=False)

    total_parses_done = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_active_at = Column(DateTime(timezone=True), onupdate=func.now())

    jobs = relationship("ParseJob", back_populates="bot_user", lazy="select")


class ParseJob(Base):
    """Represents one parsing task lifecycle from creation to delivery."""

    __tablename__ = "parse_jobs"

    # ── Partial unique index ───────────────────────────────────────────────────
    # Enforces: at most one pending|processing job per user at any given time.
    #
    # This is the database-level race guard for concurrent confirm-button taps.
    # When two requests slip past the handler-level in-flight guard and both
    # reach create_parse_job(), the loser gets an IntegrityError which
    # parse_orchestrator.start_parsing() catches, refunds the loser's credit,
    # and returns the winner's job with created_new=False.
    #
    # Defined here (not only in the Alembic migration) so that
    # Base.metadata.create_all() in test environments creates it too.
    # Both PostgreSQL and SQLite 3.8.9+ support partial (WHERE-filtered) indexes.
    __table_args__ = (
        Index(
            "one_active_job_per_user",
            "bot_user_id",
            unique=True,
            postgresql_where=text("status IN ('pending', 'processing')"),
            sqlite_where=text("status IN ('pending', 'processing')"),
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    bot_user_id = Column(BigInteger, ForeignKey("bot_users.telegram_id"), nullable=False)

    channel_username = Column(String(255), nullable=False)
    channel_title = Column(String(512), nullable=True)
    post_limit = Column(Integer, nullable=False)

    # Job execution state
    # pending    → created, waiting for Celery to pick up
    # processing → Celery worker has started
    # completed  → parse succeeded, results in DB
    # failed     → parse failed, error_message set
    status = Column(String(50), default="pending", nullable=False)

    # Credit lifecycle (always transitions in one direction: reserved → consumed|refunded)
    # reserved  → credit decremented when job was created
    # consumed  → credit finalized after successful parse
    # refunded  → credit returned after failure (idempotent via conditional UPDATE)
    credit_status = Column(String(50), default="reserved", nullable=False)

    celery_task_id = Column(String(255), nullable=True)
    error_message = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    # Set to now() when Celery worker picks up the task — used for stale detection
    processing_started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Delivery idempotency guard — prevents duplicate notifications on Celery retry.
    # deliver_task atomically flips this from False → True (WHERE result_delivered=False).
    # If it returns False, the task skips sending and logs a warning.
    result_delivered = Column(Boolean, default=False, nullable=False)
    delivered_at = Column(DateTime(timezone=True), nullable=True)

    bot_user = relationship("BotUser", back_populates="jobs")
    posts = relationship("PostResult", back_populates="job", cascade="all, delete-orphan")


class PostResult(Base):
    """Parsed data for a single Telegram post."""

    __tablename__ = "post_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("parse_jobs.id"), nullable=False)

    post_id = Column(BigInteger, nullable=False)
    post_link = Column(String(512), nullable=False)
    post_text = Column(Text, nullable=True)

    # "photo" | "video" | "none"
    media_type = Column(String(20), default="none", nullable=False)

    # JSON array of URLs extracted from post text, stored as TEXT.
    # Example: '["https://example.com", "https://other.com"]'
    extracted_links = Column(Text, nullable=True)

    views = Column(Integer, default=0)
    reactions_count = Column(Integer, default=0)
    comments_count = Column(Integer, default=0)

    job = relationship("ParseJob", back_populates="posts")
    commenters = relationship(
        "CommenterResult",
        back_populates="post",
        cascade="all, delete-orphan",
    )


class CommenterResult(Base):
    """Username of one commenter on a post."""

    __tablename__ = "commenter_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    post_result_id = Column(Integer, ForeignKey("post_results.id"), nullable=False)

    # "@handle" or "id:{user_id}" when sender has no username
    username = Column(String(255), nullable=False)

    post = relationship("PostResult", back_populates="commenters")

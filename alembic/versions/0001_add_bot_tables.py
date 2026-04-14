"""add_bot_tables

Revision ID: 0001
Revises:
Create Date: 2026-04-14

Creates all telegram_bot tables without touching any existing backend tables.
Partial unique index enforces: one active job (pending|processing) per user.
"""

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── bot_users ─────────────────────────────────────────────────────────────
    op.create_table(
        "bot_users",
        sa.Column("telegram_id", sa.BigInteger(), primary_key=True),
        sa.Column("username", sa.String(255), nullable=True),
        sa.Column("first_name", sa.String(255), nullable=True),
        # Unified credit field (free + paid, no distinction at storage level)
        sa.Column("available_credits", sa.Integer(), nullable=False, server_default="5"),
        sa.Column("total_parses_done", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.Column("last_active_at", sa.DateTime(timezone=True), nullable=True),
    )

    # ── parse_jobs ────────────────────────────────────────────────────────────
    op.create_table(
        "parse_jobs",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "bot_user_id",
            sa.BigInteger(),
            sa.ForeignKey("bot_users.telegram_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("channel_username", sa.String(255), nullable=False),
        sa.Column("channel_title", sa.String(512), nullable=True),
        sa.Column("post_limit", sa.Integer(), nullable=False),
        # pending | processing | completed | failed
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        # reserved | consumed | refunded
        sa.Column("credit_status", sa.String(50), nullable=False, server_default="reserved"),
        sa.Column("celery_task_id", sa.String(255), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        # Set when Celery worker picks up the task; used for stale-processing detection
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Partial unique index: only one pending/processing job allowed per user.
    # Prevents double-tap race conditions and duplicate job creation.
    op.create_index(
        "one_active_job_per_user",
        "parse_jobs",
        ["bot_user_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'processing')"),
    )

    # ── post_results ──────────────────────────────────────────────────────────
    op.create_table(
        "post_results",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "job_id",
            sa.Integer(),
            sa.ForeignKey("parse_jobs.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("post_id", sa.BigInteger(), nullable=False),
        sa.Column("post_link", sa.String(512), nullable=False),
        sa.Column("post_text", sa.Text(), nullable=True),
        # "photo" | "video" | "none"
        sa.Column("media_type", sa.String(20), nullable=False, server_default="none"),
        # JSON array of URLs extracted from post text
        sa.Column("extracted_links", sa.Text(), nullable=True),
        sa.Column("views", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reactions_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("comments_count", sa.Integer(), nullable=False, server_default="0"),
    )

    # ── commenter_results ─────────────────────────────────────────────────────
    op.create_table(
        "commenter_results",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column(
            "post_result_id",
            sa.Integer(),
            sa.ForeignKey("post_results.id", ondelete="CASCADE"),
            nullable=False,
        ),
        # "@handle" or "id:{user_id}" when sender has no public username
        sa.Column("username", sa.String(255), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("commenter_results")
    op.drop_table("post_results")
    op.drop_index("one_active_job_per_user", table_name="parse_jobs")
    op.drop_table("parse_jobs")
    op.drop_table("bot_users")

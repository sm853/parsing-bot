"""Add result_delivered and delivered_at to parse_jobs.

Delivery idempotency guard: deliver_task atomically claims the delivery slot
by flipping result_delivered False → True (WHERE result_delivered = FALSE).
If the claim fails (returns 0 rows), the task was already delivered and the
current invocation is a no-op.  This prevents duplicate Telegram messages
when Celery retries a deliver_task that actually succeeded on the first attempt.

Revision ID: 0002
Revises:     0001
Create Date: 2026-04-14
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # result_delivered — delivery idempotency flag (False by default)
    op.add_column(
        "parse_jobs",
        sa.Column(
            "result_delivered",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("FALSE"),
        ),
    )
    # delivered_at — timestamp when delivery was claimed (NULL until delivered)
    op.add_column(
        "parse_jobs",
        sa.Column(
            "delivered_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    # Index to speed up the idempotency guard UPDATE (WHERE id=? AND result_delivered=FALSE)
    op.create_index(
        "ix_parse_jobs_undelivered",
        "parse_jobs",
        ["id"],
        unique=False,
        postgresql_where=sa.text("result_delivered = FALSE"),
    )


def downgrade() -> None:
    op.drop_index("ix_parse_jobs_undelivered", table_name="parse_jobs")
    op.drop_column("parse_jobs", "delivered_at")
    op.drop_column("parse_jobs", "result_delivered")

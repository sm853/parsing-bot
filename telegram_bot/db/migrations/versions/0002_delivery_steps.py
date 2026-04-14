"""add_delivery_step_tracking

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-15

Adds per-step delivery progress columns to parse_jobs so that
deliver_task retries can resume from the failed step without
re-sending messages that were already delivered.

Also adds result_delivered / delivered_at which belong in 0001 but
were missing from the initial migration.  Uses ADD COLUMN IF NOT EXISTS
so the migration is idempotent — safe on databases where those columns
already exist.
"""

from alembic import op
import sqlalchemy as sa


revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # result_delivered / delivered_at were in the ORM model but missing from 0001.
    # ADD COLUMN IF NOT EXISTS handles both fresh installs and existing DBs.
    op.execute("""
        ALTER TABLE parse_jobs
            ADD COLUMN IF NOT EXISTS result_delivered       BOOLEAN     NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS delivered_at           TIMESTAMPTZ          DEFAULT NULL,
            ADD COLUMN IF NOT EXISTS delivery_summary_sent  BOOLEAN     NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS delivery_document_sent BOOLEAN     NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS delivery_keyboard_sent BOOLEAN     NOT NULL DEFAULT FALSE,
            ADD COLUMN IF NOT EXISTS delivery_failure_sent  BOOLEAN     NOT NULL DEFAULT FALSE
    """)


def downgrade() -> None:
    op.execute("""
        ALTER TABLE parse_jobs
            DROP COLUMN IF EXISTS delivery_failure_sent,
            DROP COLUMN IF EXISTS delivery_keyboard_sent,
            DROP COLUMN IF EXISTS delivery_document_sent,
            DROP COLUMN IF EXISTS delivery_summary_sent,
            DROP COLUMN IF EXISTS delivered_at,
            DROP COLUMN IF EXISTS result_delivered
    """)

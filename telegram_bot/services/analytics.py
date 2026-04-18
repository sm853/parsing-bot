"""
analytics.py — Bot-side session/attempt tracking for the admin dashboard.

Call these functions from Celery tasks (parse_task.py, deliver_task.py) to log
parsing activity into the parsing_sessions and parsing_attempts tables.

All functions accept a SQLAlchemy Session and use plain SQL via `session.execute(text(...))`
to keep analytics fully decoupled from the main ORM models.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy import text


def create_parsing_session(
    session: Session,
    telegram_user_id: int,
    username: str,
    channel: str,
    post_limit: int,
    options: dict[str, Any],
    parse_job_id: Optional[int] = None,
) -> int:
    """Insert (or idempotently return) a parsing_sessions row.

    Uses INSERT … ON CONFLICT (parse_job_id) so that acks_late re-deliveries
    of the same Celery task never create a duplicate session row — the second
    call returns the id of the row created by the first call.

    Args:
        session:          SQLAlchemy database session.
        telegram_user_id: Telegram user ID (integer).
        username:         Telegram username (without @), may be empty string.
        channel:          Channel handle being parsed, e.g. '@crypto_news'.
        post_limit:       Maximum number of posts requested.
        options:          Arbitrary dict of additional options (stored as JSONB).
        parse_job_id:     parse_jobs.id — used as the idempotency key.
                          When provided, duplicate calls return the existing id.

    Returns:
        The parsing_sessions id (new or existing).
    """
    sql = text("""
        INSERT INTO parsing_sessions (
            telegram_user_id,
            username,
            started_at,
            status,
            selected_channel,
            selected_options,
            attempts_count,
            created_at,
            updated_at,
            parse_job_id
        ) VALUES (
            :telegram_user_id,
            :username,
            NOW(),
            'running',
            :channel,
            :options::jsonb,
            0,
            NOW(),
            NOW(),
            :parse_job_id
        )
        ON CONFLICT (parse_job_id)
            WHERE parse_job_id IS NOT NULL
            DO UPDATE SET updated_at = NOW()
        RETURNING id
    """)
    result = session.execute(
        sql,
        {
            "telegram_user_id": telegram_user_id,
            "username": username or None,
            "channel": channel,
            "options": json.dumps({**(options or {}), "post_limit": post_limit}),
            "parse_job_id": parse_job_id,
        },
    )
    session.commit()
    row = result.fetchone()
    return int(row[0])


def start_parsing_attempt(
    session: Session,
    session_id: int,
    attempt_number: int,
    celery_task_id: str,
) -> int:
    """Insert a parsing_attempts row and atomically increment attempts_count.

    Both writes (attempt INSERT + session UPDATE) are committed together in a
    single transaction, so attempts_count can never drift from the actual
    number of attempt rows regardless of mid-flight worker crashes.

    Args:
        session:        SQLAlchemy database session.
        session_id:     Parent parsing_sessions.id.
        attempt_number: 1-based attempt counter (use self.request.retries + 1).
        celery_task_id: Celery task ID string for traceability.

    Returns:
        The newly created attempt id (bigint).
    """
    insert_sql = text("""
        INSERT INTO parsing_attempts (
            session_id,
            attempt_number,
            started_at,
            status,
            meta,
            created_at
        ) VALUES (
            :session_id,
            :attempt_number,
            NOW(),
            'running',
            :meta::jsonb,
            NOW()
        )
        RETURNING id
    """)
    result = session.execute(
        insert_sql,
        {
            "session_id": session_id,
            "attempt_number": attempt_number,
            "meta": json.dumps({"celery_task_id": celery_task_id}),
        },
    )
    attempt_id = int(result.fetchone()[0])

    # Increment the denormalized counter in the same transaction so it always
    # equals COUNT(parsing_attempts WHERE session_id = :id).
    session.execute(
        text("""
            UPDATE parsing_sessions
            SET attempts_count = attempts_count + 1,
                updated_at     = NOW()
            WHERE id = :session_id
        """),
        {"session_id": session_id},
    )
    session.commit()
    return attempt_id


def complete_parsing_attempt(
    session: Session,
    attempt_id: int,
    status: str,
    duration_ms: int,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update attempt row with final status and duration.

    Args:
        session:       SQLAlchemy database session.
        attempt_id:    parsing_attempts.id to update.
        status:        Final status: 'success' or 'failed'.
        duration_ms:   Elapsed wall-clock time in milliseconds.
        error_code:    Short machine-readable error code (optional).
        error_message: Human-readable error description (optional).
    """
    sql = text("""
        UPDATE parsing_attempts
        SET
            finished_at   = NOW(),
            status        = :status::attempt_status,
            duration_ms   = :duration_ms,
            error_code    = :error_code,
            error_message = :error_message
        WHERE id = :attempt_id
    """)
    session.execute(
        sql,
        {
            "attempt_id": attempt_id,
            "status": status,
            "duration_ms": duration_ms,
            "error_code": error_code,
            "error_message": error_message,
        },
    )
    session.commit()


def complete_parsing_session(
    session: Session,
    session_id: int,
    status: str,
    duration_ms: int,
    result_rows: Optional[int] = None,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    """Update session row with final status, duration, result_rows.

    Args:
        session:       SQLAlchemy database session.
        session_id:    parsing_sessions.id to update.
        status:        Final status string matching session_status enum.
        duration_ms:   Total parse duration in milliseconds.
        result_rows:   Number of rows/posts successfully parsed (optional).
        error_code:    Short machine-readable error code (optional).
        error_message: Human-readable error description (optional).
    """
    sql = text("""
        UPDATE parsing_sessions
        SET
            finished_at         = NOW(),
            status              = :status::session_status,
            parsing_duration_ms = :duration_ms,
            result_rows         = :result_rows,
            error_code          = :error_code,
            error_message       = :error_message,
            updated_at          = NOW()
        WHERE id = :session_id
    """)
    session.execute(
        sql,
        {
            "session_id": session_id,
            "status": status,
            "duration_ms": duration_ms,
            "result_rows": result_rows,
            "error_code": error_code,
            "error_message": error_message,
        },
    )
    session.commit()


def increment_session_attempts(session: Session, session_id: int) -> None:
    """Atomically increment attempts_count on a session.

    Args:
        session:    SQLAlchemy database session.
        session_id: parsing_sessions.id to update.
    """
    sql = text("""
        UPDATE parsing_sessions
        SET
            attempts_count = attempts_count + 1,
            updated_at     = NOW()
        WHERE id = :session_id
    """)
    session.execute(sql, {"session_id": session_id})
    session.commit()

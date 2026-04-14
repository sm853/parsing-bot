"""
Anti-duplication tests.

Covers:
  1. mark_result_delivered_if_not_yet — first call returns True, subsequent False
  2. deliver_task idempotency — second invocation with same job_id is a no-op
  3. parse_orchestrator.start_parsing returns structured StartParsingResult
  4. Double-tap handler path: second confirmation uses the existing-job branch
  5. No-credits path returns no_credits=True with job=None

All DB operations use an in-memory SQLite session; Celery / Telegram API calls
are mocked so no network or broker is needed.
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from telegram_bot.db.models import Base, BotUser, ParseJob
from telegram_bot.db.repository_sync import mark_result_delivered_if_not_yet


# ── In-memory SQLite fixture ───────────────────────────────────────────────────

@pytest.fixture()
def db_session():
    """Provide a fresh in-memory SQLite session with all tables created."""
    engine = create_engine("sqlite:///:memory:", future=True)

    # SQLite doesn't support RETURNING by default in older SQLAlchemy builds;
    # enable it for SQLAlchemy 2.x / SQLite 3.35+
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    Base.metadata.create_all(engine)

    with Session(engine) as session:
        # Seed: one user + one completed job (result_delivered=False)
        user = BotUser(
            telegram_id=111,
            username="testuser",
            first_name="Test",
            available_credits=3,
        )
        session.add(user)
        session.flush()

        job = ParseJob(
            bot_user_id=111,
            channel_username="testchannel",
            post_limit=20,
            status="completed",
            credit_status="consumed",
            result_delivered=False,
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        yield session, job.id


# ── mark_result_delivered_if_not_yet ──────────────────────────────────────────

class TestMarkResultDelivered:
    def test_first_call_claims_slot(self, db_session):
        session, job_id = db_session
        result = mark_result_delivered_if_not_yet(session, job_id)
        assert result is True

    def test_second_call_is_noop(self, db_session):
        session, job_id = db_session
        mark_result_delivered_if_not_yet(session, job_id)   # first: True
        result = mark_result_delivered_if_not_yet(session, job_id)  # second: False
        assert result is False

    def test_delivered_at_set_after_claim(self, db_session):
        session, job_id = db_session
        before = datetime.now(timezone.utc)
        mark_result_delivered_if_not_yet(session, job_id)
        session.expire_all()
        job = session.get(ParseJob, job_id)
        assert job.result_delivered is True
        assert job.delivered_at is not None
        # delivered_at should be at or after our before timestamp
        # (SQLite stores as naive UTC; compare after stripping tzinfo)
        delivered_naive = job.delivered_at.replace(tzinfo=None) if job.delivered_at.tzinfo else job.delivered_at
        before_naive = before.replace(tzinfo=None)
        assert delivered_naive >= before_naive

    def test_concurrent_calls_only_one_succeeds(self, db_session):
        """Simulate two callers racing: only one True expected total."""
        session, job_id = db_session
        # With SQLite we can't truly race, but we can call N times and assert exactly 1 True.
        results = [mark_result_delivered_if_not_yet(session, job_id) for _ in range(5)]
        assert results.count(True) == 1
        assert results.count(False) == 4

    def test_unknown_job_id_returns_false(self, db_session):
        session, _ = db_session
        result = mark_result_delivered_if_not_yet(session, 99999)
        assert result is False


# ── deliver_task idempotency ───────────────────────────────────────────────────

class TestDeliverTaskIdempotency:
    """
    deliver_task.deliver_result should call notify_success/failure exactly once
    even when invoked multiple times for the same job.

    We test the idempotency logic directly by calling the inner function with
    the DB-backed repository; Celery + Telegram API calls are fully mocked.
    """

    @staticmethod
    def _make_session_ctx(session):
        """Return a context-manager factory that yields the given session."""
        from contextlib import contextmanager

        @contextmanager
        def _ctx():
            yield session

        return _ctx

    @staticmethod
    def _make_task_self(retries=0, max_retries=3):
        t = MagicMock()
        t.request.retries = retries
        t.max_retries = max_retries
        t.retry = MagicMock(side_effect=Exception("retry"))
        return t

    def test_first_invocation_sends(self, db_session):
        import telegram_bot.tasks.deliver_task as dt  # ensure module is loaded

        session, job_id = db_session
        notify = MagicMock()

        with (
            patch.object(dt, "get_sync_session", self._make_session_ctx(session)),
            patch.object(dt.repo, "mark_result_delivered_if_not_yet",
                         lambda s, jid: mark_result_delivered_if_not_yet(s, jid)),
            patch.object(dt.delivery, "notify_success", notify),
            patch.object(dt.delivery, "notify_failure", notify),
        ):
            dt.deliver_result.__wrapped__(self._make_task_self(), job_id, 42, "success")

        notify.assert_called_once_with(job_id, 42)

    def test_second_invocation_skipped(self, db_session):
        import telegram_bot.tasks.deliver_task as dt

        session, job_id = db_session
        notify = MagicMock()
        call_count = {"n": 0}

        def fake_guard(s, jid):
            call_count["n"] += 1
            return mark_result_delivered_if_not_yet(s, jid)

        with (
            patch.object(dt, "get_sync_session", self._make_session_ctx(session)),
            patch.object(dt.repo, "mark_result_delivered_if_not_yet", fake_guard),
            patch.object(dt.delivery, "notify_success", notify),
            patch.object(dt.delivery, "notify_failure", notify),
        ):
            dt.deliver_result.__wrapped__(self._make_task_self(), job_id, 42, "success")
            dt.deliver_result.__wrapped__(self._make_task_self(), job_id, 42, "success")

        # notify called exactly once — second run hit the guard and returned early
        notify.assert_called_once()
        assert call_count["n"] == 2  # guard was checked twice


# ── parse_orchestrator.start_parsing structured result ────────────────────────

class TestStartParsingResult:
    """
    start_parsing() should return the correct StartParsingResult variant
    without touching Celery or Telegram.
    """

    @pytest.mark.asyncio
    async def test_no_credits_returns_no_credits_true(self):
        from telegram_bot.app.parse_orchestrator import start_parsing

        mock_session = AsyncMock()

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.get_active_job",
                AsyncMock(return_value=None),
            ),
            patch(
                "telegram_bot.app.parse_orchestrator.limits.reserve_credit",
                AsyncMock(return_value=False),   # no credits
            ),
        ):
            result = await start_parsing(
                session=mock_session,
                bot_user_id=1,
                channel_username="test",
                channel_title="Test",
                post_limit=20,
                chat_id=100,
            )

        assert result.no_credits is True
        assert result.job is None
        assert result.created_new is False

    @pytest.mark.asyncio
    async def test_existing_job_returns_created_new_false(self):
        from telegram_bot.app.parse_orchestrator import start_parsing

        existing_job = MagicMock(spec=ParseJob)
        existing_job.id = 7
        existing_job.status = "processing"

        mock_session = AsyncMock()

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.get_active_job",
                AsyncMock(return_value=existing_job),
            ),
        ):
            result = await start_parsing(
                session=mock_session,
                bot_user_id=1,
                channel_username="test",
                channel_title="Test",
                post_limit=20,
                chat_id=100,
            )

        assert result.created_new is False
        assert result.no_credits is False
        assert result.job is existing_job

    @pytest.mark.asyncio
    async def test_new_job_returns_created_new_true(self):
        from telegram_bot.app.parse_orchestrator import start_parsing

        new_job = MagicMock(spec=ParseJob)
        new_job.id = 42
        new_job.channel_username = "test"
        new_job.status = "pending"

        celery_result = MagicMock()
        celery_result.id = "abc-123"

        mock_session = AsyncMock()
        mock_session.refresh = AsyncMock()

        # run_parse_job is imported lazily inside start_parsing, so we need to
        # patch it in the parse_task module AND make the mock have a .delay attribute.
        mock_run_parse_job = MagicMock()
        mock_run_parse_job.delay = MagicMock(return_value=celery_result)

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.get_active_job",
                AsyncMock(return_value=None),
            ),
            patch(
                "telegram_bot.app.parse_orchestrator.limits.reserve_credit",
                AsyncMock(return_value=True),
            ),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.create_parse_job",
                AsyncMock(return_value=new_job),
            ),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.update_job_status",
                AsyncMock(),
            ),
            patch(
                "telegram_bot.tasks.parse_task.run_parse_job",
                mock_run_parse_job,
            ),
        ):
            result = await start_parsing(
                session=mock_session,
                bot_user_id=1,
                channel_username="test",
                channel_title="Test",
                post_limit=20,
                chat_id=100,
            )

        assert result.created_new is True
        assert result.no_credits is False
        assert result.job is new_job


# ── Handler double-tap guard ───────────────────────────────────────────────────

class TestHandlerDoubleTap:
    """
    handle_confirmation should answer() immediately (before any async work)
    and show the 'already active' message on the second tap.
    """

    @pytest.mark.asyncio
    async def test_callback_answered_immediately(self):
        """callback.answer() must be awaited before start_parsing() is called."""
        from telegram_bot.handlers.parsing_flow import handle_confirmation
        from telegram_bot.app.parse_orchestrator import StartParsingResult

        # Single shared timeline so we can compare ordering across callsites.
        call_log: list[str] = []

        callback = MagicMock()
        callback.data = "confirm:start"
        callback.from_user.id = 1
        callback.message.chat.id = 100
        callback.message.edit_text = AsyncMock()

        async def mock_answer(**kwargs):
            call_log.append("answer")

        async def mock_start_parsing(**kwargs):
            call_log.append("start_parsing")
            job = MagicMock(spec=ParseJob)
            job.id = 1
            job.channel_username = "test"
            return StartParsingResult(job=job, created_new=True, no_credits=False)

        callback.answer = mock_answer

        state = AsyncMock()
        state.get_state = AsyncMock(return_value="ParseFlow:CONFIRMING")
        state.get_data = AsyncMock(return_value={
            "channel_username": "test",
            "channel_title": "Test Channel",
            "post_count": 20,
        })
        state.clear = AsyncMock()

        with (
            patch("telegram_bot.handlers.parsing_flow.get_async_session") as mock_ctx,
            patch(
                "telegram_bot.handlers.parsing_flow.parse_orchestrator.start_parsing",
                side_effect=mock_start_parsing,
            ),
        ):
            mock_session = AsyncMock()
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            await handle_confirmation(callback, state)

        assert "answer" in call_log, "callback.answer() was never called"
        assert "start_parsing" in call_log, "start_parsing() was never called"
        # answer() must appear earlier in the shared timeline than start_parsing()
        assert call_log.index("answer") < call_log.index("start_parsing"), (
            f"answer() must be called BEFORE start_parsing(). "
            f"Call order was: {call_log}"
        )

    @pytest.mark.asyncio
    async def test_duplicate_confirmation_shows_already_active_message(self):
        from telegram_bot.handlers.parsing_flow import handle_confirmation
        from telegram_bot.app.parse_orchestrator import StartParsingResult

        existing_job = MagicMock(spec=ParseJob)
        existing_job.id = 5
        existing_job.status = "processing"
        existing_job.channel_username = "testchannel"

        callback = MagicMock()
        callback.data = "confirm:start"
        callback.from_user.id = 1
        callback.message.chat.id = 100
        callback.answer = AsyncMock()
        callback.message.edit_text = AsyncMock()

        state = AsyncMock()
        state.get_state = AsyncMock(return_value="ParseFlow:CONFIRMING")
        state.get_data = AsyncMock(return_value={
            "channel_username": "testchannel",
            "channel_title": "Test",
            "post_count": 20,
        })
        state.clear = AsyncMock()

        with (
            patch("telegram_bot.handlers.parsing_flow.get_async_session") as mock_ctx,
            patch(
                "telegram_bot.handlers.parsing_flow.parse_orchestrator.start_parsing",
                AsyncMock(return_value=StartParsingResult(
                    job=existing_job, created_new=False, no_credits=False
                )),
            ),
        ):
            mock_session = AsyncMock()
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            await handle_confirmation(callback, state)

        # Should have edited text to show "already active" message
        edit_calls = callback.message.edit_text.call_args_list
        assert len(edit_calls) >= 1
        last_text = edit_calls[-1][0][0] if edit_calls[-1][0] else edit_calls[-1][1].get("text", "")
        assert "already" in last_text.lower() or "parsing" in last_text.lower()

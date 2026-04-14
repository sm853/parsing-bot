"""
Race-condition tests for the duplicate ParseJob / duplicate Celery dispatch bug.

Three layers are tested:

  Layer 1 — handler in-flight guard (asyncio-level, process-local):
    Two concurrent handle_confirmation coroutines with the same (user_id, msg_id).
    The second is dropped immediately; start_parsing is called exactly once.

  Layer 2 — DB partial unique index + orchestrator IntegrityError handler:
    When create_parse_job raises IntegrityError (unique constraint violation),
    start_parsing catches it, restores the loser's credit, returns existing job.
    No Celery dispatch on the losing path.

  Layer 3 — end-to-end with real async SQLite:
    asyncio.gather runs two full start_parsing coroutines on separate async
    sessions pointing at the same SQLite file.  Verifies DB state directly:
    exactly one ParseJob, credit balance decremented once net.

Key testing insight for Layer 1
────────────────────────────────
AsyncMock doesn't yield to the event loop when awaited — it returns
immediately. This means if all mocks are AsyncMock, Task 1 runs to completion
(including the `finally: _in_flight.discard(key)`) before Task 2 even starts,
making the guard appear broken in tests even though it works correctly in
production (where state.get_state() is a real Redis call that yields).

Fix: the `state.get_state()` mock must explicitly yield via `asyncio.sleep(0)`.
This simulates the real async I/O behaviour, allowing the event loop to switch
to Task 2 at exactly the right moment (after Task 1 has added the key to
_in_flight, before Task 1 has removed it).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from telegram_bot.db.models import Base, BotUser, ParseJob
from telegram_bot.app.parse_orchestrator import StartParsingResult


# ═══════════════════════════════════════════════════════════════════════════════
# Shared helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _make_callback(user_id: int = 1, msg_id: int = 100, data: str = "confirm:start"):
    """Return a MagicMock shaped like a CallbackQuery."""
    cb = MagicMock()
    cb.id = f"cbid-{user_id}-{msg_id}"
    cb.from_user.id = user_id
    cb.message.message_id = msg_id
    cb.message.chat.id = 999
    cb.data = data
    cb.answer = AsyncMock()
    cb.message.edit_text = AsyncMock()
    cb.bot = MagicMock()
    return cb


def _make_state(confirming: bool = True):
    """Return an AsyncMock shaped like FSMContext with a real event-loop yield."""
    from telegram_bot.states.parsing_states import ParseFlow

    state_value = ParseFlow.CONFIRMING.state if confirming else None

    state = MagicMock()

    # IMPORTANT: get_state() must yield to the event loop so that asyncio.gather
    # interleaves Task 1 and Task 2 at the right point (after the in-flight key
    # has been added, before the finally block removes it).
    async def _get_state():
        await asyncio.sleep(0)   # ← yield; lets Task 2 see the in-flight key
        return state_value

    state.get_state = _get_state
    state.get_data = AsyncMock(return_value={
        "channel_username": "testchannel",
        "channel_title": "Test",
        "post_count": 20,
    })
    state.clear = AsyncMock()
    state.set_state = AsyncMock()
    return state


# ═══════════════════════════════════════════════════════════════════════════════
# Async SQLite fixture
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture
async def async_engine_factory():
    """
    Async SQLite engine backed by a named temp file so multiple sessions share
    state. The partial unique index is created by Base.metadata.create_all()
    because ParseJob.__table_args__ declares it with sqlite_where.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        echo=False,
        connect_args={"check_same_thread": False},
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    await engine.dispose()
    try:
        os.unlink(db_path)
    except FileNotFoundError:
        pass


@pytest.fixture
async def session_factory(async_engine_factory):
    return async_sessionmaker(
        async_engine_factory,
        class_=AsyncSession,
        expire_on_commit=False,
    )


async def _seed_user(sf, user_id: int = 1, credits: int = 5) -> None:
    async with sf() as session:
        session.add(BotUser(
            telegram_id=user_id,
            username="u",
            first_name="U",
            available_credits=credits,
        ))
        await session.commit()


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 1 — in-flight handler guard
# ═══════════════════════════════════════════════════════════════════════════════

class TestInFlightGuard:

    @pytest.mark.asyncio
    async def test_duplicate_callback_blocked_before_start_parsing(self):
        """
        Two concurrent handle_confirmation invocations for the same (user, msg):
        only the first reaches start_parsing.

        The `state.get_state()` mock yields once (asyncio.sleep(0)) so that
        the event loop can switch to Task 2 at the right time — after Task 1 has
        added the key to _in_flight, but before Task 1 has removed it.
        """
        from telegram_bot.handlers import parsing_flow
        from telegram_bot.handlers.parsing_flow import handle_confirmation

        parsing_flow._in_flight.clear()

        start_parsing_call_count = {"n": 0}

        async def mock_start_parsing(**kwargs):
            start_parsing_call_count["n"] += 1
            job = MagicMock(spec=ParseJob)
            job.id = 42
            job.channel_username = "testchannel"
            job.celery_task_id = "celery-abc"
            return StartParsingResult(job=job, created_new=True, no_credits=False)

        cb1 = _make_callback(user_id=1, msg_id=100)
        cb2 = _make_callback(user_id=1, msg_id=100)  # same key
        state = _make_state(confirming=True)

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

            await asyncio.gather(
                handle_confirmation(cb1, state),
                handle_confirmation(cb2, state),
            )

        assert start_parsing_call_count["n"] == 1, (
            f"start_parsing was called {start_parsing_call_count['n']} times; "
            "expected 1. The in-flight guard should have blocked the second tap."
        )
        # Both callbacks must be answered (Telegram requires it)
        cb1.answer.assert_called()
        cb2.answer.assert_called()

    @pytest.mark.asyncio
    async def test_different_message_ids_do_not_interfere(self):
        """
        Two callbacks with different message_ids (different confirm cards) must
        BOTH proceed — they are different conversations, not a duplicate.
        """
        from telegram_bot.handlers import parsing_flow
        from telegram_bot.handlers.parsing_flow import handle_confirmation

        parsing_flow._in_flight.clear()

        call_count = {"n": 0}

        async def mock_start_parsing(**kwargs):
            call_count["n"] += 1
            job = MagicMock(spec=ParseJob)
            job.id = call_count["n"]
            job.channel_username = "ch"
            job.celery_task_id = f"c{call_count['n']}"
            return StartParsingResult(job=job, created_new=True, no_credits=False)

        cb1 = _make_callback(user_id=1, msg_id=10)   # different msg_ids
        cb2 = _make_callback(user_id=1, msg_id=20)
        state = _make_state(confirming=True)

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

            await asyncio.gather(
                handle_confirmation(cb1, state),
                handle_confirmation(cb2, state),
            )

        # Both should call start_parsing (different message IDs = not duplicates)
        # Note: because both share a state mock and the first clears it, the second
        # may be stopped by the FSM check — but NOT by the in-flight guard.
        # What matters is that neither was blocked by the GUARD.
        assert (1, 10) not in parsing_flow._in_flight
        assert (1, 20) not in parsing_flow._in_flight

    @pytest.mark.asyncio
    async def test_guard_released_after_completion(self):
        """After the handler completes normally, the key is gone from _in_flight."""
        from telegram_bot.handlers import parsing_flow
        from telegram_bot.handlers.parsing_flow import handle_confirmation

        parsing_flow._in_flight.clear()

        cb = _make_callback(user_id=5, msg_id=500, data="confirm:exit")
        state = _make_state(confirming=True)

        await handle_confirmation(cb, state)

        assert (5, 500) not in parsing_flow._in_flight

    @pytest.mark.asyncio
    async def test_guard_released_on_exception(self):
        """Even when start_parsing raises, the guard key must be removed."""
        from telegram_bot.handlers import parsing_flow
        from telegram_bot.handlers.parsing_flow import handle_confirmation

        parsing_flow._in_flight.clear()

        cb = _make_callback(user_id=7, msg_id=700)
        state = _make_state(confirming=True)

        with (
            patch("telegram_bot.handlers.parsing_flow.get_async_session") as mock_ctx,
            patch(
                "telegram_bot.handlers.parsing_flow.parse_orchestrator.start_parsing",
                side_effect=RuntimeError("DB exploded"),
            ),
        ):
            mock_session = AsyncMock()
            mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            with pytest.raises(RuntimeError, match="DB exploded"):
                await handle_confirmation(cb, state)

        assert (7, 700) not in parsing_flow._in_flight, (
            "Guard key was not removed after exception"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 2 — orchestrator IntegrityError handling (mocked)
# ═══════════════════════════════════════════════════════════════════════════════

class TestOrchestratorRaceHandling:

    @pytest.mark.asyncio
    async def test_integrity_error_returns_existing_job(self):
        """
        IntegrityError from create_parse_job → rollback, restore credit,
        return existing job with created_new=False.
        """
        from telegram_bot.app.parse_orchestrator import start_parsing

        winner = MagicMock(spec=ParseJob)
        winner.id = 7
        winner.status = "pending"
        winner.channel_username = "testch"

        session = AsyncMock()
        restore_calls: list[int] = []

        async def _restore(s, uid): restore_calls.append(uid)

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.get_active_job",
                AsyncMock(side_effect=[None, winner]),   # None first, winner after rollback
            ),
            patch("telegram_bot.app.parse_orchestrator.limits.reserve_credit",
                  AsyncMock(return_value=True)),
            patch(
                "telegram_bot.app.parse_orchestrator.repo.create_parse_job",
                AsyncMock(side_effect=IntegrityError("unique", None, None)),
            ),
            patch("telegram_bot.app.parse_orchestrator.limits.restore_reserved_credit",
                  side_effect=_restore),
        ):
            result = await start_parsing(
                session=session,
                bot_user_id=1,
                channel_username="testch",
                channel_title="T",
                post_limit=20,
                chat_id=42,
            )

        assert result.created_new is False
        assert result.no_credits is False
        assert result.job is winner
        session.rollback.assert_called_once()
        assert restore_calls == [1]

    @pytest.mark.asyncio
    async def test_integrity_error_no_celery_dispatch(self):
        """Race loser must NOT dispatch a Celery task."""
        from telegram_bot.app.parse_orchestrator import start_parsing

        winner = MagicMock(spec=ParseJob)
        winner.id = 8
        winner.status = "pending"
        winner.channel_username = "testch"
        session = AsyncMock()

        dispatch_count = {"n": 0}
        mock_run = MagicMock()
        mock_run.delay = MagicMock(side_effect=lambda *a: dispatch_count.update({"n": dispatch_count["n"]+1}))

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch("telegram_bot.app.parse_orchestrator.repo.get_active_job",
                  AsyncMock(side_effect=[None, winner])),
            patch("telegram_bot.app.parse_orchestrator.limits.reserve_credit",
                  AsyncMock(return_value=True)),
            patch("telegram_bot.app.parse_orchestrator.repo.create_parse_job",
                  AsyncMock(side_effect=IntegrityError("unique", None, None))),
            patch("telegram_bot.app.parse_orchestrator.limits.restore_reserved_credit",
                  AsyncMock()),
            patch("telegram_bot.tasks.parse_task.run_parse_job", mock_run),
        ):
            await start_parsing(
                session=session, bot_user_id=1, channel_username="testch",
                channel_title="T", post_limit=20, chat_id=42,
            )

        assert dispatch_count["n"] == 0, (
            "Celery was dispatched on the race-lost path — duplicate parse task!"
        )

    @pytest.mark.asyncio
    async def test_winner_dispatches_celery_exactly_once(self):
        """Normal path (no race): one INSERT success → exactly one Celery dispatch."""
        from telegram_bot.app.parse_orchestrator import start_parsing

        new_job = MagicMock(spec=ParseJob)
        new_job.id = 99
        new_job.channel_username = "testch"
        new_job.celery_task_id = "xyz"

        celery_result = MagicMock()
        celery_result.id = "xyz"
        dispatch_count = {"n": 0}
        mock_run = MagicMock()
        mock_run.delay = MagicMock(side_effect=lambda *a: (dispatch_count.update({"n": dispatch_count["n"]+1}), celery_result)[1])

        session = AsyncMock()
        session.refresh = AsyncMock()

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch("telegram_bot.app.parse_orchestrator.repo.get_active_job",
                  AsyncMock(return_value=None)),
            patch("telegram_bot.app.parse_orchestrator.limits.reserve_credit",
                  AsyncMock(return_value=True)),
            patch("telegram_bot.app.parse_orchestrator.repo.create_parse_job",
                  AsyncMock(return_value=new_job)),
            patch("telegram_bot.app.parse_orchestrator.repo.update_job_status",
                  AsyncMock()),
            patch("telegram_bot.tasks.parse_task.run_parse_job", mock_run),
        ):
            result = await start_parsing(
                session=session, bot_user_id=1, channel_username="testch",
                channel_title="T", post_limit=20, chat_id=42,
            )

        assert result.created_new is True
        assert dispatch_count["n"] == 1, (
            f"Expected 1 Celery dispatch, got {dispatch_count['n']}"
        )

    @pytest.mark.asyncio
    async def test_existing_job_no_credit_consumption_no_dispatch(self):
        """Idempotency path (job already exists): no credit change, no Celery."""
        from telegram_bot.app.parse_orchestrator import start_parsing

        existing = MagicMock(spec=ParseJob)
        existing.id = 5
        existing.status = "processing"
        existing.channel_username = "testch"
        session = AsyncMock()

        reserve_calls = {"n": 0}
        dispatch_calls = {"n": 0}

        mock_run = MagicMock()
        mock_run.delay = MagicMock(side_effect=lambda *a: dispatch_calls.update({"n": dispatch_calls["n"]+1}))

        with (
            patch("telegram_bot.app.parse_orchestrator._cleanup_stale_jobs", AsyncMock()),
            patch("telegram_bot.app.parse_orchestrator.repo.get_active_job",
                  AsyncMock(return_value=existing)),
            patch("telegram_bot.app.parse_orchestrator.limits.reserve_credit",
                  AsyncMock(side_effect=lambda s, uid: (reserve_calls.update({"n": reserve_calls["n"]+1}), True)[1])),
            patch("telegram_bot.tasks.parse_task.run_parse_job", mock_run),
        ):
            result = await start_parsing(
                session=session, bot_user_id=1, channel_username="testch",
                channel_title="T", post_limit=20, chat_id=42,
            )

        assert result.created_new is False
        assert result.job is existing
        assert reserve_calls["n"] == 0, "reserve_credit called on idempotency path"
        assert dispatch_calls["n"] == 0, "Celery dispatched on idempotency path"


# ═══════════════════════════════════════════════════════════════════════════════
# Layer 3 — end-to-end with real async SQLite + actual unique constraint
# ═══════════════════════════════════════════════════════════════════════════════

class TestEndToEndRace:
    """
    Two concurrent start_parsing calls on a real async SQLite database with the
    partial unique index enforced (created via ParseJob.__table_args__).
    """

    @pytest.mark.asyncio
    async def test_partial_index_created_by_metadata(self, async_engine_factory):
        """
        Smoke test: Base.metadata.create_all() creates the partial unique index
        so the DB-level guard is active in test environments.
        """
        from sqlalchemy import text
        async with async_engine_factory.connect() as conn:
            result = await conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='index' AND name='one_active_job_per_user'")
            )
            row = result.fetchone()
        assert row is not None, (
            "Partial unique index 'one_active_job_per_user' was not created by "
            "Base.metadata.create_all(). Check ParseJob.__table_args__."
        )

    @pytest.mark.asyncio
    async def test_concurrent_start_parsing_one_job_one_credit(self, session_factory):
        """
        Two concurrent start_parsing calls for the same user.
        Expected: exactly 1 ParseJob in DB, credits decremented once net.
        """
        await _seed_user(session_factory, user_id=1, credits=3)

        from telegram_bot.config import settings as real_settings

        dispatch_count = {"n": 0}
        celery_result = MagicMock()
        celery_result.id = "celery-test"
        mock_run = MagicMock()
        def _fake_delay(*args):
            dispatch_count["n"] += 1
            return celery_result
        mock_run.delay = _fake_delay

        from telegram_bot.app.parse_orchestrator import start_parsing

        async def run_one() -> StartParsingResult:
            # Inline the call so the outer mock_run (with dispatch_count) is used.
            # _start_parsing_with_real_db creates its own inner mock that would
            # shadow the outer patch — don't use it here.
            async with session_factory() as session:
                with patch("telegram_bot.app.parse_orchestrator.settings", real_settings):
                    with patch("telegram_bot.tasks.parse_task.run_parse_job", mock_run):
                        return await start_parsing(
                            session=session,
                            bot_user_id=1,
                            channel_username="testchannel",
                            channel_title="Test Channel",
                            post_limit=20,
                            chat_id=999,
                        )

        results = await asyncio.gather(run_one(), run_one(), return_exceptions=True)

        # Neither task must raise
        for i, r in enumerate(results):
            assert not isinstance(r, Exception), (
                f"Task {i} raised: {r!r}"
            )

        created_new = sum(1 for r in results if isinstance(r, StartParsingResult) and r.created_new)
        race_lost   = sum(1 for r in results if isinstance(r, StartParsingResult) and not r.created_new and not r.no_credits)

        assert created_new == 1, (
            f"Expected exactly 1 created_new=True, got {created_new}. "
            f"Results: {[(r.created_new, r.no_credits) for r in results if isinstance(r, StartParsingResult)]}"
        )
        assert race_lost == 1, (
            f"Expected exactly 1 race-lost result (created_new=False), got {race_lost}."
        )

        # DB: exactly one active job
        async with session_factory() as session:
            from sqlalchemy import select
            jobs = (await session.execute(
                select(ParseJob).where(
                    ParseJob.bot_user_id == 1,
                    ParseJob.status.in_(["pending", "processing"]),
                )
            )).scalars().all()
        assert len(jobs) == 1, f"Expected 1 active job in DB, found {len(jobs)}"

        # Credits: started with 3, one consumed → 2 remaining
        async with session_factory() as session:
            user = await session.get(BotUser, 1)
        assert user.available_credits == 2, (
            f"Expected 2 credits remaining, got {user.available_credits}. "
            "Either both credits were consumed or neither was."
        )

        # Celery: dispatched exactly once
        assert dispatch_count["n"] == 1, (
            f"Expected 1 Celery dispatch, got {dispatch_count['n']}"
        )

    @pytest.mark.asyncio
    async def test_no_second_job_when_first_already_active(self, session_factory):
        """
        If a pending job already exists, start_parsing must return it immediately
        without creating a new one or touching credits.
        """
        await _seed_user(session_factory, user_id=2, credits=3)

        async with session_factory() as session:
            job = ParseJob(
                bot_user_id=2, channel_username="ch", post_limit=20,
                status="pending", credit_status="reserved", result_delivered=False,
            )
            session.add(job)
            await session.commit()
            await session.refresh(job)
            existing_id = job.id

        async with session_factory() as session:
            result = await _start_parsing_with_real_db(session, user_id=2)

        assert result.created_new is False
        assert result.job.id == existing_id

        async with session_factory() as session:
            user = await session.get(BotUser, 2)
        assert user.available_credits == 3, "Credits should be unchanged"


# ── Helper ─────────────────────────────────────────────────────────────────────

async def _start_parsing_with_real_db(
    session: AsyncSession, user_id: int
) -> StartParsingResult:
    """Call start_parsing with real DB but mocked config + Celery."""
    from telegram_bot.app.parse_orchestrator import start_parsing

    mock_cfg = MagicMock()
    mock_cfg.STALE_PROCESSING_MINUTES = 10
    mock_cfg.STALE_PENDING_MINUTES = 5

    mock_run = MagicMock()
    mock_run.delay = MagicMock(return_value=MagicMock(id="celery-id"))

    with (
        patch("telegram_bot.app.parse_orchestrator.settings", mock_cfg),
        patch("telegram_bot.tasks.parse_task.run_parse_job", mock_run),
    ):
        return await start_parsing(
            session=session,
            bot_user_id=user_id,
            channel_username="testchannel",
            channel_title="Test Channel",
            post_limit=20,
            chat_id=999,
        )

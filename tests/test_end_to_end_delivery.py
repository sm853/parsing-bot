"""
End-to-end delivery pipeline tests.

Covers gaps not addressed by existing test files:

  test_race_conditions.py      — Layer 1 (handler guard) + Layer 2 (DB unique index)
  test_anti_duplication.py     — deliver_task idempotency (mark_result_delivered)
  test_csv_delivery.py         — notify_success CSV content + retry safety

This file adds:
  1. parse_task idempotency guard
       run_parse_job must skip (no parse, no deliver enqueue) when job is
       already in a terminal state (completed / failed).
       This guards against acks_late re-delivery after a worker crash.

  2. Full pipeline: two invocations of deliver_result → one notify_success
       Even if deliver_result is called twice for the same job (Celery retry,
       duplicate task), notify_success must be called exactly once and the
       document must be sent exactly once.

  3. notify_success API-call count
       Exactly three Telegram API calls per invocation:
         sendMessage (summary), sendDocument (CSV), sendMessage (keyboard).
       No silent loops, no repeated sends.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from telegram_bot.db.models import Base, BotUser, ParseJob
from telegram_bot.db.repository_sync import mark_result_delivered_if_not_yet


# ═══════════════════════════════════════════════════════════════════════════════
# Shared fixtures
# ═══════════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def db_engine():
    """Fresh in-memory SQLite engine with all tables + the partial unique index."""
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _set_wal(dbapi_conn, _):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.close()

    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine):
    """
    Session seeded with:
      - one BotUser (telegram_id=1, credits=3)
      - one ParseJob  (id auto, status='completed', result_delivered=False)
    Yields (session, job_id).
    """
    with Session(db_engine) as session:
        session.add(BotUser(telegram_id=1, username="u", first_name="U", available_credits=3))
        session.flush()
        job = ParseJob(
            bot_user_id=1,
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


@contextmanager
def _session_ctx(session):
    """Context-manager factory that yields the given session (for patching get_sync_session)."""
    @contextmanager
    def _ctx():
        yield session
    return _ctx()


def _task_self(retries=0, max_retries=3, request_id="celery-req-test"):
    t = MagicMock()
    t.request.retries = retries
    t.request.id = request_id
    t.max_retries = max_retries
    t.retry = MagicMock(side_effect=Exception("retry"))
    return t


# ═══════════════════════════════════════════════════════════════════════════════
# 1. parse_task idempotency guard
# ═══════════════════════════════════════════════════════════════════════════════

class TestParseTaskIdempotency:
    """
    run_parse_job must skip silently when the job is already in a terminal state.
    This prevents acks_late re-delivery from re-parsing a completed job.
    """

    @staticmethod
    def _make_parse_task_session_ctx(job_status: str):
        """Return a get_sync_session replacement whose session.get returns a job with the given status."""
        job = ParseJob(
            id=1, bot_user_id=1,
            channel_username="testchannel", post_limit=20,
            status=job_status, credit_status="consumed",
            result_delivered=True,
        )
        session = MagicMock()
        session.get = MagicMock(return_value=job)

        @contextmanager
        def _ctx():
            yield session

        return _ctx

    def test_skips_completed_job(self):
        """
        Job status='completed': no DB mutation, no asyncio.run (no parse),
        no deliver_task enqueue.
        """
        import telegram_bot.tasks.parse_task as pt

        update_calls = {"n": 0}
        deliver_calls = {"n": 0}

        mock_deliver = MagicMock()
        mock_deliver.delay = MagicMock(side_effect=lambda *a: deliver_calls.update({"n": deliver_calls["n"] + 1}))

        with (
            patch.object(pt, "get_sync_session", self._make_parse_task_session_ctx("completed")),
            patch.object(pt.repo, "update_job_status",
                         side_effect=lambda *a, **kw: update_calls.update({"n": update_calls["n"] + 1})),
            patch("telegram_bot.tasks.parse_task.asyncio.run",
                  side_effect=AssertionError("asyncio.run must NOT be called on a completed job")),
            patch("telegram_bot.tasks.deliver_task.deliver_result", mock_deliver),
        ):
            pt.run_parse_job.__wrapped__(
                _task_self(request_id="celery-redelivery"),
                1, 42, "testchannel", 20,
            )

        assert update_calls["n"] == 0, (
            "update_job_status must not be called for a completed job"
        )
        assert deliver_calls["n"] == 0, (
            "deliver_task must not be enqueued for a completed job"
        )

    def test_skips_failed_job(self):
        """Job status='failed': same skip — no re-parse, no re-enqueue."""
        import telegram_bot.tasks.parse_task as pt

        update_calls = {"n": 0}

        with (
            patch.object(pt, "get_sync_session", self._make_parse_task_session_ctx("failed")),
            patch.object(pt.repo, "update_job_status",
                         side_effect=lambda *a, **kw: update_calls.update({"n": update_calls["n"] + 1})),
            patch("telegram_bot.tasks.parse_task.asyncio.run",
                  side_effect=AssertionError("asyncio.run must NOT be called on a failed job")),
        ):
            pt.run_parse_job.__wrapped__(
                _task_self(request_id="celery-redelivery"),
                1, 42, "testchannel", 20,
            )

        assert update_calls["n"] == 0

    def test_skips_missing_job(self):
        """
        Job not found in DB: return immediately without any mutation or enqueue.
        This handles the edge case of a job being deleted between dispatch and execution.
        """
        import telegram_bot.tasks.parse_task as pt

        session = MagicMock()
        session.get = MagicMock(return_value=None)

        @contextmanager
        def _ctx():
            yield session

        update_calls = {"n": 0}

        with (
            patch.object(pt, "get_sync_session", _ctx),
            patch.object(pt.repo, "update_job_status",
                         side_effect=lambda *a, **kw: update_calls.update({"n": update_calls["n"] + 1})),
            patch("telegram_bot.tasks.parse_task.asyncio.run",
                  side_effect=AssertionError("asyncio.run must NOT be called for missing job")),
        ):
            pt.run_parse_job.__wrapped__(
                _task_self(),
                999, 42, "testchannel", 20,
            )

        assert update_calls["n"] == 0

    def test_pending_job_proceeds_to_processing(self):
        """
        Job status='pending': the guard passes and update_job_status is called
        with 'processing' before the parse begins.

        We raise inside asyncio.run to stop execution after the guard so we
        don't need to mock the full Telethon / parse pipeline.
        """
        import telegram_bot.tasks.parse_task as pt

        job = ParseJob(
            id=1, bot_user_id=1,
            channel_username="testchannel", post_limit=20,
            status="pending", credit_status="reserved",
            result_delivered=False,
        )
        session = MagicMock()
        session.get = MagicMock(return_value=job)

        @contextmanager
        def _ctx():
            yield session

        update_log: list[str] = []

        # update_job_status is called with (session, job_id, status[, error_message][, **kw])
        def _fake_update(s, jid, status, error_message=None, **kw):
            update_log.append(status)

        def _fake_run(coro):
            # Close the coroutine to silence "coroutine never awaited" warning,
            # then raise to stop the task without running the full parse.
            try:
                coro.close()
            except Exception:
                pass
            raise RuntimeError("stop here — parse not needed for this test")

        mock_deliver = MagicMock()
        mock_deliver.delay = MagicMock()

        with (
            patch.object(pt, "get_sync_session", _ctx),
            patch.object(pt.repo, "update_job_status", side_effect=_fake_update),
            patch("telegram_bot.tasks.parse_task.asyncio.run", side_effect=_fake_run),
            patch.object(pt, "refund_credit", MagicMock()),
            patch("telegram_bot.tasks.deliver_task.deliver_result", mock_deliver),
        ):
            pt.run_parse_job.__wrapped__(
                _task_self(), 1, 42, "testchannel", 20,
            )

        # Guard passed: update_job_status('processing') must have been called first
        assert update_log, "update_job_status was never called"
        assert update_log[0] == "processing", (
            f"Expected first update to be 'processing' (guard passed), got: {update_log}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Full pipeline: two deliver_result invocations → one notify_success
# ═══════════════════════════════════════════════════════════════════════════════

class TestFullDeliveryPipeline:
    """
    deliver_result called twice for the same job produces exactly one
    notify_success call and exactly one document send.

    Guard mechanism (new): deliver_result reads job.result_delivered at the
    START of each invocation.  After the first call completes all steps and
    marks result_delivered=True, the second invocation sees True and exits
    without calling notify_success at all.
    """

    @staticmethod
    def _make_session_ctx(session):
        @contextmanager
        def _ctx():
            yield session
        return _ctx

    @staticmethod
    def _task_self_for_deliver(retries=0):
        t = MagicMock()
        t.request.retries = retries
        t.max_retries = 3
        t.retry = MagicMock(side_effect=Exception("retry"))
        return t

    def test_two_invocations_call_notify_once(self, db_session):
        """
        Two deliver_result calls for the same job:
          - first: result_delivered=False → calls notify_success → marks True
          - second: result_delivered=True → exits immediately, notify_success NOT called
        """
        import telegram_bot.tasks.deliver_task as dt

        session, job_id = db_session
        notify_calls = {"n": 0}

        def _count_notify(jid, cid):
            notify_calls["n"] += 1

        with (
            patch.object(dt, "get_sync_session", self._make_session_ctx(session)),
            patch.object(dt.repo, "mark_result_delivered_if_not_yet",
                         lambda s, jid: mark_result_delivered_if_not_yet(s, jid)),
            patch.object(dt.delivery, "notify_success", side_effect=_count_notify),
            patch.object(dt.delivery, "notify_failure", MagicMock()),
        ):
            dt.deliver_result.__wrapped__(
                self._task_self_for_deliver(), job_id, 42, "success"
            )
            dt.deliver_result.__wrapped__(
                self._task_self_for_deliver(), job_id, 42, "success"
            )

        assert notify_calls["n"] == 1, (
            f"notify_success called {notify_calls['n']} times; "
            "expected exactly 1 — second invocation should be blocked by idempotency guard"
        )

    def test_notify_failure_also_idempotent(self, db_session):
        """Same idempotency guarantee holds for the failure path."""
        import telegram_bot.tasks.deliver_task as dt

        session, job_id = db_session
        notify_calls = {"n": 0}

        with (
            patch.object(dt, "get_sync_session", self._make_session_ctx(session)),
            patch.object(dt.repo, "mark_result_delivered_if_not_yet",
                         lambda s, jid: mark_result_delivered_if_not_yet(s, jid)),
            patch.object(dt.delivery, "notify_success", MagicMock()),
            patch.object(dt.delivery, "notify_failure",
                         side_effect=lambda jid, cid: notify_calls.update({"n": notify_calls["n"] + 1})),
        ):
            dt.deliver_result.__wrapped__(
                self._task_self_for_deliver(), job_id, 42, "failure"
            )
            dt.deliver_result.__wrapped__(
                self._task_self_for_deliver(), job_id, 42, "failure"
            )

        assert notify_calls["n"] == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 3. notify_success API-call count
# ═══════════════════════════════════════════════════════════════════════════════

class TestNotifySuccessApiCallCount:
    """
    notify_success must make exactly 3 Telegram API calls per invocation:
      1. sendMessage  — summary text
      2. sendDocument — CSV file
      3. sendMessage  — after-parse keyboard

    If any loop or conditional branch causes extra calls, these tests catch it.
    """

    def _make_job(self):
        j = MagicMock()
        j.channel_username = "testchannel"
        j.post_limit = 20
        j.posts = []
        j.error_message = None
        return j

    def _run_notify(self, job_id=1, chat_id=42):
        """
        Run notify_success with _send_message, _send_document, _send_after_parse_keyboard
        replaced by call-counting spies.
        Returns (msg_calls, doc_calls, kb_calls) as lists.
        """
        from telegram_bot.services import delivery

        job = self._make_job()

        send_msg_calls = []
        send_doc_calls = []
        send_kb_calls = []

        with (
            patch.object(delivery, "get_sync_session") as mock_ctx,
            patch.object(delivery, "get_job_with_posts", return_value=job),
            patch.object(delivery, "_send_message",
                         side_effect=lambda cid, txt: send_msg_calls.append((cid, txt))),
            patch.object(delivery, "_send_document",
                         side_effect=lambda cid, data, fn: send_doc_calls.append((cid, data, fn))),
            patch.object(delivery, "_send_after_parse_keyboard",
                         side_effect=lambda cid: send_kb_calls.append(cid)),
        ):
            mock_session = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            delivery.notify_success(job_id, chat_id)

        return send_msg_calls, send_doc_calls, send_kb_calls

    def test_exactly_one_summary_message(self):
        msg_calls, _, _ = self._run_notify()
        assert len(msg_calls) == 1, (
            f"Expected 1 sendMessage(summary), got {len(msg_calls)}"
        )

    def test_exactly_one_document(self):
        _, doc_calls, _ = self._run_notify()
        assert len(doc_calls) == 1, (
            f"Expected 1 sendDocument, got {len(doc_calls)}"
        )

    def test_exactly_one_keyboard(self):
        _, _, kb_calls = self._run_notify()
        assert len(kb_calls) == 1, (
            f"Expected 1 sendMessage(keyboard), got {len(kb_calls)}"
        )

    def test_document_payload_is_bytes(self):
        _, doc_calls, _ = self._run_notify()
        assert len(doc_calls) == 1
        _, file_content, _ = doc_calls[0]
        assert isinstance(file_content, bytes), (
            f"sendDocument received {type(file_content).__name__}, expected bytes"
        )

    def test_total_api_calls_is_three(self):
        """Aggregate guard: total Telegram API calls must be exactly 3."""
        msg, doc, kb = self._run_notify()
        total = len(msg) + len(doc) + len(kb)
        assert total == 3, (
            f"Expected exactly 3 Telegram API calls total, got {total} "
            f"(msg={len(msg)}, doc={len(doc)}, kb={len(kb)})"
        )

    def test_two_notify_calls_make_six_api_calls_total(self):
        """
        If notify_success is called twice (e.g., before the idempotency guard
        blocks the second deliver_result), each call makes exactly 3 API calls.
        Total must be 6, not 4 or 5 (which would indicate shared state).
        """
        from telegram_bot.services import delivery

        job = self._make_job()
        api_call_count = {"n": 0}

        def _count(*a, **kw):
            api_call_count["n"] += 1

        with (
            patch.object(delivery, "get_sync_session") as mock_ctx,
            patch.object(delivery, "get_job_with_posts", return_value=job),
            patch.object(delivery, "_send_message", side_effect=_count),
            patch.object(delivery, "_send_document", side_effect=_count),
            patch.object(delivery, "_send_after_parse_keyboard", side_effect=_count),
        ):
            mock_session = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)
            delivery.notify_success(1, 42)
            delivery.notify_success(1, 42)

        assert api_call_count["n"] == 6, (
            f"Expected 6 API calls for two notify_success invocations, "
            f"got {api_call_count['n']}. Each call must produce exactly 3."
        )

"""
Delivery step idempotency tests.

Covers:
  1. claim_delivery_step / reset_delivery_step — repository unit tests
  2. _send_step — skip when already done, reset flag on failure, stay set on success
  3. Retry resumes from the failed step, not from the beginning
  4. Four calls to notify_success produce exactly one of each user-visible message
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from telegram_bot.db.models import Base, BotUser, ParseJob
from telegram_bot.db.repository_sync import (
    claim_delivery_step,
    reset_delivery_step,
)


# ── Fixtures ───────────────────────────────────────────────────────────────────

@pytest.fixture()
def db_engine():
    engine = create_engine("sqlite:///:memory:", future=True)

    @event.listens_for(engine, "connect")
    def _wal(dbapi_conn, _):
        dbapi_conn.cursor().execute("PRAGMA journal_mode=WAL")

    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine):
    with Session(db_engine) as session:
        session.add(BotUser(
            telegram_id=1, username="u", first_name="U", available_credits=3,
        ))
        session.flush()
        job = ParseJob(
            bot_user_id=1, channel_username="chan", post_limit=20,
            status="completed", credit_status="consumed", result_delivered=False,
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        yield session, job.id


def _make_session_ctx(session):
    """Return a get_sync_session replacement that always yields the given session."""
    @contextmanager
    def _ctx():
        yield session
    return _ctx


def _make_mock_job():
    j = MagicMock()
    j.channel_username = "testchan"
    j.post_limit = 20
    j.posts = []
    return j


# ── 1. Repository helpers ──────────────────────────────────────────────────────

class TestClaimDeliveryStep:

    def test_first_claim_returns_true(self, db_session):
        session, job_id = db_session
        assert claim_delivery_step(session, job_id, "delivery_summary_sent") is True

    def test_second_claim_returns_false(self, db_session):
        session, job_id = db_session
        claim_delivery_step(session, job_id, "delivery_summary_sent")
        assert claim_delivery_step(session, job_id, "delivery_summary_sent") is False

    def test_reset_allows_reclaim(self, db_session):
        session, job_id = db_session
        claim_delivery_step(session, job_id, "delivery_summary_sent")
        reset_delivery_step(session, job_id, "delivery_summary_sent")
        assert claim_delivery_step(session, job_id, "delivery_summary_sent") is True

    def test_steps_are_independent(self, db_session):
        session, job_id = db_session
        claim_delivery_step(session, job_id, "delivery_summary_sent")
        # Claiming one step must not affect the others
        assert claim_delivery_step(session, job_id, "delivery_document_sent") is True
        assert claim_delivery_step(session, job_id, "delivery_keyboard_sent") is True
        assert claim_delivery_step(session, job_id, "delivery_failure_sent")  is True


# ── 2. _send_step behaviour ────────────────────────────────────────────────────

class TestSendStep:

    def test_executes_send_fn_when_unclaimed(self, db_session):
        from telegram_bot.services import delivery
        session, job_id = db_session
        calls: list[str] = []

        with patch.object(delivery, "get_sync_session", _make_session_ctx(session)):
            delivery._send_step(
                job_id, "delivery_summary_sent", "summary", lambda: calls.append("sent")
            )

        assert calls == ["sent"]

    def test_skips_send_fn_when_already_claimed(self, db_session):
        from telegram_bot.services import delivery
        session, job_id = db_session
        # Pre-claim the step
        claim_delivery_step(session, job_id, "delivery_summary_sent")

        calls: list[str] = []
        with patch.object(delivery, "get_sync_session", _make_session_ctx(session)):
            delivery._send_step(
                job_id, "delivery_summary_sent", "summary", lambda: calls.append("sent")
            )

        assert calls == []  # must be skipped

    def test_flag_reset_on_send_failure(self, db_session):
        from telegram_bot.services import delivery
        session, job_id = db_session

        def _fail():
            raise RuntimeError("network error")

        with patch.object(delivery, "get_sync_session", _make_session_ctx(session)):
            with pytest.raises(RuntimeError, match="network error"):
                delivery._send_step(
                    job_id, "delivery_summary_sent", "summary", _fail
                )

        # Flag must be False so the retry can re-attempt this step
        assert claim_delivery_step(session, job_id, "delivery_summary_sent") is True

    def test_flag_stays_set_on_success(self, db_session):
        from telegram_bot.services import delivery
        session, job_id = db_session

        with patch.object(delivery, "get_sync_session", _make_session_ctx(session)):
            delivery._send_step(
                job_id, "delivery_summary_sent", "summary", lambda: None
            )

        # Second claim must fail — step is permanently done
        assert claim_delivery_step(session, job_id, "delivery_summary_sent") is False


# ── 3. Retry resumes from the failed step ─────────────────────────────────────

class TestRetryResumesFromFailedStep:
    """
    Steps 1 (summary) and 2 (document) succeed on the first attempt.
    Step 3 (keyboard) fails on the first attempt.
    On retry, only step 3 must be re-attempted — steps 1 and 2 must not be re-sent.
    """

    def test_retry_sends_only_failed_step(self, db_session):
        from telegram_bot.services import delivery
        session, job_id = db_session
        ctx = _make_session_ctx(session)
        calls: list[str] = []

        keyboard_attempt = {"n": 0}

        def _send_keyboard():
            keyboard_attempt["n"] += 1
            if keyboard_attempt["n"] == 1:
                raise RuntimeError("400 Bad Request")
            calls.append("keyboard")

        patches = dict(
            get_sync_session=ctx,
            get_job_with_posts=_make_mock_job(),
        )

        with (
            patch.object(delivery, "get_sync_session", ctx),
            patch.object(delivery, "get_job_with_posts", return_value=_make_mock_job()),
            patch.object(delivery.report, "build_summary_text", return_value="txt"),
            patch.object(delivery.report, "build_csv",
                         return_value=MagicMock(getvalue=lambda: b"")),
            patch.object(delivery, "_send_message",
                         side_effect=lambda c, t: calls.append("summary")),
            patch.object(delivery, "_send_document",
                         side_effect=lambda c, d, f: calls.append("document")),
            patch.object(delivery, "_send_after_parse_keyboard",
                         side_effect=lambda c: _send_keyboard()),
        ):
            # Attempt 1: summary and document succeed, keyboard fails
            with pytest.raises(RuntimeError, match="400 Bad Request"):
                delivery.notify_success(job_id, 42)

            assert calls == ["summary", "document"], (
                f"Attempt 1 should send summary+document, got: {calls}"
            )
            calls.clear()

            # Attempt 2 (retry): only keyboard should be attempted
            delivery.notify_success(job_id, 42)

            assert calls == ["keyboard"], (
                f"Retry must send only 'keyboard', got: {calls}"
            )


# ── 4. Four retries → each message sent exactly once ─────────────────────────

class TestNoDuplicatesAcrossRetries:
    """
    Even if notify_success is called N times (simulating N retries),
    each step sends at most once thanks to per-step DB flags.
    """

    def test_four_calls_send_each_step_once(self, db_session):
        from telegram_bot.services import delivery
        session, job_id = db_session
        ctx = _make_session_ctx(session)
        calls: list[str] = []

        with (
            patch.object(delivery, "get_sync_session", ctx),
            patch.object(delivery, "get_job_with_posts", return_value=_make_mock_job()),
            patch.object(delivery.report, "build_summary_text", return_value="txt"),
            patch.object(delivery.report, "build_csv",
                         return_value=MagicMock(getvalue=lambda: b"")),
            patch.object(delivery, "_send_message",
                         side_effect=lambda c, t: calls.append("msg")),
            patch.object(delivery, "_send_document",
                         side_effect=lambda c, d, f: calls.append("doc")),
            patch.object(delivery, "_send_after_parse_keyboard",
                         side_effect=lambda c: calls.append("kb")),
        ):
            for _ in range(4):
                delivery.notify_success(job_id, 42)

        assert calls.count("msg") == 1, (
            f"summary sent {calls.count('msg')}x — expected exactly 1"
        )
        assert calls.count("doc") == 1, (
            f"document sent {calls.count('doc')}x — expected exactly 1"
        )
        assert calls.count("kb") == 1, (
            f"keyboard sent {calls.count('kb')}x — expected exactly 1"
        )

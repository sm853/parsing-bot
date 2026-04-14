"""
CSV delivery bug regression tests.

Root cause being guarded against:
  csv.writer requires a text-mode stream.  The old implementation wrapped a
  BytesIO in io.TextIOWrapper and passed it to csv.writer.  When build_csv()
  returned, the TextIOWrapper went out of scope; CPython's reference-counting
  immediately called TextIOWrapper.__del__ → close(), which propagated close()
  to the underlying BytesIO.  By the time _send_document() ran, the BytesIO
  was already closed → "ValueError: I/O operation on closed file".

Tests in this module verify:
  1. build_csv() returns an open, readable BytesIO (buffer is never closed)
  2. The returned buffer is positioned at offset 0 (ready for getvalue/read)
  3. getvalue() on the returned buffer yields non-empty bytes with UTF-8 BOM
  4. Calling build_csv() N times produces N independent buffers (no shared state)
  5. notify_success() builds CSV fresh on every call (retry-safe)
  6. _send_document receives bytes, not a BytesIO (no stream lifetime risk)
  7. deliver_task retry path resets the slot and re-calls notify_success (fresh data)
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch, call

import pytest


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_post(
    post_link="https://t.me/ch/1",
    post_text="hello",
    media_type="none",
    extracted_links="[]",
    views=100,
    reactions_count=5,
    comments_count=2,
    commenters=None,
):
    """Return a MagicMock that looks like a PostResult ORM row."""
    p = MagicMock()
    p.post_link = post_link
    p.post_text = post_text
    p.media_type = media_type
    p.extracted_links = extracted_links
    p.views = views
    p.reactions_count = reactions_count
    p.comments_count = comments_count
    p.commenters = commenters or []
    return p


def _make_job(channel_username="testch", post_limit=20):
    j = MagicMock()
    j.channel_username = channel_username
    j.post_limit = post_limit
    return j


# ── report.build_csv ──────────────────────────────────────────────────────────

class TestBuildCsv:
    def test_returns_open_bytesio(self):
        """Buffer must be open and readable after build_csv() returns."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([_make_post()])
        # If the TextIOWrapper bug were present, buf.closed would be True here
        assert not buf.closed, "BytesIO was closed — TextIOWrapper GC bug is back"

    def test_position_at_zero(self):
        """Buffer position must be 0 (ready to read from the start)."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([_make_post()])
        assert buf.tell() == 0

    def test_getvalue_returns_bytes(self):
        """getvalue() must return bytes, not crash."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([_make_post()])
        data = buf.getvalue()
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_utf8_bom_present(self):
        """Output must start with UTF-8 BOM for Excel compatibility."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([_make_post()])
        assert buf.getvalue()[:3] == b"\xef\xbb\xbf"

    def test_header_row_in_output(self):
        """CSV must contain the column header row."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([])
        text = buf.getvalue().decode("utf-8-sig")  # strip BOM
        assert "post_link" in text
        assert "views" in text

    def test_post_data_in_output(self):
        """Post values must appear in the CSV output."""
        from telegram_bot.services.report import build_csv
        post = _make_post(post_link="https://t.me/ch/99", views=9999)
        buf = build_csv([post])
        text = buf.getvalue().decode("utf-8-sig")
        assert "https://t.me/ch/99" in text
        assert "9999" in text

    def test_each_call_returns_independent_buffer(self):
        """Two calls must return two independent BytesIO objects."""
        from telegram_bot.services.report import build_csv
        posts = [_make_post()]
        buf1 = build_csv(posts)
        buf2 = build_csv(posts)
        assert buf1 is not buf2
        # Consuming buf1 must not affect buf2
        buf1.read()
        assert not buf2.closed
        assert buf2.tell() == 0

    def test_empty_posts_list(self):
        """Empty post list must produce header-only CSV without error."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([])
        assert not buf.closed
        assert len(buf.getvalue()) > 0

    def test_getvalue_callable_multiple_times(self):
        """getvalue() is position-independent — must work after read()."""
        from telegram_bot.services.report import build_csv
        buf = build_csv([_make_post()])
        first = buf.getvalue()
        buf.read()          # advance position to EOF
        second = buf.getvalue()
        assert first == second


# ── delivery.notify_success ───────────────────────────────────────────────────

class TestNotifySuccess:
    """
    notify_success must:
      - Build CSV fresh on every call (no shared state between retries)
      - Pass bytes (not BytesIO) to _send_document
      - Log csv byte length before calling sendDocument
    """

    def _run_notify(self, job_id=1, chat_id=42):
        """
        Run notify_success with all network calls mocked.
        Returns (send_message_calls, send_document_calls, send_kb_calls).
        """
        from telegram_bot.services import delivery

        job = _make_job()
        job.posts = [_make_post()]

        with (
            patch.object(delivery, "get_sync_session") as mock_ctx,
            patch.object(delivery, "get_job_with_posts", return_value=job),
            patch.object(delivery, "_send_message") as mock_msg,
            patch.object(delivery, "_send_document") as mock_doc,
            patch.object(delivery, "_send_after_parse_keyboard") as mock_kb,
        ):
            mock_session = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            delivery.notify_success(job_id, chat_id)

        return mock_msg.call_args_list, mock_doc.call_args_list, mock_kb.call_args_list

    def test_sends_all_three_messages(self):
        _, doc_calls, kb_calls = self._run_notify()
        # _send_message called once (summary), _send_document once, _send_after_parse_keyboard once
        msg_calls, doc_calls, kb_calls = self._run_notify()
        assert len(msg_calls) == 1
        assert len(doc_calls) == 1
        assert len(kb_calls) == 1

    def test_document_payload_is_bytes_not_bytesio(self):
        """_send_document must receive bytes, not a stream object."""
        _, doc_calls, _ = self._run_notify()
        assert len(doc_calls) == 1
        # _send_document(chat_id, file_content, filename)
        file_content_arg = doc_calls[0][0][1]  # second positional arg
        assert isinstance(file_content_arg, bytes), (
            f"Expected bytes, got {type(file_content_arg).__name__}. "
            "Passing a BytesIO risks 'I/O on closed file' on retries."
        )

    def test_document_bytes_non_empty(self):
        _, doc_calls, _ = self._run_notify()
        file_content = doc_calls[0][0][1]
        assert len(file_content) > 0

    def test_document_has_utf8_bom(self):
        """CSV bytes sent to Telegram must include UTF-8 BOM."""
        _, doc_calls, _ = self._run_notify()
        file_content = doc_calls[0][0][1]
        assert file_content[:3] == b"\xef\xbb\xbf"

    def test_fresh_csv_on_each_call(self):
        """
        Two calls to notify_success must build two independent byte strings.
        If a shared mutable buffer were reused, second call would either
        fail or return empty bytes.
        """
        from telegram_bot.services import delivery

        job = _make_job()
        job.posts = [_make_post()]
        doc_payloads = []

        def capture_doc(chat_id, file_content, filename):
            doc_payloads.append(file_content)

        with (
            patch.object(delivery, "get_sync_session") as mock_ctx,
            patch.object(delivery, "get_job_with_posts", return_value=job),
            patch.object(delivery, "_send_message"),
            patch.object(delivery, "_send_document", side_effect=capture_doc),
            patch.object(delivery, "_send_after_parse_keyboard"),
        ):
            mock_session = MagicMock()
            mock_ctx.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_ctx.return_value.__exit__ = MagicMock(return_value=False)

            delivery.notify_success(1, 42)
            delivery.notify_success(1, 42)

        assert len(doc_payloads) == 2
        # Both must be valid non-empty bytes (second call was not poisoned by first)
        assert all(isinstance(b, bytes) and len(b) > 0 for b in doc_payloads)
        # They should be equal in content (same data) but independent objects
        assert doc_payloads[0] == doc_payloads[1]
        assert doc_payloads[0] is not doc_payloads[1]


# ── deliver_task retry path ────────────────────────────────────────────────────

class TestDeliverTaskRetry:
    """
    When the first delivery attempt fails (Telegram is down), the slot must be
    reset so the retry builds and sends a completely fresh payload.
    """

    @staticmethod
    def _make_task_self(retries=0, max_retries=3):
        t = MagicMock()
        t.request.retries = retries
        t.max_retries = max_retries
        t.retry = MagicMock(side_effect=Exception("celery-retry"))
        return t

    def test_slot_reset_on_failure(self):
        """
        If notify_success raises, the delivery slot (result_delivered) must be
        reset to False so the next retry can re-claim it.
        """
        import telegram_bot.tasks.deliver_task as dt
        from telegram_bot.db.repository_sync import mark_result_delivered_if_not_yet
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        from telegram_bot.db.models import Base, BotUser, ParseJob

        engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(engine)

        with Session(engine) as setup_session:
            user = BotUser(telegram_id=1, available_credits=3)
            setup_session.add(user)
            setup_session.flush()
            job = ParseJob(
                bot_user_id=1,
                channel_username="ch",
                post_limit=20,
                status="completed",
                credit_status="consumed",
                result_delivered=False,
            )
            setup_session.add(job)
            setup_session.commit()
            job_id = job.id

        from contextlib import contextmanager

        @contextmanager
        def session_factory():
            with Session(engine) as s:
                yield s

        # Patch: first call to notify_success raises; guard and slot-reset use real DB
        with (
            patch.object(dt, "get_sync_session", session_factory),
            patch.object(dt.repo, "mark_result_delivered_if_not_yet",
                         lambda s, jid: mark_result_delivered_if_not_yet(s, jid)),
            patch.object(dt.delivery, "notify_success",
                         side_effect=RuntimeError("Telegram down")),
        ):
            task_self = self._make_task_self(retries=0)
            with pytest.raises(Exception):  # self.retry() raises
                dt.deliver_result.__wrapped__(task_self, job_id, 42, "success")

        # After the failed attempt, result_delivered must be False again
        with Session(engine) as verify_session:
            refreshed = verify_session.get(ParseJob, job_id)
            assert refreshed.result_delivered is False, (
                "result_delivered was not reset after failed delivery — "
                "a retry would skip sending because the guard sees True"
            )

    def test_retry_can_reclaim_slot_and_send(self):
        """
        After a slot reset, the NEXT invocation of deliver_result must be
        able to claim the slot again and call notify_success.
        """
        import telegram_bot.tasks.deliver_task as dt
        from telegram_bot.db.repository_sync import mark_result_delivered_if_not_yet
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        from telegram_bot.db.models import Base, BotUser, ParseJob

        engine = create_engine("sqlite:///:memory:", future=True)
        Base.metadata.create_all(engine)

        with Session(engine) as setup_session:
            user = BotUser(telegram_id=1, available_credits=3)
            setup_session.add(user)
            setup_session.flush()
            job = ParseJob(
                bot_user_id=1,
                channel_username="ch",
                post_limit=20,
                status="completed",
                credit_status="consumed",
                result_delivered=False,
            )
            setup_session.add(job)
            setup_session.commit()
            job_id = job.id

        from contextlib import contextmanager

        @contextmanager
        def session_factory():
            with Session(engine) as s:
                yield s

        call_log = []

        def mock_notify_success(jid, cid):
            call_log.append("notify")

        attempt1_raises = {"raised": False}

        def notify_first_fails(jid, cid):
            if not attempt1_raises["raised"]:
                attempt1_raises["raised"] = True
                raise RuntimeError("Telegram down")
            mock_notify_success(jid, cid)

        with (
            patch.object(dt, "get_sync_session", session_factory),
            patch.object(dt.repo, "mark_result_delivered_if_not_yet",
                         lambda s, jid: mark_result_delivered_if_not_yet(s, jid)),
            patch.object(dt.delivery, "notify_success", side_effect=notify_first_fails),
        ):
            # Attempt 1 — fails
            task_self_1 = self._make_task_self(retries=0)
            with pytest.raises(Exception):
                dt.deliver_result.__wrapped__(task_self_1, job_id, 42, "success")

            # Attempt 2 — retry, slot was reset, should succeed
            task_self_2 = self._make_task_self(retries=1)
            dt.deliver_result.__wrapped__(task_self_2, job_id, 42, "success")

        assert "notify" in call_log, "notify_success was never called on retry"

        with Session(engine) as verify_session:
            refreshed = verify_session.get(ParseJob, job_id)
            assert refreshed.result_delivered is True

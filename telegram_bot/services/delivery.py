"""
Delivery service — the SOLE communication channel between workers and users.

All Telegram API calls from the worker side go through here.
parse_task NEVER calls the Telegram API directly.

Both functions are synchronous (called from Celery workers).
They use httpx to call the Telegram Bot API over HTTP.

Architecture note:
  Parsing task (parse_task)   →  data processing only, zero Telegram I/O
  Delivery task (deliver_task) → calls notify_success / notify_failure here
  These two tasks are separate Celery jobs so delivery can be retried
  independently if Telegram has a temporary outage.

Retry-safety contract
─────────────────────
Every call to notify_success() is self-contained:
  1. Load posts fresh from DB (no cached ORM objects carried between retries)
  2. Build summary text
  3. Build CSV → immediately extract bytes via .getvalue()
  4. Send summary message
  5. Send CSV document (bytes, not a live stream object)
  6. Send after-parse keyboard

Using bytes (not BytesIO) for sendDocument ensures:
  - No "I/O operation on closed file" even if the buffer goes out of scope
  - Retries start from a clean slate — no consumed / partially-read stream
"""

from __future__ import annotations

import logging

import httpx

from telegram_bot.config import settings
from telegram_bot.db.engine import get_sync_session
from telegram_bot.db.repository_sync import get_job_with_posts
from telegram_bot.keyboards.after_parse import after_parse_kb
from telegram_bot.services import report

logger = logging.getLogger(__name__)

_BASE = f"https://api.telegram.org/bot{settings.BOT_TOKEN}"


def notify_success(job_id: int, chat_id: int) -> None:
    """
    Load results from DB, build report, send to user via Bot API.

    Sends three messages in order:
      1. Text summary  (channel averages)
      2. CSV document  (full post data)
      3. After-parse keyboard  (Parse another / Exit)

    Every call is fully self-contained: data is loaded fresh from DB, the CSV
    is built fresh, and bytes are extracted before any network call.  This
    makes the function safe to call multiple times (deliver_task retries).

    Raises httpx.HTTPStatusError on Telegram API errors — deliver_task retries.
    """
    # ── Load data ─────────────────────────────────────────────────────────────
    with get_sync_session() as session:
        job = get_job_with_posts(session, job_id)
        if job is None:
            logger.error("notify_success: job %s not found in DB — skipping delivery", job_id)
            return

        posts = job.posts  # eagerly loaded by get_job_with_posts (joinedload)

        # Build report text while session is still open (ORM objects are live)
        summary = report.build_summary_text(job, posts)

        # Build CSV and extract bytes IMMEDIATELY while the buffer is guaranteed open.
        # getvalue() works even at position 0 — it reads the whole internal buffer.
        csv_buffer = report.build_csv(posts)
        csv_bytes = csv_buffer.getvalue()   # immutable bytes — safe outside this block

        # Capture scalar attributes we need after the session closes.
        # Scalar columns on detached objects are still accessible in SQLAlchemy,
        # but capturing them explicitly documents the intent and avoids any risk
        # of DetachedInstanceError on unusual SA configurations.
        channel_username = job.channel_username
        post_limit = job.post_limit

    logger.info(
        "notify_success: ENTER  job=%s channel=@%s posts=%d csv_bytes=%d chat=%s",
        job_id, channel_username, len(posts), len(csv_bytes), chat_id,
    )

    # ── Send messages — each logged individually so duplicates are visible ────
    logger.info("notify_success: sendMessage(summary)  job=%s chat=%s", job_id, chat_id)
    _send_message(chat_id, summary)

    filename = f"channel_{channel_username}_{post_limit}_posts.csv"
    logger.info(
        "notify_success: sendDocument(%r)  job=%s chat=%s size=%d",
        filename, job_id, chat_id, len(csv_bytes),
    )
    _send_document(chat_id, csv_bytes, filename)

    logger.info("notify_success: sendMessage(keyboard)  job=%s chat=%s", job_id, chat_id)
    _send_after_parse_keyboard(chat_id)

    logger.info("notify_success: DONE  job=%s chat=%s", job_id, chat_id)


def notify_failure(job_id: int, chat_id: int) -> None:
    """
    Send an error notification to the user.
    Called by deliver_task when parse_task sets status='failed'.
    Credit has already been refunded by parse_task at this point.
    """
    with get_sync_session() as session:
        from telegram_bot.db.models import ParseJob  # local import to avoid circular
        job = session.get(ParseJob, job_id)
        error_msg = job.error_message if job else "Unknown error"

    text = (
        "❌ <b>Parsing failed.</b>\n\n"
        f"Reason: {error_msg or 'Unknown error'}\n\n"
        "Your credit has been <b>refunded</b>. You can try again."
    )
    _send_message(chat_id, text)
    _send_after_parse_keyboard(chat_id)


# ── Internal HTTP helpers ──────────────────────────────────────────────────────

def _send_message(chat_id: int, text: str) -> None:
    response = httpx.post(
        f"{_BASE}/sendMessage",
        json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=30,
    )
    _check_response("sendMessage", chat_id, response)


def _send_document(chat_id: int, file_content: bytes, filename: str) -> None:
    """
    Upload a file to Telegram via multipart/form-data.

    Args:
        chat_id:      Telegram chat to send to.
        file_content: Raw bytes of the file.  Using bytes (not BytesIO) avoids
                      any "I/O on closed file" risk — bytes are immutable and
                      have no stream position to lose.
        filename:     Name shown to the user in Telegram.

    The files= dict format is the canonical httpx multipart upload:
      (filename, content, mime_type)
    httpx builds the Content-Disposition header from filename and the
    Content-Type header from mime_type automatically.
    """
    logger.debug(
        "_send_document: chat=%s filename=%r size=%d bytes",
        chat_id, filename, len(file_content),
    )
    response = httpx.post(
        f"{_BASE}/sendDocument",
        data={"chat_id": str(chat_id)},
        files={"document": (filename, file_content, "text/csv")},
        timeout=60,
    )
    _check_response("sendDocument", chat_id, response)


def _send_after_parse_keyboard(chat_id: int) -> None:
    """Send the after-parse inline keyboard as a plain JSON call."""
    kb = after_parse_kb()
    response = httpx.post(
        f"{_BASE}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": "What would you like to do next?",
            "reply_markup": kb.model_dump(),
        },
        timeout=30,
    )
    _check_response("sendMessage[after_parse_kb]", chat_id, response)


def _check_response(method: str, chat_id: int, response: httpx.Response) -> None:
    """
    Log the Telegram API result and raise on HTTP or API-level errors.

    Telegram returns HTTP 200 even for some logical errors (ok=false in the
    JSON body), so we check both the HTTP status and the ok field.

    Raises:
        httpx.HTTPStatusError  — non-2xx HTTP response
        RuntimeError           — HTTP 200 but ok=false in Telegram JSON
    """
    try:
        body = response.json()
    except Exception:
        body = {"raw": response.text}

    ok = body.get("ok", False) if isinstance(body, dict) else False

    if response.is_success and ok:
        logger.info("%s → ok  chat=%s", method, chat_id)
        return

    # Log full details before raising so the worker log captures everything
    logger.error(
        "%s failed — chat=%s  http_status=%s  ok=%s  body=%s",
        method, chat_id, response.status_code, ok, body,
    )

    if not response.is_success:
        response.raise_for_status()   # raises httpx.HTTPStatusError

    # HTTP 200 but Telegram reported ok=false (e.g. chat not found, bot blocked)
    raise RuntimeError(
        f"Telegram {method} returned ok=false: {body.get('description', body)}"
    )

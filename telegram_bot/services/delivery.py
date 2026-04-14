"""
Delivery service — the SOLE communication channel between workers and users.

All Telegram API calls from the worker side go through here.
parse_task NEVER calls the Telegram API directly.

Both functions are synchronous (called from Celery workers).
They use httpx to call the Telegram Bot API over HTTP.

Step-idempotency contract
─────────────────────────
Each outbound Telegram call is wrapped in _send_step(), which:
  1. Atomically claims a per-step DB flag before sending
     (UPDATE … SET step=True WHERE step=False RETURNING id)
  2. If claimed → executes the send
  3. On send failure → resets the flag so the retry can re-attempt only this step
  4. If not claimed → step already done, skip silently

This guarantees each user-visible message is sent at most once even across
multiple Celery retries.  Earlier steps are never re-sent on retry.

Steps for notify_success:
  1. summary_text         → delivery_summary_sent
  2. csv_document         → delivery_document_sent
  3. after_parse_keyboard → delivery_keyboard_sent

Steps for notify_failure:
  1. failure_text         → delivery_failure_sent
  2. after_parse_keyboard → delivery_keyboard_sent
"""

from __future__ import annotations

import logging

import httpx

from telegram_bot.config import settings
from telegram_bot.db.engine import get_sync_session
from telegram_bot.db.repository_sync import (
    get_job_with_posts,
    claim_delivery_step,
    reset_delivery_step,
)
from telegram_bot.keyboards.after_parse import after_parse_kb
from telegram_bot.services import report

logger = logging.getLogger(__name__)

_BASE = f"https://api.telegram.org/bot{settings.BOT_TOKEN}"

# Column names for each delivery step — must match ParseJob Boolean columns.
STEP_SUMMARY  = "delivery_summary_sent"
STEP_DOCUMENT = "delivery_document_sent"
STEP_KEYBOARD = "delivery_keyboard_sent"
STEP_FAILURE  = "delivery_failure_sent"


def notify_success(job_id: int, chat_id: int) -> None:
    """
    Send 3 messages for a successful parse. Step-idempotent.

    Steps:
      1. summary text  (STEP_SUMMARY)
      2. CSV document  (STEP_DOCUMENT)
      3. keyboard      (STEP_KEYBOARD)

    On Telegram API error: raises — deliver_task retries from the failed step only.
    """
    # Load data once — all three send steps use values captured here.
    with get_sync_session() as session:
        job = get_job_with_posts(session, job_id)
        if job is None:
            logger.error("notify_success: job %s not found in DB — skipping", job_id)
            return

        posts = job.posts  # eagerly loaded by get_job_with_posts (joinedload)
        summary = report.build_summary_text(job, posts)
        csv_buffer = report.build_csv(posts)
        csv_bytes = csv_buffer.getvalue()   # immutable bytes — safe outside this block
        channel_username = job.channel_username
        post_limit = job.post_limit

    logger.info(
        "notify_success: ENTER  job=%s channel=@%s posts=%d csv_bytes=%d chat=%s",
        job_id, channel_username, len(posts), len(csv_bytes), chat_id,
    )

    _send_step(job_id, STEP_SUMMARY,  "summary_text",
               lambda: _send_message(chat_id, summary))

    filename = f"channel_{channel_username}_{post_limit}_posts.csv"
    _send_step(job_id, STEP_DOCUMENT, "csv_document",
               lambda: _send_document(chat_id, csv_bytes, filename))

    _send_step(job_id, STEP_KEYBOARD, "after_parse_keyboard",
               lambda: _send_after_parse_keyboard(chat_id))

    logger.info("notify_success: DONE  job=%s chat=%s", job_id, chat_id)


def notify_failure(job_id: int, chat_id: int) -> None:
    """
    Send failure notification. Step-idempotent.

    Steps:
      1. failure text  (STEP_FAILURE)
      2. keyboard      (STEP_KEYBOARD)

    Credit has already been refunded by parse_task before this is called.
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
    _send_step(job_id, STEP_FAILURE,  "failure_text",
               lambda: _send_message(chat_id, text))
    _send_step(job_id, STEP_KEYBOARD, "after_parse_keyboard",
               lambda: _send_after_parse_keyboard(chat_id))


# ── Step execution helper ──────────────────────────────────────────────────────

def _send_step(
    job_id: int,
    step_col: str,
    step_name: str,
    send_fn,
) -> None:
    """
    Atomically claim and execute one delivery step.

    Claim:  UPDATE parse_jobs SET {step_col}=True
            WHERE id=:job_id AND {step_col}=False  RETURNING id
    If claimed  → call send_fn(); on send failure: reset flag for retry
    If not claimed → step already done, skip silently

    Guarantees: send_fn is called at most once per step even under concurrent retries.
    """
    with get_sync_session() as session:
        claimed = claim_delivery_step(session, job_id, step_col)

    if not claimed:
        logger.info("_send_step: SKIP  job=%s step=%s (already done)", job_id, step_name)
        return

    logger.info("_send_step: SENDING  job=%s step=%s", job_id, step_name)
    try:
        send_fn()
        logger.info("_send_step: OK  job=%s step=%s", job_id, step_name)
    except Exception:
        logger.warning(
            "_send_step: FAILED  job=%s step=%s — resetting flag for retry",
            job_id, step_name,
        )
        with get_sync_session() as session:
            reset_delivery_step(session, job_id, step_col)
        raise  # propagates to deliver_task for Celery retry scheduling


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
            "reply_markup": kb.model_dump(exclude_none=True),  # exclude nulls — avoids 400
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

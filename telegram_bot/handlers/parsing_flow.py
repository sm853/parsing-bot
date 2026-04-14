"""
Parsing conversation flow handler — thin UI wrapper.

Responsibilities:
  - Receive user input for each FSM step
  - Delegate all business logic to parse_orchestrator
  - Render responses and advance FSM state

No business logic, no direct DB access, no Celery imports.

Duplicate-callback defence (Layer 1)
──────────────────────────────────────
The confirm keyboard can fire multiple callbacks before the bot manages to
disable it (Telegram delivers callbacks asynchronously; the user may tap twice
before the edit_text round-trip completes).

We keep a module-level set _in_flight keyed by (user_id, message_id).
The membership check AND the set.add() happen with no await between them,
which in asyncio (cooperative multitasking) makes them effectively atomic:
no other coroutine can run between the two synchronous statements.

  if key in _in_flight:      ← synchronous
      await callback.answer()
      return
  _in_flight.add(key)        ← synchronous, happens before first await

The second identical callback is dropped with a logged warning.
The database partial unique index (`one_active_job_per_user`) is Layer 2 and
handles the residual case where two requests reach the DB simultaneously
(e.g., multiple bot process replicas behind a load balancer).
"""

import logging
from typing import Final

from aiogram import Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from telegram_bot.app import parse_orchestrator
from telegram_bot.db.engine import get_async_session
from telegram_bot.keyboards.confirm import confirm_kb
from telegram_bot.keyboards.post_count import VALID_COUNTS, post_count_kb
from telegram_bot.services.channel_validator import (
    ChannelNotFoundError,
    InvalidLinkError,
    PrivateChannelError,
    resolve_channel,
)
from telegram_bot.parser.client import get_bot_client
from telegram_bot.states.parsing_states import ParseFlow

logger = logging.getLogger(__name__)
router = Router(name="parsing_flow")

# ── In-flight guard (Layer 1 duplicate-callback protection) ───────────────────
# Keys are (telegram_user_id, message_id_of_the_confirm_message).
# Checked and updated synchronously before any `await` in handle_confirmation.
_in_flight: set[tuple[int, int]] = set()


# ── Step 0: "Start parsing" button from main menu ─────────────────────────────

@router.callback_query(lambda c: c.data == "menu:start")
async def btn_start_parsing(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Entry point for the parsing flow.
    Checks for an existing active job first — if found, shows status instead.
    """
    async with get_async_session() as session:
        active = await parse_orchestrator.get_job_status(
            session, callback.from_user.id
        )

    if active is not None:
        logger.info(
            "btn_start_parsing: user=%d active_job=%d status=%s channel=%r",
            callback.from_user.id, active.id, active.status, active.channel_username,
        )
        status_text = (
            f"⏳ You already have an active job for <b>@{active.channel_username}</b>.\n"
            f"Status: <b>{active.status}</b>\n\n"
            "Please wait for it to complete before starting a new one."
        )
        await callback.message.answer(status_text)
        await callback.answer()
        return

    await state.set_state(ParseFlow.WAITING_CHANNEL_LINK)
    await callback.message.answer(
        "📌 <b>Step 1 of 3 — Channel link</b>\n\n"
        "Send me the public channel link or username.\n"
        "Examples: <code>@durov</code>  <code>t.me/channel</code>  <code>https://t.me/channel</code>"
    )
    await callback.answer()


# ── Step 1: Receive channel link ──────────────────────────────────────────────

@router.message(ParseFlow.WAITING_CHANNEL_LINK)
async def receive_channel_link(message: Message, state: FSMContext) -> None:
    """Validate the channel link; on success advance to post count selection."""
    raw_input = (message.text or "").strip()
    logger.info(
        "receive_channel_link: user=%d raw_input=%r",
        message.from_user.id, raw_input,
    )

    client = get_bot_client()

    try:
        meta = await resolve_channel(client, raw_input)
    except InvalidLinkError as e:
        logger.info("receive_channel_link: InvalidLinkError: %s", e)
        await message.answer(
            "❌ <b>Invalid link.</b>\n"
            "Please send a valid public channel link, e.g. <code>@channel</code> or <code>t.me/channel</code>."
        )
        return
    except PrivateChannelError as e:
        logger.info("receive_channel_link: PrivateChannelError: %s", e)
        await message.answer(
            "🔒 <b>Private channel.</b>\n"
            "I can only parse <b>public</b> channels. Please try a different link."
        )
        return
    except ChannelNotFoundError as e:
        logger.info("receive_channel_link: ChannelNotFoundError: %s", e)
        await message.answer(
            "❓ <b>Channel not found.</b>\n"
            "Make sure the username is correct and the channel is public."
        )
        return

    logger.info(
        "receive_channel_link: resolved  username=%r title=%r members=%d",
        meta.username, meta.title, meta.member_count,
    )

    await state.update_data(
        channel_username=meta.username,
        channel_title=meta.title,
        member_count=meta.member_count,
    )
    await state.set_state(ParseFlow.WAITING_POST_COUNT)

    members_str = f"{meta.member_count:,}" if meta.member_count else "unknown"
    await message.answer(
        f"✅ Found: <b>{meta.title}</b> (@{meta.username})\n"
        f"👥 Members: <b>{members_str}</b>\n\n"
        "📊 <b>Step 2 of 3 — Number of posts</b>\n\n"
        "How many recent posts should I analyse?",
        reply_markup=post_count_kb(),
    )


# ── Step 2: Post count selection ──────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("count:"))
async def receive_post_count(callback: CallbackQuery, state: FSMContext) -> None:
    current_state = await state.get_state()
    if current_state != ParseFlow.WAITING_POST_COUNT.state:
        await callback.answer()
        return

    value = callback.data.split(":")[1]
    if value == "cancel":
        await state.clear()
        await callback.message.edit_text(
            "❌ Cancelled. Send /start to begin again.",
            reply_markup=None,
        )
        await callback.answer()
        return

    try:
        count = int(value)
        assert count in VALID_COUNTS
    except (ValueError, AssertionError):
        await callback.answer("Invalid selection.", show_alert=True)
        return

    await state.update_data(post_count=count)
    await state.set_state(ParseFlow.CONFIRMING)

    data = await state.get_data()
    logger.debug(
        "receive_post_count: user=%d count=%d channel=%r",
        callback.from_user.id, count, data.get("channel_username"),
    )

    await callback.message.edit_text(
        f"📋 <b>Step 3 of 3 — Confirm</b>\n\n"
        f"📌 Channel: <b>@{data['channel_username']}</b>\n"
        f"📊 Posts: <b>{count}</b>\n"
        f"📁 Output: CSV file + summary\n\n"
        "Shall I start parsing?",
        reply_markup=confirm_kb(),
    )
    await callback.answer()


# ── Step 3: Confirmation ──────────────────────────────────────────────────────

@router.callback_query(lambda c: c.data and c.data.startswith("confirm:"))
async def handle_confirmation(callback: CallbackQuery, state: FSMContext) -> None:
    # ── In-flight guard (Layer 1) ──────────────────────────────────────────────
    # These two lines are synchronous — no await between them — so they are
    # atomic within the asyncio event loop. A second concurrent coroutine
    # cannot execute between the membership test and the set.add().
    _key = (callback.from_user.id, callback.message.message_id)
    if _key in _in_flight:
        logger.warning(
            "handle_confirmation: DUPLICATE DROPPED  "
            "callback_id=%s user_id=%d msg_id=%d data=%r  "
            "(in-flight guard hit — first tap still processing)",
            callback.id, callback.from_user.id,
            callback.message.message_id, callback.data,
        )
        await callback.answer()   # must always answer; suppress spinner
        return
    _in_flight.add(_key)
    # ──────────────────────────────────────────────────────────────────────────

    logger.info(
        "handle_confirmation: ENTER  "
        "callback_id=%s user_id=%d msg_id=%d data=%r",
        callback.id, callback.from_user.id,
        callback.message.message_id, callback.data,
    )

    try:
        await _handle_confirmation_inner(callback, state)
    finally:
        # Always release the guard — even on unexpected exceptions — so the
        # user is not permanently locked out of the confirm flow.
        _in_flight.discard(_key)
        logger.debug(
            "handle_confirmation: EXIT  user_id=%d msg_id=%d",
            callback.from_user.id, callback.message.message_id,
        )


async def _handle_confirmation_inner(
    callback: CallbackQuery, state: FSMContext
) -> None:
    """
    Core logic for the confirmation step, called only when the in-flight guard
    has been claimed.  Separated from handle_confirmation to keep the guard
    machinery readable.
    """
    # ── FSM guard ──────────────────────────────────────────────────────────────
    current_state = await state.get_state()
    if current_state != ParseFlow.CONFIRMING.state:
        logger.info(
            "handle_confirmation: FSM state mismatch  state=%s user=%d  "
            "(callback arrived after FSM was already cleared)",
            current_state, callback.from_user.id,
        )
        await callback.answer()
        return

    action = callback.data.split(":")[1]

    if action == "exit":
        await state.clear()
        await callback.message.edit_text(
            "❌ Cancelled. Send /start whenever you're ready.",
            reply_markup=None,
        )
        await callback.answer()
        return

    # ── action == "start" ──────────────────────────────────────────────────────

    # Answer the callback FIRST — removes the spinner immediately.
    # This prevents the user from seeing the button "stuck" and tapping again.
    logger.info(
        "handle_confirmation: answering callback (removing spinner)  "
        "user_id=%d callback_id=%s msg_id=%d",
        callback.from_user.id, callback.id, callback.message.message_id,
    )
    await callback.answer()

    data = await state.get_data()
    channel_username: str = data["channel_username"]
    channel_title: str = data.get("channel_title", "")
    post_count: int = data["post_count"]
    chat_id: int = callback.message.chat.id
    user_id: int = callback.from_user.id

    logger.info(
        "handle_confirmation: START ACTION  "
        "user_id=%d chat_id=%d channel=%r post_count=%d "
        "callback_id=%s msg_id=%d",
        user_id, chat_id, channel_username, post_count,
        callback.id, callback.message.message_id,
    )

    # Disable the confirm keyboard immediately.
    # Removes the buttons from the Telegram UI before start_parsing() runs,
    # so there is nothing to tap during the DB round-trip.
    keyboard_disabled = False
    try:
        await callback.message.edit_text(
            f"⏳ <b>Starting…</b>\n\n"
            f"📌 @{channel_username}  ·  {post_count} posts",
            reply_markup=None,
        )
        keyboard_disabled = True
    except Exception as e:
        # edit_text can fail if the message was already edited by a concurrent
        # handler (race condition that slipped past the in-flight guard, or a
        # Telegram network error).  Log and continue — the job creation below
        # has its own duplicate-safe guard via the DB unique constraint.
        logger.warning(
            "handle_confirmation: edit_text failed (keyboard NOT disabled)  "
            "user_id=%d msg_id=%d error=%s",
            user_id, callback.message.message_id, e,
        )

    logger.info(
        "handle_confirmation: keyboard_disabled=%s  user_id=%d msg_id=%d",
        keyboard_disabled, user_id, callback.message.message_id,
    )

    # Clear FSM — job state is tracked in DB from this point
    await state.clear()

    # ── Dispatch to orchestrator ───────────────────────────────────────────────
    logger.info(
        "handle_confirmation: calling start_parsing  user_id=%d channel=%r",
        user_id, channel_username,
    )

    async with get_async_session() as session:
        result = await parse_orchestrator.start_parsing(
            session=session,
            bot_user_id=user_id,
            channel_username=channel_username,
            channel_title=channel_title,
            post_limit=post_count,
            chat_id=chat_id,
        )

    # Log the full outcome for diagnosis
    logger.info(
        "handle_confirmation: start_parsing returned  "
        "created_new=%s no_credits=%s job_id=%s celery_task_id=%s  user_id=%d",
        result.created_new,
        result.no_credits,
        result.job.id if result.job else None,
        result.job.celery_task_id if result.job else None,
        user_id,
    )

    # ── Handle the three possible outcomes ────────────────────────────────────

    if result.no_credits:
        from telegram_bot.handlers.payments import send_stars_invoice  # noqa: PLC0415
        await callback.message.edit_text(
            "💳 <b>You've used all your free parsings.</b>\n\n"
            "Donate <b>100 Stars</b> to get 4 more parsings.",
            reply_markup=None,
        )
        await send_stars_invoice(callback.message.chat.id, callback.bot)
        return

    if not result.created_new:
        # Duplicate-tap path: a job was already active (either found in the
        # pre-check, or this request lost the race at the DB INSERT level).
        logger.warning(
            "handle_confirmation: DUPLICATE JOB  user_id=%d  job_id=%s  "
            "status=%s  (second tap reached start_parsing after guard)",
            user_id,
            result.job.id if result.job else "?",
            result.job.status if result.job else "?",
        )
        await callback.message.edit_text(
            f"⏳ <b>Already parsing @{result.job.channel_username}…</b>\n\n"
            "Your previous job is still running. "
            "I'll send you the results when it's done.",
            reply_markup=None,
        )
        return

    # ── Normal path: fresh job created and Celery task dispatched ─────────────
    logger.info(
        "handle_confirmation: JOB DISPATCHED  "
        "job_id=%d celery_task_id=%s  user_id=%d  channel=%r  post_count=%d",
        result.job.id,
        result.job.celery_task_id,
        user_id,
        channel_username,
        post_count,
    )

    await callback.message.edit_text(
        f"⏳ <b>Parsing in progress…</b>\n\n"
        f"📌 @{channel_username}  ·  {post_count} posts\n\n"
        "I'll send you the results as soon as it's done. Please wait.",
        reply_markup=None,
    )


# ── "Parse another channel" after delivery ────────────────────────────────────

@router.callback_query(lambda c: c.data == "after:add")
async def btn_parse_another(callback: CallbackQuery, state: FSMContext) -> None:
    """Re-enter the flow from the after-parse keyboard."""
    await state.clear()
    await state.set_state(ParseFlow.WAITING_CHANNEL_LINK)
    await callback.message.answer(
        "📌 <b>Step 1 of 3 — Channel link</b>\n\n"
        "Send me the public channel link or username."
    )
    await callback.answer()

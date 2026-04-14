"""
/start command handler and main menu callbacks.

This is the UI layer only:
  - Receives user input
  - Calls orchestrator or repo for data
  - Sends responses

No business logic here.
"""

from aiogram import Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from telegram_bot.db.engine import get_async_session
from telegram_bot.db.repository_async import upsert_bot_user
from telegram_bot.keyboards.main_menu import main_menu_kb

router = Router(name="start")

WELCOME_TEXT = (
    "👋 <b>Welcome to the Telegram Channel Parser!</b>\n\n"
    "I can analyse public Telegram channels and give you:\n"
    "• Per-post stats: views, reactions, comments\n"
    "• Media type & extracted links per post\n"
    "• List of commenters per post\n"
    "• Channel averages summary\n"
    "• Downloadable CSV report\n\n"
    "You have <b>{credits} free parse(s)</b> remaining.\n\n"
    "Tap <b>Start parsing</b> to begin."
)


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext) -> None:
    """Register/update user and show the main menu."""
    await state.clear()

    async with get_async_session() as session:
        user = await upsert_bot_user(
            session,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
        )
        credits = user.available_credits

    await message.answer(
        WELCOME_TEXT.format(credits=credits),
        reply_markup=main_menu_kb(),
    )


@router.callback_query(lambda c: c.data == "menu:exit")
async def btn_exit(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        "👋 Goodbye! Send /start anytime to parse again."
    )
    await callback.answer()


@router.callback_query(lambda c: c.data in ("after:exit",))
async def btn_after_exit(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.answer(
        "👋 Goodbye! Send /start anytime to parse again.",
        reply_markup=main_menu_kb(),
    )
    await callback.answer()

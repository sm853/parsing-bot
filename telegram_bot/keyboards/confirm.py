from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="▶️ Start", callback_data="confirm:start"),
                InlineKeyboardButton(text="❌ Exit", callback_data="confirm:exit"),
            ]
        ]
    )

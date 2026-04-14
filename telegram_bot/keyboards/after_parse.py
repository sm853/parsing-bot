from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def after_parse_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 Parse another channel", callback_data="after:add"
                ),
                InlineKeyboardButton(text="❌ Exit", callback_data="after:exit"),
            ]
        ]
    )

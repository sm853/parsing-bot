from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Start parsing", callback_data="menu:start"),
                InlineKeyboardButton(text="❌ Exit", callback_data="menu:exit"),
            ]
        ]
    )

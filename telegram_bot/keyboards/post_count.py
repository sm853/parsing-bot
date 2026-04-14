from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

VALID_COUNTS = (20, 50, 100)


def post_count_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="20 posts", callback_data="count:20"),
                InlineKeyboardButton(text="50 posts", callback_data="count:50"),
                InlineKeyboardButton(text="100 posts", callback_data="count:100"),
            ],
            [
                InlineKeyboardButton(text="❌ Cancel", callback_data="count:cancel"),
            ],
        ]
    )

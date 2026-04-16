from typing import Any

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup


def after_parse_kb() -> InlineKeyboardMarkup:
    """aiogram model — used by bot handlers (callback routing)."""
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


def after_parse_kb_payload() -> dict[str, Any]:
    """
    Plain-dict keyboard payload for the Telegram Bot API JSON body.

    Never uses aiogram model_dump() — no null fields can leak through.
    Only callback_data buttons; url is absent entirely (not null, absent).
    """
    return {
        "inline_keyboard": [
            [
                {"text": "📊 Parse another channel", "callback_data": "after:add"},
                {"text": "❌ Exit",                  "callback_data": "after:exit"},
            ]
        ]
    }

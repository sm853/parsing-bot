"""
Tests for telegram_bot/utils/text_helpers.py — channel input normalization.

Run with:
  cd /Users/stas/Desktop/parsing-bot
  python -m pytest tests/test_normalize.py -v
"""

import pytest
from telegram_bot.utils.text_helpers import normalize_channel_input, is_invite_link


# ── normalize_channel_input ────────────────────────────────────────────────────

class TestNormalizeChannelInput:

    # ── Accepted formats ───────────────────────────────────────────────────────

    def test_at_username(self):
        assert normalize_channel_input("@exploitex") == "exploitex"

    def test_bare_username(self):
        assert normalize_channel_input("exploitex") == "exploitex"

    def test_tme_url(self):
        assert normalize_channel_input("https://t.me/exploitex") == "exploitex"

    def test_tme_url_no_scheme(self):
        assert normalize_channel_input("t.me/exploitex") == "exploitex"

    def test_telegram_me_url(self):
        assert normalize_channel_input("https://telegram.me/exploitex") == "exploitex"

    def test_uppercase_normalized_to_lower(self):
        assert normalize_channel_input("@EXPLOITEX") == "exploitex"
        assert normalize_channel_input("https://t.me/EXPLOITEX") == "exploitex"

    def test_trailing_whitespace_stripped(self):
        assert normalize_channel_input("  @exploitex  ") == "exploitex"
        assert normalize_channel_input("  https://t.me/exploitex  ") == "exploitex"

    # ── Post-ID suffix stripped ────────────────────────────────────────────────

    def test_post_link_strips_post_id(self):
        """https://t.me/exploitex/123 should normalize to exploitex."""
        assert normalize_channel_input("https://t.me/exploitex/123") == "exploitex"

    def test_post_link_no_scheme_strips_post_id(self):
        assert normalize_channel_input("t.me/exploitex/456") == "exploitex"

    def test_post_link_with_query_string(self):
        assert normalize_channel_input("https://t.me/exploitex/123?single") == "exploitex"

    # ── Invite / private links → None ─────────────────────────────────────────

    def test_invite_link_plus(self):
        """t.me/+hash is a private invite link and must be rejected."""
        assert normalize_channel_input("https://t.me/+ABCdef123xyz") is None

    def test_invite_link_plus_no_scheme(self):
        assert normalize_channel_input("t.me/+ABCdef123xyz") is None

    def test_invite_link_joinchat(self):
        """t.me/joinchat/hash is a private invite link and must be rejected."""
        assert normalize_channel_input("https://t.me/joinchat/abcdef123") is None

    def test_invite_link_joinchat_no_scheme(self):
        assert normalize_channel_input("t.me/joinchat/abcdef123") is None

    # ── Reserved paths → None ─────────────────────────────────────────────────

    def test_reserved_path_joinchat_bare(self):
        """'joinchat' by itself must not be treated as a username."""
        assert normalize_channel_input("https://t.me/joinchat") is None

    def test_reserved_path_share(self):
        assert normalize_channel_input("https://t.me/share") is None

    def test_reserved_path_addstickers(self):
        assert normalize_channel_input("t.me/addstickers") is None

    # ── Invalid / too short ────────────────────────────────────────────────────

    def test_empty_string(self):
        assert normalize_channel_input("") is None

    def test_too_short(self):
        """Usernames under 5 characters should be rejected."""
        assert normalize_channel_input("abc") is None
        assert normalize_channel_input("@abc") is None

    def test_random_text(self):
        assert normalize_channel_input("hello world") is None

    def test_http_non_telegram_url(self):
        assert normalize_channel_input("https://example.com/channel") is None


# ── is_invite_link ─────────────────────────────────────────────────────────────

class TestIsInviteLink:

    def test_plus_invite(self):
        assert is_invite_link("https://t.me/+ABCdef123") is True

    def test_plus_no_scheme(self):
        assert is_invite_link("t.me/+ABCdef123") is True

    def test_joinchat(self):
        assert is_invite_link("https://t.me/joinchat/abcdef") is True

    def test_joinchat_no_scheme(self):
        assert is_invite_link("t.me/joinchat/abcdef") is True

    def test_public_channel_not_invite(self):
        assert is_invite_link("https://t.me/exploitex") is False
        assert is_invite_link("@exploitex") is False
        assert is_invite_link("exploitex") is False

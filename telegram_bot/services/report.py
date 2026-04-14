"""
Report generation — pure data transformation, no I/O.

Converts PostResult/CommenterResult ORM rows into:
  - HTML summary text for the Telegram message
  - In-memory CSV bytes (via BytesIO) for the file attachment

BytesIO safety note
───────────────────
The previous implementation used io.TextIOWrapper(output) as an adapter for
csv.writer.  TextIOWrapper stores a reference to the underlying BytesIO and
calls close() on it when the wrapper is garbage-collected.  In CPython,
reference-counting means the GC runs the instant the wrapper leaves scope —
i.e. before build_csv() even returns — so the BytesIO arrives at the caller
already closed.

Fix: write to io.StringIO (text-mode), then encode the entire result to bytes
at the end and wrap in a fresh BytesIO.  No wrapper object touches the final
buffer, so there is nothing to close it prematurely.
"""

from __future__ import annotations

import csv
import io
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from telegram_bot.db.models import ParseJob, PostResult


def build_summary_text(job: "ParseJob", posts: list["PostResult"]) -> str:
    """
    Build the HTML summary message sent to the user after parsing.
    Includes per-channel averages and a brief description of the CSV.
    """
    total = len(posts)
    avg_views = sum(p.views for p in posts) / total if total else 0
    avg_reactions = sum(p.reactions_count for p in posts) / total if total else 0
    avg_comments = sum(p.comments_count for p in posts) / total if total else 0

    return (
        f"✅ <b>Parsing complete!</b>\n\n"
        f"📌 Channel: <b>@{job.channel_username}</b>\n"
        f"📊 Posts analysed: <b>{total}</b>\n\n"
        f"📈 <b>Channel averages:</b>\n"
        f"  👁 Views:     <b>{avg_views:,.0f}</b>\n"
        f"  ❤️ Reactions: <b>{avg_reactions:,.1f}</b>\n"
        f"  💬 Comments:  <b>{avg_comments:,.1f}</b>\n\n"
        "📎 Full data is attached as a CSV file below."
    )


def build_csv(posts: list["PostResult"]) -> io.BytesIO:
    """
    Build an in-memory CSV from PostResult rows and return a **fresh, open**
    BytesIO positioned at offset 0.

    Columns:
      post_link, post_text, media_type, extracted_links,
      views, reactions_count, comments_count, commenters

    Encoding:
      UTF-8 with BOM (\\xef\\xbb\\xbf) so Excel opens it without a re-encoding
      dialog on Windows.

    Implementation note:
      csv.writer requires a text-mode stream.  We use io.StringIO as the text
      buffer, then encode the entire string to bytes at the end.  This avoids
      the TextIOWrapper-over-BytesIO anti-pattern where the wrapper's __del__
      closes the underlying BytesIO before the caller can use it.
    """
    text_buf = io.StringIO()
    writer = csv.writer(text_buf, quoting=csv.QUOTE_MINIMAL)

    writer.writerow([
        "post_link",
        "post_text",
        "media_type",
        "extracted_links",
        "views",
        "reactions_count",
        "comments_count",
        "commenters",
    ])

    for post in posts:
        # extracted_links is stored as a JSON array string
        try:
            links = "; ".join(json.loads(post.extracted_links or "[]"))
        except (ValueError, TypeError):
            links = ""

        # commenters: join usernames from related CommenterResult rows
        commenter_names = "; ".join(c.username for c in (post.commenters or []))

        writer.writerow([
            post.post_link,
            (post.post_text or "").replace("\n", " "),
            post.media_type,
            links,
            post.views,
            post.reactions_count,
            post.comments_count,
            commenter_names,
        ])

    # Encode to bytes: UTF-8 BOM + CSV content.
    # BytesIO(bytes) constructor sets the initial position to 0 automatically —
    # no seek(0) needed, but we call it explicitly for clarity.
    csv_bytes = b"\xef\xbb\xbf" + text_buf.getvalue().encode("utf-8")
    output = io.BytesIO(csv_bytes)
    output.seek(0)
    return output

# Telegram Channel Parsing Bot

A Telegram bot that parses public channel posts and delivers results as a CSV
file with a per-channel summary. Built with aiogram 3, Celery, PostgreSQL, and
Telethon.

## Architecture

```
User (Telegram)
  └─ Bot process (aiogram) ──── Redis FSM ──────────────────┐
       └─ parse_orchestrator                                 │
            └─ Celery worker (parse_task)                    │
                 └─ Telethon (channel parse)                 │
                 └─ deliver_task ─────────────────────────►──┘
                      └─ Bot API HTTP (summary + CSV)
```

- **Bot process** handles the conversation flow (FSM) and job dispatch.
- **Celery worker** runs the Telethon parse in isolation — zero Bot API calls.
- **deliver_task** is a separate Celery task so delivery can be retried
  independently of the parse without re-running Telethon.
- **Idempotency** at every layer: handler in-flight guard → DB partial unique
  index → delivery slot (`result_delivered` flag).

## Quick start

```bash
# 1. Clone
git clone <your-repo-url>
cd parsing-bot

# 2. Create .env from the template and fill in your credentials
cp .env.example .env
$EDITOR .env

# 3. Install Python dependencies (for session init and tests)
pip install -r telegram_bot/requirements.txt

# 4. Authorise Telethon sessions — one-time interactive step
python -m telegram_bot.scripts.init_session bot_parser_session
python -m telegram_bot.scripts.init_session session_worker_0

# 5. Start all services
docker-compose up --build
```

## Configuration

All configuration lives in `.env`. Copy `.env.example` → `.env` and fill in
real values. Every variable is documented in `.env.example`.

| Variable | Description |
|---|---|
| `BOT_TOKEN` | Telegram Bot token from @BotFather |
| `API_ID` / `API_HASH` | MTProto credentials from my.telegram.org/apps |
| `TELEGRAM_PHONE` | Phone number linked to your Telegram account |
| `DB_PASSWORD` | PostgreSQL password (used by both the app and docker-compose) |
| `DATABASE_URL_ASYNC` | asyncpg connection string for the bot process |
| `DATABASE_URL_SYNC` | psycopg2 connection string for Celery workers |
| `INITIAL_CREDITS` | Free parsings granted to new users (default: 5) |
| `STARS_PRICE` | Telegram Stars price for a credit pack (default: 100) |
| `STALE_PROCESSING_MINUTES` | Auto-fail jobs stuck in processing (default: 10) |
| `STALE_PENDING_MINUTES` | Auto-fail jobs never picked up by Celery (default: 5) |

## Database migrations

```bash
# Apply all migrations (run from the project root)
DATABASE_URL_SYNC=postgresql://postgres:<password>@localhost:5432/parsing_bot \
  alembic upgrade head
```

## Running tests

```bash
pip install pytest pytest-asyncio aiosqlite greenlet
python -m pytest tests/ -v --asyncio-mode=auto
```

---

## Security

### What must NEVER be committed

| File / pattern | What it contains |
|---|---|
| `.env` | Live bot token, MTProto API credentials, phone number, DB passwords |
| `*.session` | Logged-in Telegram account state — equivalent to a password |
| `sessions/` | Directory holding all Telethon session files |

All three are covered by `.gitignore`. Verify before every push:

```bash
git status          # .env and *.session must NOT appear in staged files
git ls-files .env   # must return nothing
git ls-files "*.session" sessions/   # must return nothing
```

### Setting up credentials safely

1. **Local dev**: copy `.env.example` → `.env`, fill in real values. The file
   stays on your machine only.
2. **Server / CI**: inject secrets as environment variables or Docker secrets
   directly — never store `.env` in the repository or bake it into an image.
3. **Sessions**: the `sessions/` directory is mounted as a Docker named volume
   (`telegram_sessions`) so `.session` files never enter any image layer.

### If a secret is accidentally committed

Act immediately — git history is public even after the file is deleted:

1. **Revoke the exposed credential right now:**
   - Bot token → @BotFather → `/revoke`
   - MTProto API hash → my.telegram.org/apps → regenerate
   - DB password → change in your database, update `.env`
2. **Scrub the history:**
   ```bash
   git filter-repo --path .env --invert-paths
   # or for session files:
   git filter-repo --path-glob "*.session" --invert-paths
   ```
3. Force-push and ask all collaborators to re-clone.

### Telethon session file security

A `.session` file is a serialised login for your Telegram account. Whoever
holds it can read messages, join channels, and act as that account.

- Session files are written to `sessions/` by `init_session.py` — never to
  the project root or any tracked path.
- `sessions/` is listed in `.gitignore`.
- In Docker they live in the `telegram_sessions` named volume, isolated from
  the image filesystem.

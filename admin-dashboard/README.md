# Parsing Bot — Admin Dashboard

Next.js 14 admin dashboard for monitoring Telegram parsing sessions.

---

## Folder Structure

```
admin-dashboard/
├── cloudflare-worker/
│   ├── cron.ts                  # Scheduled Worker that calls /api/admin/refresh daily
│   └── wrangler.toml
├── migrations/
│   ├── 001_create_tables.sql    # Creates parsing_sessions, parsing_attempts, daily_stats
│   └── 002_seed_data.sql        # ~30 realistic seed rows for development
├── scripts/
│   └── migrate.js               # Node.js migration runner
├── src/
│   ├── app/
│   │   ├── api/admin/
│   │   │   ├── overview/route.ts
│   │   │   ├── runs/route.ts
│   │   │   ├── runs/[id]/route.ts
│   │   │   ├── channels/route.ts
│   │   │   ├── errors/route.ts
│   │   │   ├── timeseries/route.ts
│   │   │   └── refresh/route.ts
│   │   ├── admin/
│   │   │   ├── page.tsx         # Overview dashboard
│   │   │   └── runs/
│   │   │       ├── page.tsx     # Runs list with filters + pagination
│   │   │       └── [id]/page.tsx # Session detail
│   │   ├── layout.tsx
│   │   ├── page.tsx             # Redirects to /admin
│   │   └── globals.css
│   ├── components/
│   │   ├── charts/
│   │   │   ├── RunsPerDayChart.tsx
│   │   │   └── SuccessRateChart.tsx
│   │   ├── filters/
│   │   │   └── DashboardFilters.tsx
│   │   ├── tables/
│   │   │   └── RunsTable.tsx
│   │   └── ui/
│   │       ├── KpiCard.tsx
│   │       ├── RefreshButton.tsx
│   │       └── StatusBadge.tsx
│   └── lib/
│       ├── db.ts                # Singleton pg Pool
│       ├── queries.ts           # All SQL query functions
│       └── types.ts             # TypeScript interfaces
├── .env.example
├── next.config.ts
├── package.json
├── postcss.config.js
├── tailwind.config.ts
└── tsconfig.json
```

---

## Step-by-step Setup

### 1. Install dependencies

```bash
cd admin-dashboard
npm install
```

### 2. Configure environment

```bash
cp .env.example .env.local
# Edit .env.local and set:
#   DATABASE_URL=postgresql://user:pass@localhost:5432/parsing_bot
#   ADMIN_SECRET=your-secret-here
```

### 3. Run migrations

```bash
npm run migrate
# This runs migrations/001_create_tables.sql
```

### 4. Seed development data (optional)

```bash
psql "$DATABASE_URL" -f migrations/002_seed_data.sql
```

### 5. Start development server

```bash
npm run dev
# Open http://localhost:3000/admin
```

### 6. Build for production

```bash
npm run build
npm start
```

---

## MVP — What to wire up first

- [ ] Call `create_parsing_session()` at the start of `parse_task.py` (store the returned `session_id` in Celery task state or pass it forward)
- [ ] Call `start_parsing_attempt()` when Celery picks up the task
- [ ] Call `increment_session_attempts()` on every retry
- [ ] Call `complete_parsing_attempt()` when an attempt finishes (success or failure)
- [ ] Call `complete_parsing_session()` at the very end of the pipeline (after delivery)
- [ ] Deploy the dashboard to Fly.io / Railway / Vercel with `DATABASE_URL` pointing at your prod Postgres
- [ ] Set `ADMIN_SECRET` and deploy the Cloudflare Worker for daily stats refresh

---

## Wiring analytics.py into the bot

### parse_task.py

```python
from services.analytics import (
    create_parsing_session,
    start_parsing_attempt,
    increment_session_attempts,
    complete_parsing_attempt,
    complete_parsing_session,
)
import time

@celery.task(bind=True, max_retries=3)
def parse_task(self, telegram_user_id, username, channel, post_limit, options=None):
    with db_session() as session:
        # Create session on first attempt only
        if self.request.retries == 0:
            session_id = create_parsing_session(
                session, telegram_user_id, username, channel, post_limit, options or {}
            )
            # Store in task state so deliver_task can retrieve it
            self.update_state(state='PROGRESS', meta={'session_id': session_id})
        else:
            session_id = self.request.kwargs.get('session_id')

        increment_session_attempts(session, session_id)
        attempt_id = start_parsing_attempt(
            session, session_id, self.request.retries + 1, self.request.id
        )

    t0 = time.monotonic()
    try:
        result = do_parse(channel, post_limit)  # your existing parse logic
        duration_ms = int((time.monotonic() - t0) * 1000)

        with db_session() as session:
            complete_parsing_attempt(session, attempt_id, 'success', duration_ms)
            complete_parsing_session(
                session, session_id, 'success', duration_ms, result_rows=len(result)
            )

        return {'session_id': session_id, 'rows': len(result), 'data': result}

    except Exception as exc:
        duration_ms = int((time.monotonic() - t0) * 1000)
        with db_session() as session:
            complete_parsing_attempt(
                session, attempt_id, 'failed', duration_ms,
                error_code=type(exc).__name__, error_message=str(exc)
            )
            if self.request.retries >= self.max_retries:
                complete_parsing_session(
                    session, session_id, 'failed', duration_ms,
                    error_code=type(exc).__name__, error_message=str(exc)
                )
        raise self.retry(exc=exc, countdown=10)
```

### deliver_task.py

```python
# If delivery fails after a successful parse, mark as partial_success
from services.analytics import complete_parsing_session

with db_session() as session:
    complete_parsing_session(
        session, session_id, 'partial_success', duration_ms,
        result_rows=rows_parsed,
        error_code='DELIVERY_FAILED',
        error_message=str(exc),
    )
```

---

## Deploying the Cloudflare Worker

The worker in `cloudflare-worker/` calls `POST /api/admin/refresh` daily at 03:00 UTC to recompute `daily_stats`.

```bash
cd cloudflare-worker
npm install -g wrangler   # if not installed
wrangler login

# Set secrets (never put them in wrangler.toml)
wrangler secret put ADMIN_SECRET

# Update ADMIN_BASE_URL in wrangler.toml to your deployed domain
# Then deploy:
wrangler deploy
```

To test the trigger locally:

```bash
wrangler dev --test-scheduled
# In another terminal:
curl "http://localhost:8787/__scheduled?cron=0+3+*+*+*"
```

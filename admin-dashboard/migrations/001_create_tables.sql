-- Enums
DO $$ BEGIN
  CREATE TYPE session_status AS ENUM ('queued','running','success','failed','partial_success','cancelled');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
  CREATE TYPE attempt_status AS ENUM ('running','success','failed');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- parsing_sessions: one row per user-initiated parse flow
CREATE TABLE IF NOT EXISTS parsing_sessions (
  id                  BIGSERIAL PRIMARY KEY,
  telegram_user_id    BIGINT NOT NULL,
  username            TEXT,
  started_at          TIMESTAMPTZ NOT NULL,
  finished_at         TIMESTAMPTZ,
  status              session_status NOT NULL DEFAULT 'queued',
  selected_channel    TEXT,
  selected_period     TEXT,
  selected_format     TEXT,
  selected_options    JSONB,
  attempts_count      INT NOT NULL DEFAULT 0,
  parsing_duration_ms BIGINT,
  result_rows         INT,
  result_file_type    TEXT,
  result_file_url     TEXT,
  error_code          TEXT,
  error_message       TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- parsing_attempts: one row per Celery task attempt
CREATE TABLE IF NOT EXISTS parsing_attempts (
  id             BIGSERIAL PRIMARY KEY,
  session_id     BIGINT NOT NULL REFERENCES parsing_sessions(id) ON DELETE CASCADE,
  attempt_number INT NOT NULL DEFAULT 1,
  started_at     TIMESTAMPTZ NOT NULL,
  finished_at    TIMESTAMPTZ,
  status         attempt_status NOT NULL DEFAULT 'running',
  duration_ms    BIGINT,
  error_code     TEXT,
  error_message  TEXT,
  meta           JSONB,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- daily_stats: pre-aggregated per-day metrics
CREATE TABLE IF NOT EXISTS daily_stats (
  stat_date          DATE PRIMARY KEY,
  total_sessions     INT NOT NULL DEFAULT 0,
  successful         INT NOT NULL DEFAULT 0,
  failed             INT NOT NULL DEFAULT 0,
  partial_success    INT NOT NULL DEFAULT 0,
  cancelled          INT NOT NULL DEFAULT 0,
  unique_users       INT NOT NULL DEFAULT 0,
  avg_duration_ms    BIGINT,
  total_result_rows  BIGINT NOT NULL DEFAULT 0,
  top_channel        TEXT,
  computed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sessions_telegram_user ON parsing_sessions(telegram_user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_started_at ON parsing_sessions(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON parsing_sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_channel ON parsing_sessions(selected_channel);
CREATE INDEX IF NOT EXISTS idx_attempts_session ON parsing_attempts(session_id);

-- Updated_at trigger
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$;

DROP TRIGGER IF EXISTS sessions_updated_at ON parsing_sessions;
CREATE TRIGGER sessions_updated_at BEFORE UPDATE ON parsing_sessions
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

import { query } from './db';
import type {
  OverviewMetrics,
  RunRow,
  ChannelStat,
  ErrorStat,
  TimeseriesPoint,
  ParsingSession,
  ParsingAttempt,
  RunFilters,
} from './types';

export async function getOverviewMetrics(): Promise<OverviewMetrics> {
  const sql = `
    WITH totals AS (
      SELECT COUNT(*)::int AS total_sessions
      FROM parsing_sessions
    ),
    success_rate AS (
      SELECT
        CASE WHEN COUNT(*) = 0 THEN 0
             ELSE ROUND(
               COUNT(*) FILTER (WHERE status = 'success')::numeric * 100.0 / COUNT(*), 2
             )
        END AS success_rate
      FROM parsing_sessions
    ),
    avg_dur AS (
      SELECT AVG(parsing_duration_ms)::bigint AS avg_duration_ms
      FROM parsing_sessions
      WHERE parsing_duration_ms IS NOT NULL
        AND status IN ('success', 'partial_success')
    ),
    active AS (
      SELECT COUNT(*)::int AS active_now
      FROM parsing_sessions
      WHERE status = 'running'
    ),
    users AS (
      SELECT COUNT(DISTINCT telegram_user_id)::int AS total_users
      FROM parsing_sessions
    ),
    today AS (
      SELECT
        COUNT(*) FILTER (WHERE status = 'success')::int  AS success_today,
        COUNT(*) FILTER (WHERE status = 'failed')::int   AS failed_today,
        COUNT(*)::int                                      AS runs_today
      FROM parsing_sessions
      WHERE started_at >= CURRENT_DATE
    )
    SELECT
      t.total_sessions,
      sr.success_rate,
      ad.avg_duration_ms,
      a.active_now,
      u.total_users,
      td.success_today,
      td.failed_today,
      td.runs_today
    FROM totals t, success_rate sr, avg_dur ad, active a, users u, today td
  `;

  const rows = await query<{
    total_sessions: number;
    success_rate: string;
    avg_duration_ms: string | null;
    active_now: number;
    total_users: number;
    success_today: number;
    failed_today: number;
    runs_today: number;
  }>(sql);

  const r = rows[0];
  return {
    totalSessions: r.total_sessions,
    successRate: parseFloat(r.success_rate),
    avgDurationMs: r.avg_duration_ms ? parseInt(r.avg_duration_ms, 10) : null,
    activeNow: r.active_now,
    totalUsers: r.total_users,
    successToday: r.success_today,
    failedToday: r.failed_today,
    runsToday: r.runs_today,
  };
}

export async function getRuns(
  filters: RunFilters = {}
): Promise<{ rows: RunRow[]; total: number }> {
  const { from, to, status, channel, username, page = 1, limit = 20 } = filters;

  const conditions: string[] = [];
  const params: unknown[] = [];
  let idx = 1;

  if (from) {
    conditions.push(`started_at >= $${idx++}`);
    params.push(from);
  }
  if (to) {
    conditions.push(`started_at <= $${idx++}`);
    params.push(to);
  }
  if (status) {
    conditions.push(`status = $${idx++}`);
    params.push(status);
  }
  if (channel) {
    conditions.push(`selected_channel ILIKE $${idx++}`);
    params.push(`%${channel}%`);
  }
  if (username) {
    conditions.push(`username ILIKE $${idx++}`);
    params.push(`%${username}%`);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  const countSql = `SELECT COUNT(*)::int AS total FROM parsing_sessions ${where}`;
  const countRows = await query<{ total: number }>(countSql, params);
  const total = countRows[0].total;

  const offset = (page - 1) * limit;
  // Cast timestamps to text so pg returns strings instead of Date objects.
  // date-fns parseISO() calls .split() internally and crashes on Date objects.
  const dataSql = `
    SELECT
      id, telegram_user_id, username, selected_channel,
      status, parsing_duration_ms, result_rows,
      started_at::text AS started_at,
      finished_at::text AS finished_at,
      error_code, attempts_count
    FROM parsing_sessions
    ${where}
    ORDER BY started_at DESC
    LIMIT $${idx++} OFFSET $${idx++}
  `;

  const rows = await query<RunRow>(dataSql, [...params, limit, offset]);
  return { rows, total };
}

export async function getChannelStats(): Promise<ChannelStat[]> {
  const sql = `
    SELECT
      selected_channel AS channel,
      COUNT(*)::int AS count,
      ROUND(
        COUNT(*) FILTER (WHERE status = 'success')::numeric * 100.0 / NULLIF(COUNT(*), 0), 2
      ) AS success_rate
    FROM parsing_sessions
    WHERE selected_channel IS NOT NULL
    GROUP BY selected_channel
    ORDER BY count DESC
    LIMIT 20
  `;
  const rows = await query<{ channel: string; count: number; success_rate: string }>(sql);
  return rows.map((r) => ({
    channel: r.channel,
    count: r.count,
    successRate: parseFloat(r.success_rate),
  }));
}

export async function getErrorStats(): Promise<ErrorStat[]> {
  const sql = `
    SELECT
      error_code,
      MAX(error_message) AS error_message,
      COUNT(*)::int AS count
    FROM parsing_sessions
    WHERE error_code IS NOT NULL
    GROUP BY error_code
    ORDER BY count DESC
    LIMIT 20
  `;
  return query<ErrorStat>(sql);
}

export async function getTimeseries(days: number = 30): Promise<TimeseriesPoint[]> {
  const sql = `
    WITH date_series AS (
      SELECT generate_series(
        CURRENT_DATE - ($1 - 1) * INTERVAL '1 day',
        CURRENT_DATE,
        INTERVAL '1 day'
      )::date AS day
    ),
    session_counts AS (
      SELECT
        started_at::date AS day,
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status = 'success')::int AS success,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed
      FROM parsing_sessions
      WHERE started_at >= CURRENT_DATE - ($1 - 1) * INTERVAL '1 day'
      GROUP BY started_at::date
    )
    SELECT
      ds.day::text AS date,
      COALESCE(sc.total, 0)   AS total,
      COALESCE(sc.success, 0) AS success,
      COALESCE(sc.failed, 0)  AS failed
    FROM date_series ds
    LEFT JOIN session_counts sc ON sc.day = ds.day
    ORDER BY ds.day ASC
  `;
  return query<TimeseriesPoint>(sql, [days]);
}

export async function getSessionById(
  id: string
): Promise<(ParsingSession & { attempts: ParsingAttempt[] }) | null> {
  const sessionSql = `
    SELECT *,
      started_at::text  AS started_at,
      finished_at::text AS finished_at,
      created_at::text  AS created_at,
      updated_at::text  AS updated_at
    FROM parsing_sessions WHERE id = $1
  `;
  const sessions = await query<ParsingSession>(sessionSql, [id]);
  if (sessions.length === 0) return null;

  const session = sessions[0];

  const attemptsSql = `
    SELECT * FROM parsing_attempts
    WHERE session_id = $1
    ORDER BY attempt_number ASC
  `;
  const attempts = await query<ParsingAttempt>(attemptsSql, [id]);

  return { ...session, attempts };
}

export async function recomputeDailyStats(): Promise<void> {
  const sql = `
    INSERT INTO daily_stats (
      stat_date, total_sessions, successful, failed, partial_success, cancelled,
      unique_users, avg_duration_ms, total_result_rows, top_channel, computed_at
    )
    WITH date_series AS (
      SELECT generate_series(
        CURRENT_DATE - 29 * INTERVAL '1 day',
        CURRENT_DATE,
        INTERVAL '1 day'
      )::date AS day
    ),
    agg AS (
      SELECT
        started_at::date AS day,
        COUNT(*)::int AS total_sessions,
        COUNT(*) FILTER (WHERE status = 'success')::int          AS successful,
        COUNT(*) FILTER (WHERE status = 'failed')::int           AS failed,
        COUNT(*) FILTER (WHERE status = 'partial_success')::int  AS partial_success,
        COUNT(*) FILTER (WHERE status = 'cancelled')::int        AS cancelled,
        COUNT(DISTINCT telegram_user_id)::int                    AS unique_users,
        AVG(parsing_duration_ms)::bigint                         AS avg_duration_ms,
        COALESCE(SUM(result_rows), 0)::bigint                    AS total_result_rows
      FROM parsing_sessions
      WHERE started_at >= CURRENT_DATE - 29 * INTERVAL '1 day'
      GROUP BY started_at::date
    ),
    top_channels AS (
      SELECT DISTINCT ON (started_at::date)
        started_at::date AS day,
        selected_channel AS top_channel
      FROM parsing_sessions
      WHERE started_at >= CURRENT_DATE - 29 * INTERVAL '1 day'
        AND selected_channel IS NOT NULL
      GROUP BY started_at::date, selected_channel
      ORDER BY started_at::date, COUNT(*) DESC
    )
    SELECT
      ds.day AS stat_date,
      COALESCE(a.total_sessions, 0)    AS total_sessions,
      COALESCE(a.successful, 0)        AS successful,
      COALESCE(a.failed, 0)            AS failed,
      COALESCE(a.partial_success, 0)   AS partial_success,
      COALESCE(a.cancelled, 0)         AS cancelled,
      COALESCE(a.unique_users, 0)      AS unique_users,
      a.avg_duration_ms,
      COALESCE(a.total_result_rows, 0) AS total_result_rows,
      tc.top_channel,
      NOW() AS computed_at
    FROM date_series ds
    LEFT JOIN agg a ON a.day = ds.day
    LEFT JOIN top_channels tc ON tc.day = ds.day
    ON CONFLICT (stat_date) DO UPDATE SET
      total_sessions    = EXCLUDED.total_sessions,
      successful        = EXCLUDED.successful,
      failed            = EXCLUDED.failed,
      partial_success   = EXCLUDED.partial_success,
      cancelled         = EXCLUDED.cancelled,
      unique_users      = EXCLUDED.unique_users,
      avg_duration_ms   = EXCLUDED.avg_duration_ms,
      total_result_rows = EXCLUDED.total_result_rows,
      top_channel       = EXCLUDED.top_channel,
      computed_at       = EXCLUDED.computed_at
  `;
  await query(sql);
}

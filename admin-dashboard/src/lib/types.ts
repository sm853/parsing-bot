export type SessionStatus =
  | 'queued'
  | 'running'
  | 'success'
  | 'failed'
  | 'partial_success'
  | 'cancelled';

export type AttemptStatus = 'running' | 'success' | 'failed';

export interface ParsingSession {
  id: string;
  telegram_user_id: string;
  username: string | null;
  started_at: string;
  finished_at: string | null;
  status: SessionStatus;
  selected_channel: string | null;
  selected_period: string | null;
  selected_format: string | null;
  selected_options: Record<string, unknown> | null;
  attempts_count: number;
  parsing_duration_ms: number | null;
  result_rows: number | null;
  result_file_type: string | null;
  result_file_url: string | null;
  error_code: string | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
}

export interface ParsingAttempt {
  id: string;
  session_id: string;
  attempt_number: number;
  started_at: string;
  finished_at: string | null;
  status: AttemptStatus;
  duration_ms: number | null;
  error_code: string | null;
  error_message: string | null;
  meta: Record<string, unknown> | null;
  created_at: string;
}

export interface DailyStat {
  stat_date: string;
  total_sessions: number;
  successful: number;
  failed: number;
  partial_success: number;
  cancelled: number;
  unique_users: number;
  avg_duration_ms: number | null;
  total_result_rows: number;
  top_channel: string | null;
  computed_at: string;
}

export interface OverviewMetrics {
  totalSessions: number;
  successRate: number;
  avgDurationMs: number | null;
  activeNow: number;
  totalUsers: number;
  successToday: number;
  failedToday: number;
  runsToday: number;
}

export interface RunRow {
  id: string;
  telegram_user_id: string;
  username: string | null;
  selected_channel: string | null;
  status: SessionStatus;
  parsing_duration_ms: number | null;
  result_rows: number | null;
  started_at: string;
  finished_at: string | null;
  error_code: string | null;
  attempts_count: number;
}

export interface ChannelStat {
  channel: string;
  count: number;
  successRate: number;
}

export interface ErrorStat {
  error_code: string;
  error_message: string;
  count: number;
}

export interface TimeseriesPoint {
  date: string;
  total: number;
  success: number;
  failed: number;
}

export interface RunsResponse {
  rows: RunRow[];
  total: number;
  page: number;
  limit: number;
}

export interface RunFilters {
  from?: string;
  to?: string;
  status?: SessionStatus | '';
  channel?: string;
  username?: string;
  page?: number;
  limit?: number;
}

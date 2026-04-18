-- Seed parsing_sessions with ~30 realistic rows over last 14 days
INSERT INTO parsing_sessions
  (telegram_user_id, username, started_at, finished_at, status, selected_channel, selected_period, selected_format, selected_options, attempts_count, parsing_duration_ms, result_rows, result_file_type, error_code, error_message)
VALUES
  (101001, 'alice_dev',    NOW() - INTERVAL '13 days' + INTERVAL '9 hours',  NOW() - INTERVAL '13 days' + INTERVAL '9 hours' + INTERVAL '4200 ms',  'success',         '@crypto_news',   'last_7_days', 'csv',  '{"include_media": true}',                        1, 4200,  210, 'csv',  NULL,                NULL),
  (101002, 'bob_trader',   NOW() - INTERVAL '13 days' + INTERVAL '11 hours', NOW() - INTERVAL '13 days' + INTERVAL '11 hours' + INTERVAL '8900 ms', 'success',         '@finance_hub',   'last_30_days','json', '{"include_media": false}',                       1, 8900,  480, 'json', NULL,                NULL),
  (101003, 'carol_news',   NOW() - INTERVAL '13 days' + INTERVAL '14 hours', NOW() - INTERVAL '13 days' + INTERVAL '14 hours' + INTERVAL '1200 ms', 'failed',          '@news_channel',  'last_7_days', 'csv',  '{}',                                             1, 1200,  NULL,'csv',  'CHANNEL_NOT_FOUND', 'Channel @news_channel not found or private'),
  (101004, 'dave_sports',  NOW() - INTERVAL '12 days' + INTERVAL '8 hours',  NOW() - INTERVAL '12 days' + INTERVAL '8 hours' + INTERVAL '6700 ms',  'success',         '@sports_live',   'all_time',    'csv',  '{"include_media": true, "max_images": 50}',       1, 6700,  390, 'csv',  NULL,                NULL),
  (101001, 'alice_dev',    NOW() - INTERVAL '12 days' + INTERVAL '15 hours', NOW() - INTERVAL '12 days' + INTERVAL '15 hours' + INTERVAL '3100 ms', 'success',         '@tech_daily',    'last_7_days', 'json', '{}',                                             1, 3100,  140, 'json', NULL,                NULL),
  (101005, 'eve_analyst',  NOW() - INTERVAL '11 days' + INTERVAL '10 hours', NOW() - INTERVAL '11 days' + INTERVAL '10 hours' + INTERVAL '15200 ms','partial_success', '@crypto_news',   'all_time',    'csv',  '{"include_reactions": true}',                    2, 15200, 300, 'csv',  'PARTIAL_TIMEOUT',   'Some posts could not be fetched due to rate limit'),
  (101002, 'bob_trader',   NOW() - INTERVAL '11 days' + INTERVAL '13 hours', NOW() - INTERVAL '11 days' + INTERVAL '13 hours' + INTERVAL '5500 ms', 'success',         '@finance_hub',   'last_7_days', 'csv',  '{}',                                             1, 5500,  260, 'csv',  NULL,                NULL),
  (101006, 'frank_crypto', NOW() - INTERVAL '10 days' + INTERVAL '9 hours',  NOW() - INTERVAL '10 days' + INTERVAL '9 hours' + INTERVAL '22000 ms', 'success',         '@crypto_news',   'last_30_days','json', '{"include_media": true, "include_reactions": true}',1,22000, 490, 'json', NULL,                NULL),
  (101003, 'carol_news',   NOW() - INTERVAL '10 days' + INTERVAL '12 hours', NULL,                                                                  'failed',          '@tech_daily',    'last_7_days', 'csv',  '{}',                                             2, NULL,  NULL, NULL, 'RATE_LIMITED',      'Too many requests, please try again later'),
  (101007, 'grace_bot',    NOW() - INTERVAL '10 days' + INTERVAL '16 hours', NOW() - INTERVAL '10 days' + INTERVAL '16 hours' + INTERVAL '4800 ms', 'success',         '@news_channel',  'last_7_days', 'json', '{}',                                             1, 4800,  185, 'json', NULL,                NULL),
  (101004, 'dave_sports',  NOW() - INTERVAL '9 days'  + INTERVAL '11 hours', NOW() - INTERVAL '9 days'  + INTERVAL '11 hours' + INTERVAL '7300 ms', 'success',         '@sports_live',   'last_7_days', 'csv',  '{"include_media": false}',                       1, 7300,  310, 'csv',  NULL,                NULL),
  (101005, 'eve_analyst',  NOW() - INTERVAL '9 days'  + INTERVAL '14 hours', NOW() - INTERVAL '9 days'  + INTERVAL '14 hours' + INTERVAL '2500 ms', 'cancelled',       '@finance_hub',   'last_30_days','csv',  '{}',                                             0, 2500,  NULL, NULL, 'USER_CANCELLED',    'Cancelled by user'),
  (101008, 'henry_trade',  NOW() - INTERVAL '8 days'  + INTERVAL '8 hours',  NOW() - INTERVAL '8 days'  + INTERVAL '8 hours' + INTERVAL '11000 ms', 'success',         '@finance_hub',   'last_30_days','json', '{"include_media": true}',                        1, 11000, 420, 'json', NULL,                NULL),
  (101001, 'alice_dev',    NOW() - INTERVAL '8 days'  + INTERVAL '10 hours', NOW() - INTERVAL '8 days'  + INTERVAL '10 hours' + INTERVAL '3800 ms', 'success',         '@crypto_news',   'last_7_days', 'csv',  '{}',                                             1, 3800,  175, 'csv',  NULL,                NULL),
  (101006, 'frank_crypto', NOW() - INTERVAL '8 days'  + INTERVAL '17 hours', NOW() - INTERVAL '8 days'  + INTERVAL '17 hours' + INTERVAL '9200 ms', 'partial_success', '@tech_daily',    'all_time',    'csv',  '{"include_reactions": true}',                    2, 9200,  220, 'csv',  'PARTIAL_TIMEOUT',   'Rate limit hit after 220 posts'),
  (101009, 'iris_market',  NOW() - INTERVAL '7 days'  + INTERVAL '9 hours',  NOW() - INTERVAL '7 days'  + INTERVAL '9 hours' + INTERVAL '6100 ms',  'success',         '@news_channel',  'last_7_days', 'json', '{}',                                             1, 6100,  245, 'json', NULL,                NULL),
  (101002, 'bob_trader',   NOW() - INTERVAL '7 days'  + INTERVAL '13 hours', NOW() - INTERVAL '7 days'  + INTERVAL '13 hours' + INTERVAL '18500 ms','success',         '@crypto_news',   'all_time',    'csv',  '{"include_media": true}',                        1, 18500, 500, 'csv',  NULL,                NULL),
  (101003, 'carol_news',   NOW() - INTERVAL '7 days'  + INTERVAL '15 hours', NULL,                                                                  'failed',          '@sports_live',   'last_7_days', 'csv',  '{}',                                             1, NULL,  NULL, NULL, 'CHANNEL_PRIVATE',   'Channel is private or restricted'),
  (101010, 'jack_data',    NOW() - INTERVAL '6 days'  + INTERVAL '10 hours', NOW() - INTERVAL '6 days'  + INTERVAL '10 hours' + INTERVAL '5400 ms', 'success',         '@tech_daily',    'last_7_days', 'json', '{"include_media": false}',                       1, 5400,  200, 'json', NULL,                NULL),
  (101007, 'grace_bot',    NOW() - INTERVAL '6 days'  + INTERVAL '12 hours', NOW() - INTERVAL '6 days'  + INTERVAL '12 hours' + INTERVAL '3200 ms', 'success',         '@finance_hub',   'last_7_days', 'csv',  '{}',                                             1, 3200,  150, 'csv',  NULL,                NULL),
  (101005, 'eve_analyst',  NOW() - INTERVAL '5 days'  + INTERVAL '9 hours',  NOW() - INTERVAL '5 days'  + INTERVAL '9 hours' + INTERVAL '24500 ms', 'success',         '@crypto_news',   'last_30_days','json', '{"include_reactions": true, "include_media": true}',1,24500,495, 'json', NULL,                NULL),
  (101008, 'henry_trade',  NOW() - INTERVAL '5 days'  + INTERVAL '14 hours', NOW() - INTERVAL '5 days'  + INTERVAL '14 hours' + INTERVAL '1800 ms', 'failed',          '@news_channel',  'last_7_days', 'csv',  '{}',                                             1, 1800,  NULL, NULL, 'PARSE_ERROR',       'Unexpected response format from Telegram'),
  (101001, 'alice_dev',    NOW() - INTERVAL '4 days'  + INTERVAL '11 hours', NOW() - INTERVAL '4 days'  + INTERVAL '11 hours' + INTERVAL '7800 ms', 'success',         '@sports_live',   'last_30_days','csv',  '{"include_media": true}',                        1, 7800,  360, 'csv',  NULL,                NULL),
  (101009, 'iris_market',  NOW() - INTERVAL '4 days'  + INTERVAL '13 hours', NOW() - INTERVAL '4 days'  + INTERVAL '13 hours' + INTERVAL '4100 ms', 'success',         '@finance_hub',   'last_7_days', 'json', '{}',                                             1, 4100,  190, 'json', NULL,                NULL),
  (101006, 'frank_crypto', NOW() - INTERVAL '3 days'  + INTERVAL '10 hours', NOW() - INTERVAL '3 days'  + INTERVAL '10 hours' + INTERVAL '12300 ms','success',         '@crypto_news',   'last_30_days','csv',  '{"include_reactions": true}',                    1, 12300, 435, 'csv',  NULL,                NULL),
  (101010, 'jack_data',    NOW() - INTERVAL '3 days'  + INTERVAL '15 hours', NULL,                                                                  'failed',          '@tech_daily',    'last_7_days', 'csv',  '{}',                                             2, NULL,  NULL, NULL, 'NETWORK_ERROR',     'Connection timeout after 3 retries'),
  (101004, 'dave_sports',  NOW() - INTERVAL '2 days'  + INTERVAL '8 hours',  NOW() - INTERVAL '2 days'  + INTERVAL '8 hours' + INTERVAL '8600 ms',  'success',         '@sports_live',   'all_time',    'json', '{"include_media": true}',                        1, 8600,  405, 'json', NULL,                NULL),
  (101002, 'bob_trader',   NOW() - INTERVAL '2 days'  + INTERVAL '16 hours', NOW() - INTERVAL '2 days'  + INTERVAL '16 hours' + INTERVAL '6300 ms', 'partial_success', '@finance_hub',   'last_30_days','csv',  '{}',                                             2, 6300,  280, 'csv',  'PARTIAL_TIMEOUT',   'Stopped after 280 rows due to timeout'),
  (101007, 'grace_bot',    NOW() - INTERVAL '1 day'   + INTERVAL '9 hours',  NOW() - INTERVAL '1 day'   + INTERVAL '9 hours' + INTERVAL '5100 ms',  'success',         '@news_channel',  'last_7_days', 'csv',  '{"include_media": false}',                       1, 5100,  230, 'csv',  NULL,                NULL),
  (101001, 'alice_dev',    NOW() - INTERVAL '1 day'   + INTERVAL '14 hours', NOW() - INTERVAL '1 day'   + INTERVAL '14 hours' + INTERVAL '3600 ms', 'success',         '@tech_daily',    'last_7_days', 'json', '{}',                                             1, 3600,  160, 'json', NULL,                NULL),
  (101005, 'eve_analyst',  NOW() - INTERVAL '2 hours',                        NULL,                                                                  'running',         '@crypto_news',   'last_30_days','csv',  '{"include_reactions": true}',                    1, NULL,  NULL, NULL, NULL,                NULL),
  (101003, 'carol_news',   NOW() - INTERVAL '30 minutes',                     NULL,                                                                  'queued',          '@finance_hub',   'last_7_days', 'json', '{}',                                             0, NULL,  NULL, NULL, NULL,                NULL);

-- Seed parsing_attempts linked to the sessions above
-- We reference sessions by order of insertion; use a subquery approach
INSERT INTO parsing_attempts (session_id, attempt_number, started_at, finished_at, status, duration_ms, error_code, error_message, meta)
SELECT s.id, 1,
  s.started_at,
  s.finished_at,
  CASE s.status
    WHEN 'success'         THEN 'success'::attempt_status
    WHEN 'failed'          THEN 'failed'::attempt_status
    WHEN 'partial_success' THEN 'success'::attempt_status
    WHEN 'cancelled'       THEN 'failed'::attempt_status
    WHEN 'running'         THEN 'running'::attempt_status
    ELSE 'running'::attempt_status
  END,
  s.parsing_duration_ms,
  s.error_code,
  s.error_message,
  jsonb_build_object('celery_task_id', 'task-' || s.id || '-1', 'worker', 'celery@worker1')
FROM parsing_sessions s;

-- Add second attempts for sessions that have attempts_count = 2
INSERT INTO parsing_attempts (session_id, attempt_number, started_at, finished_at, status, duration_ms, error_code, error_message, meta)
SELECT s.id, 2,
  s.started_at + INTERVAL '5 seconds',
  s.finished_at,
  CASE s.status
    WHEN 'success'         THEN 'success'::attempt_status
    WHEN 'partial_success' THEN 'success'::attempt_status
    ELSE 'failed'::attempt_status
  END,
  s.parsing_duration_ms,
  NULL,
  NULL,
  jsonb_build_object('celery_task_id', 'task-' || s.id || '-2', 'worker', 'celery@worker2')
FROM parsing_sessions s
WHERE s.attempts_count = 2;

-- Seed daily_stats for last 14 days
INSERT INTO daily_stats (stat_date, total_sessions, successful, failed, partial_success, cancelled, unique_users, avg_duration_ms, total_result_rows, top_channel)
VALUES
  (CURRENT_DATE - 13, 3, 2, 1, 0, 0, 3, 4767,  690,  '@crypto_news'),
  (CURRENT_DATE - 12, 2, 2, 0, 0, 0, 2, 4900,  530,  '@sports_live'),
  (CURRENT_DATE - 11, 2, 1, 1, 1, 0, 2, 10350, 560,  '@crypto_news'),
  (CURRENT_DATE - 10, 3, 2, 1, 0, 0, 3, 9333,  675,  '@crypto_news'),
  (CURRENT_DATE - 9,  2, 1, 0, 0, 1, 2, 4900,  310,  '@sports_live'),
  (CURRENT_DATE - 8,  3, 2, 0, 1, 0, 3, 8000,  815,  '@finance_hub'),
  (CURRENT_DATE - 7,  3, 2, 1, 0, 0, 3, 9533,  945,  '@crypto_news'),
  (CURRENT_DATE - 6,  2, 2, 0, 0, 0, 2, 4300,  350,  '@tech_daily'),
  (CURRENT_DATE - 5,  2, 1, 1, 0, 0, 2, 13150, 495,  '@crypto_news'),
  (CURRENT_DATE - 4,  2, 2, 0, 0, 0, 2, 5950,  550,  '@sports_live'),
  (CURRENT_DATE - 3,  2, 1, 1, 0, 0, 2, 6150,  435,  '@crypto_news'),
  (CURRENT_DATE - 2,  2, 1, 0, 1, 0, 2, 7450,  685,  '@sports_live'),
  (CURRENT_DATE - 1,  2, 2, 0, 0, 0, 2, 4350,  390,  '@news_channel'),
  (CURRENT_DATE,      2, 0, 0, 0, 0, 2, NULL,  0,    NULL)
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
  computed_at       = NOW();

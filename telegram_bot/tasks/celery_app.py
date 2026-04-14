"""
Celery application instance.

worker_prefetch_multiplier=1 is required:
  Each Celery worker process holds one Telethon client with its own session file.
  Session files cannot be safely used by multiple concurrent tasks in one process.
  One task per process at a time ensures clean session state.

task_acks_late=True:
  Task is not acknowledged until it completes.
  If the worker crashes mid-task, the task is re-queued on restart.
  Combined with max_retries=0 in parse_task, the task runs exactly once per attempt.
"""

from celery import Celery

from telegram_bot.config import settings

celery_app = Celery(
    "telegram_bot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "telegram_bot.tasks.parse_task",
        "telegram_bot.tasks.deliver_task",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_acks_late=True,
    # One task per worker process — required for Telethon session safety
    worker_prefetch_multiplier=1,
    # Route all tasks to the "parsing" queue
    task_routes={
        "telegram_bot.tasks.parse_task.run_parse_job": {"queue": "parsing"},
        "telegram_bot.tasks.deliver_task.deliver_result": {"queue": "parsing"},
    },
    # parse_task timeout: 5 minutes
    # deliver_task timeout: 60 seconds (set per-task via soft_time_limit)
    task_soft_time_limit=300,
    task_time_limit=360,
)

"""
FSM state groups for the parsing conversation flow.

IMPORTANT: FSM tracks UI dialog context only.
Job execution state lives in DB (ParseJob.status) — NOT in FSM.
The worker never writes to FSM.

State transitions:
  [idle]              → /start          → [idle, menu shown]
  [idle]              → "Start parsing" → check active job in DB
                                           → if found: show status, stay idle
                                           → if not:   WAITING_CHANNEL_LINK
  WAITING_CHANNEL_LINK → valid link     → WAITING_POST_COUNT
                       → invalid        → stay (error shown)
  WAITING_POST_COUNT   → 20|50|100      → CONFIRMING
  CONFIRMING           → "Start"        → job dispatched → FSM cleared to idle
                       → "Exit"         → idle
"""

from aiogram.fsm.state import State, StatesGroup


class ParseFlow(StatesGroup):
    WAITING_CHANNEL_LINK = State()   # Bot asked for channel link; user must provide one
    WAITING_POST_COUNT = State()     # Bot showed count options; waiting for selection
    CONFIRMING = State()             # Bot showed confirmation card; waiting for Start/Exit

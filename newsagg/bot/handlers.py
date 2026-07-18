"""Transport-agnostic Telegram command/callback/text handlers.

Placeholder — /start, /topics, /schedule, /brief, /help, the interest-picker
callback protocol (t:<slug>, t:done, c:<cadence>, h:<hour>), and free-text
RAG dispatch are implemented in Phase 3. This stub exists so
newsagg.bot.handlers is importable (used by both the poller and the
api/main.py webhook mode per the plan's "same handlers, different
transport" design).
"""


async def handle_update(api, update: dict) -> None:
    raise NotImplementedError("PHASE-3")

"""Thin async wrapper around the Telegram Bot API (ADR-2/ADR-9).

Placeholder — the real implementation (httpx-based get_updates/send_message/
edit_reply_markup/answer_callback, HTML parse_mode + html.escape everywhere)
lands in Phase 3. This stub exists so newsagg.bot.telegram_api is importable
for the package restructure and for other Phase-1-and-earlier modules to
reference the class name/shape.
"""


class TelegramAPI:
    def __init__(self, token: str):
        self.token = token

    async def get_updates(self, offset: int) -> list[dict]:
        raise NotImplementedError("PHASE-3")

    async def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> dict:
        raise NotImplementedError("PHASE-3")

    async def edit_reply_markup(self, chat_id: str, message_id: int, reply_markup: dict) -> dict:
        raise NotImplementedError("PHASE-3")

    async def answer_callback(self, callback_query_id: str, text: str = "") -> dict:
        raise NotImplementedError("PHASE-3")

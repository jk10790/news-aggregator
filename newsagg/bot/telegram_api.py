"""Thin async wrapper around the Telegram Bot API (ADR-2, ADR-9).

Long-polling (bot/poller.py) and the webhook endpoint (api/main.py) both
build one of these and hand it to newsagg.bot.handlers — same handler
logic, different transport.

parse_mode is always HTML (ADR-9 — three prior formatting hotfixes all
fought Markdown-mode parse failures). Callers are responsible for
html.escape()-ing every piece of dynamic text before it lands in a
message; this module never does implicit escaping since a caller may
legitimately want to send pre-built HTML (e.g. `<b>`, `<i>`, `<a href>`).
"""
import httpx

from newsagg import config


class TelegramAPI:
    def __init__(self, token: str):
        self.token = token
        self.base = f"https://api.telegram.org/bot{token}"
        self.client = httpx.AsyncClient(timeout=60)

    async def get_updates(self, offset: int) -> list[dict]:
        r = await self.client.get(
            f"{self.base}/getUpdates",
            params={
                "offset": offset,
                "timeout": config.TELEGRAM_POLL_TIMEOUT,
                "allowed_updates": '["message","callback_query"]',
            },
        )
        r.raise_for_status()
        return r.json()["result"]

    async def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> dict:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        r = await self.client.post(f"{self.base}/sendMessage", json=payload)
        r.raise_for_status()
        return r.json()

    async def edit_reply_markup(self, chat_id: str, message_id: int, reply_markup: dict) -> dict:
        r = await self.client.post(
            f"{self.base}/editMessageReplyMarkup",
            json={"chat_id": chat_id, "message_id": message_id, "reply_markup": reply_markup},
        )
        r.raise_for_status()
        return r.json()

    async def answer_callback(self, callback_query_id: str, text: str = "") -> dict:
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        r = await self.client.post(f"{self.base}/answerCallbackQuery", json=payload)
        r.raise_for_status()
        return r.json()

    async def aclose(self) -> None:
        await self.client.aclose()

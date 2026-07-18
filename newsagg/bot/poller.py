"""Long-poll (getUpdates) loop entry point (ADR-2 — one product bot, no
public URL/ngrok required for local dev). The FastAPI webhook endpoint
(newsagg/api/main.py) is kept as a deploy-time alternative; both call the
same newsagg.bot.handlers.handle_update().
"""
import asyncio
import logging

import httpx

from newsagg import config
from newsagg.bot import handlers
from newsagg.bot.telegram_api import TelegramAPI

logger = logging.getLogger(__name__)


async def run():
    api = TelegramAPI(config.TELEGRAM_BOT_TOKEN)
    offset = 0
    while True:
        try:
            updates = await api.get_updates(offset)
            for upd in updates:
                offset = upd["update_id"] + 1
                try:
                    await handlers.handle_update(api, upd)
                except Exception:
                    # Never let one bad update kill the poll loop.
                    logger.exception("update %s failed", upd.get("update_id"))
        except (httpx.HTTPError, asyncio.TimeoutError):
            logger.warning("Telegram getUpdates connection error, reconnecting shortly")
            await asyncio.sleep(3)


def main():
    """Entry point for the `newsagg-bot` console script."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    asyncio.run(run())


if __name__ == "__main__":
    main()

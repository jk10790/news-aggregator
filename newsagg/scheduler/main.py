"""Asyncio scheduler service (ADR-6 — one long-running, dependency-free
1-minute-tick loop instead of a workflow-orchestrator framework; hour-gated
brief delivery + daily 03:00 UTC retention cleanup).
"""
import asyncio
import datetime
import logging

from newsagg.processor.brief_engine import run_hour

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

TICK_SECONDS = 60


async def run():
    """One iteration per minute, forever. Every branch is individually
    guarded so a failure in one (e.g. run_hour blowing up because Chroma or
    the LLM gateway is down) never takes out the other or kills the loop.
    """
    logger.info("Scheduler started.")
    while True:
        try:
            now = datetime.datetime.now(datetime.timezone.utc)

            if now.minute == 0:
                try:
                    await run_hour(now)
                except Exception as e:  # noqa: BLE001 — scheduler must never die
                    logger.error("run_hour failed for %s: %s", now.isoformat(), e)

                if now.hour == 3:
                    try:
                        from newsagg.storage.cleanup import prune_expired
                        prune_expired()
                    except Exception as e:  # noqa: BLE001 — scheduler must never die
                        logger.error("prune_expired failed: %s", e)
        except Exception as e:  # noqa: BLE001 — belt-and-suspenders around the tick itself
            logger.error("Scheduler tick failed: %s", e)

        await asyncio.sleep(TICK_SECONDS)


def main():
    """Entry point for the `newsagg-scheduler` console script."""
    asyncio.run(run())


if __name__ == "__main__":
    main()

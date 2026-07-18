"""Asyncio scheduler service (ADR-6 — replaces Prefect; one long-running
1-minute-tick loop, hour-gated brief delivery + daily 03:00 UTC retention
cleanup). Full implementation: Phase 6.
"""
import asyncio


async def run():
    raise NotImplementedError("PHASE-6")


def main():
    """Entry point for the `newsagg-scheduler` console script."""
    asyncio.run(run())


if __name__ == "__main__":
    main()

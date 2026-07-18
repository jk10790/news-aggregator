"""Long-poll (getUpdates) loop entry point (ADR-2 — one product bot, no
public URL/ngrok required for local dev). Full implementation: Phase 3.
"""
import asyncio


async def run():
    raise NotImplementedError("PHASE-3")


def main():
    """Entry point for the `newsagg-bot` console script."""
    asyncio.run(run())


if __name__ == "__main__":
    main()

"""Topic-centric brief engine (ADR-5 — replaces processor/daily_brief.py).

One "topic module" per active topic per day, cached in Postgres
(TopicModule); per-user briefs are template-stitched from modules with
zero per-user LLM calls. Full implementation: Phase 6.
"""
from datetime import datetime


def active_interests(user, now: datetime) -> list[str]:
    raise NotImplementedError("PHASE-6")


def fetch_topic_articles(slug: str, now: datetime) -> list[dict]:
    raise NotImplementedError("PHASE-6")


async def build_topic_module(slug: str, date):
    raise NotImplementedError("PHASE-6")


def assemble_brief(user, modules):
    raise NotImplementedError("PHASE-6")


async def deliver(user, html_text: str):
    raise NotImplementedError("PHASE-6")


async def run_hour(now: datetime):
    raise NotImplementedError("PHASE-6")

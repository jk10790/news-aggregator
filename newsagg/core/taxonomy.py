"""Single source of truth for the fixed topic taxonomy (ADR-4).

Triage classifies articles into these slugs, Chroma stores per-topic
boolean metadata keyed off them, the Telegram interest picker renders
them, and the Observer maps free text onto them. No free-form topic
strings should appear anywhere else in metadata or interests.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Topic:
    slug: str      # stable id: used in DB, Chroma metadata key suffix, callback data
    label: str     # shown to users
    emoji: str


TAXONOMY: list[Topic] = [
    Topic("ai",          "AI & ML",             "\U0001f916"),
    Topic("cloud",       "Cloud & Infra",       "☁️"),
    Topic("security",    "Security",            "\U0001f510"),
    Topic("startups",    "Startups & VC",       "\U0001f680"),
    Topic("programming", "Programming",         "\U0001f4bb"),
    Topic("distsys",     "Distributed Systems", "\U0001f578️"),
    Topic("databases",   "Databases",           "\U0001f5c4️"),
    Topic("business",    "Business & Markets",  "\U0001f4c8"),
    Topic("science",     "Science",             "\U0001f52c"),
    Topic("sports",      "Sports",              "\U0001f3df️"),
    Topic("top",         "Top News",            "\U0001f30d"),  # pseudo-topic: importance >= 8, any category
]

SLUGS = {t.slug for t in TAXONOMY}
BY_SLUG = {t.slug: t for t in TAXONOMY}
# Slugs the triage LLM may assign (everything except the pseudo-topic):
CLASSIFIABLE = [t for t in TAXONOMY if t.slug != "top"]


def chroma_key(slug: str) -> str:
    return f"topic_{slug}"

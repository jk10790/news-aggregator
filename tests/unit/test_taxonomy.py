"""Unit tests for newsagg.core.taxonomy (ADR-4) and the interest keyboard
builder in newsagg.bot.handlers (§3b of the plan).
"""
from newsagg.bot.handlers import interest_keyboard
from newsagg.core.taxonomy import TAXONOMY, chroma_key


def test_slugs_are_unique():
    slugs = [t.slug for t in TAXONOMY]
    assert len(slugs) == len(set(slugs))


def test_chroma_key_prefixes_slug():
    assert chroma_key("ai") == "topic_ai"


def test_keyboard_shape_six_topic_rows_plus_done():
    # 11 topics at 2 columns -> 5 full rows + 1 row of 1 = 6 topic rows,
    # plus a trailing Done row.
    assert len(TAXONOMY) == 11

    keyboard = interest_keyboard(selected=set())
    rows = keyboard["inline_keyboard"]

    assert len(rows) == 7
    topic_rows, done_row = rows[:-1], rows[-1]
    assert len(topic_rows) == 6
    for row in topic_rows[:-1]:
        assert len(row) == 2
    assert len(topic_rows[-1]) == 1  # 11th topic, odd one out

    assert len(done_row) == 1
    assert done_row[0]["callback_data"] == "t:done"

    total_topic_buttons = sum(len(row) for row in topic_rows)
    assert total_topic_buttons == len(TAXONOMY)


def test_checkmark_prefix_only_on_selected():
    keyboard = interest_keyboard(selected={"ai"})
    buttons = [btn for row in keyboard["inline_keyboard"][:-1] for btn in row]

    ai_button = next(b for b in buttons if b["callback_data"] == "t:ai")
    other_buttons = [b for b in buttons if b["callback_data"] != "t:ai"]

    assert ai_button["text"].startswith("✅ ")
    assert all(not b["text"].startswith("✅ ") for b in other_buttons)


def test_no_selection_has_no_checkmarks():
    keyboard = interest_keyboard(selected=set())
    buttons = [btn for row in keyboard["inline_keyboard"][:-1] for btn in row]
    assert all("✅" not in b["text"] for b in buttons)

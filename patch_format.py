import sys

with open("processor/daily_brief.py", "r") as f:
    content = f.read()

# 1. Insert _plain() helper before deliver_brief
old_fn = "\ndef deliver_brief(phone_number: str, daily_brief_data: DailyBrief):"
new_fn = """
import re as _re

def _plain(text: str) -> str:
    # Strip any LLM-generated markdown so Telegram formatting stays clean
    text = _re.sub(r'\\*\\*(.*?)\\*\\*', r'\\1', text)   # **bold** -> plain
    text = _re.sub(r'\\*(.*?)\\*', r'\\1', text)           # *italic* -> plain
    text = _re.sub(r'__(.*?)__', r'\\1', text)             # __bold__ -> plain
    text = _re.sub(r'_(.*?)_', r'\\1', text)               # _italic_ -> plain
    text = _re.sub(r'`(.*?)`', r'\\1', text)               # `code` -> plain
    text = _re.sub(r'\\[(.*?)\\]\\(.*?\\)', r'\\1', text)  # [text](url) -> text
    return text.strip()

def deliver_brief(phone_number: str, daily_brief_data: DailyBrief):"""

if old_fn in content:
    content = content.replace(old_fn, new_fn, 1)
    print("Inserted _plain() helper")
else:
    print("Could not find deliver_brief definition")
    sys.exit(1)

# 2. Use _plain() around dynamic text in the formatter
content = content.replace(
    'body += f"_{daily_brief_data.headline_summary}_\\n\\n"',
    'body += f"_{_plain(daily_brief_data.headline_summary)}_\\n\\n"'
)
content = content.replace(
    'body += f"📰 *{article.title}*\\n"',
    'body += f"📰 *{_plain(article.title)}*\\n"'
)
content = content.replace(
    'body += f"  ✦ _{insight}_\\n"',
    'body += f"  ✦ _{_plain(insight)}_\\n"'
)

with open("processor/daily_brief.py", "w") as f:
    f.write(content)

print("Patched deliver_brief to sanitize LLM markdown")

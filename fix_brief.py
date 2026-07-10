with open("processor/daily_brief.py", "r") as f:
    lines = f.readlines()

new_lines = []
skip = False
for line in lines:
    if "body = f\"*{daily_brief_data.date} Briefing*\\n{daily_brief_data.headline_summary}\\n\\n\"" in line:
        skip = True
        new_lines.append('    body = f"*{daily_brief_data.date} Briefing*\\n_{daily_brief_data.headline_summary}_\\n\\n"\n')
        new_lines.append('    for category in daily_brief_data.categories:\n')
        new_lines.append('        body += f"*{category.name}*\\n"\n')
        new_lines.append('        for article in category.articles:\n')
        new_lines.append('            body += f"🔹 [{article.title}]({article.url})\\n"\n')
        new_lines.append('            for insight in article.key_insights:\n')
        new_lines.append('                body += f"   • {insight}\\n"\n')
        new_lines.append('            body += "\\n"\n')
        new_lines.append('    body += "Reply to this message to ask questions about today\'s news!"\n')
    elif skip and "body += \"Reply to this message" in line:
        skip = False
    elif not skip:
        new_lines.append(line)

with open("processor/daily_brief.py", "w") as f:
    f.writelines(new_lines)

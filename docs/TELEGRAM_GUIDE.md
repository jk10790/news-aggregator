# Telegram Bot Integration Guide

The Agentic Newsroom now supports **Telegram** as a 100% free alternative to Twilio WhatsApp. This allows you to deploy conversational RAG and daily briefs without incurring per-message fees.

---

## 1. How it Works
1. **Inbound (User -> Bot):**
   - You send a message to your Telegram Bot.
   - Telegram forwards the JSON payload to our webhook (`POST /webhook/telegram`).
   - The API extracts your Telegram `chat_id`, treats it as your unique identifier, and runs the Observer + RAG pipeline.
2. **Outbound (Bot -> User):**
   - The RAG system or `daily_brief.py` script uses simple REST calls (`httpx.post`) to the official Telegram Bot API to send Markdown-formatted messages back to your `chat_id`.

---

## 2. Setting Up Your Telegram Bot
Telegram makes bot creation incredibly easy via "BotFather".

1. **Create the Bot:**
   - Open Telegram and search for the user `@BotFather`.
   - Send the command `/newbot` and follow the prompts to name your bot.
   - BotFather will reply with an HTTP API Token (e.g., `123456789:ABCdefGHIjklMNOpqrSTUvwxYZ`). **Save this token.**

2. **Configure Your Environment:**
   Open your `.env` file and set the messaging provider to `telegram` along with your token:
   ```env
   MESSAGING_PROVIDER=telegram
   TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrSTUvwxYZ
   ```

---

## 3. Configuring the Webhook
Telegram needs to know where to send incoming messages. Since you are running FastAPI locally on port 8050, you must expose it to the internet using a tool like `ngrok`.

1. Run: `ngrok http 8050`
2. Note your forwarding URL (e.g., `https://a1b2c3d4.ngrok-free.app`).
3. **Register the Webhook with Telegram:**
   Open your web browser or terminal and hit this URL to tell Telegram where your API lives:
   ```bash
   curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/setWebhook?url=https://a1b2c3d4.ngrok-free.app/webhook/telegram"
   ```
   You should receive a JSON response: `{"ok":true,"result":true,"description":"Webhook was set"}`.

---

## 4. Pricing & Costs

Unlike Twilio WhatsApp, the Telegram Bot API is **100% Free**.

- **Service Conversations (RAG Queries):** $0.00
- **Proactive Messages (Daily Briefs):** $0.00
- **Daily Volume Limits:** Highly generous. You can send thousands of messages per second for free.

If you are deploying this for a startup MVP or personal use, Telegram is architecturally the best choice to keep operating costs at absolute zero.

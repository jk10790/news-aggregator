# Twilio WhatsApp Integration Guide

The Agentic Newsroom uses the **Twilio API for WhatsApp** to deliver hyper-personalized daily briefs and handle real-time conversational RAG queries.

---

## 1. How it Works
The integration consists of two primary flows:

1. **Inbound Messages (Conversational RAG & Observer):**
   - When a user sends a WhatsApp message, Twilio hits our FastAPI webhook (`POST /webhook/twilio`).
   - The API triggers the LangGraph CRAG flow to generate an instant response (TwiML) and simultaneously fires the background Observer Agent to extract and update the user's interests.
2. **Outbound Messages (Daily Briefs):**
   - Once the Prefect `daily_brief.py` script finishes compiling the Map-Reduced newsletter, it uses the Twilio Python SDK (`twilio_client.messages.create`) to push the final brief directly to the user's WhatsApp.

---

## 2. Setting Up the Twilio WhatsApp Sandbox
To test this locally without waiting for Meta to approve a WhatsApp Business Account, you must use the Twilio Sandbox.

1. **Create a Twilio Account:** Sign up at [Twilio.com](https://www.twilio.com/).
2. **Activate the Sandbox:** Navigate to **Messaging > Try it out > Send a WhatsApp message**.
3. **Join the Sandbox:** Twilio will provide a Sandbox Phone Number (e.g., `+14155238886`) and a join code (e.g., `join abstract-lemon`). Send that code from your personal WhatsApp to the Sandbox number to link your device.

---

## 3. Configuring Local Environment
Once you have your account, grab your credentials from the Twilio Console homepage and update your `.env` file:

```env
TWILIO_ACCOUNT_SID=ACXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
TWILIO_AUTH_TOKEN=your_auth_token_here
TWILIO_WHATSAPP_SENDER=whatsapp:+14155238886
```

### Localhost Webhook Testing (ngrok)
Twilio needs a public URL to send incoming messages to. Since you are running FastAPI locally on port 8050, you must use a tunneling service like `ngrok`:

1. Run: `ngrok http 8050`
2. Copy the generated Forwarding URL (e.g., `https://a1b2c3d4.ngrok-free.app`).
3. Go to your Twilio WhatsApp Sandbox settings and paste that URL into the **"When a message comes in"** webhook field, appending the endpoint: `https://a1b2c3d4.ngrok-free.app/webhook/twilio`.

---

## 4. Pricing & Costs

Twilio prices WhatsApp messages using a **Conversation-Based Pricing** model. A "conversation" is a 24-hour window that begins when the first message is delivered.

*Note: Pricing varies slightly by region, but below are the standard North American estimates.*

### The Sandbox Phase (Development)
- **Cost:** $0.00
- The WhatsApp Sandbox is **free** for development and testing.

### Production Phase (Live WhatsApp Business API)
Once you register an official WhatsApp Business Profile:

1. **Service Conversations (User-Initiated):**
   - When a user texts the bot to ask a question (RAG Query), a 24-hour window opens.
   - **Cost:** ~$0.0088 per 24-hour conversation.
   - You can exchange unlimited messages with the user during that 24-hour window for no additional cost.

2. **Marketing/Utility Conversations (Business-Initiated):**
   - When the server pushes the proactive Daily Brief in the morning, it triggers a utility/marketing conversation.
   - **Cost:** ~$0.015 to $0.025 per 24-hour conversation (depending on the template category).

**Cost Estimation Example:**
If you have 100 users, and you send them exactly 1 Daily Brief every morning:
- 100 users × $0.015 = **$1.50 per day.**
- **Monthly Cost:** ~$45.00 / month.

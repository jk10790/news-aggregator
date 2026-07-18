# Roadmap — future hooks (out of scope for v1)

These are explicitly **not implemented** in v1 (see `OVERHAUL_PLAN.md` ADR-14
— "ship the promise first"). Each one has a concrete hook point already in
place from the overhaul so it can be added later without re-architecting.
Future proposals belong here, in the repo, rather than in ad hoc agent
brain/notes folders.

- [ ] **Real-time alerts (premium tier).** Hook: `storage/consumer.py`,
      right after `store_article`'s upsert — if `importance_score >= 8`,
      look up users subscribed to the article's topic slugs with a new
      `delivery_cadence` value of `real-time`, and call `deliver()`
      immediately instead of waiting for the next scheduled brief.

- [ ] **Web dashboard.** Hook: a `/start <payload>` deep-link branch in
      `bot/handlers.py` (Telegram supports `tg://resolve?domain=<bot>&start=<token>`)
      to bind a web session to a `telegram_chat_id`; a JWT + preferences API
      layered over the existing `users`/`interests` tables — no schema
      changes needed to add read/write endpoints for topics/schedule.

- [ ] **Second delivery channel (e.g. WhatsApp).** Hook: implement the same
      `deliver(user, html_text)` signature `brief_engine.deliver` already
      uses in a new `bot/whatsapp_api.py`, and add a `channel` column to
      `users` so `run_hour` can dispatch by channel. `handlers.py`'s logic
      is already transport-agnostic (it takes an API-shaped object, not a
      concrete client), so a second channel is a new adapter module, not a
      rewrite.

- [ ] **Local timezone scheduling.** Hook: `users.timezone` is already
      stored (IANA name) but unused for conversion — `brief_engine._is_due`
      currently compares `delivery_hour_utc` directly against UTC wall
      clock hour. Converting `now` to the user's local time before that
      comparison is a self-contained change to `_is_due` alone.

- [ ] **Observability (OTel spans + token counters).** Hook:
      `newsagg/core/llm.complete` is the single choke point for every LLM
      call in the codebase (ADR-3) — wrap it once with OpenTelemetry spans
      and token/cost counters and every call site gets instrumented for
      free. Add an `-sdk`-style OTel dependency when this is picked up (it
      was deliberately left out of `pyproject.toml` in Phase 0/1 as a
      no-op dependency).

- [ ] **AWS migration.** Deferred until the local v1 architecture has run
      clean for a few weeks. See `docs/AWS_NORTH_STAR.md` for the
      previously-drafted enterprise/cloud target (Kinesis, OpenSearch,
      Bedrock) — it predates this overhaul and will need reconciling with
      the ADRs in `OVERHAUL_PLAN.md` (in particular ADR-3's Taut gateway
      and ADR-7's Postgres-via-Alembic-only rule) before being picked back
      up.

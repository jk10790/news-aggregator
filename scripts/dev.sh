#!/usr/bin/env bash
# Local dev launcher (Phase 8). Brings up docker-compose infra (redpanda,
# chroma, postgres, taut), runs migrations, then launches all five
# newsagg entry points in the foreground. Ctrl-C kills everything.
#
# Requires: docker compose, `pip install -e ".[test]"` already done, and a
# .env with at least TELEGRAM_BOT_TOKEN + GEMINI_API_KEY.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "==> Starting docker-compose infra (redpanda, chroma, postgres, taut)..."
docker compose up -d

echo "==> Running Alembic migrations..."
alembic upgrade head

trap 'echo "==> Shutting down..."; kill 0' EXIT

echo "==> Launching newsagg services (api, triage, storage, bot, scheduler)..."
newsagg-api &
newsagg-triage &
newsagg-storage &
newsagg-bot &
newsagg-scheduler &

wait

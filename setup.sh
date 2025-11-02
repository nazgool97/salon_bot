#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

log() { printf "\033[1;34m[setup]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*"; }
err()  { printf "\033[1;31m[error]\033[0m %s\n" "$*"; }

# Detect compose command (Docker plugin preferred)
detect_compose() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
  else
    echo ""
  fi
}

# Preflight: docker present
if ! command -v docker >/dev/null 2>&1; then
  err "Docker is not installed or not in PATH. Please install Docker and retry."
  exit 1
fi

COMPOSE_CMD="$(detect_compose)"
if [ -z "$COMPOSE_CMD" ]; then
  err "Neither 'docker compose' nor 'docker-compose' found. Please install Docker Compose."
  exit 1
fi

# Bootstrap .env if missing
if [ ! -f .env ]; then
  log ".env not found. Creating from template..."
  if [ -f .env.example ]; then
    cp .env.example .env
  else
    cat > .env <<'EOF'
# --- Telegram ---
BOT_TOKEN= # paste from BotFather

# --- Database (used by docker-compose and the app) ---
DB_USER=app_user
DB_PASSWORD=change_me
DB_NAME=booking_app
DB_HOST=db
DB_PORT=5432
# App connection URL (compose uses db host in the Docker network)
DATABASE_URL=postgres://app_user:change_me@db:5432/booking_app

# --- Admins ---
ADMIN_IDS= # comma-separated Telegram IDs

# --- Language ---
BOT_LANGUAGE=en # uk | ru | en

# --- Contacts ---
CONTACT_PHONE=
CONTACT_INSTAGRAM=
CONTACT_ADDRESS=

# --- Payments (optional) ---
TELEGRAM_PAYMENT_PROVIDER_TOKEN=

# --- Booking rules ---
RESERVATION_HOLD_MINUTES=15
CLIENT_RESCHEDULE_LOCK_HOURS=3
CLIENT_CANCEL_LOCK_HOURS=3

# --- Demo seed ---
RUN_BOOTSTRAP=0
EOF
  fi
  warn "Created .env from template. Please review and edit tokens/URLs if needed."
fi

# Load .env values (best-effort)
set -a
. ./.env || true
set +a

# Validate critical env
FAILED=0
if [ -z "${BOT_TOKEN:-}" ] || [ "${BOT_TOKEN}" = "your_bot_token_here" ]; then
  err "BOT_TOKEN is missing in .env (get it from @BotFather)."
  FAILED=1
fi
if [ -z "${DATABASE_URL:-}" ]; then
  err "DATABASE_URL is missing in .env."
  FAILED=1
fi

# Non-fatal warnings
if [ -z "${TELEGRAM_PAYMENT_PROVIDER_TOKEN:-}" ]; then
  warn "TELEGRAM_PAYMENT_PROVIDER_TOKEN is empty — online payments will be disabled."
fi

case "${DATABASE_URL:-}" in
  *change_me*) warn "DATABASE_URL contains default credentials ('change_me'). Consider changing for security." ;;
esac

if [ "$FAILED" -eq 1 ]; then
  err "Fix the errors above in .env and rerun this script."
  exit 1
fi

log "Starting Docker services (db, migrations, bot)..."
$COMPOSE_CMD up --build

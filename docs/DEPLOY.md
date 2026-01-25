# Deployment Guide

## Quick Start (Docker Compose)

```bash
# 1) Clone
git clone https://github.com/nazgool97/salon_bot.git
cd salon_bot

# 2) Configure
cp .env.example .env

# 3) Run
docker-compose up -d
```

- Services: bot, PostgreSQL, workers.
- Inspect logs to verify FSM, bookings, reminders.
- Adjust `.env` for timezone, currency, policies, payment keys.

## Environment Configuration

- `BOT_TOKEN`: Main Telegram bot token issued by BotFather. Required for the bot to start.
- `DATABASE_URL=postgresql+asyncpg://app_user:change_me@db:5432/booking_app`: Connection string for the database (PostgreSQL + asyncpg).
- `TELEGRAM_PAYMENT_PROVIDER_TOKEN`: Telegram payment provider token (issued via BotFather).
- `ADMIN_IDS`: List of Telegram IDs of admins who receive notifications.
- `DEFAULT_LANGUAGE`: Default interface language (e.g., uk, en).
- `DEFAULT_CURRENCY`: Default currency for prices and payments.
- `LOCAL_TIMEZONE`: Server timezone.
- `BUSINESS_TIMEZONE=Europe/Kyiv`: Business timezone (salon), used for client-facing times.
- `SETTINGS_CACHE_TTL_SECONDS`: Settings cache time-to-live in seconds.
- `PAGINATION_PAGE_SIZE`: Number of items per page for pagination.
- `TWA_WEBAPP_URL`: Public HTTPS URL of the Telegram Mini App (WebApp) frontend. Must be accessible from Telegram clients.
- `TWA_ALLOW_ALL_ORIGINS`: Enables wildcard CORS for the WebApp API. Set to true only for development; keep false in production.


## Migrations
- Alembic manages schema.
- Run before each deploy: `alembic upgrade head` (or via your entrypoint).
- Never skip migrations between versions.

## Production Notes
- Keep all timestamps in UTC internally; render local time in handlers.
- Monitor worker health (reminders, hold cleanup).
- Back up PostgreSQL regularly; test restores.

## Security
- Principle of least privilege for DB user.
- Protect secrets; do not log payment payloads.
- HTTPS for webhooks if used; otherwise long polling is acceptable for dev.

## Scaling
- Handlers are stateless; you can run multiple replicas.
- Workers are idempotent; safe to run multiple replicas.
- Advisory locks keep booking writes safe under concurrency.

## Troubleshooting
- Booking not confirmed: check payment status and hold timeout worker.
- Duplicate reminders: verify idempotency keys and worker logs.
- Wrong time: verify `BUSINESS_TZ` and rendering layer.
- Admin access issues: check role assignment in services.

## Telegram Mini App Setup (Client UI)

The project includes a built-in Telegram Mini App for client bookings.

### Required Environment Variable


TWA_WEBAPP_URL=https://your-domain.com


This URL must be publicly accessible and served over HTTPS.

### How It Works
- The bot provides a button that opens the Mini App.
- The Mini App communicates with the backend API.
- Clients can book and manage visits without chat commands.

### Production Notes
- Use a domain (e.g. salon.example.com)
- Configure HTTPS (nginx + certbot recommended)
- Make sure ports for frontend and API are exposed

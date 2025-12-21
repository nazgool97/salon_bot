# Telegram Salon Booking Bot (Open Core)

A self-hosted Telegram booking system for beauty salons and small service businesses.

Clients book appointments, masters manage schedules, and owners see analytics — all inside Telegram.

This repository contains the open-core version of the project. The production-ready edition with support and updates is available separately.

👉 Get the full version: https://ko-fi.com/s/937c0881d1

## What is Open Core here?

This repository provides:

- the core booking logic
- Telegram bot architecture
- database schema & migrations
- Docker-based setup

The commercial version includes:

- guided setup
- long-term support
- stable updates
- production usage license

This approach keeps the project transparent while allowing sustainable development.

## What problem does it solve?

Many salons still manage bookings using:

- Telegram chats
- spreadsheets
- manual confirmations

This leads to:

- missed messages
- double bookings
- extra admin work

This bot automates the booking flow while staying simple and human-friendly.

## Who is this for?

Designed for:

- Beauty salons
- Barbershops
- Solo masters
- Small local service businesses

A good fit if you:

- already communicate with clients via Telegram
- want to reduce manual work
- prefer self-hosted tools
- don’t want subscriptions or SaaS platforms

## How it works

**Clients**

1) Choose service
2) Choose master
3) Pick date & time
4) Confirm booking

**Masters**

- View personal schedule
- See upcoming appointments
- Manage availability

**Owner / Admin**

- Full booking overview
- Manage services and masters
- Basic analytics and stats

Everything works directly inside Telegram.

## Features

- Telegram-native UX
- Client self-booking
- Multi-master support
- Admin panel
- Analytics overview
- Docker-based deployment
- PostgreSQL database
- Self-hosted (you own your data)

## Project structure

```
bot/
├── app/
│   ├── core        # DB, logging, notifications
│   ├── domain      # Business models
│   ├── services    # Booking logic
│   ├── telegram    # Handlers & keyboards
│   └── workers     # Reminders & background jobs
├── migrations      # Alembic migrations
docker-compose.yml
Dockerfile
```

The codebase is structured for clarity and long-term maintenance.

## Running locally (for evaluation)

```bash
cp .env.example .env
docker compose up -d
```

This setup is intended for evaluation and development.

For production use, please refer to the commercial edition.

## License & usage

This repository is provided under an open-core model. You are free to:

- explore the code
- run it locally
- learn from the architecture

Commercial and production usage requires a paid license. Support, updates, and long-term maintenance are included in the paid version.

## Support

Friendly support is available with the commercial edition:

- help with setup
- usage questions
- updates and fixes

Custom development and feature requests are not included.

## Philosophy

I build tools for real businesses.

- No overengineering.
- No unnecessary complexity.
- Just practical automation that saves time and reduces stress.

If your business already lives in Telegram — this bot was built for you.

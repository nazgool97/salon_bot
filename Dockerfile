FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Копируем всё приложение
COPY . .

# ВАЖНО: копируем собранный фронт в образ
COPY webapp/dist /app/web

ENV PYTHONPATH=/app

CMD ["python", "-m", "bot.app.run_bot"]

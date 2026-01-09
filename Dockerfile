# ---------------------------------------
# Этап 1: Сборка фронтенда (Node.js)
# ---------------------------------------
FROM node:18-alpine as builder

WORKDIR /app_front

# Копируем package.json и устанавливаем зависимости
COPY webapp/package*.json ./
RUN npm install

# Копируем исходный код фронтенда и собираем его
COPY webapp/ .
# Эта команда создаст папку dist с готовыми файлами
RUN npm run build 


# ---------------------------------------
# Этап 2: Сборка основного образа (Python)
# ---------------------------------------
FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Копируем код бота
COPY . .

# !!! ГЛАВНОЕ: Копируем собранный фронтенд из первого этапа (builder)
# Мы берем папку dist из этапа 'builder' и кладем её в /app/web
COPY --from=builder /app_front/dist /app/web

ENV PYTHONPATH=/app

# Запускаем бота
CMD ["python", "-m", "bot.app.run_bot"]
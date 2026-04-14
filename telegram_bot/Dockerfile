FROM python:3.11-slim

# Build deps for psycopg2 and Telethon
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies before copying source for better layer caching
COPY telegram_bot/requirements.txt /app/telegram_bot/requirements.txt
RUN pip install --no-cache-dir -r telegram_bot/requirements.txt

# Copy bot source
COPY telegram_bot/ /app/telegram_bot/

# Alembic ini lives at project root
COPY alembic.ini /app/alembic.ini

# Ensure telegram_bot is importable as a package from /app
ENV PYTHONPATH=/app

# Sessions directory (Telethon .session files mounted as a Docker volume)
RUN mkdir -p /app/sessions

CMD ["python", "-m", "telegram_bot.main"]

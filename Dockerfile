FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Kyiv

WORKDIR /app

# Install tzdata for timezone handling
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata && \
    rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy bot sources (only necessary files)
COPY telegram_bot.py /app/
COPY scheduler.py /app/
COPY dialog_manager.py /app/
COPY ui.py /app/
COPY templates/ /app/templates/
COPY supabase_migrations/ /app/supabase_migrations/
COPY create_test_appointment.sql /app/
COPY README.md /app/

# Run bot (long polling)
CMD ["python", "telegram_bot.py"]

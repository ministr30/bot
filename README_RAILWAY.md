# Deploy Telegram Bot to Railway

## 1) Variables (Railway → Settings → Variables)
- TELEGRAM_BOT_TOKEN: your Telegram bot token
- SUPABASE_URL: https://<project>.supabase.co
- SUPABASE_KEY: anon or service key
- TZ: Europe/Kyiv (optional; Dockerfile sets TZ)

## 2) Service
- Source: GitHub repo
- Detected: Dockerfile at repo root
- Start command: from Dockerfile (python telegram_bot.py)

## 3) Health & logs
- Bot uses long polling. No web port/listen required.
- Check Logs tab for "🚀 Запуск Telegram бота..."

## 4) Local test
```
python -m venv venv
# Windows
venv\Scripts\activate
# macOS/Linux
# source venv/bin/activate
pip install -r requirements.txt
set TELEGRAM_BOT_TOKEN=...  # PowerShell: $env:TELEGRAM_BOT_TOKEN="..."
set SUPABASE_URL=...
set SUPABASE_KEY=...
python telegram_bot.py
```

## Notes
- Timezone conversions handled via ZoneInfo("Europe/Kyiv").
- Supabase access uses PostgREST via python-supabase client.

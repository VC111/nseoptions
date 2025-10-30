# NIFTY CE/PE Option Chain Telegram Bot

This repository provides a Telegram bot that fetches NIFTY CE/PE OI & LTP data (example: from Quantsapp) and posts updates to your Telegram channel.

## 🚀 Deployment Options
### 1️⃣ Run Locally
```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
playwright install
python bt5.py
```

### 2️⃣ Deploy on Render.com
1. Push this repo to GitHub.
2. Go to [Render.com](https://render.com) → "New" → "Web Service" or "Background Worker".
3. Connect your GitHub repo.
4. Render reads the **Procfile** and **render.yaml** automatically.
5. Add these Environment Variables under "Environment" tab:
   - `TELEGRAM_TOKEN`: your Telegram bot token.
   - `TELEGRAM_CHAT_ID`: your channel or group ID (e.g., `@nseopn`).
6. Deploy!

### 🧩 File Overview
- `bt5.py` — main bot script.
- `requirements.txt` — Python dependencies.
- `Procfile` — tells Render to run `python bt5.py`.
- `render.yaml` — defines Render environment.
- `.gitignore`, `LICENSE`, `README.md` — standard project files.

### 🧠 Notes
- Never hardcode your Telegram credentials; use environment variables.
- Works best on Render's **Background Worker** plan (free tier works).

License: MIT

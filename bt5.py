import os
#!/usr/bin/env python3
"""
bt5.py - Quantsapp CE/PE scraper that posts ΔOI and ΔLTP to Telegram.
Drop-in: update QUANTSAPP_URL, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, and if needed adjust IDX mapping.
"""

import os
import json
import asyncio
from datetime import datetime, time as dt_time
from playwright.async_api import async_playwright
from telegram import Bot
import logging

# ---------- CONFIG ----------
QUANTSAPP_URL = "https://web.quantsapp.com/option-chain"  # <-- set correct page / URL you use
CACHE_FILE = "last_oi.json"
TELEGRAM_TOKEN = "8438185244:AAGt75e741i4XBsS14EiZAQS4VUZVV1w3RU"
TELEGRAM_CHAT_ID = "@nseopn"  # int or str
FETCH_INTERVAL_SECONDS = 900  # when running continuously
RUN_DURING_MARKET_HOURS = True  # set False to run 24/7
MARKET_START = dt_time(9, 15)
MARKET_END = dt_time(15, 30)
# Column indices for the Quantsapp table (0-based). Adjust if your table layout differs.
# Typical mapping: [.., ce_oi, ce_ltp, strike, pe_ltp, pe_oi, ..] but confirm on your page.
IDX = {
    "strike": 6,
    "ce_oi": 3,
    "ce_ltp": 5,
    "pe_ltp": 7,
    "pe_oi": 9,
}
# ----------------------------

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("bt5")

# Telegram bot (synchronous send wrapped via asyncio.to_thread)
bot = Bot(token=TELEGRAM_TOKEN)


# ---------- Parsing helpers ----------
def parse_num_oi(s):
    """Parse OI-like strings to integer (supports K, L, CR, commas)."""
    if s is None:
        return 0
    t = str(s).strip().upper().replace(",", "")
    if t in ("", "-", "--"):
        return 0
    try:
        # Common suffixes
        if t.endswith("CR"):
            return int(float(t[:-2]) * 1e7)
        if t.endswith("L"):
            return int(float(t[:-1]) * 1e5)
        if t.endswith("K"):
            return int(float(t[:-1]) * 1e3)
        # plain number
        return int(float(t))
    except Exception:
        # fallback: extract digits and dot then to float
        cleaned = "".join(ch for ch in t if (ch.isdigit() or ch in ".-"))
        try:
            return int(float(cleaned)) if cleaned else 0
        except Exception:
            return 0


def parse_ltp_value(s):
    """Parse LTP display strings into float. Safe fallback 0.0."""
    if s is None:
        return 0.0
    t = str(s).strip()
    if t in ("", "-", "--"):
        return 0.0
    try:
        return float(t.replace(",", ""))
    except Exception:
        cleaned = "".join(ch for ch in t if (ch.isdigit() or ch in ".-"))
        try:
            return float(cleaned) if cleaned else 0.0
        except Exception:
            return 0.0


def human_fmt(n):
    """Readable OI formatting without sign (for display)."""
    try:
        n = float(n)
    except Exception:
        return str(n)
    n_abs = abs(n)
    if n_abs >= 1e7:
        return f"{n_abs/1e7:.2f}Cr"
    if n_abs >= 1e5:
        return f"{n_abs/1e5:.2f}L"
    if n_abs >= 1e3:
        return f"{n_abs/1e3:.2f}K"
    if n_abs.is_integer():
        return f"{int(n_abs)}"
    return f"{n_abs:.2f}"


def fmt_delta_oi(n):
    sign = "+" if n >= 0 else "-"
    n_abs = abs(n)
    if n_abs >= 1e7:
        return f"{sign}{n_abs/1e7:.2f}Cr"
    if n_abs >= 1e5:
        return f"{sign}{n_abs/1e5:.2f}L"
    if n_abs >= 1e3:
        return f"{sign}{n_abs/1e3:.2f}K"
    return f"{sign}{int(n_abs)}"


def fmt_delta_ltp(n):
    sign = "+" if n >= 0 else "-"
    return f"{sign}{abs(n):.2f}"


# ---------- Cache helpers ----------
def load_last_oi():
    """Load JSON cache and migrate older formats if necessary."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r") as f:
            store = json.load(f)
    except Exception as e:
        logger.warning("Failed reading cache: %s", e)
        return {}

    migrated = False
    for strike, entry in list(store.items()):
        if not isinstance(entry, dict):
            store[strike] = {"ce": 0, "pe": 0, "ce_ltp": 0.0, "pe_ltp": 0.0}
            migrated = True
            continue
        if "ce_ltp" not in entry:
            entry["ce_ltp"] = float(entry.get("ce_ltp_num", 0.0) or 0.0)
            migrated = True
        if "pe_ltp" not in entry:
            entry["pe_ltp"] = float(entry.get("pe_ltp_num", 0.0) or 0.0)
            migrated = True
        if "ce" not in entry:
            entry["ce"] = int(entry.get("ce_oi", 0) or 0)
            migrated = True
        if "pe" not in entry:
            entry["pe"] = int(entry.get("pe_oi", 0) or 0)
            migrated = True

    if migrated:
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(store, f)
            logger.info("Migrated old cache to include ce_ltp/pe_ltp/ce/pe")
        except Exception as e:
            logger.warning("Failed writing migrated cache: %s", e)

    return store


def save_last_oi(data_rows):
    """Save normalized numeric OI & LTP for each strike to cache."""
    store = {}
    for d in data_rows:
        strike = d.get("strike", "<no-strike>")
        ce_ltp_val = float(d.get("ce_ltp", d.get("ce_ltp_num", 0.0) or 0.0))
        pe_ltp_val = float(d.get("pe_ltp", d.get("pe_ltp_num", 0.0) or 0.0))
        store[strike] = {
            "ce": int(d.get("ce_oi", 0) or 0),
            "pe": int(d.get("pe_oi", 0) or 0),
            "ce_ltp": ce_ltp_val,
            "pe_ltp": pe_ltp_val,
        }
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(store, f)
    except Exception as e:
        logger.warning("Failed saving cache: %s", e)


# ---------- Fetching & delta computation ----------
async def fetch_quantsapp_data():
    """
    Scrape Quantsapp table and return list of rows with guaranteed keys:
      strike (str), ce_oi (int), pe_oi (int), ce_oi_raw, pe_oi_raw,
      ce_ltp (float), pe_ltp (float), ce_ltp_raw, pe_ltp_raw
    """
    out = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        logger.info("Navigating to %s", QUANTSAPP_URL)
        await page.goto(QUANTSAPP_URL, timeout=60000)
        await page.wait_for_selector("table tbody tr", timeout=60000)
        # small wait to allow JS table fill
        await page.wait_for_timeout(1200)
        rows = await page.query_selector_all("table tbody tr")
        logger.info("Found %d rows", len(rows))

        for row in rows:
            try:
                cols = await row.query_selector_all("td")
                # ensure we have enough columns; skip otherwise
                if len(cols) <= max(IDX.values()):
                    continue

                strike = (await cols[IDX["strike"]].inner_text()).strip()
                ce_oi_raw = (await cols[IDX["ce_oi"]].inner_text()).strip()
                ce_ltp_raw = (await cols[IDX["ce_ltp"]].inner_text()).strip()
                pe_ltp_raw = (await cols[IDX["pe_ltp"]].inner_text()).strip()
                pe_oi_raw = (await cols[IDX["pe_oi"]].inner_text()).strip()

                ce_oi_num = parse_num_oi(ce_oi_raw)
                pe_oi_num = parse_num_oi(pe_oi_raw)
                ce_ltp_num = parse_ltp_value(ce_ltp_raw)
                pe_ltp_num = parse_ltp_value(pe_ltp_raw)

                out.append({
                    "strike": strike,
                    "ce_oi": ce_oi_num,
                    "pe_oi": pe_oi_num,
                    "ce_oi_raw": ce_oi_raw,
                    "pe_oi_raw": pe_oi_raw,
                    "ce_ltp_raw": ce_ltp_raw,
                    "pe_ltp_raw": pe_ltp_raw,
                    "ce_ltp": float(ce_ltp_num),
                    "pe_ltp": float(pe_ltp_num),
                    "ce_ltp_num": float(ce_ltp_num),
                    "pe_ltp_num": float(pe_ltp_num),
                })
            except Exception as e:
                # don't crash on single-row parse failure
                logger.debug("Row parse failed: %s", e)
                continue

        await browser.close()
    return out


def calc_delta(data_rows, prev_cache):
    """
    For each row in data_rows, compute:
      ce_delta (formatted), pe_delta
      ce_ltp_change (formatted), pe_ltp_change
    And ensure numeric ce_ltp/pe_ltp exist on each row.
    """
    result = []
    for d in data_rows:
        try:
            strike = d.get("strike", "<no-strike>")
            prev_entry = prev_cache.get(strike, {})

            ce_old = int(prev_entry.get("ce", prev_entry.get("ce_oi", 0) or 0))
            pe_old = int(prev_entry.get("pe", prev_entry.get("pe_oi", 0) or 0))
            ce_old_ltp = float(prev_entry.get("ce_ltp", prev_entry.get("ce_ltp_num", 0.0) or 0.0))
            pe_old_ltp = float(prev_entry.get("pe_ltp", prev_entry.get("pe_ltp_num", 0.0) or 0.0))

            ce_curr_oi = int(d.get("ce_oi", 0) or 0)
            pe_curr_oi = int(d.get("pe_oi", 0) or 0)
            ce_curr_ltp = float(d.get("ce_ltp", d.get("ce_ltp_num", 0.0) or 0.0))
            pe_curr_ltp = float(d.get("pe_ltp", d.get("pe_ltp_num", 0.0) or 0.0))

            # write normalized numeric LTPs back onto the row
            d["ce_ltp"] = ce_curr_ltp
            d["pe_ltp"] = pe_curr_ltp

            ce_delta_val = ce_curr_oi - ce_old
            pe_delta_val = pe_curr_oi - pe_old
            ce_ltp_delta = ce_curr_ltp - ce_old_ltp
            pe_ltp_delta = pe_curr_ltp - pe_old_ltp

            d["ce_delta"] = fmt_delta_oi(ce_delta_val)
            d["pe_delta"] = fmt_delta_oi(pe_delta_val)
            d["ce_ltp_change"] = fmt_delta_ltp(ce_ltp_delta)
            d["pe_ltp_change"] = fmt_delta_ltp(pe_ltp_delta)

            result.append(d)
        except Exception as e:
            logger.debug("calc_delta failed for strike=%s: %s", d.get("strike", "?"), e)
            continue
    return result


# ---------- Formatters & Telegram send ----------
def format_ce_message(rows, top_n=15):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = f"📈 *NIFTY CE (Call)*\n🕒 {now}\n\n"
    body = "```\nStrike | OI       | ΔOI     | LTP (Δ)\n"
    body += "-------------------------------------\n"
    for d in (rows or [])[:top_n]:
        strike = d.get("strike", "-")
        oi_disp = d.get("ce_oi_raw") or human_fmt(d.get("ce_oi", 0))
        delta = d.get("ce_delta", "+0")
        ltp = d.get("ce_ltp", d.get("ce_ltp_num", 0.0)) or 0.0
        ltp_change = d.get("ce_ltp_change", "+0.00")
        body += f"{strike:6} | {oi_disp:8} | {delta:7} | {ltp:6.2f} ({ltp_change})\n"
    body += "```\nSource: web.quantsapp.com"
    return header + body

def format_pe_message(rows, top_n=15):
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = f"📉 *NIFTY PE (Put)*\n🕒 {now}\n\n"
    body = "```\nStrike | OI       | ΔOI     | LTP (Δ)\n"
    body += "-------------------------------------\n"
    for d in (rows or [])[:top_n]:
        strike = d.get("strike", "-")
        oi_disp = d.get("pe_oi_raw") or human_fmt(d.get("pe_oi", 0))
        delta = d.get("pe_delta", "+0")
        ltp = d.get("pe_ltp", d.get("pe_ltp_num", 0.0)) or 0.0
        ltp_change = d.get("pe_ltp_change", "+0.00")
        body += f"{strike:6} | {oi_disp:8} | {delta:7} | {ltp:6.2f} ({ltp_change})\n"
    body += "```\nSource: web.quantsapp.com"
    return header + body


async def send_to_telegram(text, parse_mode="Markdown"):
    """Send message using python-telegram-bot Bot in a thread to avoid blocking."""
    try:
        await asyncio.to_thread(bot.send_message, chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=parse_mode)
    except Exception as e:
        logger.warning("Telegram send failed: %s", e)


# ---------- Orchestration ----------
async def fetch_and_post_once():
    logger.info("Fetching Quantsapp data at %s ...", datetime.now())
    data = await fetch_quantsapp_data()
    if not data:
        logger.warning("No rows fetched.")
        return

    prev = load_last_oi()
    computed = calc_delta(data, prev)
    # sort if needed: example sort by strike numeric ascending
    try:
        computed.sort(key=lambda r: float(r.get("strike", 0)))
    except Exception:
        # if strike is not numeric, leave order as-is
        pass

    # Save cache for next run
    save_last_oi(computed)
    # Format and send messages (split CE / PE)
    ce_msg = format_ce_message(computed, top_n=15)
    pe_msg = format_pe_message(computed, top_n=15)

    # send CE then PE
    await send_to_telegram(ce_msg)
    await asyncio.sleep(0.5)  # small spacing
    await send_to_telegram(pe_msg)
    logger.info("Posted CE and PE messages to Telegram.")


def in_market_hours():
    if not RUN_DURING_MARKET_HOURS:
        return True
    now = datetime.now()
    if now.weekday() >= 5:  # Sat=5, Sun=6
        return False
    t = now.time()
    return (MARKET_START <= t <= MARKET_END)


async def main_loop():
    logger.info("Starting CE/PE Option Chain Bot...")
    # quick health check for Telegram
    try:
        await asyncio.to_thread(bot.get_me)
        logger.info("Telegram connection OK.")
    except Exception as e:
        logger.error("Telegram connection failed: %s", e)
        return

    while True:
        if in_market_hours():
            try:
                await fetch_and_post_once()
            except Exception as e:
                logger.exception("fetch_and_post_once failed: %s", e)
        else:
            logger.info("Outside market hours. Sleeping.")
        await asyncio.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    # Quick run: if you prefer single-run, replace main_loop() with fetch_and_post_once()
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")


# --- Auto Environment Config ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

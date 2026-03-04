import os
import json
import asyncio
import time
import shutil
import glob
from datetime import datetime, time as dt_time, timezone, timedelta
import requests
from telegram import Bot
from telegram.request import HTTPXRequest
import logging
import sys
from pathlib import Path

# ---------- CONFIG ----------
CACHE_FILE = "last_oi.json"
ARCHIVE_DIR = "oi_archive"               # daily archives stored here
RESET_MARKER = "last_reset_date.txt"     # stores date of last reset
ARCHIVE_MARKER = "last_archive_date.txt" # stores date of last archive
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
FETCH_INTERVAL_SECONDS = 900   # not used in GitHub Actions
RUN_DURING_MARKET_HOURS = True
MARKET_START = dt_time(9, 15)   # actual trading start
MARKET_END = dt_time(15, 40)
ATM_RANGE = 300                  # ±300 around ATM

# NSE India API endpoints
NSE_HOME = "https://www.nseindia.com"
EXP_URL = "https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY"
OC_URL = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={exp}"

FORCE_RUN = os.getenv("FORCE_RUN", "false").lower() == "true"
# ----------------------------

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("bt6-fixed")

# Telegram bot (lazy init)
bot = None

# Session with proper headers
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com",
    "Connection": "keep-alive",
})

# ---------- Timezone helpers ----------
def get_current_time_ist():
    """Returns current datetime in IST (timezone-aware)."""
    utc_now = datetime.now(timezone.utc)
    ist_offset = timedelta(hours=5, minutes=30)
    return utc_now + ist_offset

def get_today_date_str():
    """Returns today's date as YYYY-MM-DD in IST."""
    return get_current_time_ist().strftime("%Y-%m-%d")

def get_yesterday_date_str():
    """Returns yesterday's date as YYYY-MM-DD in IST."""
    return (get_current_time_ist() - timedelta(days=1)).strftime("%Y-%m-%d")

# ---------- Archive management ----------
def ensure_archive_dir():
    Path(ARCHIVE_DIR).mkdir(exist_ok=True)

def get_archive_files():
    """Returns sorted list of archive files (oldest first)."""
    files = glob.glob(os.path.join(ARCHIVE_DIR, "last_oi_*.json"))
    files.sort()  # alphabetical order = chronological by date
    return files

def prune_old_archives(keep=10):
    """Deletes archives older than the most recent `keep` files."""
    files = get_archive_files()
    if len(files) > keep:
        for f in files[:-keep]:
            try:
                os.remove(f)
                logger.info(f"Removed old archive: {f}")
            except Exception as e:
                logger.warning(f"Failed to remove {f}: {e}")

def read_marker(marker_file):
    """Returns date string from marker file, or None."""
    if os.path.exists(marker_file):
        with open(marker_file, "r") as f:
            return f.read().strip()
    return None

def write_marker(marker_file, date_str):
    with open(marker_file, "w") as f:
        f.write(date_str)

def should_reset():
    """
    Returns True if:
      - current time is between 9:00 and 10:00 (approx) AND
      - reset hasn't been done today (marker date != today)
    """
    now_ist = get_current_time_ist()
    today = now_ist.strftime("%Y-%m-%d")
    # Reset window: 9:00 AM to 10:00 AM
    reset_start = dt_time(9, 0)
    reset_end = dt_time(10, 0)
    if reset_start <= now_ist.time() <= reset_end:
        last_reset = read_marker(RESET_MARKER)
        if last_reset != today:
            return True
    return False

def perform_reset():
    """
    - If CACHE_FILE exists and has data, move it to archive with yesterday's date.
    - Then clear CACHE_FILE (create empty JSON).
    - Update reset marker.
    """
    logger.info("Performing daily reset at 9 AM...")
    # Archive previous day's cache if it exists and has data
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r") as f:
                data = json.load(f)
            if data:  # non-empty
                # Determine which date to use: from file's mtime or yesterday
                mtime = os.path.getmtime(CACHE_FILE)
                file_date = datetime.fromtimestamp(mtime, tz=timezone.utc) + timedelta(hours=5, minutes=30)
                archive_date = file_date.strftime("%Y-%m-%d")
                # If file_date is today (possible if script ran early morning), use yesterday
                if archive_date == get_today_date_str():
                    archive_date = get_yesterday_date_str()
                archive_path = os.path.join(ARCHIVE_DIR, f"last_oi_{archive_date}.json")
                shutil.move(CACHE_FILE, archive_path)
                logger.info(f"Archived previous cache to {archive_path}")
        except Exception as e:
            logger.warning(f"Could not archive previous cache: {e}")
    # Ensure cache file exists and is empty
    with open(CACHE_FILE, "w") as f:
        json.dump({}, f)
    logger.info("Cache cleared for new trading day.")
    # Update marker
    write_marker(RESET_MARKER, get_today_date_str())

def should_archive():
    """
    Returns True if:
      - current time is between 15:40 and 16:00 (approx) AND
      - archive hasn't been done today
    """
    now_ist = get_current_time_ist()
    today = now_ist.strftime("%Y-%m-%d")
    archive_start = dt_time(15, 40)
    archive_end = dt_time(16, 0)
    if archive_start <= now_ist.time() <= archive_end:
        last_archive = read_marker(ARCHIVE_MARKER)
        if last_archive != today:
            return True
    return False

def perform_archive():
    """
    - Copy current CACHE_FILE to archive with today's date.
    - Prune old archives (keep last 10).
    - Update archive marker.
    """
    logger.info("Performing end-of-day archive at 3:45 PM...")
    if os.path.exists(CACHE_FILE):
        try:
            today = get_today_date_str()
            archive_path = os.path.join(ARCHIVE_DIR, f"last_oi_{today}.json")
            shutil.copy2(CACHE_FILE, archive_path)  # copy, keep original for intraday
            logger.info(f"Archived today's OI data to {archive_path}")
            prune_old_archives(keep=10)
            write_marker(ARCHIVE_MARKER, today)
        except Exception as e:
            logger.warning(f"Failed to archive today's data: {e}")
    else:
        logger.info("No cache file found; nothing to archive.")

# ---------- Formatting helpers (unchanged) ----------
def fmt_oi(n):
    try:
        n = float(n)
    except:
        return n
    if n >= 1e7:
        return f"{n/1e7:.1f}Cr"
    if n >= 1e5:
        return f"{n/1e5:.1f}L"
    if n >= 1e3:
        return f"{n/1e3:.1f}K"
    return f"{n:.0f}"

def fmt_plain(n):
    try:
        n = float(n)
    except:
        return n
    if abs(n) >= 1e6:
        return f"{n:,.0f}"
    elif abs(n) >= 1e3:
        return f"{n:,.0f}"
    else:
        return f"{n:.0f}"

def convert_to_float(value):
    try:
        return float(value)
    except:
        return 0.0

def fmt_delta_oi(n):
    sign = "+" if n >= 0 else "-"
    n_abs = abs(n)
    if n_abs >= 1e7:
        return f"{sign}{n_abs/1e7:.1f}Cr"
    if n_abs >= 1e5:
        return f"{sign}{n_abs/1e5:.1f}L"
    if n_abs >= 1e3:
        return f"{sign}{n_abs/1e3:.1f}K"
    return f"{sign}{int(n_abs)}"

def fmt_delta_ltp(n):
    sign = "+" if n >= 0 else "-"
    return f"{sign}{abs(n):.2f}"

# ---------- NSE fetch helpers (updated with retry and validation) ----------
def get_nse_cookies():
    try:
        session.get(NSE_HOME, timeout=10)
        logger.info("NSE session cookies obtained.")
        return True
    except Exception as e:
        logger.warning(f"Failed to get NSE cookies: {e}")
        return False

def fetch_expiry():
    get_nse_cookies()
    try:
        resp = session.get(EXP_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and "expiryDates" in data:
            expiry_list = data["expiryDates"]
            if isinstance(expiry_list, list) and len(expiry_list) > 0:
                expiry = expiry_list[0]
                logger.info(f"Nearest expiry: {expiry}")
                return expiry
            else:
                logger.error("expiryDates list is empty or not a list")
        else:
            logger.error(f"Unexpected response format: {data}")
    except Exception as e:
        logger.error(f"Expiry fetch failed: {e}")
    return None

def fetch_option_chain(expiry):
    url = OC_URL.format(exp=expiry)
    logger.info("Fetching option chain...")
    try:
        time.sleep(0.5)
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Option chain fetch failed: {e}")
        return None

def get_spot_price(json_data):
    try:
        underlying = json_data.get("records", {}).get("underlyingValue", 0)
        if underlying:
            logger.info(f"Spot price: {underlying}")
            return float(underlying)
    except Exception as e:
        logger.error(f"Failed to get spot price: {e}")
    return 0.0

def is_data_valid(json_data):
    """Check if option chain data is fresh (non-zero spot and at least one non-zero OI)."""
    spot = get_spot_price(json_data)
    if spot == 0:
        return False
    data = parse_nse_data(json_data)
    if not data:
        return False
    # At least one strike has non-zero OI in CE or PE
    for r in data:
        if r["ce_oi"] > 0 or r["pe_oi"] > 0:
            return True
    return False

def parse_nse_data(json_data):
    rows = []
    try:
        records = json_data.get("records", {}).get("data", [])
        for item in records:
            strike = str(item.get("strikePrice"))
            ce = item.get("CE", {})
            pe = item.get("PE", {})
            ce_oi = ce.get("openInterest", 0)
            pe_oi = pe.get("openInterest", 0)
            ce_ltp = ce.get("lastPrice", 0.0) or 0.0
            pe_ltp = pe.get("lastPrice", 0.0) or 0.0
            rows.append({
                "strike": strike,
                "strike_num": float(strike),
                "ce_oi": int(ce_oi),
                "pe_oi": int(pe_oi),
                "ce_ltp": float(ce_ltp),
                "pe_ltp": float(pe_ltp),
                "ce_oi_fmt": fmt_oi(ce_oi),
                "pe_oi_fmt": fmt_oi(pe_oi),
                "ce_ltp_fmt": f"{ce_ltp:.2f}" if ce_ltp else "-",
                "pe_ltp_fmt": f"{pe_ltp:.2f}" if pe_ltp else "-",
                "ce_oi_plain": fmt_plain(ce_oi),
                "pe_oi_plain": fmt_plain(pe_oi),
            })
        logger.info(f"Parsed {len(rows)} strikes")
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
    return rows

def filter_atm_strikes(rows, spot_price, atm_range=300):
    if not rows or spot_price == 0:
        return rows
    min_strike = spot_price - atm_range
    max_strike = spot_price + atm_range
    filtered = [r for r in rows if min_strike <= r["strike_num"] <= max_strike]
    logger.info(f"Filtered to {len(filtered)} ATM strikes (±{atm_range} around {spot_price:.2f})")
    return filtered

# ---------- Cache helpers (unchanged except using updated load/save) ----------
def load_last_oi():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r") as f:
            store = json.load(f)
    except Exception as e:
        logger.warning("Failed reading cache: %s", e)
        return {}
    # Migrate old format if needed (as before)
    migrated = False
    for strike, entry in list(store.items()):
        if not isinstance(entry, dict):
            store[strike] = {"ce": 0, "pe": 0, "ce_ltp": 0.0, "pe_ltp": 0.0}
            migrated = True
            continue
        if "ce_ltp" not in entry:
            entry["ce_ltp"] = convert_to_float(entry.get("ce_ltp_num", 0.0))
            migrated = True
        if "pe_ltp" not in entry:
            entry["pe_ltp"] = convert_to_float(entry.get("pe_ltp_num", 0.0))
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
            logger.info("Migrated old cache")
        except Exception as e:
            logger.warning("Failed writing migrated cache: %s", e)
    return store

def save_last_oi(data_rows):
    store = {}
    for d in data_rows:
        strike = d.get("strike", "<no-strike>")
        store[strike] = {
            "ce": int(d.get("ce_oi", 0)),
            "pe": int(d.get("pe_oi", 0)),
            "ce_ltp": float(d.get("ce_ltp", 0.0)),
            "pe_ltp": float(d.get("pe_ltp", 0.0)),
        }
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(store, f)
        logger.info(f"Cache saved with {len(store)} strikes")
    except Exception as e:
        logger.warning("Failed saving cache: %s", e)

# ---------- Delta computation (unchanged) ----------
def calc_delta(data_rows, prev_cache):
    result = []
    for d in data_rows:
        try:
            strike = d.get("strike", "<no-strike>")
            prev_entry = prev_cache.get(strike, {})
            ce_old = int(prev_entry.get("ce", 0))
            pe_old = int(prev_entry.get("pe", 0))
            ce_old_ltp = convert_to_float(prev_entry.get("ce_ltp", 0.0))
            pe_old_ltp = convert_to_float(prev_entry.get("pe_ltp", 0.0))
            ce_curr_oi = int(d.get("ce_oi", 0))
            pe_curr_oi = int(d.get("pe_oi", 0))
            ce_curr_ltp = float(d.get("ce_ltp", 0.0))
            pe_curr_ltp = float(d.get("pe_ltp", 0.0))
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

# ---------- Telegram formatters & sender (unchanged) ----------
def format_ce_message(rows, spot_price, top_n=15):
    now = get_current_time_ist().strftime("%Y-%m-%d %H:%M")
    header = f"📈 *NIFTY CE (Call)*\n🕒 {now} IST\n📊 Spot: {spot_price:.2f}\n\n"
    body = "```\nStrike | OI     | ΔOI   | LTP (Δ)\n"
    body += "---------------------------------\n"
    for d in (rows or [])[:top_n]:
        strike = d.get("strike", "-")
        oi_disp = d.get("ce_oi_fmt")
        delta = d.get("ce_delta", "+0")
        ltp = d.get("ce_ltp", 0.0)
        ltp_change = d.get("ce_ltp_change", "+0.00")
        strike_marker = "🔹" if abs(d.get("strike_num", 0) - spot_price) < 50 else ""
        body += f"{strike:6} {strike_marker}| {oi_disp:6} | {delta:6} | {ltp:6.2f} ({ltp_change})\n"
    body += "```\nSource: NSE India | ATM ±{0}".format(ATM_RANGE)
    return header + body

def format_pe_message(rows, spot_price, top_n=15):
    now = get_current_time_ist().strftime("%Y-%m-%d %H:%M")
    header = f"📉 *NIFTY PE (Put)*\n🕒 {now} IST\n📊 Spot: {spot_price:.2f}\n\n"
    body = "```\nStrike | OI     | ΔOI   | LTP (Δ)\n"
    body += "---------------------------------\n"
    for d in (rows or [])[:top_n]:
        strike = d.get("strike", "-")
        oi_disp = d.get("pe_oi_fmt")
        delta = d.get("pe_delta", "+0")
        ltp = d.get("pe_ltp", 0.0)
        ltp_change = d.get("pe_ltp_change", "+0.00")
        strike_marker = "🔹" if abs(d.get("strike_num", 0) - spot_price) < 50 else ""
        body += f"{strike:6} {strike_marker}| {oi_disp:6} | {delta:6} | {ltp:6.2f} ({ltp_change})\n"
    body += "```\nSource: NSE India | ATM ±{0}".format(ATM_RANGE)
    return header + body

async def send_to_telegram(text, parse_mode="Markdown"):
    global bot
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not set")
        return False
    try:
        if bot is None:
            request = HTTPXRequest(connection_pool_size=1)
            bot = Bot(token=TELEGRAM_TOKEN, request=request)
            me = await bot.get_me()
            logger.info(f"Telegram connected: @{me.username}")
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode=parse_mode)
        logger.info("Message sent to Telegram")
        return True
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
        return False

# ---------- Market hours check (updated start time to 9:15) ----------
def in_market_hours():
    if FORCE_RUN:
        logger.info("FORCE_RUN enabled - bypassing market hours")
        return True
    if not RUN_DURING_MARKET_HOURS:
        return True
    now = get_current_time_ist()
    if now.weekday() >= 5:
        logger.info("Weekend - market closed")
        return False
    t = now.time()
    is_open = (MARKET_START <= t <= MARKET_END)
    if is_open:
        logger.info(f"Market open at {t.strftime('%H:%M')} IST")
    else:
        logger.info(f"Market closed at {t.strftime('%H:%M')} IST")
    return is_open

# ---------- Main function for GitHub Actions ----------
async def run_once():
    logger.info("=" * 60)
    logger.info("🚀 NSE Option Chain Bot Started (GitHub Actions)")
    logger.info("=" * 60)

    # Ensure archive directory exists
    ensure_archive_dir()

    # --- Daily reset at 9 AM ---
    if should_reset():
        perform_reset()

    # --- Market hours check ---
    if not in_market_hours():
        logger.info("Exiting - outside market hours")
        return

    # --- Credentials check ---
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("❌ TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set in GitHub Secrets")
        return

    # --- Fetch with retry and validation ---
    try:
        expiry = fetch_expiry()
        if not expiry:
            logger.error("Could not fetch expiry. Aborting.")
            return

        max_retries = 3
        retry_delay = 30  # seconds
        json_data = None
        for attempt in range(max_retries):
            json_data = fetch_option_chain(expiry)
            if json_data and is_data_valid(json_data):
                break
            logger.warning(f"Attempt {attempt+1}: Data invalid or stale. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)
            get_nse_cookies()  # refresh cookies before retry
        else:
            logger.error("No valid data after retries. Sending warning to Telegram.")
            await send_to_telegram("⚠️ NSE data not available yet. Market may be pre-open or API issue.")
            return

        spot_price = get_spot_price(json_data)
        if spot_price == 0:
            logger.warning("Spot price is zero; using fallback but may be inaccurate.")

        data = parse_nse_data(json_data)
        if not data:
            logger.warning("No rows parsed.")
            return

        atm_data = filter_atm_strikes(data, spot_price, ATM_RANGE)
        if not atm_data:
            logger.warning("No ATM strikes found.")
            return

        prev = load_last_oi()
        computed = calc_delta(atm_data, prev)

        # Sort by strike
        try:
            computed.sort(key=lambda r: float(r.get("strike", 0)))
        except Exception:
            pass

        save_last_oi(computed)

        # --- End-of-day archive at 3:45 PM ---
        if should_archive():
            perform_archive()

        ce_msg = format_ce_message(computed, spot_price, top_n=len(computed))
        pe_msg = format_pe_message(computed, spot_price, top_n=len(computed))

        await send_to_telegram(ce_msg)
        await asyncio.sleep(0.5)
        await send_to_telegram(pe_msg)

        logger.info(f"✅ Successfully posted {len(computed)} ATM strikes to Telegram")
        logger.info("=" * 60)

    except Exception as e:
        logger.exception(f"Error in main process: {e}")

# ---------- Entry Point ----------
if __name__ == "__main__":
    try:
        asyncio.run(run_once())
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")
        sys.exit(1)

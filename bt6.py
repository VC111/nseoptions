import os
import json
import asyncio
import time
from datetime import datetime, time as dt_time
import requests
from telegram import Bot
from telegram.request import HTTPXRequest
import logging
import sys

# ---------- CONFIG ----------
CACHE_FILE = "last_oi.json"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")  # GitHub Secrets se lega
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")  # GitHub Secrets se lega
FETCH_INTERVAL_SECONDS = 900  # Not used in GitHub Actions (will run on schedule)
RUN_DURING_MARKET_HOURS = True
MARKET_START = dt_time(9, 15)
MARKET_END = dt_time(15, 30)
ATM_RANGE = 300  # ±300 around ATM

# NSE India API endpoints
NSE_HOME = "https://www.nseindia.com"
EXP_URL = "https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY"
OC_URL = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={exp}"

# For testing - bypass market hours
FORCE_RUN = os.getenv("FORCE_RUN", "false").lower() == "true"
# ----------------------------

# Setup logging (for GitHub Actions - stdout)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("bt6-github")

# Telegram bot (lazy initialization)
bot = None

# Simple session like stb.py
session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# ---------- Timezone helpers ----------
def get_current_time_ist():
    """Get current time in IST"""
    # GitHub Actions uses UTC, so add 5:30
    utc_now = datetime.utcnow()
    ist_offset = 5.5 * 3600  # 5.5 hours in seconds
    ist_time = utc_now.timestamp() + ist_offset
    return datetime.fromtimestamp(ist_time)

# ---------- Formatting helpers (from stb.py) ----------
def fmt_oi(n):
    """Format OI numbers to Cr, L, K"""
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
    """Format numbers as plain numbers with commas for thousands"""
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
    """Safely convert any value to float"""
    try:
        return float(value)
    except:
        return 0.0

def fmt_delta_oi(n):
    """Format OI delta with sign"""
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
    """Format LTP delta with sign"""
    sign = "+" if n >= 0 else "-"
    return f"{sign}{abs(n):.2f}"


# ---------- NSE fetch helpers ----------
def get_nse_cookies():
    """Initialize session by visiting NSE homepage to obtain cookies."""
    try:
        session.get(NSE_HOME, timeout=10)
        logger.info("NSE session cookies obtained.")
        return True
    except Exception as e:
        logger.warning(f"Failed to get NSE cookies: {e}")
        return False


def fetch_expiry():
    """Fetch the nearest expiry date from NSE."""
    get_nse_cookies()
    try:
        resp = session.get(EXP_URL, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        # Response is a dictionary with "expiryDates" key
        if isinstance(data, dict) and "expiryDates" in data:
            expiry_list = data["expiryDates"]
            if isinstance(expiry_list, list) and len(expiry_list) > 0:
                expiry = expiry_list[0]
                logger.info(f"Nearest expiry: {expiry}")
                return expiry
            else:
                logger.error("expiryDates list is empty or not a list")
                return None
        else:
            logger.error(f"Unexpected response format: {data}")
            return None
    except Exception as e:
        logger.error(f"Expiry fetch failed: {e}")
        return None


def fetch_option_chain(expiry):
    """Fetch option chain data for given expiry."""
    url = OC_URL.format(exp=expiry)
    logger.info(f"Fetching option chain...")
    
    try:
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data
    except Exception as e:
        logger.error(f"Option chain fetch failed: {e}")
        return None


def get_spot_price(json_data):
    """Extract underlying spot price from option chain data."""
    try:
        underlying = json_data.get("records", {}).get("underlyingValue", 0)
        if underlying:
            logger.info(f"Spot price: {underlying}")
            return float(underlying)
    except Exception as e:
        logger.error(f"Failed to get spot price: {e}")
    return 0


def parse_nse_data(json_data):
    """
    Extract strikes, CE/PE OI and LTP from NSE JSON.
    Returns list of dicts with keys:
        strike (str), ce_oi (int), pe_oi (int), ce_ltp (float), pe_ltp (float)
    """
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
                # formatted display values using fmt_oi
                "ce_oi_fmt": fmt_oi(ce_oi),
                "pe_oi_fmt": fmt_oi(pe_oi),
                "ce_ltp_fmt": f"{ce_ltp:.2f}" if ce_ltp else "-",
                "pe_ltp_fmt": f"{pe_ltp:.2f}" if pe_ltp else "-",
                # plain format with commas
                "ce_oi_plain": fmt_plain(ce_oi),
                "pe_oi_plain": fmt_plain(pe_oi),
            })
        logger.info(f"Parsed {len(rows)} strikes")
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
    return rows


def filter_atm_strikes(rows, spot_price, atm_range=300):
    """Filter strikes within ±range of spot price."""
    if not rows or spot_price == 0:
        return rows
    
    min_strike = spot_price - atm_range
    max_strike = spot_price + atm_range
    
    filtered = [r for r in rows if min_strike <= r["strike_num"] <= max_strike]
    logger.info(f"Filtered to {len(filtered)} ATM strikes (±{atm_range} around {spot_price:.2f})")
    
    return filtered


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

    # Ensure structure
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
            logger.info("Migrated old cache to include ce_ltp/pe_ltp/ce/pe")
        except Exception as e:
            logger.warning("Failed writing migrated cache: %s", e)

    return store


def save_last_oi(data_rows):
    """Save normalized numeric OI & LTP for each strike to cache."""
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


# ---------- Delta computation ----------
def calc_delta(data_rows, prev_cache):
    """
    For each row in data_rows, compute:
      ce_delta (formatted), pe_delta
      ce_ltp_change (formatted), pe_ltp_change
    """
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


# ---------- Formatters & Telegram send ----------
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
        # Add marker for ATM strike
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
        # Add marker for ATM strike
        strike_marker = "🔹" if abs(d.get("strike_num", 0) - spot_price) < 50 else ""
        body += f"{strike:6} {strike_marker}| {oi_disp:6} | {delta:6} | {ltp:6.2f} ({ltp_change})\n"
    body += "```\nSource: NSE India | ATM ±{0}".format(ATM_RANGE)
    return header + body


async def send_to_telegram(text, parse_mode="Markdown"):
    """Send message using python-telegram-bot Bot."""
    global bot
    
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("Telegram credentials not set")
        return False
    
    try:
        # Lazy initialize bot
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


# ---------- Market hours check ----------
def in_market_hours():
    if FORCE_RUN:
        logger.info("FORCE_RUN enabled - bypassing market hours")
        return True
        
    if not RUN_DURING_MARKET_HOURS:
        return True
    
    now = get_current_time_ist()
    if now.weekday() >= 5:  # Sat=5, Sun=6
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
    """Run one fetch and post cycle (for GitHub Actions)"""
    logger.info("=" * 60)
    logger.info("🚀 NSE Option Chain Bot Started (GitHub Actions)")
    logger.info("=" * 60)
    
    # Check market hours
    if not in_market_hours():
        logger.info("Exiting - outside market hours")
        return
    
    # Check credentials
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("❌ TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set in GitHub Secrets")
        return
    
    # Fetch and post
    try:
        expiry = fetch_expiry()
        if not expiry:
            logger.error("Could not fetch expiry. Aborting.")
            return

        json_data = fetch_option_chain(expiry)
        if not json_data:
            logger.error("Could not fetch option chain. Aborting.")
            return

        # Get spot price
        spot_price = get_spot_price(json_data)
        if spot_price == 0:
            logger.warning("Could not get spot price")

        data = parse_nse_data(json_data)
        if not data:
            logger.warning("No rows parsed.")
            return

        # Filter ATM strikes
        atm_data = filter_atm_strikes(data, spot_price, ATM_RANGE)
        if not atm_data:
            logger.warning("No ATM strikes found.")
            return

        prev = load_last_oi()
        computed = calc_delta(atm_data, prev)

        # Sort by strike numeric ascending
        try:
            computed.sort(key=lambda r: float(r.get("strike", 0)))
        except Exception:
            pass

        save_last_oi(computed)

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

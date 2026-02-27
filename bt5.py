import os
import json
import asyncio
import sys
import requests
import pytz
from datetime import datetime, time as dt_time
from telegram import Bot
from telegram.request import HTTPXRequest
import logging

# ---------- CONFIGURATION ----------
CACHE_FILE = "last_oi.json"
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Market timings (IST)
MARKET_START = dt_time(9, 15)   # 9:15 AM IST
MARKET_END = dt_time(15, 30)    # 3:30 PM IST

# NSE API endpoints
NSE_HOME = "https://www.nseindia.com"
EXPIRY_URL = "https://www.nseindia.com/api/option-chain-contract-info?symbol=NIFTY"
OPTION_CHAIN_URL = "https://www.nseindia.com/api/option-chain-v3?type=Indices&symbol=NIFTY&expiry={exp}"

# Request settings
REQUEST_TIMEOUT = 15
MAX_RETRIES = 3
RETRY_DELAY = 2

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("nse-bot")

# Timezone
IST = pytz.timezone('Asia/Kolkata')
# ------------------------------------

class NSESession:
    """Manage NSE session with cookies"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/",
        })
    
    def get_cookies(self):
        """Initialize session cookies"""
        try:
            self.session.get(NSE_HOME, timeout=REQUEST_TIMEOUT)
            logger.info("✅ NSE session cookies obtained")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Failed to get cookies: {e}")
            return False
    
    def fetch_json(self, url):
        """Fetch JSON with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    asyncio.sleep(RETRY_DELAY)
        return None


class CacheManager:
    """Handle OI cache storage"""
    
    def __init__(self, cache_file=CACHE_FILE):
        self.cache_file = cache_file
    
    def load(self):
        """Load cache from file"""
        if not os.path.exists(self.cache_file):
            logger.info("📁 No cache file found, starting fresh")
            return {}
        
        try:
            with open(self.cache_file, 'r') as f:
                data = json.load(f)
            
            # Validate and clean
            clean_data = {}
            for strike, entry in data.items():
                if isinstance(entry, dict):
                    clean_data[strike] = {
                        "ce": int(entry.get("ce", 0)),
                        "pe": int(entry.get("pe", 0)),
                        "ce_ltp": float(entry.get("ce_ltp", 0.0)),
                        "pe_ltp": float(entry.get("pe_ltp", 0.0)),
                    }
            
            logger.info(f"📂 Loaded {len(clean_data)} strikes from cache")
            return clean_data
        except Exception as e:
            logger.warning(f"⚠️ Cache load failed: {e}")
            return {}
    
    def save(self, data_rows):
        """Save current data to cache"""
        cache_data = {}
        for row in data_rows:
            strike = row.get("strike")
            if strike:
                cache_data[strike] = {
                    "ce": int(row.get("ce_oi", 0)),
                    "pe": int(row.get("pe_oi", 0)),
                    "ce_ltp": float(row.get("ce_ltp", 0.0)),
                    "pe_ltp": float(row.get("pe_ltp", 0.0)),
                }
        
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            logger.info(f"💾 Saved {len(cache_data)} strikes to cache")
            return True
        except Exception as e:
            logger.error(f"❌ Cache save failed: {e}")
            return False


class DataParser:
    """Parse NSE option chain data"""
    
    @staticmethod
    def format_number(num):
        """Format large numbers to K/L/Cr"""
        try:
            num = float(num)
        except:
            return str(num)
        
        abs_num = abs(num)
        if abs_num >= 1e7:
            return f"{abs_num/1e7:.2f}Cr"
        if abs_num >= 1e5:
            return f"{abs_num/1e5:.2f}L"
        if abs_num >= 1e3:
            return f"{abs_num/1e3:.2f}K"
        return str(int(abs_num)) if abs_num.is_integer() else f"{abs_num:.2f}"
    
    @staticmethod
    def format_delta(num, is_oi=True):
        """Format delta with sign"""
        if abs(num) < 0.01:
            return "  0" if is_oi else "  0.00"
        
        sign = "+" if num > 0 else "-"
        abs_num = abs(num)
        
        if is_oi:
            if abs_num >= 1e7:
                return f"{sign}{abs_num/1e7:.2f}Cr"
            if abs_num >= 1e5:
                return f"{sign}{abs_num/1e5:.2f}L"
            if abs_num >= 1e3:
                return f"{sign}{abs_num/1e3:.2f}K"
            return f"{sign}{int(abs_num)}"
        else:
            return f"{sign}{abs_num:.2f}"
    
    def parse_option_chain(self, json_data):
        """Extract option chain data from JSON"""
        rows = []
        try:
            records = json_data.get("records", {}).get("data", [])
            
            for item in records:
                strike = str(item.get("strikePrice"))
                ce = item.get("CE", {})
                pe = item.get("PE", {})
                
                # Skip if no data
                if not ce and not pe:
                    continue
                
                ce_oi = ce.get("openInterest", 0)
                pe_oi = pe.get("openInterest", 0)
                ce_ltp = ce.get("lastPrice", 0.0) or 0.0
                pe_ltp = pe.get("lastPrice", 0.0) or 0.0
                
                rows.append({
                    "strike": strike,
                    "ce_oi": int(ce_oi),
                    "pe_oi": int(pe_oi),
                    "ce_ltp": float(ce_ltp),
                    "pe_ltp": float(pe_ltp),
                    "ce_oi_display": self.format_number(ce_oi),
                    "pe_oi_display": self.format_number(pe_oi),
                })
            
            logger.info(f"📊 Parsed {len(rows)} strikes")
        except Exception as e:
            logger.error(f"❌ Parse failed: {e}")
        
        return rows
    
    def calculate_changes(self, current_rows, previous_cache):
        """Calculate OI and LTP changes"""
        result = []
        
        for row in current_rows:
            strike = row["strike"]
            prev = previous_cache.get(strike, {})
            
            # Current values
            ce_curr = row["ce_oi"]
            pe_curr = row["pe_oi"]
            ce_ltp_curr = row["ce_ltp"]
            pe_ltp_curr = row["pe_ltp"]
            
            # Previous values
            ce_prev = prev.get("ce", 0)
            pe_prev = prev.get("pe", 0)
            ce_ltp_prev = prev.get("ce_ltp", 0.0)
            pe_ltp_prev = prev.get("pe_ltp", 0.0)
            
            # Calculate changes
            row["ce_delta"] = self.format_delta(ce_curr - ce_prev, is_oi=True)
            row["pe_delta"] = self.format_delta(pe_curr - pe_prev, is_oi=True)
            row["ce_ltp_change"] = self.format_delta(ce_ltp_curr - ce_ltp_prev, is_oi=False)
            row["pe_ltp_change"] = self.format_delta(pe_ltp_curr - pe_ltp_prev, is_oi=False)
            
            # Raw values for sorting
            row["ce_delta_raw"] = ce_curr - ce_prev
            row["pe_delta_raw"] = pe_curr - pe_prev
            
            result.append(row)
        
        return result


class TelegramSender:
    """Handle Telegram messages"""
    
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.bot = None
    
    async def initialize(self):
        """Initialize bot connection"""
        if not self.token or not self.chat_id:
            logger.error("❌ Telegram credentials missing")
            return False
        
        try:
            request = HTTPXRequest(connection_pool_size=1)
            self.bot = Bot(token=self.token, request=request)
            me = await asyncio.to_thread(self.bot.get_me)
            logger.info(f"✅ Telegram connected: @{me.username}")
            return True
        except Exception as e:
            logger.error(f"❌ Telegram init failed: {e}")
            return False
    
    def format_option_message(self, data_rows, option_type="CE"):
        """Format message for CE or PE"""
        now = datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        
        # Header
        if option_type == "CE":
            header = f"📈 *NIFTY Call Options (CE)*\n🕒 {now} IST\n\n"
            oi_key = "ce_oi_display"
            delta_key = "ce_delta"
            ltp_key = "ce_ltp"
            ltp_change_key = "ce_ltp_change"
            delta_raw_key = "ce_delta_raw"
        else:
            header = f"📉 *NIFTY Put Options (PE)*\n🕒 {now} IST\n\n"
            oi_key = "pe_oi_display"
            delta_key = "pe_delta"
            ltp_key = "pe_ltp"
            ltp_change_key = "pe_ltp_change"
            delta_raw_key = "pe_delta_raw"
        
        # Sort by biggest change first
        sorted_rows = sorted(
            data_rows,
            key=lambda x: abs(x.get(delta_raw_key, 0)),
            reverse=True
        )[:15]  # Top 15
        
        # Sort by strike for display
        sorted_rows.sort(key=lambda x: float(x.get("strike", 0)))
        
        # Build message
        body = "```\n"
        body += "Strike | OI       | ΔOI     | LTP   (ΔLTP)\n"
        body += "------|----------|---------|---------------\n"
        
        for row in sorted_rows:
            strike = row.get("strike", "").rjust(6)
            oi = row.get(oi_key, "0").rjust(8)
            delta = row.get(delta_key, "  0").rjust(7)
            ltp = float(row.get(ltp_key, 0.0))
            ltp_change = row.get(ltp_change_key, "  0.00")
            
            body += f"{strike} | {oi} | {delta} | {ltp:6.2f} ({ltp_change})\n"
        
        body += "```\n"
        body += "🔹 *Top 15 by OI change*\n"
        body += "📊 Source: NSE India"
        
        return header + body
    
    async def send_message(self, text):
        """Send message to Telegram"""
        if not self.bot:
            logger.error("❌ Bot not initialized")
            return False
        
        try:
            # Split long messages
            if len(text) > 4000:
                parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for part in parts:
                    await asyncio.to_thread(
                        self.bot.send_message,
                        chat_id=self.chat_id,
                        text=part,
                        parse_mode="Markdown"
                    )
                    await asyncio.sleep(0.5)
            else:
                await asyncio.to_thread(
                    self.bot.send_message,
                    chat_id=self.chat_id,
                    text=text,
                    parse_mode="Markdown"
                )
            
            logger.info(f"📨 Message sent ({len(text)} chars)")
            return True
        except Exception as e:
            logger.error(f"❌ Send failed: {e}")
            return False


class MarketChecker:
    """Check market hours"""
    
    @staticmethod
    def is_market_open():
        """Check if market is currently open"""
        now = datetime.now(IST)
        
        # Weekend check
        if now.weekday() >= 5:  # 5=Sat, 6=Sun
            logger.info("📅 Weekend - market closed")
            return False
        
        current_time = now.time()
        is_open = MARKET_START <= current_time <= MARKET_END
        
        if is_open:
            logger.info(f"✅ Market open at {current_time.strftime('%H:%M')} IST")
        else:
            logger.info(f"⏰ Market closed at {current_time.strftime('%H:%M')} IST")
        
        return is_open


async def fetch_nse_data():
    """Fetch and process NSE data"""
    # Initialize components
    nse = NSESession()
    cache = CacheManager()
    parser = DataParser()
    
    # Get cookies
    if not nse.get_cookies():
        logger.error("❌ Failed to initialize NSE session")
        return None
    
    # Fetch expiry
    logger.info("🔍 Fetching expiry...")
    expiry_data = nse.fetch_json(EXPIRY_URL)
    if not expiry_data or not isinstance(expiry_data, list):
        logger.error("❌ Failed to fetch expiry")
        return None
    
    expiry = expiry_data[0]
    logger.info(f"📅 Expiry: {expiry}")
    
    # Fetch option chain
    logger.info("🔍 Fetching option chain...")
    url = OPTION_CHAIN_URL.format(exp=expiry)
    chain_data = nse.fetch_json(url)
    if not chain_data:
        logger.error("❌ Failed to fetch option chain")
        return None
    
    # Parse data
    current_data = parser.parse_option_chain(chain_data)
    if not current_data:
        logger.error("❌ No data parsed")
        return None
    
    # Load cache and calculate changes
    prev_cache = cache.load()
    processed_data = parser.calculate_changes(current_data, prev_cache)
    
    # Save to cache
    cache.save(processed_data)
    
    return processed_data


async def main():
    """Main function"""
    logger.info("=" * 60)
    logger.info("🚀 NSE Option Chain Bot Started")
    logger.info("=" * 60)
    
    # Check credentials
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.error("❌ TELEGRAM_TOKEN and TELEGRAM_CHAT_ID must be set")
        return
    
    # Check market hours
    if not MarketChecker.is_market_open():
        logger.info("⏰ Exiting - market closed")
        return
    
    # Initialize Telegram
    telegram = TelegramSender(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    if not await telegram.initialize():
        return
    
    # Fetch data
    data = await fetch_nse_data()
    if not data:
        logger.error("❌ No data to send")
        return
    
    # Send messages
    ce_message = telegram.format_option_message(data, "CE")
    pe_message = telegram.format_option_message(data, "PE")
    
    await telegram.send_message(ce_message)
    await asyncio.sleep(1)
    await telegram.send_message(pe_message)
    
    logger.info("✅ Bot completed successfully")
    logger.info("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Bot stopped by user")
    except Exception as e:
        logger.exception(f"💥 Unhandled error: {e}")
        sys.exit(1)

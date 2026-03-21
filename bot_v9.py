import logging
import asyncio
import re
import random
import time
import httpx
from datetime import datetime
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)

# =============================================
#              CONFIG
# =============================================
BOT_TOKEN = "8128706779:AAGSaYDAwqmDI5pd6HjD4fsMpDzT5oqEDjw"

# STEXSMS (S1)
STEXSMS_EMAIL = "shuvosrb86@gmail.com"
STEXSMS_PASSWORD = "Superdry168"
BASE_URL = "https://stexsms.com/mapi/v1"

# X.MINT (S2) - Auto Login
XMINT_EMAIL = "aboos7008@gmail.com"
XMINT_PASSWORD = "Siam12345678@"
XMINT_BASE_URL = "https://x.mnitnetwork.com/mapi/v1"

ADMIN_ID = 1984916365
CHANNEL_USERNAME = "@alwaysrvice24hours"
CHANNEL_LINK = "https://t.me/alwaysrvice24hours"

# OTP Forward Channel
OTP_CHANNEL_ID = -1002625886518

# Get 100 access control
GET100_ENABLED = False
GET100_USERS = set()

logging.basicConfig(level=logging.INFO)

user_data = {}

# Console cache
_console_cache = {"logs": [], "time": 0}
_xmint_console_cache = {"logs": [], "time": 0}

# =============================================
#         REAL APP LOGOS (Photo URLs)
# =============================================

APP_EMOJIS = {
    "FACEBOOK": "📘", "INSTAGRAM": "📸", "TIKTOK": "🎵",
    "SNAPCHAT": "👻", "TWITTER": "🐦", "GOOGLE": "🔍",
    "WHATSAPP": "💬", "TELEGRAM": "✈️", "CHATGPT": "🤖",
    "SHEIN": "👗", "TWILIO": "📞", "TWVERIFY": "✅",
    "VERIFY": "🔐", "VERIMSG": "💌", "VGSMS": "📡",
    "WORLDFIRST": "🌏", "GOFUNDME": "💰"
}

# =============================================
#         SESSION POOL SYSTEM
# =============================================

SESSION_POOL_SIZE = 50
NUMBER_GET_SLOTS = 25
OTP_CHECK_SLOTS = 25

class SessionPool:
    def __init__(self):
        self.number_sessions = asyncio.Queue()
        self.otp_sessions = asyncio.Queue()
        self.all_sessions = []
        self.initialized = False
        self.lock = asyncio.Lock()

    async def initialize(self):
        async with self.lock:
            if self.initialized:
                return
            logging.info("🔄 Session pool initialize হচ্ছে...")
            tasks = [self._login_once() for _ in range(SESSION_POOL_SIZE)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            number_count = 0
            otp_count = 0
            for r in results:
                if isinstance(r, dict) and r.get("token"):
                    self.all_sessions.append(r)
                    if number_count < NUMBER_GET_SLOTS:
                        await self.number_sessions.put(r)
                        number_count += 1
                    elif otp_count < OTP_CHECK_SLOTS:
                        await self.otp_sessions.put(r)
                        otp_count += 1

            self.initialized = True
            logging.info(f"✅ Session pool ready! Number: {number_count}, OTP: {otp_count}")

    async def _login_once(self):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    f"{BASE_URL}/mauth/login",
                    json={"email": STEXSMS_EMAIL, "password": STEXSMS_PASSWORD}
                )
            data = res.json()
            if data.get("meta", {}).get("code") == 200:
                return {
                    "token": data["data"]["token"],
                    "session": data["data"]["session_token"],
                    "time": time.time()
                }
        except Exception as e:
            logging.error(f"Login error: {e}")
        return {}

    async def get_number_session(self):
        try:
            session = await asyncio.wait_for(self.number_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                session = await self._login_once()
                if not session.get("token"):
                    session = self.all_sessions[0] if self.all_sessions else {}
            return session
        except asyncio.TimeoutError:
            return await self._login_once()

    async def get_otp_session(self):
        try:
            session = await asyncio.wait_for(self.otp_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                session = await self._login_once()
                if not session.get("token"):
                    session = self.all_sessions[0] if self.all_sessions else {}
            return session
        except asyncio.TimeoutError:
            return await self._login_once()

    async def return_number_session(self, session):
        if session and session.get("token"):
            await self.number_sessions.put(session)

    async def return_otp_session(self, session):
        if session and session.get("token"):
            await self.otp_sessions.put(session)

    async def refresh_all(self):
        logging.info("🔄 Session pool refresh হচ্ছে...")
        self.initialized = False
        while not self.number_sessions.empty():
            self.number_sessions.get_nowait()
        while not self.otp_sessions.empty():
            self.otp_sessions.get_nowait()
        self.all_sessions.clear()
        await self.initialize()

session_pool = SessionPool()

# =============================================
#         X.MINT SESSION POOL (S2)
# =============================================

class XMintSessionPool:
    def __init__(self):
        self.number_sessions = asyncio.Queue()
        self.otp_sessions = asyncio.Queue()
        self.all_sessions = []
        self.initialized = False
        self.lock = asyncio.Lock()

    async def initialize(self):
        async with self.lock:
            if self.initialized:
                return
            logging.info("🔄 X.Mint Session pool initialize হচ্ছে...")
            tasks = [self._login_once() for _ in range(50)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            number_count = 0
            otp_count = 0
            for r in results:
                if isinstance(r, dict) and r.get("token"):
                    self.all_sessions.append(r)
                    if number_count < 25:
                        await self.number_sessions.put(r)
                        number_count += 1
                    elif otp_count < 25:
                        await self.otp_sessions.put(r)
                        otp_count += 1

            self.initialized = True
            logging.info(f"✅ X.Mint Session pool ready! Number: {number_count}, OTP: {otp_count}")

    async def _login_once(self):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    f"{XMINT_BASE_URL}/mauth/login",
                    json={"email": XMINT_EMAIL, "password": XMINT_PASSWORD}
                )
            data = res.json()
            if data.get("meta", {}).get("code") == 200:
                token = data["data"].get("token")
                if token:
                    return {
                        "token": token,
                        "time": time.time()
                    }
        except Exception as e:
            logging.error(f"X.Mint Login error: {e}")
        return {}

    async def get_number_session(self):
        try:
            session = await asyncio.wait_for(self.number_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                session = await self._login_once()
                if not session.get("token"):
                    session = self.all_sessions[0] if self.all_sessions else {}
            return session
        except asyncio.TimeoutError:
            return await self._login_once()

    async def get_otp_session(self):
        try:
            session = await asyncio.wait_for(self.otp_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                session = await self._login_once()
                if not session.get("token"):
                    session = self.all_sessions[0] if self.all_sessions else {}
            return session
        except asyncio.TimeoutError:
            return await self._login_once()

    async def return_number_session(self, session):
        if session and session.get("token"):
            await self.number_sessions.put(session)

    async def return_otp_session(self, session):
        if session and session.get("token"):
            await self.otp_sessions.put(session)

    async def refresh_all(self):
        logging.info("🔄 X.Mint Session pool refresh হচ্ছে...")
        self.initialized = False
        while not self.number_sessions.empty():
            self.number_sessions.get_nowait()
        while not self.otp_sessions.empty():
            self.otp_sessions.get_nowait()
        self.all_sessions.clear()
        await self.initialize()

xmint_pool = XMintSessionPool()

# =============================================
#         X.MINT API FUNCTIONS (S2 - Updated)
# =============================================

async def api_get_number_s2(range_val, app_name="FACEBOOK"):
    """X.Mint থেকে number নাও - StexSMS style"""
    logging.info(f"🔵 X.Mint: Getting number for {app_name}, range: {range_val}")
    
    clean_range = ''.join(c for c in range_val.upper() if c.isdigit() or c == 'X')
    if not clean_range:
        return {"error": "Invalid range"}
    x_count = len(clean_range) - len(clean_range.rstrip('X'))
    if x_count < 3:
        clean_range = clean_range.rstrip('X') + 'XXX'

    session = await xmint_pool.get_number_session()
    try:
        token = session.get("token")
        if not token:
            return {"error": "No session available"}

        payload = {
            "range": clean_range,
            "is_national": False,
            "remove_plus": True,
            "app": app_name
        }

        headers = {
            'User-Agent': "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
            'Accept': "application/json",
            'Content-Type': "application/json",
            'mauthtoken': token,
            'Cookie': f"mautToken={token}"
        }

        logging.info(f"🔵 X.Mint Payload: {payload}")

        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{XMINT_BASE_URL}/mdashboard/getnum/number",
                json=payload,
                headers=headers
            )

        result = res.json()
        code = result.get('meta', {}).get('code', res.status_code)
        logging.info(f"✅ X.Mint response: HTTP {res.status_code}, API code: {code}")
        return result
    except Exception as e:
        logging.error(f"❌ api_get_number_s2 error: {e}")
        return {"error": str(e)}
    finally:
        await xmint_pool.return_number_session(session)

async def api_get_info_s2(search="", status=""):
    """X.Mint থেকে OTP info নাও - Session pool use করে"""
    session = await xmint_pool.get_otp_session()
    try:
        token = session.get("token")
        if not token:
            return {"error": "No session available"}
        
        clean_search = search.replace("+", "").strip()
        today = datetime.now().strftime("%Y-%m-%d")
        params = {"date": today, "page": 1, "search": clean_search, "status": status}
        headers = {
            'User-Agent': "Mozilla/5.0 (Linux; Android 10)",
            'Accept': "application/json",
            'Content-Type': "application/json",
            'mauthtoken': token,
            'Cookie': f"mautToken={token}"
        }
        
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{XMINT_BASE_URL}/mdashboard/getnum/info",
                params=params,
                headers=headers
            )
        return res.json()
    except Exception as e:
        logging.error(f"api_get_info_s2 error: {e}")
        return {"error": str(e)}
    finally:
        await xmint_pool.return_otp_session(session)

ALL_APPS = [
    "FACEBOOK", "INSTAGRAM", "WHATSAPP", "TELEGRAM"
]

COUNTRY_FLAGS = {
    # আফ্রিকা
    "CM": "🇨🇲", "VN": "🇻🇳", "PK": "🇵🇰", "TZ": "🇹🇿",
    "TJ": "🇹🇯", "TG": "🇹🇬", "NG": "🇳🇬", "GH": "🇬🇭",
    "KE": "🇰🇪", "BD": "🇧🇩", "IN": "🇮🇳", "PH": "🇵🇭",
    "ID": "🇮🇩", "MM": "🇲🇲", "KH": "🇰🇭", "ET": "🇪🇹",
    "CD": "🇨🇩", "MZ": "🇲🇿", "MG": "🇲🇬", "CI": "🇨🇮",
    "SN": "🇸🇳", "ML": "🇲🇱", "BF": "🇧🇫", "GN": "🇬🇳",
    "ZM": "🇿🇲", "ZW": "🇿🇼", "RW": "🇷🇼", "UG": "🇺🇬",
    "AO": "🇦🇴", "SD": "🇸🇩", "MR": "🇲🇷", "NE": "🇳🇪",
    "TD": "🇹🇩", "SO": "🇸🇴", "BI": "🇧🇮", "BJ": "🇧🇯",
    "MW": "🇲🇼", "SL": "🇸🇱", "LR": "🇱🇷", "CF": "🇨🇫",
    "GQ": "🇬🇶", "GA": "🇬🇦", "CG": "🇨🇬", "DJ": "🇩🇯",
    "ER": "🇪🇷", "GM": "🇬🇲", "GW": "🇬🇼", "CV": "🇨🇻",
    "ST": "🇸🇹", "KM": "🇰🇲", "SC": "🇸🇨", "MU": "🇲🇺",
    "ZA": "🇿🇦", "NA": "🇳🇦", "BW": "🇧🇼", "LS": "🇱🇸",
    "SZ": "🇸🇿", "EG": "🇪🇬", "LY": "🇱🇾", "TN": "🇹🇳",
    "DZ": "🇩🇿", "MA": "🇲🇦", "MX": "🇲🇽", "BR": "🇧🇷",
    "CO": "🇨🇴", "PE": "🇵🇪", "VE": "🇻🇪", "AR": "🇦🇷",
    "CL": "🇨🇱", "EC": "🇪🇨", "BO": "🇧🇴", "PY": "🇵🇾",
    "UY": "🇺🇾", "GY": "🇬🇾", "SR": "🇸🇷", "GT": "🇬🇹",
    "HN": "🇭🇳", "SV": "🇸🇻", "NI": "🇳🇮", "CR": "🇨🇷",
    "PA": "🇵🇦", "CU": "🇨🇺", "DO": "🇩🇴", "HT": "🇭🇹",
    "TH": "🇹🇭", "LA": "🇱🇦", "MY": "🇲🇾", "SG": "🇸🇬",
    "TL": "🇹🇱", "NP": "🇳🇵", "LK": "🇱🇰", "AF": "🇦🇫",
    "IR": "🇮🇷", "IQ": "🇮🇶", "SY": "🇸🇾", "YE": "🇾🇪",
    "SA": "🇸🇦", "AE": "🇦🇪", "QA": "🇶🇦", "KW": "🇰🇼",
    "BH": "🇧🇭", "OM": "🇴🇲", "JO": "🇯🇴", "LB": "🇱🇧",
    "PS": "🇵🇸", "AM": "🇦🇲", "AZ": "🇦🇿", "GE": "🇬🇪",
    "KZ": "🇰🇿", "UZ": "🇺🇿", "TM": "🇹🇲", "KG": "🇰🇬",
    "MN": "🇲🇳", "RU": "🇷🇺", "UA": "🇺🇦", "BY": "🇧🇾",
    "MD": "🇲🇩", "RO": "🇷🇴", "BG": "🇧🇬", "RS": "🇷🇸",
    "HR": "🇭🇷", "BA": "🇧🇦", "MK": "🇲🇰", "AL": "🇦🇱",
    "ME": "🇲🇪", "SI": "🇸🇮", "SK": "🇸🇰", "CZ": "🇨🇿",
    "PL": "🇵🇱", "HU": "🇭🇺", "AT": "🇦🇹", "CH": "🇨🇭",
    "DE": "🇩🇪", "FR": "🇫🇷", "ES": "🇪🇸", "IT": "🇮🇹",
    "PT": "🇵🇹", "GB": "🇬🇧", "IE": "🇮🇪", "NL": "🇳🇱",
    "BE": "🇧🇪", "LU": "🇱🇺", "DK": "🇩🇰", "SE": "🇸🇪",
    "NO": "🇳🇴", "FI": "🇫🇮", "IS": "🇮🇸", "US": "🇺🇸",
    "CA": "🇨🇦", "AU": "🇦🇺", "NZ": "🇳🇿", "JP": "🇯🇵",
    "KR": "🇰🇷", "CN": "🇨🇳", "TW": "🇹🇼", "HK": "🇭🇰",
    "SS": "🇸🇸", "XK": "🇽🇰",
}

COUNTRY_NAME_TO_CODE = {
    # আফ্রিকা
    "cameroon": "CM", "vietnam": "VN", "pakistan": "PK", "tanzania": "TZ",
    "tajikistan": "TJ", "togo": "TG", "nigeria": "NG", "ghana": "GH",
    "kenya": "KE", "bangladesh": "BD", "india": "IN", "philippines": "PH",
    "indonesia": "ID", "myanmar": "MM", "cambodia": "KH", "ethiopia": "ET",
    "congo": "CD", "dr congo": "CD", "democratic republic of congo": "CD",
    "republic of congo": "CG", "congo republic": "CG",
    "mozambique": "MZ", "madagascar": "MG", "ivory coast": "CI",
    "cote d'ivoire": "CI", "cote divoire": "CI",
    "senegal": "SN", "mali": "ML", "burkina faso": "BF", "guinea": "GN",
    "guinea-bissau": "GW", "equatorial guinea": "GQ",
    "zambia": "ZM", "zimbabwe": "ZW", "rwanda": "RW", "uganda": "UG",
    "angola": "AO", "sudan": "SD", "south sudan": "SS", "mauritania": "MR",
    "niger": "NE", "chad": "TD", "somalia": "SO", "burundi": "BI",
    "benin": "BJ", "malawi": "MW", "sierra leone": "SL", "liberia": "LR",
    "central african republic": "CF", "car": "CF", "centrafrique": "CF",
    "gabon": "GA", "djibouti": "DJ", "eritrea": "ER", "gambia": "GM",
    "cape verde": "CV", "sao tome": "ST", "comoros": "KM",
    "seychelles": "SC", "mauritius": "MU",
    "south africa": "ZA", "namibia": "NA", "botswana": "BW",
    "lesotho": "LS", "eswatini": "SZ", "swaziland": "SZ",
    "egypt": "EG", "libya": "LY", "tunisia": "TN", "algeria": "DZ",
    "morocco": "MA",
    # আমেরিকা
    "mexico": "MX", "brazil": "BR", "colombia": "CO", "peru": "PE",
    "venezuela": "VE", "argentina": "AR", "chile": "CL", "ecuador": "EC",
    "bolivia": "BO", "paraguay": "PY", "uruguay": "UY", "guyana": "GY",
    "suriname": "SR", "guatemala": "GT", "honduras": "HN",
    "el salvador": "SV", "nicaragua": "NI", "costa rica": "CR",
    "panama": "PA", "cuba": "CU", "dominican republic": "DO", "haiti": "HT",
    "usa": "US", "united states": "US", "canada": "CA",
    # এশিয়া
    "thailand": "TH", "laos": "LA", "malaysia": "MY", "singapore": "SG",
    "nepal": "NP", "sri lanka": "LK", "afghanistan": "AF", "iran": "IR",
    "iraq": "IQ", "syria": "SY", "yemen": "YE", "saudi arabia": "SA",
    "uae": "AE", "united arab emirates": "AE", "qatar": "QA",
    "kuwait": "KW", "bahrain": "BH", "oman": "OM", "jordan": "JO",
    "lebanon": "LB", "palestine": "PS", "armenia": "AM", "azerbaijan": "AZ",
    "georgia": "GE", "kazakhstan": "KZ", "uzbekistan": "UZ",
    "turkmenistan": "TM", "kyrgyzstan": "KG", "mongolia": "MN",
    "timor-leste": "TL", "east timor": "TL",
    # ইউরোপ
    "russia": "RU", "ukraine": "UA", "belarus": "BY", "moldova": "MD",
    "romania": "RO", "bulgaria": "BG", "serbia": "RS", "croatia": "HR",
    "bosnia": "BA", "north macedonia": "MK", "albania": "AL",
    "montenegro": "ME", "slovenia": "SI", "slovakia": "SK",
    "czech republic": "CZ", "czechia": "CZ", "poland": "PL",
    "hungary": "HU", "austria": "AT", "switzerland": "CH",
    "germany": "DE", "france": "FR", "spain": "ES", "italy": "IT",
    "portugal": "PT", "uk": "GB", "united kingdom": "GB",
    "ireland": "IE", "netherlands": "NL", "belgium": "BE",
    "luxembourg": "LU", "denmark": "DK", "sweden": "SE",
    "norway": "NO", "finland": "FI", "iceland": "IS",
    # ওশেনিয়া / অন্যান্য
    "australia": "AU", "new zealand": "NZ",
    "japan": "JP", "south korea": "KR", "china": "CN",
    "taiwan": "TW", "hong kong": "HK",
}

def get_flag(code):
    if not code:
        return "🌍"
    name_key = code.lower().strip()
    if name_key in COUNTRY_NAME_TO_CODE:
        return COUNTRY_FLAGS.get(COUNTRY_NAME_TO_CODE[name_key], "🌍")
    short = code.upper().strip()[:2]
    return COUNTRY_FLAGS.get(short, "🌍")

def extract_otp(message):
    if not message:
        return None
    match = re.search(r'\b(\d{8}|\d{6}|\d{5})\b', message)
    if match:
        return match.group(1)
    return None

def detect_app_from_message(message, default_app=""):
    if not message:
        return default_app
    msg_lower = message.lower()
    if "facebook" in msg_lower:
        return "FACEBOOK"
    elif "whatsapp" in msg_lower:
        return "WHATSAPP"
    elif "telegram" in msg_lower:
        return "TELEGRAM"
    elif "instagram" in msg_lower:
        return "INSTAGRAM"
    elif "google" in msg_lower:
        return "GOOGLE"
    elif "twitter" in msg_lower:
        return "TWITTER"
    elif "tiktok" in msg_lower:
        return "TIKTOK"
    elif "snapchat" in msg_lower:
        return "SNAPCHAT"
    return default_app

def has_get100_access(user_id):
    return GET100_ENABLED or user_id in GET100_USERS or user_id == ADMIN_ID

# =============================================
#         OTP CHANNEL FORWARD
# =============================================

async def hide_number(number):
    """Number এর মাঝের অংশ হাইড করো"""
    num = str(number).replace("+", "").strip()
    if len(num) > 6:
        return num[:5] + "★★" + num[-4:]
    return num

async def send_otp_to_channel(bot, number, otp, app, country, flag, raw_sms=""):
    """
    চ্যানেলে OTP পাঠান
    """
    try:
        app_cap = app.upper()
        clean_num = str(number).replace("+", "").strip()
        if len(clean_num) > 8:
            hidden_num = clean_num[:5] + "xxxx" + clean_num[-4:]
        else:
            hidden_num = clean_num

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # raw_sms এ special chars escape করো
        safe_raw = raw_sms.replace("`", "\\`").replace("_", "\\_").replace("*", "\\*").replace("[", "\\[") if raw_sms else ""

        msg = (
            f"✅ {flag} {country} {app_cap} OTP Received\n\n"
            f"🔐 OTP Code: `{otp}`\n"
            f"🕐 Time: {now}\n"
            f"☎️ Number: `{hidden_num}`\n"
            f"🌍 Country: {flag} {country}\n"
            f"⚙️ Service: {app_cap}\n"
        )

        if safe_raw:
            msg += f"\n📝 _Message:_\n{safe_raw}"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 NUMBER BOT", url="https://t.me/Fb_KiNG_Seviceotp_bot")]
        ])

        # চ্যানেলে পাঠাও
        await bot.send_message(
            chat_id=OTP_CHANNEL_ID,
            text=msg,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
        
        logging.info(f"✅ Channel OTP - {app_cap} ({country})")
        
    except Exception as e:
        logging.error(f"❌ Channel error: {e}")

# =============================================
#              API FUNCTIONS
# =============================================

async def get_token():
    session = await session_pool.get_otp_session()
    await session_pool.return_otp_session(session)
    return session.get("token"), session.get("session")

async def fresh_login():
    session = await session_pool._login_once()
    return session.get("token"), session.get("session")

_join_cache = {}

async def check_joined(user_id, bot):
    now = time.time()
    cached = _join_cache.get(user_id)
    if cached and (now - cached["time"]) < 600:
        return cached["joined"]
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        joined = member.status in ["member", "administrator", "creator"]
        _join_cache[user_id] = {"joined": joined, "time": now}
        return joined
    except:
        return True

def get_headers(token, session):
    return {
        'User-Agent': "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        'Accept': "application/json, text/plain, */*",
        'Content-Type': "application/json",
        'mauthtoken': token,
        'Cookie': f"mauthtoken={token}; session_token={session}"
    }

async def get_console_logs(force=False):
    global _console_cache
    if not force and _console_cache["logs"] and (time.time() - _console_cache["time"]) < 15:
        return _console_cache["logs"]
    try:
        token, session = await get_token()
        if not token:
            return []
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{BASE_URL}/mdashboard/console/info",
                headers=get_headers(token, session)
            )
        data = res.json()
        if data.get("meta", {}).get("code") == 200:
            logs = data["data"].get("logs", [])
            _console_cache = {"logs": logs, "time": time.time()}
            return logs
        return _console_cache["logs"]
    except Exception as e:
        logging.error(f"Console error: {e}")
        return _console_cache["logs"]

async def get_xmint_console_logs(force=False):
    global _xmint_console_cache
    if not force and _xmint_console_cache["logs"] and (time.time() - _xmint_console_cache["time"]) < 15:
        return _xmint_console_cache["logs"]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            login_res = await client.post(
                f"{XMINT_BASE_URL}/mauth/login",
                json={"email": XMINT_EMAIL, "password": XMINT_PASSWORD}
            )
        login_data = login_res.json()
        if login_data.get("meta", {}).get("code") != 200:
            return _xmint_console_cache["logs"]
        token = login_data["data"].get("token")
        if not token:
            return _xmint_console_cache["logs"]
        headers = {
            'User-Agent': "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
            'Accept': "application/json",
            'Content-Type': "application/json",
            'mauthtoken': token,
            'Cookie': f"mautToken={token}"
        }
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{XMINT_BASE_URL}/mdashboard/console/info",
                headers=headers
            )
        if res.status_code != 200 or not res.text.strip():
            logging.warning(f"X.Mint Console: HTTP {res.status_code}, empty response")
            return _xmint_console_cache["logs"]
        try:
            data = res.json()
        except Exception:
            logging.warning(f"X.Mint Console: Invalid JSON — {res.text[:100]}")
            return _xmint_console_cache["logs"]
        if data.get("meta", {}).get("code") == 200:
            logs = data["data"].get("logs", [])
            _xmint_console_cache = {"logs": logs, "time": time.time()}
            return logs
        return _xmint_console_cache["logs"]
    except Exception as e:
        logging.error(f"X.Mint Console error: {e}")
        return _xmint_console_cache["logs"]

async def get_countries_for_app(app_name, panel="S1"):
    logs = await get_xmint_console_logs() if panel == "S2" else await get_console_logs()
    seen = set()
    countries = []
    for log in logs:
        log_app = log.get("app_name", "").replace("*", "").strip().upper()
        if log_app == app_name.upper():
            country = log.get("country", "").strip()
            if country and country not in seen:
                seen.add(country)
                countries.append(country)
    return countries

async def get_carriers_for_country(app_name, country, panel="S1"):
    logs = await get_xmint_console_logs() if panel == "S2" else await get_console_logs()
    seen = set()
    carriers = []
    for log in logs:
        log_app = log.get("app_name", "").replace("*", "").strip().upper()
        log_country = log.get("country", "").strip()
        if log_app == app_name.upper() and log_country == country:
            carrier = log.get("carrier", "").strip()
            if carrier and carrier not in seen:
                seen.add(carrier)
                carriers.append(carrier)
    return carriers

async def get_ranges_for_carrier(app_name, country, carrier, panel="S1"):
    logs = await get_xmint_console_logs() if panel == "S2" else await get_console_logs()
    seen = set()
    ranges = []
    for log in logs:
        log_app = log.get("app_name", "").replace("*", "").strip().upper()
        log_country = log.get("country", "").strip()
        log_carrier = log.get("carrier", "").strip()
        if (log_app == app_name.upper() and
                log_country == country and log_carrier == carrier):
            r = log.get("range", "").strip()
            if r and r not in seen:
                seen.add(r)
                ranges.append({"range": r, "time": log.get("time", "")})
    return ranges

async def api_get_number(range_val, app_name="FACEBOOK"):
    clean_range = ''.join(c for c in range_val.upper() if c.isdigit() or c == 'X')
    if not clean_range:
        return {"error": "Invalid range"}
    x_count = len(clean_range) - len(clean_range.rstrip('X'))
    if x_count < 3:
        clean_range = clean_range.rstrip('X') + 'XXX'
    payload = {
        "range": clean_range,
        "is_national": False,
        "remove_plus": False,
        "app": app_name
    }
    session = await session_pool.get_number_session()
    try:
        token = session.get("token")
        sess = session.get("session")
        if not token:
            return {"error": "No session available"}
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{BASE_URL}/mdashboard/getnum/number",
                json=payload,
                headers=get_headers(token, sess)
            )
        data = res.json()
        msg = str(data.get("message", "")).lower()
        if any(k in msg for k in ["block", "rate", "limit", "many", "temporary"]):
            logging.warning(f"Rate limited: {msg}")
            session = await session_pool._login_once()
        return data
    except Exception as e:
        logging.error(f"api_get_number error: {e}")
        return {"error": str(e)}
    finally:
        await session_pool.return_number_session(session)

async def api_get_info(search="", status=""):
    session = await session_pool.get_otp_session()
    try:
        token = session.get("token")
        sess = session.get("session")
        if not token:
            return {"error": "Login failed"}
        clean_search = search.replace("+", "").strip()
        today = datetime.now().strftime("%Y-%m-%d")
        params = {"date": today, "page": 1, "search": clean_search, "status": status}
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{BASE_URL}/mdashboard/getnum/info",
                params=params,
                headers=get_headers(token, sess)
            )
        return res.json()
    except Exception as e:
        return {"error": str(e)}
    finally:
        await session_pool.return_otp_session(session)

# =============================================
#              HELPERS
# =============================================

def init_user(user_id):
    if user_id not in user_data:
        user_data[user_id] = {}
    d = user_data[user_id]
    d.setdefault("app", "FACEBOOK")
    d.setdefault("panel", "S1")  # S1=StexSMS, S2=X.Mint
    d.setdefault("country", None)
    d.setdefault("carrier", None)
    d.setdefault("range", None)
    d.setdefault("last_number", None)
    d.setdefault("waiting_for", None)
    d.setdefault("joined", datetime.now().strftime("%Y-%m-%d %H:%M"))
    d.setdefault("name", "User")

# =============================================
#              MENUS
# =============================================

def main_keyboard(user_id=None):
    buttons = [
        [KeyboardButton("🏠 Start"), KeyboardButton("🎯 Custom Range")],
        [KeyboardButton("📋 My Numbers"), KeyboardButton("📦 Bulk Number")],
    ]
    if user_id and user_id == ADMIN_ID:
        buttons.append([KeyboardButton("👑 Admin Panel")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def app_select_inline():
    buttons = []
    apps = ALL_APPS.copy()
    for app in apps:
        buttons.append([
            InlineKeyboardButton(f"{APP_EMOJIS.get(app, '📱')} {app.capitalize()} 1", callback_data=f"app_s1_{app}"),
            InlineKeyboardButton(f"{APP_EMOJIS.get(app, '📱')} {app.capitalize()} 2", callback_data=f"app_s2_{app}")
        ])
    return InlineKeyboardMarkup(buttons)

def country_select_inline(countries, app_name):
    buttons = []
    for c in countries:
        flag = get_flag(c)
        buttons.append([InlineKeyboardButton(
            f"{flag} {c}", callback_data=f"country_{c}"
        )])
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app")])
    return InlineKeyboardMarkup(buttons)

def carrier_select_inline(carriers, app_name, country):
    buttons = []
    for c in carriers:
        buttons.append([InlineKeyboardButton(
            f"📶 {c}", callback_data=f"carrier_{c}"
        )])
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}")])
    return InlineKeyboardMarkup(buttons)

def range_select_inline(ranges, app_name, country, carrier):
    buttons = []
    for r in ranges[:20]:
        buttons.append([InlineKeyboardButton(
            f"📡 {r['range']}", callback_data=f"range_{r['range']}"
        )])
    buttons.append([InlineKeyboardButton(
        "◀️ Back", callback_data=f"back_carrier_{app_name}|{country}"
    )])
    return InlineKeyboardMarkup(buttons)

def admin_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Bulk ON", callback_data="bulk_on"),
         InlineKeyboardButton("📦 Bulk OFF", callback_data="bulk_off")],
        [InlineKeyboardButton("👥 All Users", callback_data="admin_users"),
         InlineKeyboardButton("📊 Stats", callback_data="admin_stats")],
    ])

def after_number_inline(number, range_val):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁️ Check OTP", url="https://t.me/+SWraCXOQrWM4Mzg9")],
        [InlineKeyboardButton("🔄 Same Range", callback_data=f"same_{range_val}"),
         InlineKeyboardButton("📊 View Range", callback_data=f"viewrange_{range_val}")],
        [InlineKeyboardButton("🛑 Stop Auto OTP", callback_data="stop_auto"),
         InlineKeyboardButton("🏠 Home", callback_data="go_home")],
    ])

def otp_not_found_inline(number, range_val):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👁️ Check OTP", url="https://t.me/+SWraCXOQrWM4Mzg9")],
        [InlineKeyboardButton("🔄 Same Range", callback_data=f"same_{range_val}"),
         InlineKeyboardButton("🏠 Home", callback_data="go_home")],
    ])

# =============================================
#         AUTO OTP CHECK
# =============================================

async def auto_otp_single(number, user_id, otp_found_event, result_holder):
    clean_num = number.replace("+", "").replace(" ", "").strip()
    app = user_data[user_id].get("app", "FACEBOOK")
    panel = user_data[user_id].get("panel", "S1")

    while not otp_found_event.is_set():
        await asyncio.sleep(2) if panel == "S1" else await asyncio.sleep(5)
        if user_data[user_id].get("auto_otp_cancel") or otp_found_event.is_set():
            return
        try:
            if panel == "S1":
                data = await api_get_info(search=clean_num, status="success")
            else:
                data = await api_get_info_s2(search=clean_num, status="success")
            
            nums = []
            if data.get("meta", {}).get("code") == 200:
                nums = data["data"].get("numbers") or []

            found_otp = None
            found_num = None
            found_raw = ""
            found_country = ""
            found_app = app

            for n in nums:
                api_num = str(n.get("number", "")).replace("+", "").strip()
                if clean_num in api_num or api_num in clean_num:
                    raw_otp = (n.get("otp") or n.get("message") or "").strip()
                    otp = extract_otp(raw_otp)
                    if otp:
                        found_otp = otp
                        found_num = n.get("number", number)
                        found_raw = raw_otp
                        found_country = n.get("country", user_data[user_id].get("country", ""))
                        found_app = detect_app_from_message(raw_otp, app)
                        break

            if not found_otp:
                for n in nums:
                    if n.get("status") == "success":
                        raw_otp = (n.get("otp") or n.get("message") or "").strip()
                        otp = extract_otp(raw_otp)
                        if otp:
                            found_otp = otp
                            found_num = n.get("number", number)
                            found_raw = raw_otp
                            found_country = n.get("country", user_data[user_id].get("country", ""))
                            found_app = detect_app_from_message(raw_otp, app)
                            break

            if found_otp and not otp_found_event.is_set():
                result_holder["otp"] = found_otp
                result_holder["number"] = found_num
                result_holder["raw"] = found_raw
                result_holder["country"] = found_country
                result_holder["app"] = found_app
                otp_found_event.set()
                return

        except Exception as e:
            logging.error(f"Auto OTP check error ({number}): {e}")
            await asyncio.sleep(3)


async def auto_otp_multi(message, numbers, user_id, range_val, bot=None):
    app = user_data[user_id].get("app", "FACEBOOK")
    otp_found_event = asyncio.Event()
    result_holder = {}

    tasks = [
        asyncio.create_task(auto_otp_single(num, user_id, otp_found_event, result_holder))
        for num in numbers
    ]

    try:
        await asyncio.wait_for(otp_found_event.wait(), timeout=300)
    except asyncio.TimeoutError:
        pass

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    if user_data[user_id].get("auto_otp_cancel"):
        user_data[user_id]["auto_otp_cancel"] = False
        return

    found_otp = result_holder.get("otp")
    found_num = result_holder.get("number", "")
    found_country = result_holder.get("country", user_data[user_id].get("country", ""))
    found_app = result_holder.get("app", app)

    if found_otp:
        flag = get_flag(found_country)
        app_cap = found_app.capitalize()
        clean_found_num = str(found_num).replace("+", "").strip()
        found_raw = result_holder.get("raw", "")

        # OTP Channel এ আগে forward করো
        if bot:
            try:
                await send_otp_to_channel(bot, clean_found_num, found_otp, found_app, found_country, flag, found_raw)
            except Exception as e:
                logging.error(f"❌ Channel send error: {e}")

        # User কে OTP দেখাও
        try:
            await message.reply_text(
                f"🌎 Country : {found_country} {app_cap} {flag}\n"
                f"🔢 Number : `{clean_found_num}`\n"
                f"🔑 OTP : `{found_otp}`",
                parse_mode="Markdown",
                reply_markup=main_keyboard(user_id)
            )
        except Exception as e:
            logging.error(f"❌ User reply error: {e}")


async def auto_otp_after_number(message, number, user_id, range_val, context):
    """Backward compatibility."""
    await auto_otp_multi(message, [number], user_id, range_val, bot=context.bot if context else None)

# =============================================
#         CORE FUNCTIONS
# =============================================

async def do_get_number(message, user_id, count=1, user_name="User", bot=None):
    init_user(user_id)
    range_val = user_data[user_id].get("range")
    app = user_data[user_id].get("app", "FACEBOOK")
    panel = user_data[user_id].get("panel", "S1")

    if not range_val:
        await message.reply_text(
            "❌ Range select করা হয়নি!\n\n"
            "🏠 Start → Service → Country → Carrier → Range",
            reply_markup=main_keyboard(user_id)
        )
        return

    if count == 1:
        if panel == "S1":
            data = await api_get_number(range_val, app)
        else:
            data = await api_get_number_s2(range_val, app)
        
        if data.get("meta", {}).get("code") == 200:
            num = data["data"]
            number = num.get("number") or num.get("num") or "N/A"
            country_r = num.get("country", "")
            user_data[user_id]["last_number"] = number
            user_data[user_id]["auto_otp_cancel"] = False
            flag = get_flag(country_r)
            clean_number = str(number).replace("+", "").strip()
            await message.reply_text(
                f"✅ Number পাওয়া গেছে!\n\n"
                f"📞 `{clean_number}`\n"
                f"📱 {app}  {flag} {country_r}\n\n"
                f"🔍 OTP আসার অপেক্ষায়...",
                parse_mode="Markdown",
                reply_markup=after_number_inline(number, range_val)
            )
            asyncio.create_task(auto_otp_multi(message, [number], user_id, range_val, bot=bot))
        else:
            await message.reply_text("❌ Number পাওয়া যায়নি!", reply_markup=main_keyboard(user_id))
    else:
        await message.reply_text(f"⏳ {count}টি number নেওয়া হচ্ছে...")
        got = 0
        msg = f"📦 BULK GET — Range: {range_val}\n📱 App: {app}\n\n"
        for i in range(count):
            data = await api_get_number(range_val, app)
            if data.get("meta", {}).get("code") == 200:
                num = data["data"]
                number = num.get("number") or num.get("num") or "N/A"
                country_r = num.get("country", "")
                flag = get_flag(country_r)
                msg += f"{i+1}. {number} {flag} ✅\n"
                user_data[user_id]["last_number"] = number
                got += 1
            else:
                msg += f"{i+1}. ❌ Not found\n"
        msg += f"\n✅ Total received: {got}/{count}"
        await message.reply_text(msg, reply_markup=main_keyboard(user_id))

async def do_otp_check(message, number, user_id=None, bot=None):
    clean_number = number.replace("+", "").replace(" ", "").strip()

    await message.reply_text(
        f"🔍 OTP চেক করা হচ্ছে...\n📞 {number}"
    )

    data = await api_get_info(search=clean_number, status="success")
    nums = data.get("data", {}).get("numbers") or [] if data.get("meta", {}).get("code") == 200 else []

    found = []
    for n in nums:
        raw_otp = (n.get("otp") or n.get("message") or "").strip()
        otp = extract_otp(raw_otp)
        if otp:
            found.append((n, otp, raw_otp))

    if not found:
        data2 = await api_get_info(search=clean_number, status="")
        nums2 = data2.get("data", {}).get("numbers") or [] if data2.get("meta", {}).get("code") == 200 else []
        for n in nums2:
            if n.get("status") == "success":
                raw_otp = (n.get("otp") or n.get("message") or "").strip()
                otp = extract_otp(raw_otp)
                if otp:
                    found.append((n, otp, raw_otp))

    if found:
        n, otp, raw_otp = found[0]
        app = user_data.get(user_id, {}).get("app", "FACEBOOK") if user_id else "FACEBOOK"
        detected_app = detect_app_from_message(raw_otp, app)
        country_r = n.get("country", "") or (user_data.get(user_id, {}).get("country", "") if user_id else "")
        flag = get_flag(country_r)
        clean_num_display = str(n.get('number', number)).replace("+", "").strip()

        # ✅ Check OTP = শুধু Channel এ পাঠাও - raw SMS সহ
        if bot:
            await send_otp_to_channel(bot, clean_num_display, otp, detected_app, country_r, flag, raw_otp)
        else:
            app_cap = detected_app.capitalize()
            await message.reply_text(
                f"🌎 Country : {country_r} {app_cap} {flag}\n"
                f"🔢 Number : `{clean_num_display}`\n"
                f"🔑 OTP : `{otp}`",
                parse_mode="Markdown",
                reply_markup=main_keyboard(user_id)
            )

    else:
        await message.reply_text(
            "⏳ OTP এখনো আসেনি।\n\nকিছুক্ষণ পর আবার check করুন।",
            reply_markup=main_keyboard(user_id)
        )

# =============================================
#         COMMAND HANDLERS
# =============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    init_user(user_id)
    user_data[user_id]["name"] = user.first_name or "User"

    joined = await check_joined(user_id, context.bot)
    if not joined:
        await update.message.reply_text(
            "⚠️ Channel Join করুন!\n\n"
            "Bot ব্যবহার করতে আমাদের channel join করতে হবে।\n\n"
            "👇 নিচের button চাপুন, তারপর /start দিন।",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Channel Join করুন", url=CHANNEL_LINK)
            ]])
        )
        return

    token, _ = await get_token()
    api_status = "✅ Connected" if token else "❌ Disconnected"

    await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👋  Welcome, {user.first_name}!\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"🌐  NUMBER PANEL OTP BOT\n\n"
        f"🔗  API Status: {api_status}\n\n"
        f"📌  কিভাবে ব্যবহার করবেন:\n"
        f"Service → Country → Range → Number → OTP\n\n"
        f"👇  নিচে service select করুন:\n"
        f"━━━━━━━━━━━━━━━━━━",
        reply_markup=main_keyboard(user_id)
    )

    # App select menu
    await update.message.reply_text(
        "📱 Service Select করুন:",
        reply_markup=app_select_inline()
    )

async def cmd_get(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    init_user(user_id)
    user_data[user_id]["name"] = user.first_name or "User"
    await do_get_number(update.message, user_id, count=1, user_name=user.first_name, bot=context.bot)

async def cmd_get100(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    init_user(user_id)
    if not has_get100_access(user_id):
        await update.message.reply_text("❌ আপনার Get 100 access নেই।")
        return
    await do_get_number(update.message, user_id, count=100, user_name=user.first_name, bot=context.bot)

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    init_user(user_id)
    user_data[user_id]["auto_otp_cancel"] = True
    await update.message.reply_text(
        "🛑 Auto OTP check বন্ধ হয়েছে।",
        reply_markup=main_keyboard(user_id)
    )

async def cmd_mynum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await update.message.reply_text("⏳ Loading...")
    data = await api_get_info()
    if data.get("meta", {}).get("code") == 200:
        nums = data["data"].get("numbers", []) or []
        stats = data["data"].get("stats", {})
        msg = (
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📋  My Numbers\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"✅  Success: {stats.get('success_count', 0)}\n"
            f"⏳  Pending: {stats.get('pending_count', 0)}\n"
            f"❌  Failed: {stats.get('failed_count', 0)}\n\n"
        )
        for n in nums[:10]:
            e = "✅" if n.get("status") == "success" else "⏳" if n.get("status") == "pending" else "❌"
            msg += f"{e}  {n.get('number')}  —  {n.get('country', '')}  —  {n.get('last_activity', '')}\n"
        msg += "\n━━━━━━━━━━━━━━━━━━"
        await update.message.reply_text(msg, reply_markup=main_keyboard(user_id))
    else:
        await update.message.reply_text("❌ Load করতে ব্যর্থ।", reply_markup=main_keyboard(user_id))

# =============================================
#         ADMIN COMMANDS
# =============================================

async def cmd_allusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    msg = f"👥 Total Users: {len(user_data)}\n\n"
    for uid, uinfo in list(user_data.items())[:20]:
        msg += f"• {uid}  —  {uinfo.get('name','?')}  |  {uinfo.get('app','?')}\n"
    await update.message.reply_text(msg)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊  BOT STATS\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"👥  Users: {len(user_data)}\n"
        f"📦  Get 100: {'✅ ON' if GET100_ENABLED else '❌ OFF'}\n"
        f"👤  Get 100 Users: {len(GET100_USERS)}\n"
        f"🕐  {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"━━━━━━━━━━━━━━━━━━"
    )

async def cmd_apistatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    token, _ = await fresh_login()
    status = "✅ Connected" if token else "❌ Failed"
    number_slots = session_pool.number_sessions.qsize()
    otp_slots = session_pool.otp_sessions.qsize()
    msg = (
        f"🔗 API Status: {status}\n\n"
        f"📦 Session Pool:\n"
        f"  🔢 Number slots: {number_slots}/50\n"
        f"  🔑 OTP slots: {otp_slots}/50\n\n"
        f"📢 OTP Channel: {OTP_CHANNEL_ID}"
    )
    await update.message.reply_text(msg)

async def cmd_refreshsessions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔄 Session pool refresh হচ্ছে...")
    await session_pool.refresh_all()
    await update.message.reply_text(
        f"✅ Session pool refresh হয়েছে!\n"
        f"Number slots: {session_pool.number_sessions.qsize()}/50\n"
        f"OTP slots: {session_pool.otp_sessions.qsize()}/50"
    )

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    user_data[ADMIN_ID]["waiting_for"] = "broadcast"
    await update.message.reply_text("📢 সবাইকে কী message পাঠাবেন?")

async def cmd_get100on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global GET100_ENABLED
    if update.effective_user.id != ADMIN_ID:
        return
    GET100_ENABLED = True
    await update.message.reply_text("✅ Get 100 সবার জন্য চালু করা হয়েছে।")

async def cmd_get100off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global GET100_ENABLED
    if update.effective_user.id != ADMIN_ID:
        return
    GET100_ENABLED = False
    await update.message.reply_text("❌ Get 100 সবার জন্য বন্ধ করা হয়েছে।")

async def cmd_addget100(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /addget100 <user_id>")
        return
    try:
        uid = int(args[0])
        GET100_USERS.add(uid)
        await update.message.reply_text(f"✅ User {uid} কে Get 100 access দেওয়া হয়েছে।")
    except:
        await update.message.reply_text("❌ Invalid user ID.")

async def cmd_removeget100(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: /removeget100 <user_id>")
        return
    try:
        uid = int(args[0])
        GET100_USERS.discard(uid)
        await update.message.reply_text(f"❌ User {uid} এর Get 100 access সরানো হয়েছে।")
    except:
        await update.message.reply_text("❌ Invalid user ID.")

# =============================================
#         CALLBACK HANDLER
# =============================================

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    user_name = query.from_user.first_name or "User"
    init_user(user_id)
    user_data[user_id]["name"] = user_name
    data = query.data

    if data == "go_home":
        await query.message.reply_text(
            "📱 Service Select করুন:",
            reply_markup=app_select_inline()
        )
        return

    if data == "stop_auto":
        user_data[user_id]["auto_otp_cancel"] = True
        await query.answer("🛑 Auto OTP বন্ধ করা হয়েছে!")
        return

    if data.startswith("app_s1_"):
        app_name = data.replace("app_s1_", "")
        user_data[user_id]["app"] = app_name
        user_data[user_id]["panel"] = "S1"
        user_data[user_id]["country"] = None
        user_data[user_id]["carrier"] = None
        user_data[user_id]["range"] = None
        await query.edit_message_text(f"⏳ {app_name} লোড হচ্ছে...")
        countries = await get_countries_for_app(app_name, panel="S1")
        if not countries:
            await query.edit_message_text(
                f"❌ {app_name} এ এখন কোনো active country নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app")]])
            )
            return
        emoji = APP_EMOJIS.get(app_name, "📱")
        await query.edit_message_text(
            f"{emoji} {app_name}\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(countries, app_name)
        )

    elif data.startswith("app_s2_"):
        app_name = data.replace("app_s2_", "")
        user_data[user_id]["app"] = app_name
        user_data[user_id]["panel"] = "S2"
        user_data[user_id]["country"] = None
        user_data[user_id]["carrier"] = None
        user_data[user_id]["range"] = None
        await query.edit_message_text(f"⏳ {app_name} লোড হচ্ছে...")
        countries = await get_countries_for_app(app_name, panel="S2")
        if not countries:
            await query.edit_message_text(
                f"❌ {app_name} এ এখন কোনো active country নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app")]])
            )
            return
        emoji = APP_EMOJIS.get(app_name, "📱")
        await query.edit_message_text(
            f"{emoji} {app_name}\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(countries, app_name)
        )

    elif data == "back_app":
        await query.edit_message_text(
            "📱 Service Select করুন:",
            reply_markup=app_select_inline()
        )

    elif data.startswith("country_"):
        country = data.replace("country_", "")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        panel = user_data[user_id].get("panel", "S1")
        user_data[user_id]["country"] = country
        user_data[user_id]["carrier"] = None
        user_data[user_id]["range"] = None
        await query.edit_message_text("⏳ Range লোড হচ্ছে...")
        logs = await get_xmint_console_logs() if panel == "S2" else await get_console_logs()
        seen = set()
        ranges = []
        for log in logs:
            log_app = log.get("app_name", "").replace("*", "").strip().upper()
            log_country = log.get("country", "").strip()
            if log_app == app_name.upper() and log_country == country:
                r = log.get("range", "").strip()
                if r and r not in seen:
                    seen.add(r)
                    ranges.append({"range": r, "time": log.get("time", "")})
        if not ranges:
            await query.edit_message_text(
                f"❌ {country} তে কোনো range নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}")]])
            )
            return
        flag = get_flag(country)
        await query.edit_message_text(
            f"📱 {app_name}  |  {flag} {country}\n\n📡 Range select করুন:",
            reply_markup=range_select_inline(ranges, app_name, country, "")
        )

    elif data.startswith("back_country_"):
        app_name = data.replace("back_country_", "")
        panel = user_data[user_id].get("panel", "S1")
        user_data[user_id]["country"] = None
        await query.edit_message_text("⏳ Loading...")
        countries = await get_countries_for_app(app_name, panel=panel)
        emoji = APP_EMOJIS.get(app_name, "📱")
        await query.edit_message_text(
            f"{emoji} {app_name}\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(countries, app_name)
        )

    elif data.startswith("carrier_"):
        carrier = data.replace("carrier_", "")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        country = user_data[user_id].get("country", "")
        user_data[user_id]["carrier"] = carrier
        user_data[user_id]["range"] = None
        await query.edit_message_text("⏳ Range লোড হচ্ছে...")
        ranges = await get_ranges_for_carrier(app_name, country, carrier, panel=user_data[user_id].get("panel", "S1"))
        if not ranges:
            await query.edit_message_text(
                "❌ কোনো range পাওয়া যায়নি।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}")]])
            )
            return
        flag = get_flag(country)
        await query.edit_message_text(
            f"📱 {app_name}  |  {flag} {country}  |  📶 {carrier}\n\n📡 Range select করুন:",
            reply_markup=range_select_inline(ranges, app_name, country, carrier)
        )

    elif data.startswith("back_carrier_"):
        parts = data.replace("back_carrier_", "").split("|", 1)
        app_name = parts[0]
        country = parts[1] if len(parts) > 1 else user_data[user_id].get("country", "")
        user_data[user_id]["carrier"] = None
        carriers = await get_carriers_for_country(app_name, country, panel=user_data[user_id].get("panel", "S1"))
        flag = get_flag(country)
        await query.edit_message_text(
            f"📱 {app_name}  |  {flag} {country}\n\n📶 Carrier select করুন:",
            reply_markup=carrier_select_inline(carriers, app_name, country)
        )

    elif data.startswith("range_"):
        range_val = data.replace("range_", "")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        panel = user_data[user_id].get("panel", "S1")
        country = user_data[user_id].get("country", "")
        user_data[user_id]["range"] = range_val
        user_data[user_id]["auto_otp_cancel"] = False
        
        # Panel অনুযায়ী API call করো
        if panel == "S1":
            data_r = await api_get_number(range_val, app_name)
        else:
            data_r = await api_get_number_s2(range_val, app_name)
        
        if data_r.get("meta", {}).get("code") == 200:
            num = data_r["data"]
            number = num.get("number") or num.get("num") or "N/A"
            country_r = num.get("country", country)
            user_data[user_id]["last_number"] = number
            flag = get_flag(country_r)
            clean_number = str(number).replace("+", "").strip()
            await query.edit_message_text(
                f"✅ Number পাওয়া গেছে!\n\n"
                f"📞 `{clean_number}`\n"
                f"📱 {app_name}  {flag} {country_r}\n\n"
                f"🔍 OTP আসার অপেক্ষায়...",
                parse_mode="Markdown",
                reply_markup=after_number_inline(number, range_val)
            )
            asyncio.create_task(auto_otp_multi(query.message, [number], user_id, range_val, bot=context.bot))
        else:
            await query.edit_message_text(
                "❌ Number পাওয়া যায়নি!",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Again", callback_data=f"range_{range_val}")],
                    [InlineKeyboardButton("◀️ Back", callback_data="back_app")]
                ])
            )

    elif data.startswith("otp_"):
        number = data.replace("otp_", "")
        # ✅ Check OTP — bot pass করো channel forward এর জন্য
        await do_otp_check(query.message, number, user_id, bot=context.bot)

    elif data.startswith("same_"):
        range_val = data.replace("same_", "")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        panel = user_data[user_id].get("panel", "S1")
        country = user_data[user_id].get("country", "")
        user_data[user_id]["range"] = range_val
        user_data[user_id]["name"] = user_name
        user_data[user_id]["auto_otp_cancel"] = True
        await asyncio.sleep(0.1)
        user_data[user_id]["auto_otp_cancel"] = False
        
        # Panel অনুযায়ী API call করো
        if panel == "S1":
            data_r = await api_get_number(range_val, app_name)
        else:
            data_r = await api_get_number_s2(range_val, app_name)
        
        if data_r.get("meta", {}).get("code") == 200:
            num = data_r["data"]
            number = num.get("number") or num.get("num") or "N/A"
            country_r = num.get("country", country)
            user_data[user_id]["last_number"] = number
            flag = get_flag(country_r)
            clean_number = str(number).replace("+", "").strip()
            await query.edit_message_text(
                f"✅ Number পাওয়া গেছে!\n\n"
                f"📞 `{clean_number}`\n"
                f"📱 {app_name}  {flag} {country_r}\n\n"
                f"🔍 OTP আসার অপেক্ষায়...",
                parse_mode="Markdown",
                reply_markup=after_number_inline(number, range_val)
            )
            asyncio.create_task(auto_otp_multi(query.message, [number], user_id, range_val, bot=context.bot))
        else:
            await query.edit_message_text("❌ Number পাওয়া যায়নি!", reply_markup=main_keyboard(user_id))

    elif data.startswith("viewrange_"):
        range_val = data.replace("viewrange_", "")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        user_data[user_id]["range"] = range_val
        await query.message.reply_text(f"⏳ {range_val} থেকে numbers নেওয়া হচ্ছে...")
        results = []
        for _ in range(5):
            d = await api_get_number(range_val, app_name)
            if d.get("meta", {}).get("code") == 200:
                results.append(d["data"])
        if results:
            msg = f"📊 VIEW RANGE — {range_val}\n\n"
            for i, num in enumerate(results, 1):
                number = num.get("number") or num.get("num") or "N/A"
                flag = get_flag(num.get("country", ""))
                msg += f"{i}. {number} {flag} ✅\n"
            await query.message.reply_text(msg, reply_markup=main_keyboard(user_id))
        else:
            await query.message.reply_text("❌ Numbers পাওয়া যায়নি।", reply_markup=main_keyboard(user_id))

    elif data == "bulk_on":
        global GET100_ENABLED
        if user_id == ADMIN_ID:
            GET100_ENABLED = True
            await query.answer("✅ Bulk চালু হয়েছে!")
            await query.edit_message_reply_markup(reply_markup=admin_keyboard())
        return

    elif data == "bulk_off":
        if user_id == ADMIN_ID:
            GET100_ENABLED = False
            await query.answer("❌ Bulk বন্ধ হয়েছে!")
            await query.edit_message_reply_markup(reply_markup=admin_keyboard())
        return

    elif data == "admin_users":
        if user_id == ADMIN_ID:
            msg = f"👥 Total Users: {len(user_data)}\n\n"
            for uid, uinfo in list(user_data.items())[:15]:
                msg += f"• {uid}  —  {uinfo.get('name','?')}\n"
            await query.message.reply_text(msg)
        return

    elif data == "admin_stats":
        if user_id == ADMIN_ID:
            await query.message.reply_text(
                f"📊 BOT STATS\n\n"
                f"👥 Users: {len(user_data)}\n"
                f"📦 Bulk: {'✅ ON' if GET100_ENABLED else '❌ OFF'}\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
        return

    elif data.startswith("sendch_"):
        # Format: sendch_number_otp_app_country
        parts = data.replace("sendch_", "").split("_", 3)
        if len(parts) >= 4:
            ch_number, ch_otp, ch_app, ch_country = parts
        elif len(parts) == 3:
            ch_number, ch_otp, ch_app = parts
            ch_country = ""
        else:
            ch_number, ch_otp = parts[0], parts[1]
            ch_app, ch_country = "FACEBOOK", ""
        flag = get_flag(ch_country)
        await send_otp_to_channel(context.bot, ch_number, ch_otp, ch_app, ch_country, flag)
        await query.answer("✅ Channel এ পাঠানো হয়েছে!")

    elif data == "cancel":
        await query.message.reply_text("❌ বাতিল করা হয়েছে।", reply_markup=main_keyboard(user_id))

# =============================================
#         MESSAGE HANDLER
# =============================================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    user = update.effective_user
    user_id = user.id
    user_name = user.first_name or "User"
    init_user(user_id)
    user_data[user_id]["name"] = user_name
    waiting = user_data[user_id].get("waiting_for")

    joined = await check_joined(user_id, context.bot)
    if not joined:
        await update.message.reply_text(
            "⚠️ Channel Join করুন!\n\nJoin করে /start দিন।",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Channel Join করুন", url=CHANNEL_LINK)
            ]])
        )
        return

    if text == "🏠 Start":
        await start(update, context)
        return

    if text == "🎯 Custom Range":
        user_data[user_id]["waiting_for"] = "custom_range"
        await update.message.reply_text(
            "📡 Range লিখুন:\n\nউদাহরণ: 23762155XXX",
            reply_markup=main_keyboard(user_id)
        )
        return

    if user_data[user_id].get("waiting_for") == "custom_range":
        user_data[user_id]["waiting_for"] = None
        user_data[user_id]["range"] = text
        await do_get_number(update.message, user_id, count=1, user_name=user_name, bot=context.bot)
        return

    if text == "📋 My Numbers":
        await cmd_mynum(update, context)
        return

    if text == "📦 Bulk Number":
        if not has_get100_access(user_id):
            await update.message.reply_text(
                "❌ Bulk Number এখন বন্ধ আছে।\n\nAdmin চালু করলে use করতে পারবেন।"
            )
        else:
            await do_get_number(update.message, user_id, count=100, user_name=user_name, bot=context.bot)
        return

    if text == "👑 Admin Panel":
        if user_id == ADMIN_ID:
            get100_status = "✅ ON" if GET100_ENABLED else "❌ OFF"
            await update.message.reply_text(
                f"━━━━━━━━━━━━━━━━━━\n"
                f"👑  ADMIN PANEL\n"
                f"━━━━━━━━━━━━━━━━━━\n\n"
                f"📋  /allusers — সব users\n"
                f"📊  /stats — Bot stats\n"
                f"🔑  /apistatus — API status\n"
                f"📢  /broadcast — সবাইকে message\n\n"
                f"📦  Bulk Number: {get100_status}\n"
                f"/get100on — সবার জন্য চালু\n"
                f"/get100off — সবার জন্য বন্ধ\n"
                f"/addget100 <id> — নির্দিষ্ট user চালু\n"
                f"/removeget100 <id> — নির্দিষ্ট user বন্ধ\n\n"
                f"━━━━━━━━━━━━━━━━━━",
                reply_markup=admin_keyboard()
            )
        else:
            await update.message.reply_text("❌ Admin access নেই।")
        return

    if user_id == ADMIN_ID and waiting == "broadcast":
        user_data[user_id]["waiting_for"] = None
        sent = 0
        for uid in user_data:
            try:
                await context.bot.send_message(uid, f"📢 Admin Message:\n\n{text}")
                sent += 1
            except:
                pass
        await update.message.reply_text(f"✅ {sent} জন user কে message পাঠানো হয়েছে।")
        return

# =============================================
#              MAIN
# =============================================

async def post_init(application):
    await session_pool.initialize()
    await xmint_pool.initialize()
    logging.info("✅ StexSMS Pool (50/50) + X.Mint Pool (25/25) initialized!")

async def post_shutdown(application):
    """Bot shutdown হলে সব pending task cancel করো"""
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("✅ All tasks cancelled cleanly.")

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).read_timeout(30).write_timeout(30).connect_timeout(30).post_init(post_init).post_shutdown(post_shutdown).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("get", cmd_get))
    app.add_handler(CommandHandler("get100", cmd_get100))
    app.add_handler(CommandHandler("mynum", cmd_mynum))
    app.add_handler(CommandHandler("allusers", cmd_allusers))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("apistatus", cmd_apistatus))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("get100on", cmd_get100on))
    app.add_handler(CommandHandler("get100off", cmd_get100off))
    app.add_handler(CommandHandler("addget100", cmd_addget100))
    app.add_handler(CommandHandler("removeget100", cmd_removeget100))
    app.add_handler(CommandHandler("refreshsessions", cmd_refreshsessions))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    print("✅ Bot is running...")
    app.run_polling(drop_pending_updates=True, timeout=30)

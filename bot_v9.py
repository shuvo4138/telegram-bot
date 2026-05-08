import logging
import asyncio
import re
import random
import time
import httpx
import os
import json
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

load_dotenv()

logging.basicConfig(
    format='%(levelname)s - %(message)s',
    level=logging.INFO
)
# Suppress httpx and telegram verbose logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("hpack").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
#                    ENVIRONMENT CONFIG
# ══════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_ID = int(os.getenv("ADMIN_ID", "0").strip())

# Channel config
OTP_CHANNEL_ID = int(os.getenv("OTP_CHANNEL_ID", "0").strip())
OTP_CHANNEL_LINK = os.getenv("OTP_CHANNEL_LINK", "").strip()
JOIN_CHANNEL_USERNAME = "Foggred"
JOIN_CHANNEL_LINK = "https://t.me/Foggred"
CHANNEL2_USERNAME = os.getenv("CHANNEL2_USERNAME", "").strip()
CHANNEL2_LINK = os.getenv("CHANNEL2_LINK", "").strip()
CHANNEL2_NAME = os.getenv("CHANNEL2_NAME", "Backup Channel").strip()
RANGE_CHANNEL_ID = int(os.getenv("RANGE_CHANNEL_ID", os.getenv("OTP_CHANNEL_ID", "0")).strip())

# S3 Storage channel (numbers only)
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "0").strip())



# Channel buttons
MAIN_CHANNEL_LINK = os.getenv("OTP_CHANNEL_LINK", "").strip()
NUMBER_BOT_LINK = os.getenv("NUMBER_BOT_LINK", "").strip()

# S1 — StexSMS
STEXSMS_EMAIL = os.getenv("STEXSMS_EMAIL", "").strip()
STEXSMS_PASSWORD = os.getenv("STEXSMS_PASSWORD", "").strip()
S1_BASE_URL = "https://stexsms.com/mapi/v1"

# S2 — X.Mint
XMINT_EMAIL = os.getenv("XMINT_EMAIL", "").strip()
XMINT_PASSWORD = os.getenv("XMINT_PASSWORD", "").strip()
S2_BASE_URL = "https://x.mnitnetwork.com/mapi/v1"

# S3 — CR API
CR_API_URL = os.getenv("CR_API_URL", "").strip()
CR_API_TOKEN = os.getenv("CR_API_TOKEN", "").strip()

# Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

SUPPORT_ADMIN_LINK = os.getenv("SUPPORT_ADMIN_LINK", "https://t.me/admin").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").strip()
if not BOT_USERNAME:
    import warnings
    warnings.warn("⚠️  BOT_USERNAME is not set in .env! Bot links will be broken.", stacklevel=2)


# ══════════════════════════════════════════════════════════
#           SAFE INLINE BUTTON HELPER
# ══════════════════════════════════════════════════════════

def safe_channel_button(label="📢 Main Channel"):
    """
    Returns an InlineKeyboardButton for JOIN_CHANNEL_LINK.
    - If JOIN_CHANNEL_LINK is valid → url button
    - If empty → fallback url or noop callback_data (no crash)
    """
    link = JOIN_CHANNEL_LINK or MAIN_CHANNEL_LINK or OTP_CHANNEL_LINK or ""
    if link and len(link) > 10:
        return InlineKeyboardButton(label, url=link)
    fallback_url = OTP_CHANNEL_LINK or CHANNEL2_LINK or ""
    if fallback_url and len(fallback_url) > 10:
        return InlineKeyboardButton(label, url=fallback_url)
    return InlineKeyboardButton(label, callback_data="noop_channel")

# ══════════════════════════════════════════════════════════
#                    COUNTRY DATA
# ══════════════════════════════════════════════════════════

COUNTRY_FLAGS_CODE = {
    "1": "🇺🇸", "7": "🇷🇺", "20": "🇪🇬", "27": "🇿🇦", "30": "🇬🇷", "31": "🇳🇱",
    "32": "🇧🇪", "33": "🇫🇷", "34": "🇪🇸", "36": "🇭🇺", "39": "🇮🇹", "40": "🇷🇴",
    "41": "🇨🇭", "43": "🇦🇹", "44": "🇬🇧", "45": "🇩🇰", "46": "🇸🇪", "47": "🇳🇴",
    "48": "🇵🇱", "49": "🇩🇪", "51": "🇵🇪", "52": "🇲🇽", "53": "🇨🇺", "54": "🇦🇷",
    "55": "🇧🇷", "56": "🇨🇱", "57": "🇨🇴", "58": "🇻🇪", "60": "🇲🇾", "61": "🇦🇺",
    "62": "🇮🇩", "63": "🇵🇭", "64": "🇳🇿", "65": "🇸🇬", "66": "🇹🇭", "81": "🇯🇵",
    "82": "🇰🇷", "84": "🇻🇳", "86": "🇨🇳", "90": "🇹🇷", "91": "🇮🇳", "92": "🇵🇰",
    "93": "🇦🇫", "94": "🇱🇰", "95": "🇲🇲", "98": "🇮🇷", "212": "🇲🇦", "213": "🇩🇿",
    "216": "🇹🇳", "218": "🇱🇾", "220": "🇬🇲", "221": "🇸🇳", "222": "🇲🇷", "223": "🇲🇱",
    "224": "🇬🇳", "225": "🇨🇮", "226": "🇧🇫", "227": "🇳🇪", "228": "🇹🇬", "229": "🇧🇯",
    "230": "🇲🇺", "231": "🇱🇷", "232": "🇸🇱", "233": "🇬🇭", "234": "🇳🇬", "235": "🇹🇩",
    "236": "🇨🇫", "237": "🇨🇲", "238": "🇨🇻", "239": "🇸🇹", "240": "🇬🇶", "241": "🇬🇦",
    "242": "🇨🇬", "243": "🇨🇩", "244": "🇦🇴", "245": "🇬🇼", "248": "🇸🇨", "249": "🇸🇩",
    "250": "🇷🇼", "251": "🇪🇹", "252": "🇸🇴", "253": "🇩🇯", "254": "🇰🇪", "255": "🇹🇿",
    "256": "🇺🇬", "257": "🇧🇮", "258": "🇲🇿", "260": "🇿🇲", "261": "🇲🇬", "263": "🇿🇼",
    "264": "🇳🇦", "265": "🇲🇼", "266": "🇱🇸", "267": "🇧🇼", "268": "🇸🇿", "269": "🇰🇲",
    "291": "🇪🇷", "297": "🇦🇼", "350": "🇬🇮", "351": "🇵🇹", "352": "🇱🇺", "353": "🇮🇪",
    "354": "🇮🇸", "355": "🇦🇱", "356": "🇲🇹", "357": "🇨🇾", "358": "🇫🇮", "359": "🇧🇬",
    "370": "🇱🇹", "371": "🇱🇻", "372": "🇪🇪", "373": "🇲🇩", "374": "🇦🇲", "375": "🇧🇾",
    "376": "🇦🇩", "377": "🇲🇨", "380": "🇺🇦", "381": "🇷🇸", "382": "🇲🇪", "385": "🇭🇷",
    "386": "🇸🇮", "387": "🇧🇦", "389": "🇲🇰", "420": "🇨🇿", "421": "🇸🇰", "501": "🇧🇿",
    "502": "🇬🇹", "503": "🇸🇻", "504": "🇭🇳", "505": "🇳🇮", "506": "🇨🇷", "507": "🇵🇦",
    "509": "🇭🇹", "591": "🇧🇴", "592": "🇬🇾", "593": "🇪🇨", "595": "🇵🇾", "597": "🇸🇷",
    "598": "🇺🇾", "670": "🇹🇱", "673": "🇧🇳", "675": "🇵🇬", "676": "🇹🇴", "677": "🇸🇧",
    "678": "🇻🇺", "679": "🇫🇯", "685": "🇼🇸", "686": "🇰🇮", "688": "🇹🇻", "850": "🇰🇵",
    "852": "🇭🇰", "853": "🇲🇴", "855": "🇰🇭", "856": "🇱🇦", "880": "🇧🇩", "886": "🇹🇼",
    "960": "🇲🇻", "961": "🇱🇧", "962": "🇯🇴", "963": "🇸🇾", "964": "🇮🇶", "965": "🇰🇼",
    "966": "🇸🇦", "967": "🇾🇪", "968": "🇴🇲", "970": "🇵🇸", "971": "🇦🇪", "972": "🇮🇱",
    "973": "🇧🇭", "974": "🇶🇦", "975": "🇧🇹", "976": "🇲🇳", "977": "🇳🇵", "992": "🇹🇯",
    "993": "🇹🇲", "994": "🇦🇿", "995": "🇬🇪", "996": "🇰🇬", "998": "🇺🇿",
}

COUNTRY_NAMES_CODE = {
    "1": "USA", "7": "Russia", "20": "Egypt", "27": "South Africa", "30": "Greece",
    "31": "Netherlands", "32": "Belgium", "33": "France", "34": "Spain", "36": "Hungary",
    "39": "Italy", "40": "Romania", "41": "Switzerland", "43": "Austria", "44": "UK",
    "45": "Denmark", "46": "Sweden", "47": "Norway", "48": "Poland", "49": "Germany",
    "51": "Peru", "52": "Mexico", "53": "Cuba", "54": "Argentina", "55": "Brazil",
    "56": "Chile", "57": "Colombia", "58": "Venezuela", "60": "Malaysia", "61": "Australia",
    "62": "Indonesia", "63": "Philippines", "64": "New Zealand", "65": "Singapore",
    "66": "Thailand", "81": "Japan", "82": "South Korea", "84": "Vietnam", "86": "China",
    "90": "Turkey", "91": "India", "92": "Pakistan", "93": "Afghanistan", "94": "Sri Lanka",
    "95": "Myanmar", "98": "Iran", "212": "Morocco", "213": "Algeria", "216": "Tunisia",
    "218": "Libya", "220": "Gambia", "221": "Senegal", "222": "Mauritania", "223": "Mali",
    "224": "Guinea", "225": "Ivory Coast", "226": "Burkina Faso", "227": "Niger",
    "228": "Togo", "229": "Benin", "230": "Mauritius", "231": "Liberia", "232": "Sierra Leone",
    "233": "Ghana", "234": "Nigeria", "235": "Chad", "236": "CAR", "237": "Cameroon",
    "238": "Cape Verde", "239": "Sao Tome", "240": "Eq. Guinea", "241": "Gabon",
    "242": "Congo", "243": "DR Congo", "244": "Angola", "245": "Guinea-Bissau",
    "248": "Seychelles", "249": "Sudan", "250": "Rwanda", "251": "Ethiopia",
    "252": "Somalia", "253": "Djibouti", "254": "Kenya", "255": "Tanzania",
    "256": "Uganda", "257": "Burundi", "258": "Mozambique", "260": "Zambia",
    "261": "Madagascar", "263": "Zimbabwe", "264": "Namibia", "265": "Malawi",
    "266": "Lesotho", "267": "Botswana", "268": "Eswatini", "269": "Comoros",
    "291": "Eritrea", "351": "Portugal", "352": "Luxembourg", "353": "Ireland",
    "358": "Finland", "359": "Bulgaria", "370": "Lithuania", "371": "Latvia",
    "372": "Estonia", "373": "Moldova", "374": "Armenia", "375": "Belarus",
    "380": "Ukraine", "381": "Serbia", "385": "Croatia", "386": "Slovenia",
    "387": "Bosnia", "389": "N. Macedonia", "420": "Czech Republic", "421": "Slovakia",
    "880": "Bangladesh", "960": "Maldives", "961": "Lebanon", "962": "Jordan",
    "963": "Syria", "964": "Iraq", "965": "Kuwait", "966": "Saudi Arabia",
    "967": "Yemen", "968": "Oman", "970": "Palestine", "971": "UAE", "972": "Israel",
    "973": "Bahrain", "974": "Qatar", "975": "Bhutan", "976": "Mongolia",
    "977": "Nepal", "992": "Tajikistan", "993": "Turkmenistan", "994": "Azerbaijan",
    "995": "Georgia", "996": "Kyrgyzstan", "998": "Uzbekistan",
}

COUNTRY_FLAGS_ISO = {
    "CM": "🇨🇲", "VN": "🇻🇳", "PK": "🇵🇰", "TZ": "🇹🇿", "TJ": "🇹🇯", "TG": "🇹🇬",
    "NG": "🇳🇬", "GH": "🇬🇭", "KE": "🇰🇪", "BD": "🇧🇩", "IN": "🇮🇳", "PH": "🇵🇭",
    "ID": "🇮🇩", "MM": "🇲🇲", "KH": "🇰🇭", "ET": "🇪🇹", "CD": "🇨🇩", "MZ": "🇲🇿",
    "MG": "🇲🇬", "CI": "🇨🇮", "SN": "🇸🇳", "ML": "🇲🇱", "BF": "🇧🇫", "GN": "🇬🇳",
    "ZM": "🇿🇲", "ZW": "🇿🇼", "RW": "🇷🇼", "UG": "🇺🇬", "AO": "🇦🇴", "SD": "🇸🇩",
    "MR": "🇲🇷", "NE": "🇳🇪", "TD": "🇹🇩", "SO": "🇸🇴", "BI": "🇧🇮", "BJ": "🇧🇯",
    "MW": "🇲🇼", "SL": "🇸🇱", "LR": "🇱🇷", "CF": "🇨🇫", "GQ": "🇬🇶", "GA": "🇬🇦",
    "CG": "🇨🇬", "DJ": "🇩🇯", "ER": "🇪🇷", "GM": "🇬🇲", "GW": "🇬🇼", "CV": "🇨🇻",
    "ST": "🇸🇹", "KM": "🇰🇲", "SC": "🇸🇨", "MU": "🇲🇺", "ZA": "🇿🇦", "NA": "🇳🇦",
    "BW": "🇧🇼", "LS": "🇱🇸", "SZ": "🇸🇿", "EG": "🇪🇬", "LY": "🇱🇾", "TN": "🇹🇳",
    "DZ": "🇩🇿", "MA": "🇲🇦", "MX": "🇲🇽", "BR": "🇧🇷", "CO": "🇨🇴", "PE": "🇵🇪",
    "VE": "🇻🇪", "AR": "🇦🇷", "CL": "🇨🇱", "EC": "🇪🇨", "BO": "🇧🇴", "PY": "🇵🇾",
    "UY": "🇺🇾", "GY": "🇬🇾", "SR": "🇸🇷", "GT": "🇬🇹", "HN": "🇭🇳", "SV": "🇸🇻",
    "NI": "🇳🇮", "CR": "🇨🇷", "PA": "🇵🇦", "CU": "🇨🇺", "DO": "🇩🇴", "HT": "🇭🇹",
    "TH": "🇹🇭", "LA": "🇱🇦", "MY": "🇲🇾", "SG": "🇸🇬", "TL": "🇹🇱", "NP": "🇳🇵",
    "LK": "🇱🇰", "AF": "🇦🇫", "IR": "🇮🇷", "IQ": "🇮🇶", "SY": "🇸🇾", "YE": "🇾🇪",
    "SA": "🇸🇦", "AE": "🇦🇪", "QA": "🇶🇦", "KW": "🇰🇼", "BH": "🇧🇭", "OM": "🇴🇲",
    "JO": "🇯🇴", "LB": "🇱🇧", "PS": "🇵🇸", "AM": "🇦🇲", "AZ": "🇦🇿", "GE": "🇬🇪",
    "KZ": "🇰🇿", "UZ": "🇺🇿", "TM": "🇹🇲", "KG": "🇰🇬", "MN": "🇲🇳", "RU": "🇷🇺",
    "UA": "🇺🇦", "BY": "🇧🇾", "MD": "🇲🇩", "RO": "🇷🇴", "BG": "🇧🇬", "RS": "🇷🇸",
    "HR": "🇭🇷", "BA": "🇧🇦", "MK": "🇲🇰", "AL": "🇦🇱", "ME": "🇲🇪", "SI": "🇸🇮",
    "SK": "🇸🇰", "CZ": "🇨🇿", "PL": "🇵🇱", "HU": "🇭🇺", "AT": "🇦🇹", "CH": "🇨🇭",
    "DE": "🇩🇪", "FR": "🇫🇷", "ES": "🇪🇸", "IT": "🇮🇹", "PT": "🇵🇹", "GB": "🇬🇧",
    "IE": "🇮🇪", "NL": "🇳🇱", "BE": "🇧🇪", "LU": "🇱🇺", "DK": "🇩🇰", "SE": "🇸🇪",
    "NO": "🇳🇴", "FI": "🇫🇮", "IS": "🇮🇸", "US": "🇺🇸", "CA": "🇨🇦", "AU": "🇦🇺",
    "NZ": "🇳🇿", "JP": "🇯🇵", "KR": "🇰🇷", "CN": "🇨🇳", "TW": "🇹🇼", "HK": "🇭🇰",
}

COUNTRY_NAME_TO_ISO = {
    "cameroon": "CM", "vietnam": "VN", "pakistan": "PK", "tanzania": "TZ",
    "tajikistan": "TJ", "togo": "TG", "nigeria": "NG", "ghana": "GH",
    "kenya": "KE", "bangladesh": "BD", "india": "IN", "philippines": "PH",
    "indonesia": "ID", "myanmar": "MM", "cambodia": "KH", "ethiopia": "ET",
    "congo": "CD", "dr congo": "CD", "mozambique": "MZ", "madagascar": "MG",
    "ivory coast": "CI", "senegal": "SN", "mali": "ML", "burkina faso": "BF",
    "guinea": "GN", "zambia": "ZM", "zimbabwe": "ZW", "rwanda": "RW",
    "uganda": "UG", "angola": "AO", "sudan": "SD", "mauritania": "MR",
    "niger": "NE", "chad": "TD", "somalia": "SO", "burundi": "BI",
    "benin": "BJ", "malawi": "MW", "sierra leone": "SL", "liberia": "LR",
    "car": "CF", "gabon": "GA", "djibouti": "DJ", "eritrea": "ER",
    "gambia": "GM", "cape verde": "CV", "comoros": "KM", "seychelles": "SC",
    "mauritius": "MU", "south africa": "ZA", "namibia": "NA", "botswana": "BW",
    "lesotho": "LS", "eswatini": "SZ", "egypt": "EG", "libya": "LY",
    "tunisia": "TN", "algeria": "DZ", "morocco": "MA", "mexico": "MX",
    "brazil": "BR", "colombia": "CO", "peru": "PE", "venezuela": "VE",
    "argentina": "AR", "chile": "CL", "ecuador": "EC", "bolivia": "BO",
    "paraguay": "PY", "uruguay": "UY", "guyana": "GY", "suriname": "SR",
    "guatemala": "GT", "honduras": "HN", "el salvador": "SV", "nicaragua": "NI",
    "costa rica": "CR", "panama": "PA", "cuba": "CU", "haiti": "HT",
    "usa": "US", "united states": "US", "canada": "CA", "thailand": "TH",
    "laos": "LA", "malaysia": "MY", "singapore": "SG", "nepal": "NP",
    "sri lanka": "LK", "afghanistan": "AF", "iran": "IR", "iraq": "IQ",
    "syria": "SY", "yemen": "YE", "saudi arabia": "SA", "uae": "AE",
    "qatar": "QA", "kuwait": "KW", "bahrain": "BH", "oman": "OM",
    "jordan": "JO", "lebanon": "LB", "palestine": "PS", "armenia": "AM",
    "azerbaijan": "AZ", "georgia": "GE", "kazakhstan": "KZ", "uzbekistan": "UZ",
    "turkmenistan": "TM", "kyrgyzstan": "KG", "mongolia": "MN", "russia": "RU",
    "ukraine": "UA", "belarus": "BY", "moldova": "MD", "romania": "RO",
    "bulgaria": "BG", "serbia": "RS", "croatia": "HR", "bosnia": "BA",
    "north macedonia": "MK", "albania": "AL", "montenegro": "ME",
    "slovenia": "SI", "slovakia": "SK", "czech republic": "CZ", "poland": "PL",
    "hungary": "HU", "austria": "AT", "switzerland": "CH", "germany": "DE",
    "france": "FR", "spain": "ES", "italy": "IT", "portugal": "PT",
    "uk": "GB", "ireland": "IE", "netherlands": "NL", "belgium": "BE",
    "luxembourg": "LU", "denmark": "DK", "sweden": "SE", "norway": "NO",
    "finland": "FI", "iceland": "IS", "australia": "AU", "new zealand": "NZ",
    "japan": "JP", "south korea": "KR", "china": "CN", "taiwan": "TW",
    "hong kong": "HK",
}

APP_EMOJIS = {
    "FACEBOOK": "📘", "INSTAGRAM": "📸", "TIKTOK": "🎵",
    "SNAPCHAT": "👻", "TWITTER": "🐦", "GOOGLE": "🔍",
    "WHATSAPP": "💬", "TELEGRAM": "✈️", "CHATGPT": "🤖",
    "SHEIN": "👗", "VERIFY": "🔐", "WORLDFIRST": "🌏",
}

LOADING_TEXTS = [
    "⏳ Waiting for OTP...",
    "📡 Checking server...",
    "🔄 Scanning inbox...",
    "⌛ Please wait...",
    "🔍 Looking for OTP...",
]

# ══════════════════════════════════════════════════════════
#                    UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════

def escape_mdv2(text):
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(escape_chars), r'\\\1', str(text))

def get_flag_by_iso(code):
    if not code:
        return "🌍"
    name_key = code.lower().strip()
    if name_key in COUNTRY_NAME_TO_ISO:
        return COUNTRY_FLAGS_ISO.get(COUNTRY_NAME_TO_ISO[name_key], "🌍")
    short = code.upper().strip()[:2]
    if short in COUNTRY_FLAGS_ISO:
        return COUNTRY_FLAGS_ISO.get(short, "🌍")
    return "🌍"

def extract_otp(message):
    if not message:
        return None
    patterns = [
        r'\b(\d{8}|\d{6}|\d{5})\b',
        r'\b(\d{3} \d{3})\b',
        r'\b(\d{2} \d{3})\b',
        r'\b(\d{4,8})\b',
    ]
    for pattern in patterns:
        match = re.search(pattern, message, re.IGNORECASE)
        if match:
            otp = match.group(1).replace(" ", "")
            if not re.fullmatch(r'0+', otp):
                return otp
    return None

def extract_country_code_from_number(number):
    for length in [3, 2, 1]:
        code = number[:length]
        if code in COUNTRY_NAMES_CODE:
            return code
    return "Unknown"

def hide_number(number):
    if len(number) <= 5:
        return number
    return number[:4] + "xxxx" + number[-3:]

def detect_app_from_message(message, default_app="FACEBOOK"):
    if not message:
        return default_app
    msg_lower = message.lower()
    for app in ["facebook", "instagram", "tiktok", "snapchat", "twitter",
                "google", "whatsapp", "telegram"]:
        if app in msg_lower:
            return app.upper()
    return default_app

# Rate limiting
user_last_action = {}

def is_rate_limited(user_id):
    now = time.time()
    if now - user_last_action.get(user_id, 0) < 2:
        return True
    user_last_action[user_id] = now
    return False

# ══════════════════════════════════════════════════════════
#              S3 — SUPABASE STORAGE (Numbers)
# ══════════════════════════════════════════════════════════

numbers_pool = {}
s3_user_sessions = {}
s3_users_db = {}
otp_cache = {}

# ── S3 Supabase number save ──
async def _save_numbers(bot=None):
    """Save numbers_pool to Supabase s3_numbers table (soft delete system)."""
    try:
        rows = []
        for pool_key, nums in numbers_pool.items():
            for number in nums:
                rows.append({
                    "number": number,
                    "pool_key": pool_key,
                    "status": "available"
                })
        if not rows:
            return
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                headers={**_sb_headers(), "Prefer": "resolution=merge-duplicates"},
                json=rows
            )
    except Exception as e:
        logger.error(f"_save_numbers error: {e}")

async def _save_users_s3(bot=None):
    """Save s3_users_db to Supabase s3_users table."""
    try:
        if not s3_users_db:
            return
        rows = [
            {"user_id": uid, "username": val.get("username", uid), "joined": val.get("joined", "")}
            for uid, val in s3_users_db.items()
        ]
        async with httpx.AsyncClient(timeout=15) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/s3_users",
                headers={**_sb_headers(), "Prefer": "resolution=merge-duplicates"},
                json=rows
            )
    except Exception as e:
        logger.error(f"_save_users_s3 error: {e}")

async def tg_load_all(bot):
    """Load S3 numbers from Supabase (Telegram channel system replaced)."""
    global numbers_pool, s3_users_db

    # Load numbers from Supabase s3_numbers table
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params={"select": "*", "status": "eq.available"},
                headers=_sb_headers()
            )
        rows = res.json()
        if isinstance(rows, list):
            for row in rows:
                pk = row.get("pool_key", "")
                num = row.get("number", "")
                if pk and num:
                    if pk not in numbers_pool:
                        numbers_pool[pk] = []
                    if num not in numbers_pool[pk]:
                        numbers_pool[pk].append(num)
            logger.info(f"✅ S3 Numbers loaded from Supabase: {len(numbers_pool)} pools")
    except Exception as e:
        logger.error(f"Load numbers error: {e}")

    # Load s3_users from Supabase
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/s3_users",
                params={"select": "*"},
                headers=_sb_headers()
            )
        rows = res.json()
        if isinstance(rows, list):
            for row in rows:
                uid = str(row["user_id"])
                s3_users_db[uid] = {
                    "username": row.get("username", uid),
                    "joined": row.get("joined", "")
                }
            logger.info(f"✅ S3 Users loaded: {len(s3_users_db)}")
    except Exception as e:
        logger.error(f"Load users error: {e}")
# S3 pool helpers
def get_numbers_pool():
    return numbers_pool

def get_pool_numbers(pool_key):
    return numbers_pool.get(pool_key, [])

async def add_numbers_to_pool(bot, pool_key, new_numbers):
    existing = set(numbers_pool.get(pool_key, []))
    added = skipped = 0
    new_rows = []
    for n in new_numbers:
        if n not in existing:
            existing.add(n)
            added += 1
            new_rows.append({"number": n, "pool_key": pool_key, "status": "available"})
        else:
            skipped += 1
    numbers_pool[pool_key] = list(existing)
    # Save to Supabase
    if new_rows:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                await client.post(
                    f"{SUPABASE_URL}/rest/v1/s3_numbers",
                    headers={**_sb_headers(), "Prefer": "resolution=merge-duplicates"},
                    json=new_rows
                )
        except Exception as e:
            logger.error(f"add_numbers_to_pool Supabase error: {e}")
    return added, skipped

async def remove_number_from_pool(bot, pool_key, number):
    """Soft delete — status = dead (history থাকবে)."""
    nums = numbers_pool.get(pool_key, [])
    if number in nums:
        nums.remove(number)
        numbers_pool[pool_key] = nums
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params={"number": f"eq.{number}"},
                headers=_sb_headers(),
                json={"status": "dead"}
            )
    except Exception as e:
        logger.error(f"remove_number_from_pool error: {e}")

async def mark_number_assigned(number, user_id):
    """available → assigned"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params={"number": f"eq.{number}"},
                headers=_sb_headers(),
                json={"status": "assigned", "assigned_to": str(user_id)}
            )
    except Exception as e:
        logger.error(f"mark_number_assigned error: {e}")

async def mark_number_used(number):
    """assigned → used"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params={"number": f"eq.{number}"},
                headers=_sb_headers(),
                json={"status": "used"}
            )
    except Exception as e:
        logger.error(f"mark_number_used error: {e}")

def count_numbers(pool_key):
    return len(numbers_pool.get(pool_key, []))

def parse_pool_key(pool_key):
    parts = pool_key.split("_")
    code = parts[0]
    slot = parts[1].upper() if len(parts) >= 2 else ""
    return code, slot

def get_button_label(pool_key):
    code, slot = parse_pool_key(pool_key)
    flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
    name = COUNTRY_NAMES_CODE.get(code, code)
    if slot:
        return f"{flag} {name} Facebook {slot}"
    return f"{flag} {name} Facebook"

def get_short_label(pool_key):
    code, slot = parse_pool_key(pool_key)
    flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
    name = COUNTRY_NAMES_CODE.get(code, "Unknown")
    if slot:
        return f"{flag} {name} Facebook ({slot})"
    return f"{flag} {name} Facebook"

# S3 session helpers
def s3_add_user(user_id, username):
    uid = str(user_id)
    if uid not in s3_users_db:
        s3_users_db[uid] = {
            "username": username or str(user_id),
            "joined": datetime.now().strftime("%Y-%m-%d")
        }
        return True
    return False

def s3_is_new_user(user_id):
    return str(user_id) not in s3_users_db

def s3_get_all_users():
    return list(s3_users_db.keys())

def s3_get_user_count():
    return len(s3_users_db)

def s3_set_session(user_id, number, pool_key):
    s3_user_sessions[str(user_id)] = {
        "number": number,
        "pool_key": pool_key,
        "assigned_time": datetime.now().isoformat()
    }

def s3_get_session(user_id):
    return s3_user_sessions.get(str(user_id))

def s3_find_users_by_number(number):
    matched = []
    for uid, session in s3_user_sessions.items():
        if session.get("number") == number:
            try:
                session_time = datetime.fromisoformat(session["assigned_time"])
                if datetime.now() - session_time < timedelta(minutes=30):
                    matched.append(uid)
            except Exception:
                matched.append(uid)
    return matched

# ══════════════════════════════════════════════════════════
#              SUPABASE (S1/S2 user database)
# ══════════════════════════════════════════════════════════

def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

async def db_save_user_async(user_id, data: dict):
    try:
        payload = {
            "user_id": user_id,
            "name": data.get("name", "User"),
            "joined": data.get("joined", datetime.now().strftime("%Y-%m-%d %H:%M")),
            "app": data.get("app", "FACEBOOK"),
            "panel": data.get("panel", "S1"),
            "country": data.get("country"),
            "range": data.get("range"),
            "last_number": data.get("last_number"),
        }
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/users",
                json=payload,
                headers=_sb_headers()
            )
    except Exception as e:
        logger.error(f"DB save error: {e}")

async def db_is_posted(unique_id: str) -> bool:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/posted_sms",
                params={"unique_id": f"eq.{unique_id}", "select": "unique_id"},
                headers=_sb_headers()
            )
        return len(res.json()) > 0
    except Exception:
        return False

async def db_mark_posted(unique_id: str):
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/posted_sms",
                json={"unique_id": unique_id},
                headers=_sb_headers()
            )
    except Exception as e:
        logger.error(f"DB mark_posted error: {e}")

# ══════════════════════════════════════════════════════════
#              S1 SESSION POOL (StexSMS)
# ══════════════════════════════════════════════════════════

class S1SessionPool:
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
            number_count = otp_count = 0
            for i in range(100):
                r = await self._login_once()
                if isinstance(r, dict) and r.get("token"):
                    self.all_sessions.append(r)
                    if number_count < 50:
                        await self.number_sessions.put(r)
                        number_count += 1
                    elif otp_count < 50:
                        await self.otp_sessions.put(r)
                        otp_count += 1
                await asyncio.sleep(0.5)
            self.initialized = True
            logger.info(f"✅ S1 Pool ready! Number: {number_count}, OTP: {otp_count}")

    async def _login_once(self):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    f"{S1_BASE_URL}/mauth/login",
                    json={"email": STEXSMS_EMAIL, "password": STEXSMS_PASSWORD}
                )
            if res.status_code != 200:
                return {}
            data = res.json()
            if data.get("meta", {}).get("code") == 200:
                token = data["data"].get("token")
                session_token = data["data"].get("session_token")
                if token:
                    return {"token": token, "session": session_token, "time": time.time()}
        except Exception as e:
            logger.error(f"S1 Login error: {e}")
        return {}

    async def get_number_session(self):
        try:
            session = await asyncio.wait_for(self.number_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                new_session = await self._login_once()
                if new_session.get("token"):
                    return new_session
                session["time"] = time.time()
            return session
        except asyncio.TimeoutError:
            new_session = await self._login_once()
            if new_session.get("token"):
                return new_session
            if self.all_sessions:
                return self.all_sessions[0]
            return {}

    async def get_otp_session(self):
        try:
            session = await asyncio.wait_for(self.otp_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                new_session = await self._login_once()
                if new_session.get("token"):
                    return new_session
                session["time"] = time.time()
            return session
        except asyncio.TimeoutError:
            new_session = await self._login_once()
            if new_session.get("token"):
                return new_session
            if self.all_sessions:
                return self.all_sessions[0]
            return {}

    async def return_number_session(self, session):
        if session and session.get("token"):
            await self.number_sessions.put(session)

    async def return_otp_session(self, session):
        if session and session.get("token"):
            await self.otp_sessions.put(session)

    async def refresh_all(self):
        async with self.lock:
            self.initialized = False
            while not self.number_sessions.empty():
                try: self.number_sessions.get_nowait()
                except: break
            while not self.otp_sessions.empty():
                try: self.otp_sessions.get_nowait()
                except: break
            self.all_sessions.clear()
        await self.initialize()

class S2SessionPool:
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
            results = []
            for i in range(22):
                r = await self._login_once()
                results.append(r)
                await asyncio.sleep(1)
            number_count = 0
            otp_count = 0
            for r in results:
                if isinstance(r, dict) and r.get("token"):
                    self.all_sessions.append(r)
                    if number_count < 15:
                        await self.number_sessions.put(r)
                        number_count += 1
                    elif otp_count < 7:
                        await self.otp_sessions.put(r)
                        otp_count += 1
            self.initialized = True
            logger.info(f"✅ S2 Pool ready! Number: {number_count}, OTP: {otp_count}")

    async def _login_once(self):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    f"{S2_BASE_URL}/mauth/login",
                    json={"email": XMINT_EMAIL, "password": XMINT_PASSWORD}
                )
            if res.status_code == 403:
                return {}
            if res.status_code != 200:
                return {}
            try:
                data = res.json()
            except Exception:
                return {}
            if data.get("meta", {}).get("code") == 200:
                token = data["data"].get("token")
                if token:
                    return {"token": token, "session": "", "time": time.time()}
        except Exception as e:
            logger.error(f"S2 Login error: {e}")
        return {}

    async def get_number_session(self):
        try:
            session = await asyncio.wait_for(self.number_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                new_session = await self._login_once()
                if new_session.get("token"):
                    return new_session
                session["time"] = time.time()
            return session
        except asyncio.TimeoutError:
            new_session = await self._login_once()
            if new_session.get("token"):
                return new_session
            if self.all_sessions:
                return self.all_sessions[0]
            return {}

    async def get_otp_session(self):
        try:
            session = await asyncio.wait_for(self.otp_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > 1500:
                new_session = await self._login_once()
                if new_session.get("token"):
                    return new_session
                session["time"] = time.time()
            return session
        except asyncio.TimeoutError:
            new_session = await self._login_once()
            if new_session.get("token"):
                return new_session
            if self.all_sessions:
                return self.all_sessions[0]
            return {}

    async def return_number_session(self, session):
        if session and session.get("token"):
            await self.number_sessions.put(session)

    async def return_otp_session(self, session):
        if session and session.get("token"):
            await self.otp_sessions.put(session)

    async def refresh_all(self):
        async with self.lock:
            self.initialized = False
            while not self.number_sessions.empty():
                try: self.number_sessions.get_nowait()
                except: break
            while not self.otp_sessions.empty():
                try: self.otp_sessions.get_nowait()
                except: break
            self.all_sessions.clear()
        await self.initialize()

s1_pool = S1SessionPool()
s2_pool = S2SessionPool()

# ══════════════════════════════════════════════════════════
#              S1/S2 API FUNCTIONS
# ══════════════════════════════════════════════════════════

def get_s1_headers(token, session):
    return {
        'User-Agent': "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        'Accept': "application/json, text/plain, */*",
        'Content-Type': "application/json",
        'mauthtoken': token,
        'Cookie': f"mauthtoken={token}; session_token={session}"
    }

def get_s2_headers(token):
    return {
        'User-Agent': "Mozilla/5.0 (Linux; Android 10)",
        'Accept': "application/json",
        'Content-Type': "application/json",
        'mauthtoken': token,
        'Cookie': f"mautToken={token}"
    }

async def api_get_number_s1(range_val, app_name="FACEBOOK", _retry=0):
    clean_range = ''.join(c for c in range_val.upper() if c.isdigit() or c == 'X')
    if not clean_range:
        return {"error": "Invalid range"}, None
    base = clean_range.rstrip('X')
    clean_range = base + 'XXX'

    session = await s1_pool.get_number_session()
    try:
        token = session.get("token")
        sess = session.get("session", "")
        if not token:
            return {"error": "No session available"}, None

        payload = {"range": clean_range, "isNational": False, "isRemovePlus": True, "app": app_name}
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{S1_BASE_URL}/mdashboard/getnum/number",
                json=payload,
                headers=get_s1_headers(token, sess)
            )

        if res.status_code == 403:
            new_session = await s1_pool._login_once()
            if new_session.get("token") and _retry < 2:
                await asyncio.sleep(1)
                return await api_get_number_s1(range_val, app_name, _retry=_retry + 1)
            return {"error": "session_expired"}, None

        if res.status_code in (429, 503):
            await s1_pool.return_number_session(session)
            if _retry < 3:
                await asyncio.sleep(10 * (2 ** _retry))
                return await api_get_number_s1(range_val, app_name, _retry=_retry + 1)
            return {"error": f"HTTP {res.status_code}"}, None

        if res.status_code != 200:
            await s1_pool.return_number_session(session)
            return {"error": f"HTTP {res.status_code}"}, None

        result = res.json()
        if result.get("meta", {}).get("code") != 200:
            msg = str(result.get("message", "")).lower()
            if any(k in msg for k in ["block", "rate", "limit", "many", "temporary"]):
                await s1_pool.return_number_session(session)
                if _retry < 3:
                    await asyncio.sleep(10 * (2 ** _retry))
                    return await api_get_number_s1(range_val, app_name, _retry=_retry + 1)
            return result, None
        return result, session

    except Exception as e:
        logger.error(f"api_get_number_s1 error: {e}")
        if session and session.get("token"):
            await s1_pool.return_number_session(session)
        return {"error": str(e)}, None

async def api_get_number_s2(range_val, app_name="FACEBOOK", _retry=0):
    clean_range = ''.join(c for c in range_val.upper() if c.isdigit() or c == 'X')
    if not clean_range:
        return {"error": "Invalid range"}, None
    base = clean_range.rstrip('X')
    clean_range = base + 'XXX'

    session = await s2_pool.get_number_session()
    try:
        token = session.get("token")
        if not token:
            return {"error": "No session available"}, None

        payload = {"range": clean_range, "isNational": False, "isRemovePlus": True, "app": app_name}
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{S2_BASE_URL}/mdashboard/getnum/number",
                json=payload,
                headers=get_s2_headers(token)
            )

        if res.status_code == 403:
            new_session = await s2_pool._login_once()
            if new_session.get("token") and _retry < 2:
                await asyncio.sleep(1)
                return await api_get_number_s2(range_val, app_name, _retry=_retry + 1)
            return {"error": "session_expired"}, None

        if res.status_code in (429, 503):
            await s2_pool.return_number_session(session)
            if _retry < 3:
                await asyncio.sleep(10 * (2 ** _retry))
                return await api_get_number_s2(range_val, app_name, _retry=_retry + 1)
            return {"error": f"HTTP {res.status_code}"}, None

        if res.status_code != 200:
            await s2_pool.return_number_session(session)
            return {"error": f"HTTP {res.status_code}"}, None

        result = res.json()
        if result.get("meta", {}).get("code") != 200:
            msg = str(result.get("message", "")).lower()
            if any(k in msg for k in ["block", "rate", "limit", "many", "temporary"]):
                await s2_pool.return_number_session(session)
                if _retry < 3:
                    await asyncio.sleep(10 * (2 ** _retry))
                    return await api_get_number_s2(range_val, app_name, _retry=_retry + 1)
            return result, None
        return result, session

    except Exception as e:
        logger.error(f"api_get_number_s2 error: {e}")
        if session and session.get("token"):
            await s2_pool.return_number_session(session)
        return {"error": str(e)}, None

async def api_get_info_s1(search="", status="", saved_session=None):
    session = saved_session if saved_session and saved_session.get("token") else await s1_pool.get_otp_session()
    _from_pool = not (saved_session and saved_session.get("token"))
    try:
        token = session.get("token")
        sess = session.get("session", "")
        if not token:
            return {"error": "No session"}
        today = datetime.now().strftime("%Y-%m-%d")
        clean_search = search.replace("+", "").strip()
        params = {"date": today, "page": 1, "search": clean_search, "status": status}
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{S1_BASE_URL}/mdashboard/getnum/info",
                params=params,
                headers=get_s1_headers(token, sess)
            )
        return res.json()
    except Exception as e:
        logger.error(f"api_get_info_s1 error: {e}")
        return {"error": str(e)}
    finally:
        if _from_pool:
            await s1_pool.return_otp_session(session)

async def api_get_info_s2(search="", status="", saved_session=None):
    session = saved_session if saved_session and saved_session.get("token") else await s2_pool.get_otp_session()
    _from_pool = not (saved_session and saved_session.get("token"))
    try:
        token = session.get("token")
        if not token:
            return {"error": "No session"}
        today = datetime.now().strftime("%Y-%m-%d")
        clean_search = search.replace("+", "").strip()
        params = {"date": today, "page": 1, "search": clean_search, "status": status}
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{S2_BASE_URL}/mdashboard/getnum/info",
                params=params,
                headers=get_s2_headers(token)
            )
        return res.json()
    except Exception as e:
        logger.error(f"api_get_info_s2 error: {e}")
        return {"error": str(e)}
    finally:
        if _from_pool:
            await s2_pool.return_otp_session(session)

# ══════════════════════════════════════════════════════════
#              CONSOLE / RANGE DATA (S1/S2)
# ══════════════════════════════════════════════════════════

_s1_console_cache = {"logs": [], "time": 0}
_s2_console_cache = {"logs": [], "time": 0}
CONSOLE_CACHE_TTL = 300

async def get_console_logs_s1(force=False):
    global _s1_console_cache
    if not force and _s1_console_cache["logs"] and (time.time() - _s1_console_cache["time"]) < CONSOLE_CACHE_TTL:
        return _s1_console_cache["logs"]
    try:
        session = await s1_pool.get_otp_session()
        token = session.get("token")
        sess = session.get("session", "")
        await s1_pool.return_otp_session(session)
        if not token:
            return _s1_console_cache["logs"]
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{S1_BASE_URL}/mdashboard/console/info",
                headers=get_s1_headers(token, sess)
            )
        data = res.json()
        if data.get("meta", {}).get("code") == 200:
            logs = data["data"].get("logs", [])
            _s1_console_cache = {"logs": logs, "time": time.time()}
            return logs
        return _s1_console_cache["logs"]
    except Exception as e:
        logger.error(f"S1 console error: {e}")
        return _s1_console_cache["logs"]

async def get_console_logs_s2(force=False):
    global _s2_console_cache
    if not force and _s2_console_cache["logs"] and (time.time() - _s2_console_cache["time"]) < CONSOLE_CACHE_TTL:
        return _s2_console_cache["logs"]
    try:
        session = await s2_pool.get_otp_session()
        token = session.get("token")
        await s2_pool.return_otp_session(session)
        if not token:
            return _s2_console_cache["logs"]
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.get(
                f"{S2_BASE_URL}/mdashboard/console/info",
                headers=get_s2_headers(token)
            )
        if res.status_code != 200 or not res.text.strip():
            return _s2_console_cache["logs"]
        data = res.json()
        if data.get("meta", {}).get("code") == 200:
            logs = data["data"].get("logs", [])
            _s2_console_cache = {"logs": logs, "time": time.time()}
            return logs
        return _s2_console_cache["logs"]
    except Exception as e:
        logger.error(f"S2 console error: {e}")
        return _s2_console_cache["logs"]

async def get_countries_for_app(app_name, panel="S1"):
    logs = await get_console_logs_s2() if panel == "S2" else await get_console_logs_s1()
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

async def get_all_ranges_for_country(app_name, country, panel="S1"):
    logs = await get_console_logs_s2() if panel == "S2" else await get_console_logs_s1()
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
    return ranges

# ══════════════════════════════════════════════════════════
#              S3 — CR API
# ══════════════════════════════════════════════════════════

def fetch_cr_api_otps():
    try:
        now = datetime.now()
        dt2 = now.strftime("%Y-%m-%d %H:%M:%S")
        dt1 = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        params = {"token": CR_API_TOKEN, "dt1": dt1, "dt2": dt2, "records": 200}
        response = requests.get(CR_API_URL, params=params, timeout=15)
        if response.status_code != 200:
            return []
        raw = response.text.strip()
        if not raw:
            return []
        try:
            data = response.json()
        except Exception:
            logger.warning(f"CR API invalid JSON: {repr(raw[:100])}")
            return []
        if data.get("status") != "success":
            return []
        result = []
        for row in data.get("data", []):
            try:
                otp_dict = {
                    "dt": str(row.get("dt", "")).strip(),
                    "num": str(row.get("num", "")).strip().lstrip("+"),
                    "cli": str(row.get("cli", "")).strip().upper(),
                    "message": str(row.get("message", "")).strip(),
                }
                if otp_dict["num"] and otp_dict["message"]:
                    result.append(otp_dict)
            except:
                continue
        return result
    except Exception as e:
        logger.error(f"CR API Error: {e}")
        return []

# ══════════════════════════════════════════════════════════
#              OTP CHANNEL SENDER
# ══════════════════════════════════════════════════════════

async def safe_send_message(bot, chat_id, text, **kwargs):
    while True:
        try:
            return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except Exception as e:
            err = str(e).lower()
            if "retry after" in err or "flood" in err:
                wait = int(re.search(r'\d+', str(e)).group() or 5)
                await asyncio.sleep(wait + 1)
            else:
                raise

async def send_otp_to_channel(bot, number, otp, app, country, flag, raw_sms="", panel="S1"):
    try:
        app_cap = app.capitalize()
        clean_num = str(number).replace("+", "").strip()
        hidden_num = ("+" + clean_num[:5] + "xxxx" + clean_num[-3:]) if len(clean_num) > 8 else clean_num

        msg = (
            f"{flag} {escape_mdv2(country)}\n\n"
            f"📞 `{escape_mdv2(hidden_num)}`\n"
            f"🔐 `{otp}`\n"
            f"💬 Service: {escape_mdv2(app_cap)} \\[{escape_mdv2(panel)}\\]\n"
            f"{escape_mdv2('────────────')}\n"
            f"📩"
        )
        if raw_sms:
            quoted = "\n".join(f">{escape_mdv2(line)}" for line in raw_sms.splitlines() if line.strip())
            msg += f"\n{quoted}"

        # ✅ FIX: Keyboard safe বানাও — empty/invalid URL থাকলে button বাদ দাও
        kb_row = []
        _otp_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
        if _otp_ch_link and len(_otp_ch_link) > 10:
            kb_row.append(InlineKeyboardButton("📢 Main Channel", url=_otp_ch_link))
        if BOT_USERNAME:
            kb_row.append(InlineKeyboardButton("🤖 Number Bot", url=f"https://t.me/{BOT_USERNAME}"))
        keyboard = InlineKeyboardMarkup([kb_row]) if kb_row else None

        try:
            await safe_send_message(bot, OTP_CHANNEL_ID, msg, parse_mode="MarkdownV2", reply_markup=keyboard)
        except Exception:
            plain = f"{flag} {country}\n📞 {hidden_num}\n🔐 {otp}\n💬 {app_cap} [{panel}]"
            if raw_sms:
                plain += f"\n📩 {raw_sms[:200]}"
            await safe_send_message(bot, OTP_CHANNEL_ID, plain, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Channel OTP send error: {e}")

# ══════════════════════════════════════════════════════════
#              USER DATA (S1/S2)
# ══════════════════════════════════════════════════════════

user_data = {}
user_locks = {}
user_msg = {}
user_range_msg = {}
user_kb_msg = {}
_otp_tasks = {}
_posted_sms_ids = set()

# ── S1/S2 request lock (duplicate panel prevention) ──
processing_users = set()

# Deploy হলে এই time এর আগের OTP গুলো skip করা হবে
BOT_START_TIME = datetime.now()

GET100_ENABLED = False
GET100_USERS = set()

def has_get100_access(user_id):
    return GET100_ENABLED or user_id in GET100_USERS or user_id == ADMIN_ID

def init_user(user_id):
    if user_id not in user_data:
        user_data[user_id] = {
            "name": "User",
            "joined": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "app": "FACEBOOK",
            "panel": "S1",
            "country": None,
            "range": None,
            "last_number": None,
            "waiting_for": None,
            "otp_active": False,
            "otp_running": False,
            "auto_otp_cancel": False,
            "number_session": None,
            "country_r": None,
        }

def cancel_all_otp_tasks(user_id):
    if user_id in _otp_tasks:
        for task in _otp_tasks[user_id]:
            task.cancel()
        _otp_tasks[user_id] = []
    if user_id in user_data:
        user_data[user_id]["auto_otp_cancel"] = True
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False

def add_otp_task(user_id, task):
    if user_id not in _otp_tasks:
        _otp_tasks[user_id] = []
    _otp_tasks[user_id].append(task)
    # ── OTP task cleanup (নোট ২) ──
    _otp_tasks[user_id] = [t for t in _otp_tasks[user_id] if not t.done()]

async def cleanup_s1s2_panel(bot, user_id):
    """S1/S2 ONLY — পুরনো panel message delete করো। S3 তে ব্যবহার করা যাবে না।"""
    try:
        old_msg = user_kb_msg.get(user_id)
        if old_msg:
            try:
                await bot.delete_message(chat_id=user_id, message_id=old_msg)
            except Exception:
                pass
    except Exception:
        pass
    user_kb_msg.pop(user_id, None)

# Join cache
_join_cache = {}

async def check_all_channels_joined(user_id, bot):
    now = time.time()
    cache_key1 = f"{user_id}_ch1"
    cache_key2 = f"{user_id}_ch2"

    c1 = _join_cache.get(cache_key1)
    if c1 and (now - c1["time"]) < 300:
        ch1_joined = c1["joined"]
    else:
        try:
            m = await bot.get_chat_member(JOIN_CHANNEL_USERNAME, user_id)
            ch1_joined = m.status in ["member", "administrator", "creator"]
            _join_cache[cache_key1] = {"joined": ch1_joined, "time": now}
        except:
            ch1_joined = True

    c2 = _join_cache.get(cache_key2)
    if c2 and (now - c2["time"]) < 300:
        ch2_joined = c2["joined"]
    else:
        try:
            m = await bot.get_chat_member(CHANNEL2_USERNAME, user_id)
            ch2_joined = m.status in ["member", "administrator", "creator"]
            _join_cache[cache_key2] = {"joined": ch2_joined, "time": now}
        except:
            ch2_joined = True

    return ch1_joined, ch2_joined

def clear_join_cache(user_id):
    _join_cache.pop(f"{user_id}_ch1", None)
    _join_cache.pop(f"{user_id}_ch2", None)

# ══════════════════════════════════════════════════════════
#              KEYBOARDS
# ══════════════════════════════════════════════════════════

START_MENU_TEXT = (
    "╔══════════════════════╗\n"
    "   🎉 NUMBER PANEL BOT 🎉\n"
    "╚══════════════════════╝\n\n"
    "⚡ Super Fast OTP Service\n"
    "🌍 Worldwide Numbers\n"
    "🔒 100% Secure & Trusted\n"
    "🕐 24/7 Active Service\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    "👇 নিচের Menu থেকে শুরু করুন!"
)

def main_keyboard(user_id):
    buttons = [
        [KeyboardButton("📲 Get Number"), KeyboardButton("📡 Custom Range")],
        [KeyboardButton("📋 My Numbers"), KeyboardButton("🚦 Live Traffic")],
        [KeyboardButton("🛟 Support Admin")],
    ]
    if has_get100_access(user_id):
        buttons.insert(1, [KeyboardButton("📦 Bulk Number")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)

def admin_keyboard_s1s2():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📦 Bulk ON", callback_data="admin_bulk_on"),
         InlineKeyboardButton("📦 Bulk OFF", callback_data="admin_bulk_off")],
        [InlineKeyboardButton("👥 All Users", callback_data="admin_allusers"),
         InlineKeyboardButton("📊 Stats", callback_data="admin_stats_s12")],
        [InlineKeyboardButton("🔑 API Status", callback_data="admin_apistatus"),
         InlineKeyboardButton("🔄 Refresh Sessions", callback_data="admin_refresh")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast_s12")],
    ])

def admin_keyboard_s3():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Statistics", callback_data="s3admin_stats")],
        [InlineKeyboardButton("➕ Add Numbers", callback_data="s3admin_addnumbers")],
        [InlineKeyboardButton("📢 Broadcast", callback_data="s3admin_broadcast")],
        [InlineKeyboardButton("📈 Analytics", callback_data="s3admin_analytics")],
        [InlineKeyboardButton("🗑️ Delete Numbers", callback_data="s3admin_delete")],
        [InlineKeyboardButton("⚙️ Settings", callback_data="s3admin_settings")],
    ])

async def panel_select_inline():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Facebook S1", callback_data="select_panel_S1")],
        [InlineKeyboardButton("📘 Facebook S2", callback_data="select_panel_S2")],
        [InlineKeyboardButton("📘 Facebook S3", callback_data="select_panel_S3")],
    ])

def server_select_inline(app_name):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Facebook S1", callback_data=f"app_s1_{app_name}")],
        [InlineKeyboardButton("📘 Facebook S2", callback_data=f"app_s2_{app_name}")],
        [InlineKeyboardButton("📘 Facebook S3", callback_data=f"app_s3_{app_name}")],
    ])

def country_select_inline(countries, app_name):
    buttons = []
    row = []
    for i, c in enumerate(countries[:20]):
        country_name = c if isinstance(c, str) else c.get("country", "")
        panel = c.get("panel", "S1") if isinstance(c, dict) else "S1"
        flag = get_flag_by_iso(country_name)
        row.append(InlineKeyboardButton(
            f"{flag} {country_name}",
            callback_data=f"country_{panel}_{country_name}"
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"back_app")])
    return InlineKeyboardMarkup(buttons)

def range_select_inline(ranges, app_name, country):
    buttons = []
    for r in ranges[:15]:
        rv = r["range"] if isinstance(r, dict) else r
        if not rv.upper().endswith('X'):
            rv_display = rv + "XXX"
        else:
            rv_display = rv
        buttons.append([InlineKeyboardButton(f"📡 {rv_display}", callback_data=f"range_{rv}")])
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}")])
    return InlineKeyboardMarkup(buttons)

def after_number_inline_s1s2(number, range_val):
    buttons = [
        [InlineKeyboardButton("🔄 New Number", callback_data=f"new_number_{range_val}")],
        [InlineKeyboardButton("🌍 Change Region", callback_data="back_app")],
    ]
    _ch_link = "https://t.me/+SWraCXOQrWM4Mzg9"
    buttons.insert(1, [InlineKeyboardButton("📢 Check OTP (Channel)", url=_ch_link)])
    return InlineKeyboardMarkup(buttons)

def after_number_inline_s3(pool_key):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Change Numbers", callback_data=f"s3change:{pool_key}")],
        [InlineKeyboardButton("👁 View OTP", url="https://t.me/+SWraCXOQrWM4Mzg9")],
        [InlineKeyboardButton("🌍 Change Country", callback_data="s3changecountry")],
    ])

# ══════════════════════════════════════════════════════════
#              AUTO OTP — S1/S2
# ══════════════════════════════════════════════════════════

async def auto_otp_single_s1(number, user_id, stop_event):
    clean = number.replace("+", "").strip()
    while not stop_event.is_set():
        try:
            panel = user_data.get(user_id, {}).get("panel", "S1")
            if panel == "S1":
                data = await api_get_info_s1(search=clean, status="success")
            else:
                data = await api_get_info_s2(search=clean, status="success")

            if data.get("meta", {}).get("code") == 200:
                nums = data.get("data", {}).get("numbers") or []
                for n in nums:
                    raw_otp = (n.get("otp") or n.get("message") or "").strip()
                    otp = extract_otp(raw_otp)
                    if otp:
                        country_r = n.get("country", "") or user_data.get(user_id, {}).get("country_r", "")
                        flag = get_flag_by_iso(country_r)
                        app = user_data.get(user_id, {}).get("app", "FACEBOOK")
                        detected_app = detect_app_from_message(raw_otp, app)
                        return otp, country_r, flag, detected_app, raw_otp
        except Exception as e:
            logger.error(f"auto_otp_single error: {e}")
        await asyncio.sleep(10)
    return None, None, None, None, None

async def auto_otp_multi(message, numbers, user_id, range_val, bot=None):
    init_user(user_id)
    app = user_data[user_id].get("app", "FACEBOOK")
    panel = user_data[user_id].get("panel", "S1")
    country_r = user_data[user_id].get("country_r") or user_data[user_id].get("country", "")
    flag = get_flag_by_iso(country_r)
    number = numbers[0]
    clean_number = str(number).replace("+", "").strip()
    chat_id = message.chat.id
    otp_lines = []
    stop_event = asyncio.Event()
    user_data[user_id]["otp_active"] = True
    user_data[user_id]["otp_running"] = True
    user_data[user_id]["auto_otp_cancel"] = False

    base_text = (
        f"🔷 {app.upper()} NUMBER\n\n"
        f"📞 Number : `{clean_number}`\n"
        f"🌍 Region : {flag} {country_r}\n"
        f"🟢 Status : Active\n"
        f"─────────────────"
    )

    def build_message(suffix=""):
        return base_text + suffix

    sent_message = None

    async def update_msg(suffix):
        nonlocal sent_message
        try:
            new_text = build_message(suffix)
            if sent_message:
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=sent_message.message_id,
                        text=new_text,
                        parse_mode="Markdown",
                        reply_markup=after_number_inline_s1s2(number, range_val)
                    )
                except Exception:
                    pass
                return
            sent_message = await message.reply_text(
                new_text,
                parse_mode="Markdown",
                reply_markup=after_number_inline_s1s2(number, range_val)
            )
            if chat_id not in user_msg:
                user_msg[chat_id] = sent_message.message_id
        except Exception as e:
            logger.error(f"update_msg error: {e}")

    async def on_otp(otp, country_result, flag_result, detected_app, raw_sms):
        # ✅ Duplicate check
        if otp in otp_lines:
            return
        otp_lines.append(otp)
        otp_text = ""
        for i, o in enumerate(otp_lines, 1):
            otp_text += f"\n✅ OTP {i} : `{o}`"
        await update_msg(otp_text)
        if bot:
            await send_otp_to_channel(bot, clean_number, otp, detected_app, country_result, flag_result, raw_sms, panel)

    # Initial message
    try:
        if chat_id in user_msg:
            try:
                sent_message = await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=user_msg[chat_id],
                    text=build_message(f"\n{random.choice(LOADING_TEXTS)}"),
                    parse_mode="Markdown",
                    reply_markup=after_number_inline_s1s2(number, range_val)
                )
            except Exception:
                sent_message = await message.reply_text(
                    build_message(f"\n{random.choice(LOADING_TEXTS)}"),
                    parse_mode="Markdown",
                    reply_markup=after_number_inline_s1s2(number, range_val)
                )
                user_msg[chat_id] = sent_message.message_id
        else:
            sent_message = await message.reply_text(
                build_message(f"\n{random.choice(LOADING_TEXTS)}"),
                parse_mode="Markdown",
                reply_markup=after_number_inline_s1s2(number, range_val)
            )
            user_msg[chat_id] = sent_message.message_id
    except Exception as e:
        logger.error(f"Initial message error: {e}")
        return

    async def _run():
        OTP_TIMEOUT = 10 * 60
        elapsed = 0
        last_otp_count = 0

        async def poll_once():
            clean = number.replace("+", "").strip()
            _panel = user_data.get(user_id, {}).get("panel", "S1")
            try:
                if _panel == "S1":
                    data = await api_get_info_s1(search=clean, status="success")
                else:
                    data = await api_get_info_s2(search=clean, status="success")

                if data.get("meta", {}).get("code") == 200:
                    nums = data.get("data", {}).get("numbers") or []
                    for n in nums:
                        # Number match check
                        api_num = str(n.get("number", "")).replace("+", "").replace(" ", "").strip()
                        if clean != api_num:
                            continue

                        # Robust OTP extraction
                        _otp_field = str(n.get("otp") or "").strip()
                        _msg_field = (
                            n.get("message") or n.get("sms") or
                            n.get("raw_sms") or n.get("text") or
                            n.get("body") or n.get("content") or ""
                        ).strip()

                        if re.fullmatch(r'0+', _otp_field) or not _otp_field:
                            raw_otp = _msg_field or _otp_field
                        else:
                            raw_otp = _otp_field or _msg_field

                        otp = extract_otp(raw_otp)

                        # fallback: message field আলাদাভাবে try
                        if not otp or re.fullmatch(r'0+', otp or ""):
                            if _msg_field and _msg_field != raw_otp:
                                fallback = extract_otp(_msg_field)
                                if fallback and not re.fullmatch(r'0+', fallback):
                                    otp = fallback
                                    raw_otp = _msg_field

                        if otp and otp not in otp_lines:
                            country_result = n.get("country", "").strip()
                            if not country_result or country_result.lower() in ["postpaid", "post paid", "other", "unknown"]:
                                country_result = country_r
                            flag_result = get_flag_by_iso(country_result)
                            detected_app = detect_app_from_message(raw_otp, app)
                            # ✅ on_otp-এ append হবে — এখানে করব না (double append fix)
                            await on_otp(otp, country_result, flag_result, detected_app, raw_otp)
            except Exception as e:
                logger.error(f"poll_once error: {e}")

        while not stop_event.is_set():
            if user_data.get(user_id, {}).get("auto_otp_cancel"):
                stop_event.set()
                break

            await poll_once()

            # ✅ নতুন OTP আসলে timer reset (পুরানো script এর মতো)
            if len(otp_lines) > last_otp_count:
                last_otp_count = len(otp_lines)
                elapsed = 0

            if not otp_lines:
                await update_msg(f"\n{random.choice(LOADING_TEXTS)}")

            # ✅ প্রথম ৬০ সেকেন্ড ৫s, তারপর ১০s (পুরানো script এর মতো)
            interval = 5 if elapsed < 60 else 10
            await asyncio.sleep(interval)
            elapsed += interval

            if elapsed >= OTP_TIMEOUT:
                stop_event.set()
                break

        stop_event.set()

        saved_session = user_data[user_id].get("number_session")
        if saved_session and saved_session.get("token"):
            if panel == "S1":
                await s1_pool.return_number_session(saved_session)
            else:
                await s2_pool.return_number_session(saved_session)
            user_data[user_id]["number_session"] = None

        user_data[user_id]["otp_running"] = False
        user_data[user_id]["otp_active"] = False

    wrapper = asyncio.create_task(_run())
    add_otp_task(user_id, wrapper)

async def do_get_number(message, user_id, count=1, user_name="User", bot=None):
    init_user(user_id)
    range_val = user_data[user_id].get("range")
    app = user_data[user_id].get("app", "FACEBOOK")
    panel = user_data[user_id].get("panel", "S1")
    chat_id = message.chat.id

    if not range_val:
        await message.reply_text(
            "❌ Range select করা হয়নি!\n\n🏠 Start → Service → Country → Range",
            reply_markup=main_keyboard(user_id)
        )
        return

    # পুরান number clear করো আগেই
    user_data[user_id]["last_number"] = None

    if chat_id in user_msg:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=user_msg[chat_id])
        except Exception:
            pass
        user_msg.pop(chat_id, None)

    if panel == "S1":
        data, number_session = await api_get_number_s1(range_val, app)
    else:
        data, number_session = await api_get_number_s2(range_val, app)

    if data.get("meta", {}).get("code") == 200:
        num = data["data"]
        number = (
            num.get("number") or num.get("num") or
            num.get("phone") or num.get("mobile") or "N/A"
        )
        country_r = num.get("country", "") or user_data[user_id].get("country", "")
        user_data[user_id]["last_number"] = number
        user_data[user_id]["auto_otp_cancel"] = False
        user_data[user_id]["country_r"] = country_r
        user_data[user_id]["number_session"] = number_session
        asyncio.create_task(auto_otp_multi(message, [number], user_id, range_val, bot=bot))
    else:
        if number_session:
            if panel == "S1":
                await s1_pool.return_number_session(number_session)
            else:
                await s2_pool.return_number_session(number_session)
        err_msg = data.get("message") or data.get("error") or "Number পাওয়া যায়নি"
        await message.reply_text(f"❌ {err_msg}", reply_markup=main_keyboard(user_id))

# ══════════════════════════════════════════════════════════
#              S3 — OTP POLLING (CR API)
# ══════════════════════════════════════════════════════════

async def poll_otps_s3(context):
    try:
        if len(otp_cache) > 5000:
            otp_cache.clear()

        # Clean expired sessions (30 min)
        for uid in list(s3_user_sessions.keys()):
            session = s3_user_sessions.get(uid, {})
            try:
                session_time = datetime.fromisoformat(session.get("assigned_time", ""))
                if datetime.now() - session_time > timedelta(minutes=30):
                    s3_user_sessions.pop(uid, None)
            except Exception:
                pass

        # OTP channel — দুইটা button: Main Channel + Number Bot
        _s3_kb_buttons = []
        _s3_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
        if _s3_ch_link and len(_s3_ch_link) > 10:
            _s3_kb_buttons.append(InlineKeyboardButton("📢 Main Channel", url=_s3_ch_link))
        if BOT_USERNAME:
            _s3_kb_buttons.append(InlineKeyboardButton("🤖 Number Bot", url=f"https://t.me/{BOT_USERNAME}"))
        otp_channel_keyboard = InlineKeyboardMarkup([_s3_kb_buttons]) if _s3_kb_buttons else None

        for otp_data in fetch_cr_api_otps():
            try:
                number = otp_data.get("num", "").strip()
                message = otp_data.get("message", "").strip()
                dt = otp_data.get("dt", "").strip()
                if not number or not message or not dt:
                    continue

                # Duplicate check: number + dt + message দিয়ে (OTP নির্ভর নয়)
                import hashlib
                msg_hash = hashlib.md5(message.encode()).hexdigest()[:8]
                cache_key = f"s3:{number}:{dt}:{msg_hash}"
                if cache_key in otp_cache:
                    continue

                # Deploy এর আগের OTP skip করো
                try:
                    otp_dt = datetime.strptime(dt[:19], "%Y-%m-%d %H:%M:%S")
                    if otp_dt < BOT_START_TIME:
                        otp_cache[cache_key] = True  # cache এ রাখো যাতে পরেও skip হয়
                        continue
                except Exception:
                    pass

                otp_cache[cache_key] = True

                otp_code = extract_otp(message)
                country = extract_country_code_from_number(number)
                flag = COUNTRY_FLAGS_CODE.get(country, "🌍")
                hidden = hide_number(number)
                country_name = COUNTRY_NAMES_CODE.get(country, "Unknown")

                # Detect app from SMS message text (CLI সবসময় FACEBOOK দেয়, তাই message থেকে detect)
                detected_app = detect_app_from_message(message, default_app="FACEBOOK")
                detected_app_cap = detected_app.capitalize()

                # Channel message — OTP থাকলে OTP দেখাও, না থাকলে RAW SMS পাঠাও
                quoted_sms = "\n".join(
                    f">{escape_mdv2(line)}" for line in message.splitlines() if line.strip()
                )
                if otp_code:
                    channel_msg = (
                        f"🆕 NEW OTP — {escape_mdv2(detected_app_cap)} \\[S3\\]\n\n"
                        f"{flag} {escape_mdv2(country_name)}\n\n"
                        f"📱 `\\+{escape_mdv2(hidden)}`\n"
                        f"🔑 `{escape_mdv2(otp_code)}`\n"
                        f"🕒 {escape_mdv2(dt)}\n\n"
                        f"━━━━━━━━━━━━━━━\n📩 SMS:\n{quoted_sms}"
                    )
                else:
                    channel_msg = (
                        f"📩 NEW SMS — {escape_mdv2(detected_app_cap)} \\[S3\\]\n\n"
                        f"{flag} {escape_mdv2(country_name)}\n\n"
                        f"📱 `\\+{escape_mdv2(hidden)}`\n"
                        f"🕒 {escape_mdv2(dt)}\n\n"
                        f"━━━━━━━━━━━━━━━\n📩 SMS:\n{quoted_sms}"
                    )
                try:
                    await context.bot.send_message(
                        chat_id=OTP_CHANNEL_ID,
                        text=channel_msg,
                        parse_mode="MarkdownV2",
                        reply_markup=otp_channel_keyboard
                    )
                except Exception:
                    if otp_code:
                        plain_msg = (
                            f"🆕 NEW OTP — {detected_app_cap} [S3]\n\n"
                            f"{flag} {country_name}\n\n"
                            f"📱 +{hidden}\n🔑 {otp_code}\n🕒 {dt}\n\n"
                            f"📩 SMS:\n{message[:300]}"
                        )
                    else:
                        plain_msg = (
                            f"📩 NEW SMS — {detected_app_cap} [S3]\n\n"
                            f"{flag} {country_name}\n\n"
                            f"📱 +{hidden}\n🕒 {dt}\n\n"
                            f"📩 SMS:\n{message[:300]}"
                        )
                    try:
                        await context.bot.send_message(
                            chat_id=OTP_CHANNEL_ID,
                            text=plain_msg,
                            reply_markup=otp_channel_keyboard
                        )
                    except Exception as e:
                        logger.warning(f"Channel send failed: {e}")

                # User inbox notification (শুধু matched user এর number এ SMS/OTP এলে)
                matched_users = s3_find_users_by_number(number)
                for uid in matched_users:
                    try:
                        session = s3_get_session(int(uid))
                        pool_key = session.get("pool_key", "") if session else ""
                        inbox_label = get_short_label(pool_key) if pool_key else f"{flag} Facebook"
                        time_only = dt.split(" ")[-1] if " " in dt else dt

                        if otp_code:
                            inbox_msg = (
                                f"🔔 OTP এসেছে!\n\n"
                                f"{inbox_label} 🟢\n"
                                f"📱 `+{number}`\n"
                                f"🔐 OTP: `{otp_code}`\n"
                                f"⏰ {time_only}"
                            )
                        else:
                            inbox_msg = (
                                f"📩 SMS এসেছে!\n\n"
                                f"{inbox_label} 🟢\n"
                                f"📱 `+{number}`\n"
                                f"⏰ {time_only}\n\n"
                                f"📩 {message[:200]}"
                            )
                        await context.bot.send_message(
                            chat_id=int(uid),
                            text=inbox_msg,
                            parse_mode="Markdown"
                        )
                    except Exception as e:
                        logger.error(f"S3 Inbox error [{uid}]: {e}")

            except Exception as e:
                logger.error(f"S3 OTP process error: {e}")

    except Exception as e:
        logger.error(f"poll_otps_s3 error: {e}")

# ══════════════════════════════════════════════════════════
#              LIVE SMS POST (S1/S2 Channel)
# ══════════════════════════════════════════════════════════

LAST_POST_MESSAGE_ID = None
LAST_POST_TEXT = ""
_job_is_running = False

async def job_post_live_sms(context):
    global _posted_sms_ids, _job_is_running, LAST_POST_MESSAGE_ID, LAST_POST_TEXT
    if _job_is_running:
        return
    _job_is_running = True
    try:
        bot = context.bot
        try:
            s1_logs, s2_logs = await asyncio.wait_for(
                asyncio.gather(
                    get_console_logs_s1(force=True),
                    get_console_logs_s2(force=True),
                    return_exceptions=True
                ),
                timeout=45
            )
            if isinstance(s1_logs, Exception): s1_logs = []
            if isinstance(s2_logs, Exception): s2_logs = []
        except asyncio.TimeoutError:
            logger.error("⚠️ Both S1+S2 logs timeout")
            return

        from datetime import timezone, timedelta as _td
        now_bd = datetime.now(timezone(_td(hours=6)))

        for panel_label, logs in [("S1", s1_logs), ("S2", s2_logs)]:
            panel_post_count = 0  # প্রতি panel এ সর্বোচ্চ 7টা post
            for log in logs:
                if panel_post_count >= 7:
                    break
                app = (log.get("app_name") or log.get("app") or "").replace("*", "").strip().upper()
                if app != "FACEBOOK":
                    continue

                range_val = log.get("range", "").strip()
                slot = now_bd.strftime('%Y-%m-%d %H:') + str(now_bd.minute // 2 * 2).zfill(2)
                unique_id = f"range_{range_val}_{panel_label}_{slot}"
                if unique_id in _posted_sms_ids:
                    continue
                _posted_sms_ids.add(unique_id)

                country = log.get("country", "").strip() or "Unknown"
                flag = get_flag_by_iso(country)
                raw_sms = (
                    log.get("message", "") or
                    log.get("sms", "") or
                    log.get("raw_sms", "") or
                    log.get("text", "") or
                    log.get("body", "") or
                    log.get("content", "") or ""
                ).strip()

                # OTP from stars
                _fl = raw_sms.splitlines()[0] if raw_sms else ""
                _sg = re.findall(r'(?<![A-Za-z0-9])\*{2,}(?![A-Za-z0-9])', _fl)
                otp_len = sum(len(g) for g in _sg) if _sg else 6
                otp_len = max(5, min(8, otp_len))
                otp = ''.join([str(random.randint(0, 9)) for _ in range(otp_len)])

                display_range = range_val + "XXX" if range_val and not range_val.upper().endswith('X') else range_val

                text = (
                    f"{escape_mdv2(country)} {flag}\n\n"
                    f"📞 `{escape_mdv2(display_range)}`\n"
                    f"🔐 `{escape_mdv2(otp)}`\n"
                    f"💬 Service: Facebook \\| {escape_mdv2(panel_label)}\n"
                    f"{escape_mdv2('────────────')}\n"
                    f"📩"
                )

                if raw_sms:
                    quoted_lines = "\n".join(
                        f">{escape_mdv2(line)}" for line in raw_sms.splitlines() if line.strip()
                    )
                    text += f"\n{quoted_lines}"

                _kb_buttons = []
                _main_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
                if _main_ch_link and len(_main_ch_link) > 10:
                    _kb_buttons.append(InlineKeyboardButton("📢 Main Channel", url=_main_ch_link))
                if BOT_USERNAME:
                    _kb_buttons.append(InlineKeyboardButton("🤖 Number Bot", url=f"https://t.me/{BOT_USERNAME}"))
                keyboard = InlineKeyboardMarkup([_kb_buttons]) if _kb_buttons else None
                try:
                    await safe_send_message(
                        bot,
                        chat_id=RANGE_CHANNEL_ID,
                        text=text,
                        parse_mode="MarkdownV2",
                        reply_markup=keyboard
                    )
                    panel_post_count += 1
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.error(f"Live SMS post error: {e}")

    finally:
        _job_is_running = False

async def job_midnight_reset(context):
    global _posted_sms_ids
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            await client.delete(
                f"{SUPABASE_URL}/rest/v1/posted_sms",
                params={"unique_id": "neq.null"},
                headers=_sb_headers()
            )
        _posted_sms_ids = set()
        logger.info("✅ Midnight reset done")
    except Exception as e:
        logger.error(f"Midnight reset error: {e}")

# ══════════════════════════════════════════════════════════
#              SAFE EDIT HELPER
# ══════════════════════════════════════════════════════════

async def safe_edit(query, text, **kwargs):
    try:
        await query.edit_message_text(text, **kwargs)
    except Exception:
        pass

# ══════════════════════════════════════════════════════════
#              /start COMMAND
# ══════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    init_user(user_id)
    user_data[user_id]["name"] = user.first_name or "User"

    is_new = user_id not in s3_users_db
    s3_add_user(user_id, user.username or user.first_name)

    # Channel join check for new users
    if is_new and JOIN_CHANNEL_USERNAME:
        ch1_joined, ch2_joined = await check_all_channels_joined(user_id, context.bot)
        if not ch1_joined or not ch2_joined:
            keyboard_buttons = []
            if not ch1_joined:
                keyboard_buttons.append([safe_channel_button("🔗 Main Channel")])
            if not ch2_joined and CHANNEL2_LINK:
                keyboard_buttons.append([InlineKeyboardButton(f"🔗 {CHANNEL2_NAME}", url=CHANNEL2_LINK)])
            keyboard_buttons.append([InlineKeyboardButton("✅ Verify", callback_data="verify_join")])
            await update.message.reply_text(
                "🚦 Access Locked!\n\nAll channels join করুন তারপর Verify করুন।",
                reply_markup=InlineKeyboardMarkup(keyboard_buttons)
            )
            return

    # Cancel old OTP tasks
    cancel_all_otp_tasks(user_id)
    user_data[user_id].update({
        "range": None, "country": None,
        "otp_active": False, "otp_running": False
    })

    chat_id = update.message.chat.id
    for msg_dict in [user_msg, user_range_msg]:
        if chat_id in msg_dict:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_dict[chat_id])
            except Exception:
                pass
            msg_dict.pop(chat_id, None)

    try:
        await update.message.delete()
    except Exception:
        pass

    welcome_text = (
        f"👋 Welcome ♡ {user.first_name} ♡!\n\n"
        f"Use the menu below to get started.\n\n"
        f"⚙️ Select Service:"
    )

    inline_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Facebook S1", callback_data="select_panel_S1")],
        [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2")],
        [InlineKeyboardButton("📙 Facebook S3", callback_data="select_panel_S3")],
    ])

    await context.bot.send_message(
        chat_id=chat_id,
        text="⌨️",
        reply_markup=main_keyboard(user_id)
    )

    new_msg = await context.bot.send_message(
        chat_id=chat_id,
        text=welcome_text,
        reply_markup=inline_kb
    )
    user_msg[chat_id] = new_msg.message_id
    user_kb_msg[chat_id] = new_msg.message_id

    asyncio.create_task(db_save_user_async(user_id, user_data[user_id]))

# ══════════════════════════════════════════════════════════
#              MESSAGE HANDLER (Reply Keyboard)
# ══════════════════════════════════════════════════════════

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    chat_id = update.message.chat.id
    user_name = user.first_name or "User"
    init_user(user_id)

    # Admin broadcast handling
    waiting = user_data.get(user_id, {}).get("waiting_for")
    if waiting == "broadcast" and user_id == ADMIN_ID:
        user_data[user_id]["waiting_for"] = None
        broadcast_text = text
        all_users = list(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
        success = failed = 0
        await update.message.reply_text(f"📢 Broadcasting to {len(all_users)} users...")
        for uid in all_users:
            try:
                await context.bot.send_message(chat_id=int(uid), text=broadcast_text)
                success += 1
                await asyncio.sleep(0.05)
            except Exception:
                failed += 1
        await update.message.reply_text(
            f"✅ *Broadcast Done!*\n\n✅ Sent: `{success}`\n❌ Failed: `{failed}`",
            parse_mode="Markdown"
        )
        return

    if waiting == "custom_range":
        user_data[user_id]["waiting_for"] = None
        range_input = text.strip().upper()
        clean_range = ''.join(c for c in range_input if c.isdigit() or c == 'X')
        if not clean_range or len(clean_range) < 5:
            await update.message.reply_text(
                "❌ Invalid range!\n\nউদাহরণ: 23762155XXX",
                reply_markup=main_keyboard(user_id)
            )
            return
        base = clean_range.rstrip('X')
        clean_range = base + 'XXX'
        user_data[user_id]["range"] = clean_range
        panel = user_data[user_id].get("panel", "S1")
        app = user_data[user_id].get("app", "FACEBOOK")
        try:
            await update.message.delete()
        except Exception:
            pass
        await do_get_number(update.message, user_id, bot=context.bot)
        return

    # S3 admin: pending delete number
    if context.bot_data.get("pending_delete_number") and user_id == ADMIN_ID:
        context.bot_data["pending_delete_number"] = False
        number = text.strip().lstrip("+")
        deleted = False
        for pk in list(numbers_pool.keys()):
            if number in numbers_pool[pk]:
                numbers_pool[pk].remove(number)
                await _save_numbers(context.bot)
                deleted = True
                await update.message.reply_text(
                    f"✅ *Number Deleted!*\n\n📱 `+{number}`\n🌍 Pool: `{pk}`",
                    parse_mode="Markdown"
                )
                break
        if not deleted:
            await update.message.reply_text(f"❌ Number `+{number}` কোনো pool এ নেই", parse_mode="Markdown")
        return

    if text in ("📲 Get Number",):
        if user_id in processing_users:
            await query.answer("⏳ একটু অপেক্ষা করুন..." if False else "") if False else None
            return
        processing_users.add(user_id)
        try:
            cancel_all_otp_tasks(user_id)
            user_data[user_id]["waiting_for"] = None
            user_data[user_id]["otp_active"] = False
            user_data[user_id]["otp_running"] = False
            await cleanup_s1s2_panel(context.bot, user_id)
            inline_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("📘 Facebook S1", callback_data="select_panel_S1")],
                [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2")],
                [InlineKeyboardButton("📙 Facebook S3", callback_data="select_panel_S3")],
            ])
            new_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="⚙️ Select Service:",
                reply_markup=inline_kb
            )
            user_kb_msg[user_id] = new_msg.message_id
            user_msg[chat_id] = new_msg.message_id
        finally:
            processing_users.discard(user_id)
        return

    if text in ("📡 Custom Range",):
        panel = user_data[user_id].get("panel", "S1")
        if panel == "S3":
            await update.message.reply_text("❌ S3 তে Custom Range নেই। S1 বা S2 select করুন।", reply_markup=main_keyboard(user_id))
            return
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["waiting_for"] = "custom_range"
        app = user_data[user_id].get("app", "FACEBOOK")
        await update.message.reply_text(
            f"📡 Custom Range লিখুন:\n\n🖥 Panel: {panel}\n📱 App: {app}\n\nউদাহরণ: 23762155XXX",
            reply_markup=main_keyboard(user_id)
        )
        return

    if text in ("📋 My Numbers",):
        panel = user_data[user_id].get("panel", "S1")
        if panel == "S3":
            session = s3_get_session(user_id)
            if session:
                number = session['number']
                pool_key = session['pool_key']
                code, _ = parse_pool_key(pool_key)
                country_name = COUNTRY_NAMES_CODE.get(code, "Facebook")
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                keyboard = after_number_inline_s3(pool_key)
                await update.message.reply_text(
                    f"🌍 *{flag} {country_name} FACEBOOK [S3]*\n\n"
                    f"📱 Number: `+{number}`\n"
                    f"🟢 Status: *Assigned*\n\n"
                    f"⏳ Waiting for OTP...\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"💡 OTP না আসলে নিচের *View OTP* চাপো",
                    parse_mode="Markdown",
                    reply_markup=keyboard
                )
            else:
                await update.message.reply_text("❌ No active numbers in S3", reply_markup=main_keyboard(user_id))
        else:
            last_number = str(user_data[user_id].get("last_number", "")).replace("+", "").strip()
            if not last_number:
                await update.message.reply_text("❌ কোনো number নেওয়া হয়নি।", reply_markup=main_keyboard(user_id))
                return
            await update.message.reply_text("⏳ Loading...")
            if panel == "S1":
                data = await api_get_info_s1(search=last_number)
            else:
                data = await api_get_info_s2(search=last_number)
            if data.get("meta", {}).get("code") == 200:
                nums = data["data"].get("numbers", []) or []
                stats = data["data"].get("stats", {})
                msg = (
                    f"━━━━━━━━━━━━━━━━━━\n📋 My Numbers ({panel})\n━━━━━━━━━━━━━━━━━━\n\n"
                    f"✅ Success: {stats.get('success_count', 0)}\n"
                    f"⏳ Pending: {stats.get('pending_count', 0)}\n"
                    f"❌ Failed: {stats.get('failed_count', 0)}\n\n"
                )
                for n in nums[:10]:
                    e = "✅" if n.get("status") == "success" else "⏳" if n.get("status") == "pending" else "❌"
                    msg += f"{e} {n.get('number')} — {n.get('country', '')} — {n.get('last_activity', '')}\n"
                msg += "\n━━━━━━━━━━━━━━━━━━"
                await update.message.reply_text(msg, reply_markup=main_keyboard(user_id))
            else:
                await update.message.reply_text("❌ Load করতে ব্যর্থ।", reply_markup=main_keyboard(user_id))
        return

    if text == "🚦 Live Traffic":
        await update.message.reply_text("⏳ Live Traffic লোড হচ্ছে...", reply_markup=main_keyboard(user_id))
        try:
            s1_logs = await get_console_logs_s1(force=True)
            s2_logs = await get_console_logs_s2(force=True)
            result = {}
            for panel_label, logs in [("S1", s1_logs), ("S2", s2_logs)]:
                for log in logs:
                    app = log.get("app_name", "").replace("*", "").strip().upper()
                    country = log.get("country", "").strip()
                    if not app:
                        continue
                    if app not in result:
                        result[app] = {"total": 0, "s1": 0, "s2": 0, "countries": {}}
                    result[app]["total"] += 1
                    result[app][panel_label.lower()] += 1
                    if country:
                        result[app]["countries"][country] = result[app]["countries"].get(country, 0) + 1

            fb = result.get("FACEBOOK", {})
            total = fb.get("total", 0)
            s3_nums = sum(len(v) for v in numbers_pool.values())

            msg = f"🚦 FACEBOOK LIVE TRAFFIC\n\n"
            msg += f"🔵 S1: {fb.get('s1', 0)} ranges\n"
            msg += f"🟢 S2: {fb.get('s2', 0)} ranges\n"
            msg += f"🔴 S3: {s3_nums} numbers in pool\n\n"
            countries = sorted(fb.get("countries", {}).items(), key=lambda x: x[1], reverse=True)
            for c, cnt in countries[:10]:
                flag = get_flag_by_iso(c)
                msg += f"{flag} {c} — {cnt}\n"
            from datetime import timezone, timedelta as _td
            bd_now = datetime.now(timezone(_td(hours=6))).strftime("%I:%M %p")
            msg += f"\n🕐 Last Update: {bd_now}"
            await update.message.reply_text(msg, reply_markup=main_keyboard(user_id))
        except Exception as e:
            logger.error(f"Live Traffic error: {e}")
            await update.message.reply_text("❌ Data load error.", reply_markup=main_keyboard(user_id))
        return

    if text == "🛟 Support Admin":
        await update.message.reply_text(
            "🛟 Support এর জন্য নিচের button এ click করুন:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🛟 Support Admin", url=SUPPORT_ADMIN_LINK)
            ]])
        )
        return

    if text == "📦 Bulk Number":
        if not has_get100_access(user_id):
            await update.message.reply_text("❌ Bulk Number access নেই।", reply_markup=main_keyboard(user_id))
        else:
            panel = user_data[user_id].get("panel", "S1")
            if panel == "S3":
                await update.message.reply_text("❌ S3 তে Bulk Number নেই।", reply_markup=main_keyboard(user_id))
            else:
                await do_get_number(update.message, user_id, count=100, user_name=user_name, bot=context.bot)
        return

    if text in ("👑 Admin Panel",):
        if user_id != ADMIN_ID:
            await update.message.reply_text("❌ Admin access নেই।")
            return
        await cmd_admin(update, context)
        return

    # Unknown — delete and show keyboard
    try:
        await update.message.delete()
    except Exception:
        pass
    if chat_id not in user_kb_msg:
        try:
            kb_msg = await context.bot.send_message(
                chat_id=chat_id, text="⌨️ Menu", reply_markup=main_keyboard(user_id)
            )
            user_kb_msg[chat_id] = kb_msg.message_id
        except Exception:
            pass

# ══════════════════════════════════════════════════════════
#              TXT FILE HANDLER (S3 Number Upload)
# ══════════════════════════════════════════════════════════

async def handle_txt_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Admin only!")
        return

    file = await update.message.document.get_file()
    content = await file.download_as_bytearray()
    text = content.decode('utf-8', errors='ignore')
    filename = update.message.document.file_name

    # Filename format: 91.txt or 91_s2.txt
    country_match = re.match(r'(\d+(?:_s\d+)?)', filename, re.IGNORECASE)
    if not country_match:
        await update.message.reply_text(
            "❌ *Invalid filename!*\n\nFormat: `91.txt` or `91_s2.txt`",
            parse_mode="Markdown"
        )
        return

    pool_key = country_match.group(1).lower()
    new_numbers = [
        line.strip().lstrip('+')
        for line in text.split('\n')
        if line.strip() and len(line.strip()) >= 7
    ]

    if not new_numbers:
        await update.message.reply_text("❌ File empty or invalid format!")
        return

    added, skipped = await add_numbers_to_pool(context.bot, pool_key, new_numbers)
    await update.message.reply_text(
        f"✅ *Upload Complete!*\n\n"
        f"🌍 Pool: *{get_button_label(pool_key)}*\n"
        f"✅ Added: `{added}` numbers\n"
        f"⏭ Skipped (duplicate): `{skipped}`\n"
        f"📱 Total in pool: `{count_numbers(pool_key)}`",
        parse_mode="Markdown"
    )

# ══════════════════════════════════════════════════════════
#              ADMIN COMMANDS
# ══════════════════════════════════════════════════════════

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await update.message.reply_text("❌ Admin access নেই।")
        return
    get100_status = "✅ ON" if GET100_ENABLED else "❌ OFF"
    msg = (
        "━━━━━━━━━━━━━━━━━━\n👑 ADMIN PANEL\n━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 Bulk: {get100_status}\n\n"
        "🔵 S1/S2 Controls:\n"
        "/allusers /stats /apistatus /broadcast\n\n"
        "🔴 S3 Controls:\n"
        "Add Numbers → .txt file পাঠান\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, reply_markup=admin_keyboard_s1s2())
    await update.message.reply_text("🔴 *S3 Admin Panel:*", parse_mode="Markdown", reply_markup=admin_keyboard_s3())

async def cmd_allusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
    msg = f"👥 Total Users: {total}\n\n"
    for uid, uinfo in list(user_data.items())[:20]:
        msg += f"• {uid} — {uinfo.get('name','?')} | Panel: {uinfo.get('panel','?')}\n"
    await update.message.reply_text(msg)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    pool = get_numbers_pool()
    total_s3 = sum(len(v) for v in pool.values())
    total_users = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
    await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━\n📊 BOT STATS\n━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Total Users: {total_users}\n"
        f"📦 Bulk: {'✅ ON' if GET100_ENABLED else '❌ OFF'}\n"
        f"🔴 S3 Pool Numbers: {total_s3}\n"
        f"🔴 S3 Pools: {len(pool)}\n"
        f"🔵 S1 Pool: {s1_pool.number_sessions.qsize()}/50 number sessions\n"
        f"🟢 S2 Pool: {s2_pool.number_sessions.qsize()}/18 number sessions\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n━━━━━━━━━━━━━━━━━━"
    )

async def cmd_apistatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    s1_sess = await s1_pool._login_once()
    s1_ok = "✅" if s1_sess.get("token") else "❌"
    s2_sess = await s2_pool._login_once()
    s2_ok = "✅" if s2_sess.get("token") else "❌"
    # CR API check
    cr_otps = fetch_cr_api_otps()
    cr_ok = "✅" if cr_otps is not None else "❌"
    await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━\n🔗 API STATUS\n━━━━━━━━━━━━━━━━━━\n\n"
        f"🔵 S1 (StexSMS): {s1_ok}\n"
        f"  Number slots: {s1_pool.number_sessions.qsize()}/50\n"
        f"  OTP slots: {s1_pool.otp_sessions.qsize()}/50\n\n"
        f"🟢 S2 (X.Mint): {s2_ok}\n"
        f"  Number slots: {s2_pool.number_sessions.qsize()}/20\n"
        f"  OTP slots: {s2_pool.otp_sessions.qsize()}/12\n\n"
        f"🔴 S3 (CR API): {cr_ok}\n"
        f"  Recent OTPs: {len(cr_otps)}\n\n━━━━━━━━━━━━━━━━━━"
    )

async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    user_data[ADMIN_ID]["waiting_for"] = "broadcast"
    await update.message.reply_text("📢 সবাইকে কী message পাঠাবেন? লিখুন:")

async def cmd_get100on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global GET100_ENABLED
    if update.effective_user.id != ADMIN_ID:
        return
    GET100_ENABLED = True
    await update.message.reply_text("✅ Bulk Number চালু।")

async def cmd_get100off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global GET100_ENABLED
    if update.effective_user.id != ADMIN_ID:
        return
    GET100_ENABLED = False
    await update.message.reply_text("❌ Bulk Number বন্ধ।")

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
        await update.message.reply_text(f"✅ User {uid} কে Bulk access দেওয়া হয়েছে।")
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
        await update.message.reply_text(f"✅ User {uid} এর Bulk access সরানো হয়েছে।")
    except:
        await update.message.reply_text("❌ Invalid user ID.")

async def cmd_refreshsessions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔄 Session pool refresh হচ্ছে...")
    await asyncio.gather(s1_pool.refresh_all(), s2_pool.refresh_all())
    await update.message.reply_text(
        f"✅ Refresh Done!\n"
        f"S1 Number: {s1_pool.number_sessions.qsize()}/50\n"
        f"S2 Number: {s2_pool.number_sessions.qsize()}/20"
    )

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    init_user(user_id)
    cancel_all_otp_tasks(user_id)
    await update.message.reply_text("🛑 Auto OTP বন্ধ হয়েছে.", reply_markup=main_keyboard(user_id))

# ══════════════════════════════════════════════════════════
#              CALLBACK HANDLER
# ══════════════════════════════════════════════════════════

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"
    chat_id = query.message.chat.id
    init_user(user_id)

    # ── Verify join ──
    if data == "verify_join":
        clear_join_cache(user_id)
        ch1_joined, ch2_joined = await check_all_channels_joined(user_id, context.bot)
        if ch1_joined and ch2_joined:
            try:
                await query.message.delete()
            except Exception:
                pass
            cancel_all_otp_tasks(user_id)
            user_data[user_id].update({"range": None, "country": None, "otp_active": False, "otp_running": False})
            inline_kb = await panel_select_inline()
            new_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Verified! Welcome, {user_name}!\n\n⚙️ Select Service:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📘 Facebook S1", callback_data="select_panel_S1")],
                    [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2")],
                    [InlineKeyboardButton("📙 Facebook S3", callback_data="select_panel_S3")],
                ])
            )
            user_msg[chat_id] = new_msg.message_id
        else:
            keyboard_buttons = []
            if not ch1_joined:
                keyboard_buttons.append([safe_channel_button("🔗 Main Channel")])
            if not ch2_joined and CHANNEL2_LINK:
                keyboard_buttons.append([InlineKeyboardButton(f"🔗 {CHANNEL2_NAME}", url=CHANNEL2_LINK)])
            keyboard_buttons.append([InlineKeyboardButton("✅ Verify", callback_data="verify_join")])
            await safe_edit(query, "🚦 এখনো join করা হয়নি। Join করুন তারপর Verify করুন।",
                reply_markup=InlineKeyboardMarkup(keyboard_buttons))
        return

    # ── Noop (safe fallback for missing channel link) ──
    if data == "noop_channel":
        await query.answer("⚠️ Channel link configured নেই।", show_alert=False)
        return

    # ── Stop auto OTP ──
    if data == "stop_auto":
        cancel_all_otp_tasks(user_id)
        await query.answer("🛑 Auto OTP বন্ধ করা হয়েছে!")
        return

    # ── Panel select ──
    if data.startswith("select_panel_"):
        panel = data.replace("select_panel_", "")
        user_data[user_id]["panel"] = panel
        user_data[user_id]["country"] = None
        user_data[user_id]["range"] = None

        if panel == "S3":
            # S3 — show country pool
            pool = get_numbers_pool()
            if not pool:
                await safe_edit(query, "❌ S3 তে এখন কোনো number নেই!\n\nAdmin কে জানান।")
                return
            buttons = [
                [InlineKeyboardButton(get_button_label(pk), callback_data=f"s3getcountry:{pk}")]
                for pk in sorted(pool.keys())
            ]
            buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app")])
            await safe_edit(query,
                "🔴 *Facebook S3*\n\n🌍 Country select করুন:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        else:
            # S1/S2 — load countries
            app_name = "FACEBOOK"
            user_data[user_id]["app"] = app_name
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            countries = await get_countries_for_app(app_name, panel=panel)
            if not countries:
                await safe_edit(query,
                    f"❌ {panel} তে এখন কোনো active range নেই।\n\nকিছুক্ষণ পর আবার try করুন।",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app")]])
                )
                return
            country_list = [{"country": c, "panel": panel} for c in countries]
            await safe_edit(query,
                f"📘 Facebook {panel}\n\n🌍 Country select করুন:",
                reply_markup=country_select_inline(country_list, app_name)
            )
        return

    # ── Back to panel select ──
    if data == "back_app":
        # ✅ পুরানো OTP task cancel করো, session return করো
        cancel_all_otp_tasks(user_id)
        old_session = user_data[user_id].get("number_session")
        if old_session and old_session.get("token"):
            panel = user_data[user_id].get("panel", "S1")
            if panel == "S1":
                await s1_pool.return_number_session(old_session)
            elif panel == "S2":
                await s2_pool.return_number_session(old_session)
        user_data[user_id]["number_session"] = None
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False
        user_data[user_id]["auto_otp_cancel"] = True
        user_data[user_id]["range"] = None
        user_data[user_id]["country"] = None
        inline_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📘 Facebook S1", callback_data="select_panel_S1")],
            [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2")],
            [InlineKeyboardButton("📙 Facebook S3", callback_data="select_panel_S3")],
        ])
        await safe_edit(query, "⚙️ Select Service:", reply_markup=inline_kb)
        return

    # ── S1/S2 app panel select (legacy support) ──
    if data.startswith("app_s1_"):
        app_name = data.replace("app_s1_", "")
        user_data[user_id].update({"app": app_name, "panel": "S1", "country": None, "range": None})
        await safe_edit(query, "⏳ লোড হচ্ছে...")
        countries = await get_countries_for_app(app_name, panel="S1")
        if not countries:
            await safe_edit(query, "❌ এখন কোনো active country নেই.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app")]]))
            return
        country_list = [{"country": c, "panel": "S1"} for c in countries]
        await safe_edit(query, f"📘 {app_name} S1\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(country_list, app_name))
        return

    if data.startswith("app_s2_"):
        app_name = data.replace("app_s2_", "")
        user_data[user_id].update({"app": app_name, "panel": "S2", "country": None, "range": None})
        await safe_edit(query, "⏳ লোড হচ্ছে...")
        countries = await get_countries_for_app(app_name, panel="S2")
        if not countries:
            await safe_edit(query, "❌ এখন কোনো active country নেই.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app")]]))
            return
        country_list = [{"country": c, "panel": "S2"} for c in countries]
        await safe_edit(query, f"📘 {app_name} S2\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(country_list, app_name))
        return

    if data.startswith("app_s3_"):
        user_data[user_id]["panel"] = "S3"
        pool = get_numbers_pool()
        if not pool:
            await safe_edit(query, "❌ S3 তে এখন কোনো number নেই!")
            return
        buttons = [
            [InlineKeyboardButton(get_button_label(pk), callback_data=f"s3getcountry:{pk}")]
            for pk in sorted(pool.keys())
        ]
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app")])
        await safe_edit(query, "🔴 Facebook S3\n\n🌍 Country select করুন:",
            reply_markup=InlineKeyboardMarkup(buttons))
        return

    # ── Country select (S1/S2) ──
    if data.startswith("country_"):
        raw = data.replace("country_", "")
        if raw.startswith("S1_") or raw.startswith("S2_"):
            panel = raw[:2]
            country = raw[3:]
            user_data[user_id]["panel"] = panel
        else:
            country = raw
            panel = user_data[user_id].get("panel", "S1")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        user_data[user_id]["country"] = country
        user_data[user_id]["range"] = None
        await safe_edit(query, "⏳ Range লোড হচ্ছে...")
        ranges = await get_all_ranges_for_country(app_name, country, panel=panel)
        if not ranges:
            await safe_edit(query, f"❌ {country} তে কোনো range নেই.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}")]]))
            return
        flag = get_flag_by_iso(country)
        await safe_edit(query,
            f"{flag} {country}\n\n📡 Range select করুন:",
            reply_markup=range_select_inline(ranges, app_name, country)
        )
        return

    if data.startswith("back_country_"):
        app_name = data.replace("back_country_", "")
        panel = user_data[user_id].get("panel", "S1")
        user_data[user_id]["country"] = None
        await safe_edit(query, "⏳ Loading...")
        countries = await get_countries_for_app(app_name, panel=panel)
        country_list = [{"country": c, "panel": panel} for c in countries]
        await safe_edit(query,
            f"📘 {app_name} {panel}\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(country_list, app_name)
        )
        return

    # ── Range select ──
    if data.startswith("range_"):
        range_val = data.replace("range_", "")
        user_data[user_id]["range"] = range_val
        # ✅ পুরানো session return করো
        old_session = user_data[user_id].get("number_session")
        if old_session and old_session.get("token"):
            panel = user_data[user_id].get("panel", "S1")
            if panel == "S1":
                await s1_pool.return_number_session(old_session)
            elif panel == "S2":
                await s2_pool.return_number_session(old_session)
        user_data[user_id]["number_session"] = None
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["auto_otp_cancel"] = False
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False
        try:
            await query.message.delete()
        except Exception:
            pass
        await do_get_number(query.message, user_id, bot=context.bot)
        return

    # ── New Number (S1/S2) ──
    if data.startswith("new_number_"):
        range_val = data.replace("new_number_", "")
        user_data[user_id]["range"] = range_val
        # পুরান session return করো
        old_session = user_data[user_id].get("number_session")
        if old_session and old_session.get("token"):
            panel = user_data[user_id].get("panel", "S1")
            if panel == "S1":
                await s1_pool.return_number_session(old_session)
            elif panel == "S2":
                await s2_pool.return_number_session(old_session)
        user_data[user_id]["number_session"] = None
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["auto_otp_cancel"] = False
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False
        user_data[user_id]["last_number"] = None
        try:
            await query.message.delete()
        except Exception:
            pass
        await do_get_number(query.message, user_id, bot=context.bot)
        return

    # ══════════════════
    #  S3 CALLBACKS
    # ══════════════════

    if data.startswith("s3getcountry:"):
        if is_rate_limited(user_id):
            await query.answer("⏳ একটু ধীরে!", show_alert=True)
            return
        pool_key = data.split(":", 1)[1]
        numbers = get_pool_numbers(pool_key)
        if not numbers:
            await safe_edit(query,
                "❌ *No Numbers Available*\n\n📭 এই country তে এখন number নেই\n⏳ পরে আবার try করো",
                parse_mode="Markdown"
            )
            return

        await safe_edit(query, "⏳ Searching Number...")
        await asyncio.sleep(1)
        await safe_edit(query, "📡 Connecting Server...")
        await asyncio.sleep(1)

        # Assign number from pool
        number = random.choice(numbers)
        numbers.remove(number)
        numbers_pool[pool_key] = numbers
        asyncio.create_task(mark_number_assigned(number, user_id))

        s3_set_session(user_id, number, pool_key)
        asyncio.create_task(_save_users_s3(context.bot))

        code, _ = parse_pool_key(pool_key)
        country_name = COUNTRY_NAMES_CODE.get(code, "Facebook")
        flag = COUNTRY_FLAGS_CODE.get(code, "🌍")

        await safe_edit(query,
            f"🌍 *{flag} {country_name} FACEBOOK [S3]*\n\n"
            f"📱 Number: `+{number}`\n"
            f"🟢 Status: *Assigned*\n\n"
            f"⏳ Waiting for OTP...\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💡 OTP না আসলে নিচের *View OTP* চাপো",
            parse_mode="Markdown",
            reply_markup=after_number_inline_s3(pool_key)
        )
        return

    if data.startswith("s3change:"):
        if is_rate_limited(user_id):
            await query.answer("⏳ একটু ধীরে!", show_alert=True)
            return
        pool_key = data.split(":", 1)[1]
        session = s3_get_session(user_id)
        if session and session.get("number"):
            await remove_number_from_pool(context.bot, pool_key, session["number"])

        numbers = get_pool_numbers(pool_key)
        if not numbers:
            await safe_edit(query,
                "❌ *No Numbers Available*\n\n📭 এই country তে এখন number নেই",
                parse_mode="Markdown"
            )
            return

        number = random.choice(numbers)
        numbers.remove(number)
        numbers_pool[pool_key] = numbers
        asyncio.create_task(_save_numbers(context.bot))
        s3_set_session(user_id, number, pool_key)
        asyncio.create_task(_save_users_s3(context.bot))

        code, _ = parse_pool_key(pool_key)
        country_name = COUNTRY_NAMES_CODE.get(code, "Facebook")
        flag = COUNTRY_FLAGS_CODE.get(code, "🌍")

        await safe_edit(query,
            f"🌍 *{flag} {country_name} FACEBOOK [S3]*\n\n"
            f"📱 Number: `+{number}`\n"
            f"🟢 Status: *Assigned*\n\n"
            f"⏳ Waiting for OTP...\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💡 OTP না আসলে নিচের *View OTP* চাপো",
            parse_mode="Markdown",
            reply_markup=after_number_inline_s3(pool_key)
        )
        return

    if data == "s3changecountry":
        pool = get_numbers_pool()
        if not pool:
            await query.answer("❌ No countries available", show_alert=True)
            return
        buttons = [
            [InlineKeyboardButton(get_button_label(pk), callback_data=f"s3getcountry:{pk}")]
            for pk in sorted(pool.keys())
        ]
        await safe_edit(query,
            "🌍 *Select Country (S3):*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    # ══════════════════
    #  S1/S2 ADMIN
    # ══════════════════

    if data.startswith("admin_"):
        if user_id != ADMIN_ID:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        action = data.replace("admin_", "")

        if action == "bulk_on":
            global GET100_ENABLED
            GET100_ENABLED = True
            await query.answer("✅ Bulk ON")
            return
        if action == "bulk_off":
            GET100_ENABLED = False
            await query.answer("❌ Bulk OFF")
            return
        if action == "allusers":
            total = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
            msg = f"👥 Total Users: {total}\n\n"
            for uid, uinfo in list(user_data.items())[:15]:
                msg += f"• {uid} — {uinfo.get('name','?')} | {uinfo.get('panel','?')}\n"
            await safe_edit(query, msg)
            return
        if action == "stats_s12":
            pool = get_numbers_pool()
            total_s3 = sum(len(v) for v in pool.values())
            total_users = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
            await safe_edit(query,
                f"📊 *BOT STATS*\n\n"
                f"👥 Users: `{total_users}`\n"
                f"📦 Bulk: `{'ON' if GET100_ENABLED else 'OFF'}`\n"
                f"🔵 S1 Sessions: `{s1_pool.number_sessions.qsize()}`\n"
                f"🟢 S2 Sessions: `{s2_pool.number_sessions.qsize()}`\n"
                f"🔴 S3 Numbers: `{total_s3}`\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown"
            )
            return
        if action == "apistatus":
            s1_sess = await s1_pool._login_once()
            s1_ok = "✅" if s1_sess.get("token") else "❌"
            s2_sess = await s2_pool._login_once()
            s2_ok = "✅" if s2_sess.get("token") else "❌"
            cr_otps = fetch_cr_api_otps()
            cr_ok = "✅" if cr_otps is not None else "❌"
            await safe_edit(query,
                f"🔗 *API STATUS*\n\n"
                f"🔵 S1 (StexSMS): {s1_ok}\n"
                f"🟢 S2 (X.Mint): {s2_ok}\n"
                f"🔴 S3 (CR API): {cr_ok} ({len(cr_otps)} OTPs)\n\n"
                f"S1 Number slots: {s1_pool.number_sessions.qsize()}/50\n"
                f"S2 Number slots: {s2_pool.number_sessions.qsize()}/20",
                parse_mode="Markdown"
            )
            return
        if action == "refresh":
            await query.answer("🔄 Refreshing...")
            await asyncio.gather(s1_pool.refresh_all(), s2_pool.refresh_all())
            await query.answer("✅ Sessions refreshed!")
            return
        if action == "broadcast_s12":
            user_data[ADMIN_ID]["waiting_for"] = "broadcast"
            await safe_edit(query, "📢 *Broadcast*\n\nMessage লিখুন — সব user কে পাঠানো হবে:", parse_mode="Markdown")
            return
        return

    # ══════════════════
    #  S3 ADMIN
    # ══════════════════

    if data.startswith("s3admin_"):
        if user_id != ADMIN_ID:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        action = data.replace("s3admin_", "")

        if action == "stats":
            pool = get_numbers_pool()
            total_numbers = sum(len(v) for v in pool.values())
            await safe_edit(query,
                f"📊 *S3 Statistics*\n\n"
                f"👥 S3 Users: `{s3_get_user_count()}`\n"
                f"📱 Total Numbers: `{total_numbers}`\n"
                f"🌍 Pools: `{len(pool)}`\n"
                f"📡 Active Sessions: `{len(s3_user_sessions)}`",
                parse_mode="Markdown"
            )
            return

        if action == "addnumbers":
            await safe_edit(query,
                "📝 *Add Numbers (S3)*\n\n"
                "📌 .txt file পাঠান\n"
                "📌 Filename format: `91.txt` বা `91_s2.txt`\n"
                "📌 প্রতি line এ একটা number (country code সহ)\n\n"
                "উদাহরণ:\n"
                "```\n919876543210\n918765432109\n```",
                parse_mode="Markdown"
            )
            return

        if action == "broadcast":
            user_data[ADMIN_ID]["waiting_for"] = "broadcast"
            await safe_edit(query, "📢 *Broadcast (S3)*\n\nMessage লিখুন:", parse_mode="Markdown")
            return

        if action == "analytics":
            pool = get_numbers_pool()
            total_numbers = sum(len(v) for v in pool.values())
            total_users = s3_get_user_count()
            today = datetime.now().strftime("%Y-%m-%d")
            week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            daily_new = weekly_new = 0
            for uid, val in s3_users_db.items():
                joined = val.get("joined", "2024-01-01") if isinstance(val, dict) else "2024-01-01"
                if joined == today:
                    daily_new += 1
                if joined >= week_ago:
                    weekly_new += 1

            country_lines = ""
            for pk in sorted(pool.keys()):
                count = len(pool[pk])
                label = get_button_label(pk)
                country_lines += f"  {label}: `{count}`\n"

            await safe_edit(query,
                f"📈 *S3 Analytics*\n\n"
                f"👥 *Users*\n"
                f"  Total: `{total_users}`\n"
                f"  Today New: `{daily_new}`\n"
                f"  This Week: `{weekly_new}`\n\n"
                f"📡 Sessions: `{len(s3_user_sessions)}`\n\n"
                f"📱 *Numbers*\n"
                f"  Total: `{total_numbers}`\n"
                f"  Pools: `{len(pool)}`\n\n"
                f"🌍 *Per Country:*\n{country_lines}",
                parse_mode="Markdown"
            )
            return

        if action == "delete":
            pool = get_numbers_pool()
            if not pool:
                await safe_edit(query, "❌ No pools available")
                return
            buttons = [
                [InlineKeyboardButton(f"🗑 {get_button_label(pk)} ({len(pool[pk])})", callback_data=f"s3deletepool:{pk}")]
                for pk in sorted(pool.keys())
            ]
            buttons.append([InlineKeyboardButton("✏️ Delete Single Number", callback_data="s3deletesingle")])
            await safe_edit(query,
                "🗑️ *Delete Numbers (S3)*\n\nPool delete করতে pool এ ক্লিক করো:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return

        if action == "settings":
            await safe_edit(query, "⚙️ *S3 Settings*\n\nComing soon...", parse_mode="Markdown")
            return
        return

    if data.startswith("s3deletepool:"):
        if user_id != ADMIN_ID:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        pool_key = data.split(":", 1)[1]
        count = len(numbers_pool.get(pool_key, []))
        label = get_button_label(pool_key)
        await safe_edit(query,
            f"⚠️ *Confirm Delete*\n\n🌍 Pool: *{label}*\n📱 Numbers: `{count}`\n\nসব number delete হবে!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ হ্যাঁ Delete করো", callback_data=f"s3confirmdeletepool:{pool_key}")],
                [InlineKeyboardButton("❌ Cancel", callback_data="s3admin_delete")]
            ])
        )
        return

    if data.startswith("s3confirmdeletepool:"):
        if user_id != ADMIN_ID:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        pool_key = data.split(":", 1)[1]
        if pool_key in numbers_pool:
            count = len(numbers_pool[pool_key])
            del numbers_pool[pool_key]
            await _save_numbers(context.bot)
            await safe_edit(query,
                f"✅ *Pool Deleted!*\n\n🌍 Pool: `{pool_key}`\n🗑 Removed: `{count}` numbers",
                parse_mode="Markdown"
            )
        else:
            await safe_edit(query, "❌ Pool not found")
        return

    if data == "s3deletesingle":
        if user_id != ADMIN_ID:
            await query.answer("❌ Admin only!", show_alert=True)
            return
        context.bot_data["pending_delete_number"] = True
        await safe_edit(query,
            "✏️ *Delete Single Number*\n\nNumber পাঠান (+ ছাড়া):\n`91xxxxxxxxxx`",
            parse_mode="Markdown"
        )
        return

# ══════════════════════════════════════════════════════════
#              AUTO MENU RESTORE
# ══════════════════════════════════════════════════════════

async def auto_menu_restore(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.message.chat.id
    init_user(user_id)
    if chat_id not in user_kb_msg:
        try:
            kb_msg = await context.bot.send_message(
                chat_id=chat_id, text="⌨️ Menu", reply_markup=main_keyboard(user_id)
            )
            user_kb_msg[chat_id] = kb_msg.message_id
        except Exception:
            pass

# ══════════════════════════════════════════════════════════
#              POST INIT
# ══════════════════════════════════════════════════════════

async def job_auto_cleanup(context):
    """Background auto cleanup worker — expired sessions, stale cache, dead tasks."""
    now = time.time()
    cutoff = now - 3600  # 1 ঘণ্টা inactive

    # user_last_action থেকে inactive user data cleanup
    inactive = [uid for uid, t in user_last_action.items() if t < cutoff]
    for uid in inactive:
        user_last_action.pop(uid, None)
        user_msg.pop(uid, None)
        user_range_msg.pop(uid, None)
        _join_cache.pop(f"{uid}_ch1", None)
        _join_cache.pop(f"{uid}_ch2", None)

    # OTP task cleanup — done tasks remove
    for uid in list(_otp_tasks.keys()):
        _otp_tasks[uid] = [t for t in _otp_tasks[uid] if not t.done()]
        if not _otp_tasks[uid]:
            _otp_tasks.pop(uid, None)

    # OTP cache size limit
    if len(otp_cache) > 5000:
        otp_cache.clear()

    logger.info(f"🧹 Auto cleanup done. Removed {len(inactive)} inactive users.")

async def post_init(app):
    # ── Central Config Validation ──
    missing = []
    if not BOT_TOKEN: missing.append("BOT_TOKEN")
    if not ADMIN_ID: missing.append("ADMIN_ID")
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_KEY")
    if not STEXSMS_EMAIL: missing.append("STEXSMS_EMAIL")
    if not STEXSMS_PASSWORD: missing.append("STEXSMS_PASSWORD")
    if not XMINT_EMAIL: missing.append("XMINT_EMAIL")
    if not XMINT_PASSWORD: missing.append("XMINT_PASSWORD")
    if missing:
        logger.critical(f"❌ Missing required ENV variables: {', '.join(missing)}")
        raise SystemExit(1)

    await app.bot.delete_webhook(drop_pending_updates=True)

    logger.info("📦 Loading S3 data from Supabase...")
    await tg_load_all(app.bot)
    logger.info(f"✅ S3 Loaded: {len(numbers_pool)} pools, {sum(len(v) for v in numbers_pool.values())} numbers")

    # Preload OTP cache
    try:
        otps = fetch_cr_api_otps()
        preloaded = 0
        for otp_data in otps:
            number = otp_data.get("num", "").strip()
            message = otp_data.get("message", "").strip()
            dt = otp_data.get("dt", "").strip()
            otp_code = extract_otp(message)
            if number and otp_code and dt:
                otp_cache[f"s3:{number}:{otp_code}:{dt}"] = True
                preloaded += 1
        logger.info(f"✅ Preloaded {preloaded} OTPs into cache")
    except Exception as e:
        logger.error(f"OTP preload error: {e}")

    # Load Supabase users
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/users",
                params={"select": "*"},
                headers=_sb_headers()
            )
        rows = res.json()
        if isinstance(rows, list):
            for u in rows:
                uid = u["user_id"]
                if uid not in user_data:
                    user_data[uid] = {}
                user_data[uid].setdefault("name", u.get("name", "User"))
                user_data[uid].setdefault("joined", u.get("joined", ""))
                user_data[uid].setdefault("app", u.get("app", "FACEBOOK"))
                user_data[uid].setdefault("panel", u.get("panel", "S1"))
                user_data[uid].setdefault("country", u.get("country"))
                user_data[uid].setdefault("range", u.get("range"))
                user_data[uid].setdefault("last_number", u.get("last_number"))
                user_data[uid].setdefault("waiting_for", None)
                user_data[uid].setdefault("otp_active", False)
                user_data[uid].setdefault("otp_running", False)
                user_data[uid].setdefault("auto_otp_cancel", False)
                user_data[uid].setdefault("number_session", None)
                user_data[uid].setdefault("country_r", None)
            logger.info(f"✅ Supabase: {len(rows)} users loaded")
    except Exception as e:
        logger.error(f"Supabase load error: {e}")

    # Load posted_sms
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res2 = await client.get(
                f"{SUPABASE_URL}/rest/v1/posted_sms",
                params={"select": "unique_id"},
                headers=_sb_headers()
            )
        rows2 = res2.json()
        if isinstance(rows2, list):
            for row in rows2:
                _posted_sms_ids.add(row["unique_id"])
            logger.info(f"✅ Supabase: {len(rows2)} posted_sms loaded")
    except Exception as e:
        logger.error(f"posted_sms load error: {e}")

    # Start session pools in background
    try:
        asyncio.create_task(s1_pool.initialize())
        logger.info("✅ S1 pool background init started")
    except Exception as e:
        logger.error(f"S1 pool init error: {e}")

    try:
        asyncio.create_task(s2_pool.initialize())
        logger.info("✅ S2 pool background init started")
    except Exception as e:
        logger.error(f"S2 pool init error: {e}")

    logger.info("✅ 3-Panel Bot started! S1 + S2 + S3 active.")

async def post_shutdown(app):
    # Cancel all OTP tasks
    for uid, tasks in _otp_tasks.items():
        for task in tasks:
            task.cancel()
    # Cancel all other async tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("✅ Graceful shutdown complete.")

async def error_handler(update, context):
    err = str(context.error).lower()
    if any(x in err for x in ["message is not modified", "bad request", "message to edit not found"]):
        return
    logger.error(f"Exception: {context.error}")

# ══════════════════════════════════════════════════════════
#              MAIN
# ══════════════════════════════════════════════════════════

def main():
    logger.info("🚀 Starting 3-Panel Combined Bot (S1 + S2 + S3)...")

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .read_timeout(30)
        .write_timeout(30)
        .connect_timeout(30)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .concurrent_updates(False)
        .build()
    )

    app.add_error_handler(error_handler)

    # Commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("allusers", cmd_allusers))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("apistatus", cmd_apistatus))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("get100on", cmd_get100on))
    app.add_handler(CommandHandler("get100off", cmd_get100off))
    app.add_handler(CommandHandler("addget100", cmd_addget100))
    app.add_handler(CommandHandler("removeget100", cmd_removeget100))
    app.add_handler(CommandHandler("refreshsessions", cmd_refreshsessions))

    # Callback
    app.add_handler(CallbackQueryHandler(callback_handler))

    # TXT file upload (S3 number add)
    app.add_handler(MessageHandler(filters.Document.FileExtension("txt"), handle_txt_file))

    # Reply keyboard
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        auto_menu_restore
    ), group=0)
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        handle_message
    ), group=1)

    # Jobs
    # S3 OTP poll — every 10 seconds
    app.job_queue.run_repeating(poll_otps_s3, interval=10, first=5)

    # S1/S2 Live SMS post — every 120 seconds
    app.job_queue.run_repeating(job_post_live_sms, interval=120, first=60)

    # Background auto cleanup — every 30 minutes
    app.job_queue.run_repeating(job_auto_cleanup, interval=1800, first=300)

    # Midnight reset — Supabase posted_sms clear
    import datetime as _dt
    from datetime import timezone, timedelta as _td
    bd_now = _dt.datetime.now(timezone(_td(hours=6)))
    midnight_bd = bd_now.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)
    seconds_until_midnight = (midnight_bd - bd_now).total_seconds()
    app.job_queue.run_repeating(job_midnight_reset, interval=86400, first=seconds_until_midnight)

    logger.info("✅ 3-Panel Bot running! S1 (StexSMS) + S2 (X.Mint) + S3 (CR API)")
    app.run_polling(drop_pending_updates=True, timeout=30)

if __name__ == "__main__":
    main()

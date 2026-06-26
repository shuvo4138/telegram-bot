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
    ReplyKeyboardMarkup, KeyboardButton, CopyTextButton
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
JOIN_CHANNEL_USERNAME = "alwaysrvice24hours"
JOIN_CHANNEL_LINK = "https://t.me/alwaysrvice24hours"
OTP_CHANNEL_JOIN_LINK = "https://t.me/+SWraCXOQrWM4Mzg9"
BACKUP_CHANNEL_LINK = "https://t.me/+dutZzSJv-FxhYTdl"
# Channel IDs for join verification
MAIN_CHANNEL_CHECK_ID = -1001792312528
OTP_CHANNEL_CHECK_ID = -1002625886518
BACKUP_CHANNEL_CHECK_ID = -1003803282073
CHANNEL2_USERNAME = os.getenv("CHANNEL2_USERNAME", "").strip()
CHANNEL2_LINK = os.getenv("CHANNEL2_LINK", "").strip()
CHANNEL2_NAME = os.getenv("CHANNEL2_NAME", "Backup Channel").strip()
RANGE_CHANNEL_ID = int(os.getenv("RANGE_CHANNEL_ID", os.getenv("OTP_CHANNEL_ID", "0")).strip())

# S3 Storage channel (numbers only)
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", "0").strip())



# Channel buttons
MAIN_CHANNEL_LINK = os.getenv("MAIN_CHANNEL_LINK", "").strip()
NUMBER_BOT_LINK = os.getenv("NUMBER_BOT_LINK", "").strip()


# S2 — X.Mint
XMINT_EMAIL = os.getenv("XMINT_EMAIL", "").strip()
XMINT_PASSWORD = os.getenv("XMINT_PASSWORD", "").strip()
S2_BASE_URL = "https://x.mnitnetwork.com/mapi/v1"

# S3 — CR API
CR_API_URL = os.getenv("CR_API_URL", "").strip()
CR_API_TOKEN = os.getenv("CR_API_TOKEN", "").strip()

# A1 — ZENEX Network
ZENEX_API_KEY = os.getenv("ZENEX_API_KEY", "").strip()
ZENEX_BASE_URL = "https://api.zenexnetwork.com"

# A2 — VOLTX SMS
A2_API_KEY = os.getenv("A2_API_KEY", "").strip()
A2_BASE_URL = os.getenv("A2_BASE_URL", "https://api.2oo9.cloud/MXS47FLFX0U/tnevs/@public/api").strip()

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
        return InlineKeyboardButton(label, url=link, api_kwargs={"style": "primary"})
    fallback_url = OTP_CHANNEL_LINK or CHANNEL2_LINK or ""
    if fallback_url and len(fallback_url) > 10:
        return InlineKeyboardButton(label, url=fallback_url, api_kwargs={"style": "primary"})
    return InlineKeyboardButton(label, callback_data="noop_channel", api_kwargs={"style": "primary"})

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
    "car": "CF", "central african republic": "CF", "central africa": "CF",
    "république centrafricaine": "CF", "centrafrique": "CF",
    "eq. guinea": "GQ", "equatorial guinea": "GQ",
    "sao tome": "ST", "são tomé": "ST", "sao tome and principe": "ST",
    "guinea-bissau": "GW", "guinea bissau": "GW",
    "n. macedonia": "MK",
    "democratic republic of congo": "CD", "drc": "CD",
    "cote d'ivoire": "CI", "côte d'ivoire": "CI",
    "gabon": "GA", "djibouti": "DJ", "eritrea": "ER",
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
    # Direct name match
    for app in ["instagram", "facebook", "tiktok", "snapchat", "twitter",
                "google", "whatsapp", "telegram"]:
        if app in msg_lower:
            return app.upper()
    # Instagram keyword patterns (raw SMS often says "ig", "insta", "SIY" etc.)
    ig_patterns = ["insta", " ig ", "ig code", "ig-", "siy", "don't share it. siy",
                   "your instagram", "instagram code", "ig account"]
    for pat in ig_patterns:
        if pat in msg_lower:
            return "INSTAGRAM"
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

    # Load numbers from Supabase s3_numbers table (paginated)
    try:
        all_rows = []
        page_size = 1000
        offset = 0
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                res = await client.get(
                    f"{SUPABASE_URL}/rest/v1/s3_numbers",
                    params={"select": "*", "status": "eq.available", "limit": str(page_size), "offset": str(offset)},
                    headers=_sb_headers()
                )
                rows = res.json()
                if not isinstance(rows, list) or len(rows) == 0:
                    break
                all_rows.extend(rows)
                if len(rows) < page_size:
                    break
                offset += page_size
        for row in all_rows:
            pk = row.get("pool_key", "")
            num = row.get("number", "")
            if pk and num:
                if pk not in numbers_pool:
                    numbers_pool[pk] = []
                if num not in numbers_pool[pk]:
                    numbers_pool[pk].append(num)
        logger.info(f"✅ S3 Numbers loaded from Supabase: {len(numbers_pool)} pools, {sum(len(v) for v in numbers_pool.values())} numbers")
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
    """Permanent delete — memory pool + Supabase row DELETE (restart এর পরেও ফিরে আসবে না)."""
    # Memory pool থেকে সরাও
    nums = numbers_pool.get(pool_key, [])
    if number in nums:
        nums.remove(number)
        numbers_pool[pool_key] = nums
    # Supabase থেকে permanently DELETE — number AND pool_key দিয়ে filter (composite PK)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.delete(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params={"number": f"eq.{number}", "pool_key": f"eq.{pool_key}"},
                headers=_sb_headers()
            )
        logger.info(f"🗑 Permanent delete: {number} | pool={pool_key} | status={resp.status_code}")
    except Exception as e:
        logger.error(f"remove_number_from_pool permanent delete error: {e}")

async def mark_number_assigned(number, user_id, pool_key=""):
    """available → assigned"""
    try:
        params = {"number": f"eq.{number}"}
        if pool_key:
            params["pool_key"] = f"eq.{pool_key}"
        async with httpx.AsyncClient(timeout=15) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params=params,
                headers=_sb_headers(),
                json={"status": "assigned", "assigned_to": str(user_id)}
            )
    except Exception as e:
        logger.error(f"mark_number_assigned error: {e}")

async def mark_number_used(number, pool_key=""):
    """assigned → used"""
    try:
        params = {"number": f"eq.{number}"}
        if pool_key:
            params["pool_key"] = f"eq.{pool_key}"
        async with httpx.AsyncClient(timeout=15) as client:
            await client.patch(
                f"{SUPABASE_URL}/rest/v1/s3_numbers",
                params=params,
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
    # v1 suffix detect — 95_v1_fb or 95_fb (normal)
    if len(parts) >= 3 and parts[1] == "v1":
        service = parts[2].lower()
        variant = "v1"
    elif len(parts) >= 2:
        service = parts[-1].lower()
        variant = None
    else:
        service = "fb"
        variant = None
    return code, service, variant

SERVICE_LABELS = {
    "fb": "Facebook",
    "ig": "Instagram",
}

def get_service_label(service):
    return SERVICE_LABELS.get(service, "Facebook")

def get_button_label(pool_key):
    code, service, variant = parse_pool_key(pool_key)
    flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
    name = COUNTRY_NAMES_CODE.get(code, code)
    label = get_service_label(service)
    v1_tag = " v1" if variant == "v1" else ""
    return f"{flag} {name} {label}{v1_tag}"

def get_short_label(pool_key):
    code, service, variant = parse_pool_key(pool_key)
    flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
    name = COUNTRY_NAMES_CODE.get(code, "Unknown")
    label = get_service_label(service)
    v1_tag = " v1" if variant == "v1" else ""
    return f"{flag} {name} {label}{v1_tag}"

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

def s3_set_session(user_id, numbers, pool_key):
    # numbers এখন list অথবা single string হতে পারে
    if isinstance(numbers, str):
        numbers = [numbers]
    s3_user_sessions[str(user_id)] = {
        "number": numbers[0] if numbers else "",  # backward compat
        "numbers": numbers,
        "pool_key": pool_key,
        "assigned_time": datetime.now().isoformat()
    }

def s3_get_session(user_id):
    return s3_user_sessions.get(str(user_id))

def s3_find_users_by_number(number):
    matched = []
    for uid, session in s3_user_sessions.items():
        # numbers list অথবা single number — দুটোই check করো
        session_numbers = session.get("numbers", [])
        if not session_numbers and session.get("number"):
            session_numbers = [session["number"]]
        if number in session_numbers:
            try:
                session_time = datetime.fromisoformat(session["assigned_time"])
                limit = timedelta(minutes=120) if int(uid) == ADMIN_ID else timedelta(minutes=60)
                if datetime.now() - session_time < limit:
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
            "panel": data.get("panel", "S2"),
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

# ── S2 token cache file ──
S2_TOKEN_CACHE_FILE = ".s2_token_cache.json"

async def _s2_load_token_cache():
    """Supabase থেকে S2 token load। Expired হলে auto DELETE করে None return করে।"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/s2_token_cache",
                params={"id": "eq.1", "select": "*"},
                headers=_sb_headers()
            )
        rows = res.json()
        if not isinstance(rows, list) or len(rows) == 0:
            return None, None
        row = rows[0]
        saved_at = row.get("saved_at", 0)
        # Expired হলে Supabase থেকে DELETE করো
        if time.time() - saved_at >= 1500:
            logger.info("♻️ S2 token expired — deleting from Supabase")
            async with httpx.AsyncClient(timeout=10) as client:
                await client.delete(
                    f"{SUPABASE_URL}/rest/v1/s2_token_cache",
                    params={"id": "eq.1"},
                    headers=_sb_headers()
                )
            return None, None
        logger.info("✅ S2 token loaded from Supabase")
        return row.get("token"), row.get("session", "")
    except Exception as e:
        logger.error(f"_s2_load_token_cache error: {e}")
        return None, None

async def _s2_save_token_cache(token, session):
    """S2 token Supabase এ save করো (upsert id=1)।"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/s2_token_cache",
                headers={**_sb_headers(), "Prefer": "resolution=merge-duplicates"},
                json={"id": 1, "token": token, "session": session or "", "saved_at": time.time()}
            )
        logger.info("✅ S2 token saved to Supabase")
    except Exception as e:
        logger.error(f"_s2_save_token_cache error: {e}")

async def _s2_delete_token_cache():
    """S2 token Supabase থেকে DELETE করো।"""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.delete(
                f"{SUPABASE_URL}/rest/v1/s2_token_cache",
                params={"id": "eq.1"},
                headers=_sb_headers()
            )
        logger.info("🗑 S2 token deleted from Supabase")
    except Exception as e:
        logger.error(f"_s2_delete_token_cache error: {e}")

class S2SessionPool:
    """
    S2 (X.Mint) Session Pool — Old-style Eager Warmup
    ───────────────────────────────────────────────────
    Number Sessions : 40
    OTP Sessions    : 10
    Total           : 50

    Features:
    • Eager startup — bot start হলেই সব 50 session login
    • Deploy হলে Supabase থেকে cached token reuse (1টা)
    • Session expire হলে re-login
    • Queue timeout হলে fresh login
    """
    S2_NUM_TARGET = 40
    S2_OTP_TARGET = 10
    S2_TOTAL      = S2_NUM_TARGET + S2_OTP_TARGET  # 50
    TOKEN_TTL     = 1500  # seconds (25 min)

    def __init__(self):
        self.number_sessions = asyncio.Queue()
        self.otp_sessions    = asyncio.Queue()
        self.all_sessions    = []
        self.initialized     = False
        self.lock            = asyncio.Lock()
        # ── Supabase cache guard ──────────────────────────────────────
        # True মানে Supabase-এ exactly 1টা valid token আছে।
        # এই flag True থাকলে আর কোনো save/overwrite হবে না।
        self._cache_saved    = False

    async def initialize(self):
        async with self.lock:
            if self.initialized:
                return
            logger.info("🚀 S2 Pool warmup starting (eager)...")
            number_count = otp_count = 0

            # ── Railway ENV startup token fallback (recovery only) ──
            # Normal Supabase recovery ব্যর্থ হলে Railway ENV থেকে startup token ব্যবহার করো।
            # এটা শুধু fallback — Supabase-based primary system অপরিবর্তিত।
            _railway_startup_token = os.getenv("S2_STARTUP_TOKEN", "").strip()
            _railway_startup_session = os.getenv("S2_STARTUP_SESSION", "").strip()

            # ── Deploy/restart recovery: Supabase থেকে 1টা cached token নাও ──
            cached_token, cached_session = await _s2_load_token_cache()

            # Supabase-এ কিছু না পেলে Railway ENV token দিয়ে চেষ্টা করো
            if not cached_token and _railway_startup_token:
                logger.info("♻️ S2 Supabase cache miss — trying Railway ENV startup token")
                # Token বয়স জানা নেই, তাই একটা lightweight verify করো
                _rw_valid = False
                try:
                    async with httpx.AsyncClient(timeout=10) as _rw_client:
                        _rw_res = await _rw_client.get(
                            f"{S2_BASE_URL}/mdashboard/balance",
                            headers=get_s2_headers(_railway_startup_token)
                        )
                    _rw_valid = _rw_res.status_code == 200
                except Exception as _rw_e:
                    logger.warning(f"S2 Railway token verify error: {_rw_e}")
                if _rw_valid:
                    cached_token = _railway_startup_token
                    cached_session = _railway_startup_session
                    logger.info("✅ S2 Railway ENV startup token verified — using as recovery token")
                else:
                    logger.warning("⚠️  S2 Railway ENV startup token invalid/expired — falling back to fresh login")
            if cached_token:
                s = {"token": cached_token, "session": cached_session, "time": time.time()}
                self.all_sessions.append(s)
                await self.number_sessions.put(s)
                number_count += 1
                self._cache_saved = True  # Supabase-এ already valid token আছে
                logger.info("♻️ S2 reused cached token from Supabase (deploy recovery)")

            # ── বাকি 49 sessions fresh login — memory-only, Supabase-এ save নয় ──
            remaining = self.S2_TOTAL - (1 if cached_token else 0)
            fresh_saved = False  # initialize-এ একবারই cache save হবে
            for i in range(remaining):
                r = await self._login_once()
                if isinstance(r, dict) and r.get("token"):
                    self.all_sessions.append(r)
                    if number_count < self.S2_NUM_TARGET:
                        await self.number_sessions.put(r)
                        number_count += 1
                        # cached token না থাকলে প্রথম fresh token একবারই Supabase-এ save
                        if not cached_token and not fresh_saved:
                            await _s2_save_token_cache(r["token"], r.get("session", ""))
                            self._cache_saved = True
                            fresh_saved = True
                            logger.info("✅ S2 first fresh token saved to Supabase (cache init)")
                    elif otp_count < self.S2_OTP_TARGET:
                        await self.otp_sessions.put(r)
                        otp_count += 1
                await asyncio.sleep(1)  # flood protection
                if (i + 1) % 10 == 0:
                    logger.info(f"  S2 warmup progress: {i+1}/{remaining} | num={number_count} otp={otp_count}")

            self.initialized = True
            logger.info(f"✅ S2 Pool ready! Number: {number_count}, OTP: {otp_count}")

    async def _login_once(self, _retry=0):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    f"{S2_BASE_URL}/mauth/login",
                    json={"email": XMINT_EMAIL, "password": XMINT_PASSWORD}
                )
            if res.status_code == 429:
                wait = int(re.search(r'\d+', res.text).group(0)) if re.search(r'\d+', res.text) else 10
                logger.warning(f"⚠️  S2 Login FLOOD detected — waiting {wait}s (retry {_retry})")
                await asyncio.sleep(wait)
                if _retry < 3:
                    return await self._login_once(_retry + 1)
                return {}
            if res.status_code == 403:
                logger.warning("S2 Login 403 Forbidden")
                return {}
            if res.status_code != 200:
                logger.warning(f"S2 Login failed: HTTP {res.status_code} | {res.text[:150]}")
                if _retry < 2:
                    await asyncio.sleep(3)
                    return await self._login_once(_retry + 1)
                return {}
            try:
                data = res.json()
            except Exception:
                logger.warning(f"S2 Login JSON parse error: {res.text[:100]}")
                return {}
            if data.get("meta", {}).get("code") == 200:
                token = data["data"].get("token")
                session = data["data"].get("session_token", "")
                if token:
                    logger.info(f"✅ S2 Login OK | token={token[:12]}...")
                    return {"token": token, "session": session, "time": time.time()}
            logger.warning(f"S2 Login API error: {data.get('meta')}")
        except Exception as e:
            logger.error(f"S2 Login exception: {e}")
            if _retry < 2:
                await asyncio.sleep(3)
                return await self._login_once(_retry + 1)
        return {}

    async def get_number_session(self):
        try:
            session = await asyncio.wait_for(self.number_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > self.TOKEN_TTL:
                # ── Token expired ──
                # Supabase cached token delete করো (যদি এটাই cached ছিল)
                asyncio.create_task(_s2_delete_token_cache())
                self._cache_saved = False  # cache slot খালি হলো
                new_session = await self._login_once()
                if new_session.get("token"):
                    # নতুন token Supabase-এ save (1টাই, এখন থেকে এটাই cached)
                    await _s2_save_token_cache(new_session["token"], new_session.get("session", ""))
                    self._cache_saved = True
                    return new_session
                # fresh login failed — expired session discard, queue empty থাকুক
                return {}
            return session
        except asyncio.TimeoutError:
            # Queue empty — fresh login করো, কিন্তু Supabase save করো না
            # (cache slot এখনো valid থাকতে পারে)
            new_session = await self._login_once()
            if new_session.get("token"):
                return new_session
            # Last resort: all_sessions থেকে non-expired session নাও
            valid = [s for s in self.all_sessions if time.time() - s.get("time", 0) <= self.TOKEN_TTL]
            if valid:
                return valid[0]
            return {}

    async def get_otp_session(self):
        try:
            session = await asyncio.wait_for(self.otp_sessions.get(), timeout=30)
            if time.time() - session.get("time", 0) > self.TOKEN_TTL:
                # ── Expired OTP session — fresh login (memory-only, NO Supabase save) ──
                new_session = await self._login_once()
                if new_session.get("token"):
                    return new_session
                # fresh login failed — expired session discard
                return {}
            return session
        except asyncio.TimeoutError:
            # Queue empty — fresh login করো (memory-only)
            new_session = await self._login_once()
            if new_session.get("token"):
                return new_session
            # Last resort: all_sessions থেকে non-expired session নাও
            valid = [s for s in self.all_sessions if time.time() - s.get("time", 0) <= self.TOKEN_TTL]
            if valid:
                return valid[0]
            return {}

    async def return_number_session(self, session, failed=False):
        if session and session.get("token"):
            await self.number_sessions.put(session)

    async def return_otp_session(self, session, failed=False):
        if session and session.get("token"):
            await self.otp_sessions.put(session)

    async def refresh_all(self):
        async with self.lock:
            self.initialized = False
            self._cache_saved = False  # cache slot reset — re-init-এ নতুন করে save হবে
            while not self.number_sessions.empty():
                try: self.number_sessions.get_nowait()
                except: break
            while not self.otp_sessions.empty():
                try: self.otp_sessions.get_nowait()
                except: break
            self.all_sessions.clear()
        await self.initialize()

s2_pool = S2SessionPool()

# ══════════════════════════════════════════════════════════
#              S1/S2 API FUNCTIONS
# ══════════════════════════════════════════════════════════


def get_s2_headers(token):
    return {
        'User-Agent': "Mozilla/5.0 (Linux; Android 10)",
        'Accept': "application/json",
        'Content-Type': "application/json",
        'mauthtoken': token,
        'Cookie': f"mautToken={token}"
    }


async def api_get_number_s2(range_val, app_name="FACEBOOK", _retry=0):
    clean_range = ''.join(c for c in range_val.upper() if c.isdigit() or c == 'X')
    if not clean_range:
        return {"error": "Invalid range"}, None
    base = clean_range.rstrip('X')
    clean_range = base + 'XXX'

    session = await s2_pool.get_number_session()
    _return_session = True  # finally block session ফেরত দেবে কিনা
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
            # session ফেরত দাও, তারপর retry
            await s2_pool.return_number_session(session, failed=True)
            _return_session = False
            new_session = await s2_pool._login_once()
            if new_session.get("token") and _retry < 2:
                await asyncio.sleep(1)
                return await api_get_number_s2(range_val, app_name, _retry=_retry + 1)
            return {"error": "session_expired"}, None

        if res.status_code in (429, 503):
            await s2_pool.return_number_session(session)
            _return_session = False
            if _retry < 3:
                await asyncio.sleep(10 * (2 ** _retry))
                return await api_get_number_s2(range_val, app_name, _retry=_retry + 1)
            return {"error": f"HTTP {res.status_code}"}, None

        if res.status_code != 200:
            await s2_pool.return_number_session(session)
            _return_session = False
            return {"error": f"HTTP {res.status_code}"}, None

        result = res.json()
        if result.get("meta", {}).get("code") != 200:
            msg = str(result.get("message", "")).lower()
            if any(k in msg for k in ["block", "rate", "limit", "many", "temporary"]):
                await s2_pool.return_number_session(session)
                _return_session = False
                if _retry < 3:
                    await asyncio.sleep(10 * (2 ** _retry))
                    return await api_get_number_s2(range_val, app_name, _retry=_retry + 1)
            # success ছাড়া অন্য error — session ফেরত দাও
            await s2_pool.return_number_session(session)
            _return_session = False
            return result, None

        # সফল — caller session ফেরত দেবে
        _return_session = False
        return result, session

    except Exception as e:
        logger.error(f"api_get_number_s2 error: {e}")
        _return_session = False
        if session and session.get("token"):
            await s2_pool.return_number_session(session)
        return {"error": str(e)}, None

    finally:
        # যদি কোনো path-এ return না হয়, session leak হওয়া থেকে রক্ষা
        if _return_session and session and session.get("token"):
            await s2_pool.return_number_session(session)


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

# ══════════════════════════════════════════════════════════
#              A1 — ZENEX NETWORK API
# ══════════════════════════════════════════════════════════

_zenex_ranges_cache = {"data": [], "time": 0}
ZENEX_CACHE_TTL = 60  # 1 min

_s2_console_cache = {"logs": [], "time": 0}
CONSOLE_CACHE_TTL = 30  # 30 seconds

def get_zenex_headers():
    return {
        "mapikey": ZENEX_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

async def zenex_get_active_ranges(service=None):
    """ZENEX active ranges fetch — optional service filter."""
    global _zenex_ranges_cache
    if (time.time() - _zenex_ranges_cache["time"]) < ZENEX_CACHE_TTL and _zenex_ranges_cache["data"]:
        ranges = _zenex_ranges_cache["data"]
    else:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.get(
                    f"{ZENEX_BASE_URL}/v1/active-ranges",
                    headers=get_zenex_headers()
                )
            if res.status_code == 200:
                data = res.json()
                ranges = data.get("data", {}).get("active_ranges", [])
                _zenex_ranges_cache = {"data": ranges, "time": time.time()}
            else:
                ranges = _zenex_ranges_cache["data"]
        except Exception as e:
            logger.error(f"ZENEX active-ranges error: {e}")
            ranges = _zenex_ranges_cache["data"]
    if service:
        ranges = [r for r in ranges if r.get("service", "").upper() == service.upper()]
    return ranges

async def zenex_get_number(range_val):
    """ZENEX number provision."""
    clean = range_val.upper().replace("XXX", "").replace("X", "").strip()
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{ZENEX_BASE_URL}/v1/getnum",
                headers=get_zenex_headers(),
                json={"range": clean + "XXX", "is_national": False, "remove_plus": False}
            )
        if res.status_code == 200:
            data = res.json()
            if data.get("meta", {}).get("code") == 200:
                return data.get("data", {})
        logger.warning(f"ZENEX getnum failed: {res.status_code} {res.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"ZENEX getnum error: {e}")
        return None

async def zenex_poll_otp(target_number):
    """ZENEX OTP poll — number দিয়ে filter করে।"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{ZENEX_BASE_URL}/v1/numsuccess/info",
                headers=get_zenex_headers()
            )
        if res.status_code == 200:
            data = res.json()
            otps = data.get("data", {}).get("otps", [])
            clean_target = target_number.lstrip("+")
            for otp_entry in otps:
                num = otp_entry.get("number", "").lstrip("+")
                if num == clean_target or num.endswith(clean_target[-7:]):
                    return otp_entry
        return None
    except Exception as e:
        logger.error(f"ZENEX poll_otp error: {e}")
        return None

async def get_countries_for_app(app_name, panel="S2"):
    logs = await get_console_logs_s2()
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

async def get_all_ranges_for_country(app_name, country, panel="S2"):
    logs = await get_console_logs_s2()
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

# ── Instagram range filter (SMS keyword scan) ──
_ig_range_cache = {}  # key -> {"data": [...], "time": 0}
IG_RANGE_CACHE_TTL = 1800  # 30 min

async def get_instagram_countries(panel="S2"):
    """All countries যেখানে Instagram SMS আছে।"""
    cache_key = f"{panel}_ig_countries"
    cached = _ig_range_cache.get(cache_key, {})
    if cached.get("data") and (time.time() - cached.get("time", 0)) < IG_RANGE_CACHE_TTL:
        return cached["data"]
    logs = await get_console_logs_s2()
    seen = set()
    countries = []
    for log in logs:
        sms = (log.get("message") or log.get("sms") or log.get("body") or "").lower()
        if "instagram" not in sms:
            continue
        country = log.get("country", "").strip()
        if country and country not in seen:
            seen.add(country)
            countries.append(country)
    _ig_range_cache[cache_key] = {"data": countries, "time": time.time()}
    logger.info(f"📸 Instagram countries ({panel}): {countries}")
    return countries

async def get_instagram_ranges(country, panel="S2"):
    """Country অনুযায়ী Instagram SMS আছে এমন range গুলো।"""
    cache_key = f"{panel}_{country}_ig_ranges"
    cached = _ig_range_cache.get(cache_key, {})
    if cached.get("data") and (time.time() - cached.get("time", 0)) < IG_RANGE_CACHE_TTL:
        return cached["data"]
    logs = await get_console_logs_s2()
    seen = set()
    ranges = []
    for log in logs:
        sms = (log.get("message") or log.get("sms") or log.get("body") or "").lower()
        if "instagram" not in sms:
            continue
        log_country = log.get("country", "").strip()
        if log_country != country:
            continue
        r = log.get("range", "").strip()
        if r and r not in seen:
            seen.add(r)
            ranges.append({"range": r, "time": log.get("time", "")})
    _ig_range_cache[cache_key] = {"data": ranges, "time": time.time()}
    logger.info(f"📸 Instagram ranges ({panel}, {country}): {[x['range'] for x in ranges]}")
    return ranges

# ══════════════════════════════════════════════════════════
#              A2 — VOLTX SMS API
# ══════════════════════════════════════════════════════════


def a2_extract_country_code(rng):
    # Trailing XXX সরাও — "22465XXX" → "22465", "225076990XXX" → "225076990"
    clean = re.sub(r'X+$', '', str(rng).upper().strip())
    # Longest match first: 3 → 2 → 1 digit dial code
    for length in [3, 2, 1]:
        if len(clean) >= length:
            c = clean[:length]
            if c in COUNTRY_NAMES_CODE:
                return c
    return clean[:3] if len(clean) >= 3 else clean

_a2_ranges_cache = {"data": [], "time": 0}
A2_CACHE_TTL = 300  # 5 min

def a2_get_cached_ranges():
    """সবসময় cached ranges return করে — empty হলে empty list।"""
    return _a2_ranges_cache.get("data", [])

def get_a2_headers():
    return {
        "mauthapi": A2_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

async def a2_get_active_ranges(force=False):
    """VOLTX active ranges fetch — /liveaccess endpoint."""
    global _a2_ranges_cache
    if not force and (time.time() - _a2_ranges_cache["time"]) < A2_CACHE_TTL and _a2_ranges_cache["data"]:
        return _a2_ranges_cache["data"]
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{A2_BASE_URL}/liveaccess",
                headers=get_a2_headers()
            )
        logger.info(f"A2 liveaccess status: {res.status_code}")
        logger.info(f"A2 liveaccess raw: {res.text[:800]}")
        if res.status_code == 200:
            data = res.json()
            logger.info(f"A2 liveaccess keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
            if data.get("meta", {}).get("code") == 200:
                raw_data = data.get("data", {})
                logger.info(f"A2 liveaccess data field: {str(raw_data)[:600]}")
                ranges_out = []

                # Case 1: data = {"services": [...]}
                services = raw_data.get("services", []) if isinstance(raw_data, dict) else []
                if services:
                    for svc in services:
                        sid = svc.get("sid", "").upper()
                        for rng in svc.get("ranges", []):
                            rng_upper = rng.upper().strip()
                            ranges_out.append({
                                "range": rng_upper,
                                "rid": rng_upper,
                                "service": sid,
                                "last_at": svc.get("last_at", 0),
                            })

                # Case 2: data = {"ranges": [...]}
                elif isinstance(raw_data, dict) and raw_data.get("ranges"):
                    for rng in raw_data["ranges"]:
                        if isinstance(rng, dict):
                            rng_upper = (rng.get("range") or rng.get("rid") or rng.get("id", "")).upper().strip()
                            ranges_out.append({
                                "range": rng_upper,
                                "rid": rng_upper,
                                "service": rng.get("service", rng.get("sid", "")),
                                "last_at": rng.get("last_at", 0),
                            })
                        else:
                            rng_upper = str(rng).upper().strip()
                            ranges_out.append({"range": rng_upper, "rid": rng_upper, "service": "", "last_at": 0})

                # Case 3: data is a list directly
                elif isinstance(raw_data, list):
                    for item in raw_data:
                        if isinstance(item, dict):
                            sid = item.get("sid", item.get("service", "")).upper()
                            nested = item.get("ranges", [])
                            if nested:
                                # nested ranges array — e.g. [{"sid":"WhatsApp","ranges":["26138XXX",...]}]
                                for rng in nested:
                                    rng_upper = str(rng).upper().strip()
                                    if rng_upper:
                                        ranges_out.append({
                                            "range": rng_upper,
                                            "rid": rng_upper,
                                            "service": sid,
                                            "last_at": item.get("last_at", 0),
                                        })
                            else:
                                rng_upper = (item.get("range") or item.get("rid") or item.get("id", "")).upper().strip()
                                if rng_upper:
                                    ranges_out.append({
                                        "range": rng_upper,
                                        "rid": rng_upper,
                                        "service": sid,
                                        "last_at": item.get("last_at", 0),
                                    })
                        else:
                            rng_upper = str(item).upper().strip()
                            if rng_upper:
                                ranges_out.append({"range": rng_upper, "rid": rng_upper, "service": "", "last_at": 0})

                logger.info(f"A2 liveaccess parsed ranges: {len(ranges_out)} | samples: {ranges_out[:3]}")
                _a2_ranges_cache = {"data": ranges_out, "time": time.time()}
                return ranges_out
            else:
                logger.warning(f"A2 meta check failed: {data.get('meta')}")
        logger.warning(f"A2 liveaccess failed: {res.status_code}")
        return _a2_ranges_cache["data"]
    except Exception as e:
        logger.error(f"A2 active-ranges error: {e}")
        return _a2_ranges_cache["data"]

async def a2_get_number(rid):
    """VOLTX number provision — POST /getnum."""
    # Docs: rid = digits only, no trailing XXX — e.g. "26134" not "26134XXX"
    clean_rid = re.sub(r'X+$', '', str(rid).upper().strip())
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{A2_BASE_URL}/getnum",
                headers=get_a2_headers(),
                json={"rid": clean_rid}
            )
        logger.info(f"A2 getnum status: {res.status_code} | rid: {clean_rid} | raw: {res.text[:300]}")
        if res.status_code == 200:
            data = res.json()
            if data.get("meta", {}).get("code") == 200:
                result = data.get("data", {})
                result["_rid"] = data.get("rid", "")  # top-level rid save
                return result
        logger.warning(f"A2 getnum failed: {res.status_code} {res.text[:200]}")
        return None
    except Exception as e:
        logger.error(f"A2 getnum error: {e}")
        return None

async def a2_fetch_all_otps(rid=None):
    """VOLTX /success-otp — GET request, last 50 OTPs return করে।
    BUG FIX v17: API docs অনুযায়ী এটা GET endpoint, POST নয়।
    rid parameter নেই — API নিজেই আপনার allocated numbers এর OTP দেয়।
    """
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{A2_BASE_URL}/success-otp",
                headers=get_a2_headers(),
            )
        if res.status_code != 200:
            logger.warning(f"A2 success-otp HTTP {res.status_code} | rid={rid}")
            return []
        data = res.json()
        if data.get("meta", {}).get("code") != 200:
            logger.warning(f"A2 success-otp meta error: {data.get('meta')} | rid={rid}")
            return []
        otps_raw = data.get("data", {})
        otps = otps_raw.get("otps", []) if isinstance(otps_raw, dict) else []
        logger.info(f"A2 success-otp fetched: {len(otps)} otps | rid={rid}")
        return otps
    except Exception as e:
        logger.error(f"A2 fetch_all_otps error: {e}")
        return []


async def a2_poll_otp(target_number, seen_otp_ids=None, assign_time_ms=None, rid=None):
    """VOLTX OTP poll — GET /success-otp, number দিয়ে filter।
    NOTE: assign_time_ms check বাদ দেওয়া হয়েছে — API time field unix-ms নয়,
    API-র নিজস্ব sequence id, তাই bot time-র সাথে compare করা যায় না।
    Duplicate protection শুধু seen_otp_ids দিয়ে হয়।
    """
    otps = await a2_fetch_all_otps(rid=rid if rid else None)
    clean_target = re.sub(r'\D', '', str(target_number))

    for entry in otps:
        otp_id = entry.get("otp_id") or None

        # ── Duplicate skip ──
        if seen_otp_ids is not None and otp_id and otp_id in seen_otp_ids:
            continue

        num = re.sub(r'\D', '', str(entry.get("number", "")))
        logger.info(f"A2 OTP check: entry_num={num} | target={clean_target} | otp_id={otp_id}")

        if num and clean_target and (
            num == clean_target or
            num.endswith(clean_target[-8:]) or
            clean_target.endswith(num[-8:])
        ):
            logger.info(f"A2 OTP matched! num={num} | otp_id={otp_id}")
            return entry

    logger.info(f"A2 OTP: no match found for {clean_target}")
    return None

async def a2_get_console_hits():
    """VOLTX console hits — GET /console."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{A2_BASE_URL}/console",
                headers=get_a2_headers()
            )
        if res.status_code == 200:
            data = res.json()
            if data.get("meta", {}).get("code") == 200:
                return data.get("data", {}).get("hits", [])
        return []
    except Exception as e:
        logger.error(f"A2 console error: {e}")
        return []

async def do_get_number_a2(message, user_id, bot=None):
    """A2 (VOLTX SMS) number get — 2টা number, auto OTP poll।"""
    _bot = bot or message.bot
    chat_id = message.chat.id
    existing_msg_id = user_msg.get(chat_id)

    range_val = user_data[user_id].get("range", "")
    app_name  = user_data[user_id].get("app", "FACEBOOK")
    if not range_val:
        err_text = "❌ Range পাওয়া যায়নি। আবার শুরু করুন।"
        if existing_msg_id:
            try:
                await _bot.edit_message_text(err_text, chat_id=chat_id, message_id=existing_msg_id)
            except Exception:
                await _bot.send_message(chat_id, err_text)
        else:
            await _bot.send_message(chat_id, err_text)
        return

    rid_clean = range_val.upper().strip()  # a2_get_number() ভেতরে XXX strip করে, "26134XXX" → "26134" পাঠায়

    nums_got = []
    rids_got = []
    for attempt in range(2):
        result = await a2_get_number(rid_clean)
        if result:
            logger.info(f"A2 getnum result keys: {list(result.keys()) if isinstance(result, dict) else result}")
            # no_plus_number = "447404333228" → /success-otp এর number field এর সাথে EXACT match
            # national_number বাদ (short number, match fail) | full_number এ "+" আছে তাই after strip same
            full_num = (
                result.get("no_plus_number") or   # "447404333228" ← /success-otp number এর exact match
                result.get("full_number") or       # "+447404333228" → strip করলে same
                result.get("phone") or
                result.get("number") or
                result.get("msisdn") or
                result.get("phoneNumber") or
                result.get("phone_number") or
                ""
            )
            full_num = re.sub(r'\D', '', str(full_num)).strip()  # শুধু digits
            _rid_val = result.get("_rid", "")
            if full_num and full_num not in nums_got:
                nums_got.append(full_num)
                rids_got.append(_rid_val)
        await asyncio.sleep(0.5)

    if not nums_got:
        err = "❌ এই range এ এখন number পাওয়া যাচ্ছে না।\n\nকিছুক্ষণ পর আবার try করুন।"
        if existing_msg_id:
            try:
                await _bot.edit_message_text(err, chat_id=chat_id, message_id=existing_msg_id)
            except Exception:
                await _bot.send_message(chat_id, err)
        else:
            await _bot.send_message(chat_id, err)
        return

    user_data[user_id]["numbers"]         = nums_got
    user_data[user_id]["last_number"]     = nums_got[0]
    user_data[user_id]["panel"]           = "A2"
    user_data[user_id]["otp_active"]      = True
    user_data[user_id]["otp_running"]     = True
    user_data[user_id]["auto_otp_cancel"] = False

    asyncio.create_task(db_save_user_async(user_id, {
        "name":        user_data[user_id].get("name", "User"),
        "joined":      user_data[user_id].get("joined", ""),
        "app":         app_name,
        "panel":       "A2",
        "last_number": nums_got[0],
        "range":       range_val,
    }))

    display_range = rid_clean  # Already has XXX e.g. "22465XXX"
    _a2_country_code = a2_extract_country_code(display_range)
    _a2_country_name = COUNTRY_NAMES_CODE.get(_a2_country_code, user_data[user_id].get("country", "Unknown"))
    _a2_flag = COUNTRY_FLAGS_CODE.get(_a2_country_code, "🌍")
    card_text = (
        f"✅ <b>Numbers Assigned!</b>\n\n"
        f"<b>Service:</b> {app_name.title()} [A2]\n"
        f"🌍 <b>Country:</b> {_a2_flag} {_a2_country_name}\n"
        f"⏳ <b>Reserved:</b> 20 min\n\n"
        f"📩 OTPs forwarded automatically."
    )

    kb_rows = []
    colors = ["success", "primary"]
    for i, num in enumerate(nums_got):
        kb_rows.append([InlineKeyboardButton(
            f"📋 {num}",
            copy_text=CopyTextButton(text=num),
            api_kwargs={"style": colors[i % len(colors)]}
        )])
    kb_rows.append([InlineKeyboardButton("🔄 Change Numbers", callback_data="a2_change_numbers", api_kwargs={"style": "success"})])
    kb_rows.append([InlineKeyboardButton("🌍 Change Region",  callback_data="back_app",           api_kwargs={"style": "primary"})])
    _otp_link = OTP_CHANNEL_LINK or OTP_CHANNEL_JOIN_LINK or ""
    if _otp_link:
        kb_rows.append([InlineKeyboardButton("📢 OTP Channel", url=_otp_link, api_kwargs={"style": "primary"})])
    kb = InlineKeyboardMarkup(kb_rows)

    if existing_msg_id:
        try:
            await _bot.edit_message_text(
                card_text, chat_id=chat_id, message_id=existing_msg_id,
                parse_mode="HTML", reply_markup=kb
            )
            user_msg[chat_id] = existing_msg_id
        except Exception:
            sent = await _bot.send_message(chat_id, card_text, parse_mode="HTML", reply_markup=kb)
            user_msg[chat_id] = sent.message_id
    else:
        sent = await _bot.send_message(chat_id, card_text, parse_mode="HTML", reply_markup=kb)
        user_msg[chat_id] = sent.message_id

    cancel_all_otp_tasks(user_id)
    # ── BUG FIX: cancel_all_otp_tasks() otp_active=False করে দেয়।
    # তাই task create করার ঠিক আগে আবার True/False সঠিক করতে হবে।
    user_data[user_id]["auto_otp_cancel"] = False
    user_data[user_id]["otp_active"]      = True
    user_data[user_id]["otp_running"]     = True
    task = asyncio.create_task(auto_otp_a2(user_id, nums_got, rids_got, display_range, _bot, chat_id))
    add_otp_task(user_id, task)


async def auto_otp_a2(user_id, numbers, rids, display_range, bot, chat_id):
    """A2 OTP auto-poll — 10 sec interval, max 20 min.
    FIX: প্রতি cycle-এ একবারই /success-otp call করে সব number match করে।
    আগে প্রতি number-এ আলাদা API call হতো — double request bug ছিল।
    assign_time_ms check বাদ দেওয়া হয়েছে — API time field unix-ms নয়।
    BUG FIX v16: asyncio.create_task() immediately run হয় না — event loop এর
    পরের cycle এ run হয়। তাই task শুরুতেই state force করা হচ্ছে।
    """
    # ── CRITICAL FIX: task শুরুতে নিজেই state ensure করে নিচ্ছে ──
    # কারণ: create_task() এর পর অন্য callback cancel_all_otp_tasks() ডাকলে
    # otp_active=False হয়ে যেতে পারে এবং loop শুরুতেই break হয়।
    user_data[user_id]["auto_otp_cancel"] = False
    user_data[user_id]["otp_active"]      = True
    user_data[user_id]["otp_running"]     = True

    TIMEOUT = 1200
    start = time.time()
    received = set()
    seen_otp_ids = set()

    while True:
        if user_data.get(user_id, {}).get("auto_otp_cancel"):
            break
        if not user_data.get(user_id, {}).get("otp_active"):
            break
        if time.time() - start > TIMEOUT:
            # Timeout notification disabled
            break

        # ── BUG FIX v16: প্রতিটা number এর নিজস্ব rid দিয়ে আলাদা POST call ──
        # আগে: একবার GET করে সব OTP আনত → number match হত না
        # এখন: প্রতি number এর rid দিয়ে POST → সঠিক OTP আসে
        for idx, number in enumerate(list(numbers)):
            if number in received:
                continue

            # এই number এর rid বের করো
            _rid = rids[idx] if idx < len(rids) else None

            try:
                all_otps = await a2_fetch_all_otps(rid=_rid)
            except Exception as e:
                logger.error(f"auto_otp_a2 fetch error for {number}: {e}")
                continue

            clean_target = re.sub(r'\D', '', str(number))
            matched_entry = None

            for entry in all_otps:
                otp_id = entry.get("otp_id") or None
                if otp_id and otp_id in seen_otp_ids:
                    continue
                num = re.sub(r'\D', '', str(entry.get("number", "")))
                if num and clean_target and (
                    num == clean_target or
                    num.endswith(clean_target[-8:]) or
                    clean_target.endswith(num[-8:])
                ):
                    matched_entry = entry
                    break

            if not matched_entry:
                continue

            message_text = matched_entry.get("message", "")
            otp_code = extract_otp(message_text)
            if not otp_code:
                continue

            received.add(number)
            _otp_id_raw = matched_entry.get("otp_id") or None
            _otp_uid = _otp_id_raw if _otp_id_raw else f"{number}_{otp_code}_{message_text[:30]}"
            seen_otp_ids.add(_otp_uid)

            country_code = extract_country_code_from_number(number)
            country_name = COUNTRY_NAMES_CODE.get(country_code, "Unknown")
            flag = COUNTRY_FLAGS_CODE.get(country_code, "🌍")
            _user_app = user_data.get(user_id, {}).get("app", "FACEBOOK")
            detected_app = detect_app_from_message(message_text, _user_app)
            # SMS এ keyword না থাকলে user এর selected app use করো
            if detected_app == "FACEBOOK" and _user_app in ("WHATSAPP", "TELEGRAM", "INSTAGRAM", "TIKTOK", "SNAPCHAT"):
                detected_app = _user_app

            asyncio.create_task(
                send_otp_to_channel(bot, number, otp_code, detected_app,
                                    country_name, flag, message_text, "A2")
            )
            try:
                await send_otp_to_inbox(bot, chat_id, number, otp_code, detected_app,
                                        country_name, flag, message_text, "A2")
                logger.info(f"A2 OTP sent → user {user_id}: {otp_code} for {number} [{detected_app}]")
            except Exception as e:
                logger.error(f"auto_otp_a2 send error for {number}: {e}")

        if len(received) >= len(numbers):
            user_data[user_id]["otp_active"]  = False
            user_data[user_id]["otp_running"] = False
            break

        await asyncio.sleep(10)


async def job_a2_range_post(context):
    """A2 (VOLTX SMS) active ranges channel এ post করে।"""
    global _posted_sms_ids
    if not A2_API_KEY or not RANGE_CHANNEL_ID:
        return
    try:
        ranges = await a2_get_active_ranges()
        bot = context.bot
        from datetime import timezone, timedelta as _tdz
        now_bd = datetime.now(timezone(_tdz(hours=6)))
        post_count = 0

        # Fetch real console hits for SMS
        console_hits = await a2_get_console_hits()
        # Build range → latest SMS map (key = full range with XXX)
        range_sms_map = {}
        for hit in (console_hits or []):
            h_range = hit.get("range", "").upper().strip()
            if h_range and h_range not in range_sms_map:
                range_sms_map[h_range] = hit.get("message", "")

        for r in ranges:
            if post_count >= 3:
                break
            rng = r.get("range", "").upper().strip()
            if not rng:
                continue
            slot = now_bd.strftime('%Y-%m-%d %H:') + str(now_bd.minute // 5 * 5).zfill(2)
            unique_id = f"a2_{rng}_{slot}"
            if unique_id in _posted_sms_ids:
                continue

            clean_base = re.sub(r'X+$', '', rng).strip()
            display_range = rng  # Already has XXX e.g. "22465XXX"
            code = a2_extract_country_code(rng)
            range_flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
            country_name = COUNTRY_NAMES_CODE.get(code, code)

            # Real SMS from console — raw SMS না থাকলে skip
            raw_sms = range_sms_map.get(rng, range_sms_map.get(clean_base, ""))
            if not raw_sms:
                continue
            otp = extract_otp(raw_sms) or ""
            if not otp:
                continue

            # Clean SMS — remove <#> prefix and hash suffix
            clean_sms = re.sub(r'<#>\s*', '', raw_sms).strip() if raw_sms else ""
            lines = [l for l in clean_sms.splitlines() if l.strip() and not re.fullmatch(r'[A-Za-z0-9+/]{10,}', l.strip())]
            clean_sms = "\n".join(lines).strip()

            # ── v10 format: service tag + raw SMS + separator ──
            text = (
                f"{range_flag} {escape_mdv2(country_name)}\n\n"
                f"📞 `{escape_mdv2(display_range)}`\n"
                f"🔐 `{escape_mdv2(otp)}`\n"
                f"📘 Service: Facebook \\| A2\n"
                f"{escape_mdv2('────────────')}\n"
                f"📩"
            )
            if clean_sms:
                quoted = "\n".join(f">{escape_mdv2(line)}" for line in clean_sms.splitlines() if line.strip())
                text += f"\n{quoted}"

            # ── Keyboard: OTP copy box + Main Channel + Number Bot ──
            kb_rows = []
            kb_rows.append([InlineKeyboardButton(
                "🔑 COPY OTP",
                copy_text=CopyTextButton(text=otp),
                api_kwargs={"style": "success"}
            )])
            bottom_row = []
            _main_ch_link = OTP_CHANNEL_LINK or OTP_CHANNEL_JOIN_LINK or ""
            if _main_ch_link and len(_main_ch_link) > 10:
                bottom_row.append(InlineKeyboardButton("📢 Main Channel", url=_main_ch_link, api_kwargs={"style": "primary"}))
            if NUMBER_BOT_LINK:
                bottom_row.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
            if bottom_row:
                kb_rows.append(bottom_row)
            keyboard = InlineKeyboardMarkup(kb_rows)

            try:
                await safe_send_message(
                    bot, chat_id=RANGE_CHANNEL_ID,
                    text=text, parse_mode="MarkdownV2",
                    reply_markup=keyboard
                )
                _posted_sms_ids.add(unique_id)
                post_count += 1
                logger.info(f"A2 range post: posted {rng}")
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"A2 range post error [{rng}]: {e}")

        logger.info(f"A2 range post done: {post_count} posted")
    except Exception as e:
        logger.error(f"job_a2_range_post error: {e}")


# ══════════════════════════════════════════════════════════
#              S3 — CR API
# ══════════════════════════════════════════════════════════

async def fetch_cr_api_otps():
    try:
        if not CR_API_URL or not CR_API_TOKEN:
            logger.warning("CR API URL or TOKEN not set!")
            return []
        now = datetime.now()
        dt2 = now.strftime("%Y-%m-%d %H:%M:%S")
        dt1 = (now - timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
        params = {"token": CR_API_TOKEN, "dt1": dt1, "dt2": dt2, "records": 200}
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(CR_API_URL, params=params)
        logger.info(f"CR API status: {response.status_code}")
        if response.status_code != 200:
            logger.warning(f"CR API error: {response.text[:200]}")
            return []
        raw = response.text.strip()
        if not raw:
            logger.warning("CR API empty response")
            return []
        try:
            data = response.json()
        except Exception:
            logger.warning(f"CR API invalid JSON: {repr(raw[:100])}")
            return []
        logger.info(f"CR API response status: {data.get('status')}, records: {len(data.get('data', []))}")
        if data.get("status") != "success":
            logger.warning(f"CR API not success: {data}")
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
#              SHARK PANEL — Supabase OTP FETCH (S4/v1)
# ══════════════════════════════════════════════════════════

# Shark OTP seen cache (dedup)
_shark_otp_seen: set = set()

async def fetch_shark_otps_from_supabase() -> list:
    """
    Supabase shark_otps table থেকে last 30 min এর OTP fetch করো।
    Returns list of dicts: {number, otp, message, dt}
    """
    try:
        from datetime import timezone, timedelta as _td
        cutoff = (datetime.utcnow() - timedelta(minutes=30)).isoformat()
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/shark_otps",
                headers=_sb_headers(),
                params={
                    "select": "unique_id,number,otp,message,app,dt,created_at",
                    "created_at": f"gte.{cutoff}",
                    "order": "created_at.desc",
                    "limit": "200",
                }
            )
        if res.status_code != 200:
            logger.error(f"Shark Supabase fetch error: {res.status_code}")
            return []
        rows = res.json()
        if not isinstance(rows, list):
            return []
        return rows
    except Exception as e:
        logger.error(f"fetch_shark_otps_from_supabase error: {e}")
        return []

def is_shark_pool(pool_key: str) -> bool:
    """pool_key এ '_v1' থাকলে Shark Panel."""
    return "_v1_" in pool_key or pool_key.endswith("_v1")

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

def _build_unified_keyboard():
    """Main Channel + Number Bot buttons — same across all posts."""
    kb_row = []
    _ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
    if _ch_link and len(_ch_link) > 10:
        kb_row.append(InlineKeyboardButton("📢 Main Channel", url=_ch_link, api_kwargs={"style": "primary"}))
    if NUMBER_BOT_LINK:
        kb_row.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
    return InlineKeyboardMarkup([kb_row]) if kb_row else None

def _build_otp_post_header(number, app, country, flag, panel):
    """
    OTP Post header format:
    🇬🇳 GN FB 2246***445 [A1]
    — flag + country_iso(2) + SVC_SHORT + prefix***last3 [panel]
    """
    clean_num = str(number).replace("+", "").strip()
    # country ISO 2-letter — COUNTRY_NAME_TO_ISO দিয়ে দেখো আগে
    country_key = (country or "").lower().strip()
    iso_from_name = COUNTRY_NAME_TO_ISO.get(country_key, "")
    if iso_from_name:
        iso = iso_from_name[:2].upper()
    else:
        # number prefix থেকে বের করো
        country_code = extract_country_code_from_number(clean_num)
        iso = country_code[:2].upper() if country_code and country_code != "Unknown" else (country or "")[:2].upper()
    # service short
    app_up = (app or "").upper()
    svc = "FB" if any(x in app_up for x in ("FACEBOOK","FACE BOOK")) else (
          "IG" if "INSTAGRAM" in app_up else (
          "WA" if "WHATSAPP" in app_up else (
          "TK" if "TIKTOK" in app_up else app_up[:2])))
    # masked number: first 4 + *** + last 3
    if len(clean_num) > 7:
        masked = clean_num[:4] + "***" + clean_num[-3:]
    else:
        masked = clean_num
    panel_tag = panel.upper()
    return f"{flag} {iso} {svc} {masked} [{panel_tag}]"

async def send_otp_to_channel(bot, number, otp, app, country, flag, raw_sms="", panel="S2"):
    try:
        app_cap = app.capitalize()
        clean_num = str(number).replace("+", "").strip()
        hidden_num = ("+" + clean_num[:4] + "******" + clean_num[-3:]) if len(clean_num) > 7 else clean_num

        # Duplicate guard — same number+otp combo আগে post হলে skip
        _ch_uid = f"ch_{clean_num}_{otp}"
        if _ch_uid in _posted_sms_ids:
            logger.info(f"send_otp_to_channel: duplicate skip {_ch_uid}")
            return
        _posted_sms_ids.add(_ch_uid)

        # Clean raw SMS
        clean_sms = ""
        if raw_sms:
            clean_sms = raw_sms.replace("<#>", "").strip()

        panel_label = str(panel).upper() if panel else "S2"
        msg = (
            f"{flag} {escape_mdv2(country)}\n\n"
            f"📱 Number : {escape_mdv2(hidden_num)}\n"
            f"🔐 Code : {escape_mdv2(otp)}\n"
            f"📌 Panel : {escape_mdv2(panel_label)} \\| {escape_mdv2(app_cap)}"
        )
        if clean_sms:
            quoted = "\n".join(f">{escape_mdv2(line)}" for line in clean_sms.splitlines() if line.strip())
            msg += f"\n\n{quoted}"

        kb_rows = []
        # COPY OTP button — full width
        if otp:
            kb_rows.append([InlineKeyboardButton(
                "🔑 COPY OTP",
                copy_text=CopyTextButton(text=otp),
                api_kwargs={"style": "success"}
            )])
        # Main Channel + Number Bot
        kb_row = []
        _otp_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
        if _otp_ch_link and len(_otp_ch_link) > 10:
            kb_row.append(InlineKeyboardButton("📢 Main Channel", url=_otp_ch_link, api_kwargs={"style": "primary"}))
        if NUMBER_BOT_LINK:
            kb_row.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
        if kb_row:
            kb_rows.append(kb_row)
        keyboard = InlineKeyboardMarkup(kb_rows) if kb_rows else None

        try:
            await safe_send_message(bot, OTP_CHANNEL_ID, msg, parse_mode="MarkdownV2", reply_markup=keyboard)
        except Exception:
            plain = f"{flag} {country}\n📱 Number : {hidden_num}\n🔐 Code : {otp}"
            if clean_sms:
                plain += f"\n\n{clean_sms[:150]}"
            await safe_send_message(bot, OTP_CHANNEL_ID, plain, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Channel OTP send error: {e}")

async def send_otp_to_inbox(bot, chat_id, number, otp, app, country, flag, raw_sms="", panel="S2"):
    """
    Unified inbox OTP notification — সব panel এর জন্য।
    Design:
      🇲🇿 Mozambique Facebook (S-1) → 🟢
      📱 Phone : 258871116855
      [ 📋 Full SMS ]  [ 📋 123456 ]
    """
    try:
        clean_num = str(number).replace("+", "").strip()
        app_cap = app.capitalize()
        panel_label = panel.upper()

        header = f"{flag} {country} {app_cap} ({panel_label}) → 🟢"
        text = header

        buttons = []
        # Row 1: Phone number copy button
        buttons.append([InlineKeyboardButton(
            f"📱 Phone : {clean_num}",
            copy_text=CopyTextButton(text=clean_num),
            api_kwargs={"style": "primary"}
        )])
        # Row 2: Full SMS + OTP
        row = []
        if raw_sms:
            row.append(InlineKeyboardButton(
                "📋 Full SMS",
                copy_text=CopyTextButton(text=raw_sms[:256]),
                api_kwargs={"style": "success"}
            ))
        if otp:
            row.append(InlineKeyboardButton(
                f"📋 {otp}",
                copy_text=CopyTextButton(text=otp),
                api_kwargs={"style": "success"}
            ))
        if row:
            buttons.append(row)

        await bot.send_message(
            chat_id,
            text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons) if buttons else None
        )
    except Exception as e:
        logger.error(f"Inbox OTP send error: {e}")

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
            "panel": "S2",
            "country": None,
            "range": None,
            # ── Dual-number support ──
            "last_number": None,          # compat: first number (str)
            "numbers": [],                # [num1, num2] list
            "number_sessions": [],        # [session1, session2] list
            "waiting_for": None,
            "otp_active": False,
            "otp_running": False,
            "auto_otp_cancel": False,
            "number_session": None,       # compat: first session
            "country_r": None,
        }

def cancel_all_otp_tasks(user_id):
    if user_id in _otp_tasks:
        for task in _otp_tasks[user_id]:
            task.cancel()
        _otp_tasks[user_id] = []
    if user_id in user_data:
        user_data[user_id]["auto_otp_cancel"] = True
        user_data[user_id]["otp_active"]      = False
        user_data[user_id]["otp_running"]     = False
        # clear dual-number state
        user_data[user_id]["numbers"]         = []
        user_data[user_id]["number_sessions"] = []

def add_otp_task(user_id, task):
    if user_id not in _otp_tasks:
        _otp_tasks[user_id] = []
    _otp_tasks[user_id].append(task)
    # ── OTP task cleanup (নোট ২) ──
    _otp_tasks[user_id] = [t for t in _otp_tasks[user_id] if not t.done()]

async def cleanup_s1s2_panel(bot, user_id):
    """S2 ONLY — পুরনো panel message delete করো। S3 তে ব্যবহার করা যাবে না।"""
    # Delete number card (user_msg)
    try:
        old_card = user_msg.get(user_id)
        if old_card:
            try:
                await bot.delete_message(chat_id=user_id, message_id=old_card)
            except Exception:
                pass
    except Exception:
        pass
    user_msg.pop(user_id, None)

    # Delete keyboard panel (user_kb_msg)
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
    joined_statuses = ["member", "administrator", "creator"]
    all_joined = True

    for idx, ch_id in enumerate([MAIN_CHANNEL_CHECK_ID, OTP_CHANNEL_CHECK_ID, BACKUP_CHANNEL_CHECK_ID]):
        cache_key = f"{user_id}_ch{idx}"
        cached = _join_cache.get(cache_key)
        if cached and (now - cached["time"]) < 300:
            ch_joined = cached["joined"]
        else:
            try:
                m = await bot.get_chat_member(ch_id, user_id)
                ch_joined = m.status in joined_statuses
                _join_cache[cache_key] = {"joined": ch_joined, "time": now}
            except Exception:
                ch_joined = True  # error হলে block করব না
        if not ch_joined:
            all_joined = False

    # backward compat: return (ch1, ch2) — all_joined হলে (True, True) নইলে (False, False)
    return (all_joined, all_joined)

def clear_join_cache(user_id):
    for idx in range(3):
        _join_cache.pop(f"{user_id}_ch{idx}", None)

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
        [KeyboardButton("📲 Get Number"),   KeyboardButton("📦 My Numbers")],
        [KeyboardButton("📡 Custom Range"), KeyboardButton("🚦 Live Traffic")],
        [KeyboardButton("✈️ Telegram"),     KeyboardButton("👤 Profile")],
        [KeyboardButton("🆘 Support")],
    ]
    return ReplyKeyboardMarkup(
        buttons,
        resize_keyboard=True,
        one_time_keyboard=False,
        is_persistent=True,
    )

def admin_keyboard_unified():
    """Unified Admin Panel — single clean keyboard."""
    return InlineKeyboardMarkup([
        # ── Row 1: Stats & Users ──
        [
            InlineKeyboardButton("📊 Stats",        callback_data="admin_stats_s12",     api_kwargs={"style": "primary"}),
            InlineKeyboardButton("👥 Users",         callback_data="admin_allusers",      api_kwargs={"style": "primary"}),
        ],
        # ── Row 2: API & Traffic ──
        [
            InlineKeyboardButton("📡 API Status",   callback_data="admin_apistatus",     api_kwargs={"style": "primary"}),
            InlineKeyboardButton("🚦 Live Traffic", callback_data="admin_livetraffic",   api_kwargs={"style": "primary"}),
        ],
        # ── Row 3: Panels ──
        [
            InlineKeyboardButton("🟢 S2 Panel",     callback_data="admin_s2panel",       api_kwargs={"style": "success"}),
            InlineKeyboardButton("🔴 S3 Panel",     callback_data="s3admin_stats",       api_kwargs={"style": "danger"}),
        ],
        # ── Row 3b: A1 + A2 Panel ──
        [
            InlineKeyboardButton("🆕 A1 Panel",     callback_data="admin_a1panel",       api_kwargs={"style": "primary"}),
            InlineKeyboardButton("⚡ A2 Panel",     callback_data="admin_a2panel",       api_kwargs={"style": "success"}),
        ],
        # ── Row 4: Broadcast & Alert ──
        [
            InlineKeyboardButton("📢 Broadcast",    callback_data="admin_broadcast_s12", api_kwargs={"style": "primary"}),
            InlineKeyboardButton("🚨 Send Alert",   callback_data="s3admin_broadcast",   api_kwargs={"style": "danger"}),
        ],
        # ── Row 5: Analytics & Sessions ──
        [
            InlineKeyboardButton("📈 Analytics",    callback_data="s3admin_analytics",   api_kwargs={"style": "primary"}),
            InlineKeyboardButton("🔄 Refresh Sessions", callback_data="admin_refresh",   api_kwargs={"style": "primary"}),
        ],
        # ── Row 6: Numbers ──
        [
            InlineKeyboardButton("📤 Upload Numbers",  callback_data="s3admin_addnumbers", api_kwargs={"style": "success"}),
            InlineKeyboardButton("🗑 Delete Numbers",  callback_data="s3admin_delete",     api_kwargs={"style": "danger"}),
        ],
        # ── Row 7: Bulk ──
        [
            InlineKeyboardButton("📦 Bulk ON",      callback_data="admin_bulk_on",       api_kwargs={"style": "success"}),
            InlineKeyboardButton("📦 Bulk OFF",     callback_data="admin_bulk_off",      api_kwargs={"style": "danger"}),
        ],
        # ── Row 8: System ──
        [
            InlineKeyboardButton("⚙️ Settings",     callback_data="s3admin_settings",    api_kwargs={"style": "primary"}),
            InlineKeyboardButton("🧹 Clear Cache",  callback_data="admin_clearcache",    api_kwargs={"style": "primary"}),
        ],
        # ── Row 9: Danger Zone ──
        [
            InlineKeyboardButton("♻️ Restart Bot",  callback_data="admin_restart",       api_kwargs={"style": "danger"}),
            InlineKeyboardButton("⛔ Stop Bot",     callback_data="admin_stop",          api_kwargs={"style": "danger"}),
        ],
    ])

# Backward compat aliases
def admin_keyboard_s1s2():
    return admin_keyboard_unified()

def admin_keyboard_s3():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 Statistics",      callback_data="s3admin_stats",      api_kwargs={"style": "primary"}),
            InlineKeyboardButton("📈 Analytics",       callback_data="s3admin_analytics",  api_kwargs={"style": "primary"}),
        ],
        [
            InlineKeyboardButton("📢 Broadcast",       callback_data="s3admin_broadcast",  api_kwargs={"style": "primary"}),
            InlineKeyboardButton("📤 Upload Numbers",  callback_data="s3admin_addnumbers", api_kwargs={"style": "success"}),
        ],
        [
            InlineKeyboardButton("🗑️ Delete Numbers",  callback_data="s3admin_delete",     api_kwargs={"style": "danger"}),
            InlineKeyboardButton("⚙️ Settings",        callback_data="s3admin_settings",   api_kwargs={"style": "primary"}),
        ],
    ])

async def panel_select_inline():
    pool = get_numbers_pool()
    fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
    ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 Facebook A1", callback_data="select_panel_A1_fb", api_kwargs={"style": "success"})],
        [InlineKeyboardButton("⚡ Facebook A2", callback_data="select_panel_A2_fb", api_kwargs={"style": "success"})],
        [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2",    api_kwargs={"style": "success"})],
        [InlineKeyboardButton(f"🔴 Facebook S3 ({fb_count})", callback_data="s3app:fb", api_kwargs={"style": "danger"})],
        [InlineKeyboardButton(f"📸 Instagram S3 ({ig_count})", callback_data="s3app:ig", api_kwargs={"style": "danger"})],
        [InlineKeyboardButton("💬 WhatsApp",    callback_data="select_panel_WA",    api_kwargs={"style": "primary"})],
        [InlineKeyboardButton("✈️ Telegram",    callback_data="select_panel_TG",    api_kwargs={"style": "primary"})],
    ])

def server_select_inline(app_name):
    # Legacy — no longer used in main flow
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📘 Facebook S2", callback_data=f"app_s2_{app_name}", api_kwargs={"style": "primary"})],
    ])

def country_select_inline(countries, app_name):
    buttons = []
    row = []
    for i, c in enumerate(countries[:20]):
        country_name = c if isinstance(c, str) else c.get("country", "")
        panel = c.get("panel", "S2") if isinstance(c, dict) else "S2"
        flag = get_flag_by_iso(country_name)
        row.append(InlineKeyboardButton(
            f"{flag} {country_name}",
            callback_data=f"country_{panel}_{country_name}",
            api_kwargs={"style": "primary"}
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    # Instagram button — panel same as current
    first_panel = countries[0].get("panel", "S2") if countries and isinstance(countries[0], dict) else "S2"
    buttons.append([InlineKeyboardButton("📸 Instagram", callback_data=f"ig_panel_{first_panel}", api_kwargs={"style": "success"})])
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"back_app", api_kwargs={"style": "primary"})])
    return InlineKeyboardMarkup(buttons)

def range_select_inline(ranges, app_name, country):
    buttons = []
    for r in ranges[:15]:
        rv = r["range"] if isinstance(r, dict) else r
        if not rv.upper().endswith('X'):
            rv_display = rv + "XXX"
        else:
            rv_display = rv
        buttons.append([InlineKeyboardButton(f"📡 {rv_display}", callback_data=f"range_{rv}", api_kwargs={"style": "primary"})])
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}", api_kwargs={"style": "primary"})])
    return InlineKeyboardMarkup(buttons)

def after_number_inline_s1s2(numbers, range_val, otp_status=None):
    """S2 number card — S3 মতো design (number box)."""
    if isinstance(numbers, str):
        numbers = [numbers]
    if otp_status is None:
        otp_status = {}

    buttons = []
    colors = ["success", "primary", "success"]
    for i, num in enumerate(numbers):
        clean = str(num).replace("+", "").strip()
        has_otp = otp_status.get(clean, False)
        label = f"📋 {clean} ✅" if has_otp else f"📋 {clean}"
        color = colors[i % len(colors)]
        buttons.append([InlineKeyboardButton(
            label,
            copy_text=CopyTextButton(text=clean),
            api_kwargs={"style": color}
        )])

    _ch_link = OTP_CHANNEL_LINK or OTP_CHANNEL_JOIN_LINK or ""
    buttons.append([InlineKeyboardButton("🔄 Change Numbers", callback_data=f"new_number_{range_val}", api_kwargs={"style": "success"})])
    buttons.append([InlineKeyboardButton("🌍 Change Region", callback_data="back_app", api_kwargs={"style": "primary"})])
    if _ch_link:
        buttons.append([InlineKeyboardButton("📢 OTP Channel", url=_ch_link, api_kwargs={"style": "primary"})])
    return InlineKeyboardMarkup(buttons)

def after_number_inline_s3(pool_key, numbers=None):
    rows = []
    if numbers:
        colors = ["success", "primary", "success"]  # green, purple, green alternating
        for i, num in enumerate(numbers):
            color = colors[i % len(colors)]
            rows.append([InlineKeyboardButton(
                f"📋 {num}",
                copy_text=CopyTextButton(text=str(num)),
                api_kwargs={"style": color}
            )])
    rows.append([InlineKeyboardButton("🔄 Change Numbers", callback_data=f"s3change:{pool_key}", api_kwargs={"style": "success"})])
    rows.append([InlineKeyboardButton("🌍 Change Country", callback_data="s3changecountry", api_kwargs={"style": "primary"})])
    rows.append([InlineKeyboardButton("📢 OTP Channel", url=OTP_CHANNEL_JOIN_LINK, api_kwargs={"style": "primary"})])
    return InlineKeyboardMarkup(rows)

# ══════════════════════════════════════════════════════════
#              AUTO OTP — S1/S2
# ══════════════════════════════════════════════════════════


async def auto_otp_multi(message, numbers, user_id, range_val, bot=None):
    """
    Dual-number OTP polling for S2.
    • numbers = [num1] or [num1, num2]
    • Per-number independent 10-min timeout
    • OTP received → +5 min extension for THAT number only
    • HTML parse mode with modern copy-box UI
    """
    init_user(user_id)
    app      = user_data[user_id].get("app", "FACEBOOK")
    panel    = user_data[user_id].get("panel", "S2")
    country_r = user_data[user_id].get("country_r") or user_data[user_id].get("country", "")
    flag      = get_flag_by_iso(country_r)
    chat_id   = message.chat.id

    user_data[user_id]["otp_active"]      = True
    user_data[user_id]["otp_running"]     = True
    user_data[user_id]["auto_otp_cancel"] = False

    # ── Per-number state ──
    OTP_BASE_TIMEOUT   = 20 * 60   # 20 min
    OTP_EXTEND_SECONDS = 5  * 60   # +5 min on OTP receipt
    OTP_POLL_FAST      = 5          # first 60s
    OTP_POLL_SLOW      = 10         # after 60s

    num_state = {}   # number -> {otp_lines, elapsed, deadline, done}
    for num in numbers:
        clean = str(num).replace("+", "").strip()
        num_state[clean] = {
            "otp_lines" : [],
            "elapsed"   : 0,
            "deadline"  : time.time() + OTP_BASE_TIMEOUT,
            "done"      : False,
        }

    sent_message = None

    # ── UI Builders ──
    CIRCLE = {0: "①", 1: "②", 2: "③"}

    def _number_line(i, num):
        return f'{CIRCLE.get(i, f"[{i+1}]")} <code>+{num}</code>'

    def build_waiting_msg():
        _cr = country_r or "Unknown"
        return (
            f"✅ <b>Numbers Assigned!</b>\n\n"
            f"<b>Service:</b> {app.title()} [{panel}]\n"
            f"🌍 <b>Country:</b> {flag} {_cr}\n"
            f"⏳ <b>Reserved:</b> 20 min\n\n"
            f"📩 OTPs forwarded automatically."
        )

    def build_otp_msg():
        _cr = country_r or "Unknown"
        return (
            f"✅ <b>Numbers Assigned!</b>\n\n"
            f"<b>Service:</b> {app.title()} [{panel}]\n"
            f"🌍 <b>Country:</b> {flag} {_cr}\n"
            f"⏳ <b>Reserved:</b> 20 min\n\n"
            f"📩 OTPs forwarded automatically."
        )

    def get_otp_status():
        """copy button এ ✅ দেখানোর জন্য"""
        return {
            str(num).replace("+", "").strip(): bool(num_state.get(str(num).replace("+", "").strip(), {}).get("otp_lines"))
            for num in numbers
        }

    def any_otp_received():
        return any(s["otp_lines"] for s in num_state.values())

    def all_done():
        return all(s["done"] for s in num_state.values())

    _last_text = [None]

    async def update_msg(text):
        nonlocal sent_message
        if text == _last_text[0]:
            return  # same text — edit করবো না
        _last_text[0] = text
        kb = after_number_inline_s1s2(numbers, range_val, otp_status=get_otp_status())
        try:
            if sent_message and hasattr(sent_message, 'message_id'):
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=sent_message.message_id,
                        text=text,
                        parse_mode="HTML",
                        reply_markup=kb
                    )
                except Exception:
                    pass
            # নতুন message পাঠাবো না কখনো
        except Exception as e:
            logger.error(f"update_msg error: {e}")

    # Initial message
    try:
        init_kb = after_number_inline_s1s2(numbers, range_val, otp_status={})
        init_text = build_waiting_msg()

        class _FakeMsg:
            def __init__(self, mid): self.message_id = mid

        if chat_id in user_msg:
            try:
                sent_message = await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=user_msg[chat_id],
                    text=init_text,
                    parse_mode="HTML",
                    reply_markup=init_kb
                )
                # edit_message_text সবসময় Message object return করে না — safe করো
                if not hasattr(sent_message, 'message_id'):
                    sent_message = _FakeMsg(user_msg[chat_id])
            except Exception as e:
                logger.warning(f"Card edit failed, keeping existing: {e}")
                sent_message = _FakeMsg(user_msg[chat_id])
        else:
            sent_message = await message.reply_text(init_text, parse_mode="HTML", reply_markup=init_kb)
            user_msg[chat_id] = sent_message.message_id
        _last_text[0] = init_text
    except Exception as e:
        logger.error(f"Initial message error: {e}")
        return

    async def poll_number(clean):
        """Poll OTP for a single number."""
        st = num_state[clean]
        _panel = user_data.get(user_id, {}).get("panel", "S2")
        try:
            if False:  # S1 removed
                data = await api_get_info_s2(search=clean, status="success")
            else:
                data = await api_get_info_s2(search=clean, status="success")

            if data.get("meta", {}).get("code") != 200:
                return

            for n in (data.get("data", {}).get("numbers") or []):
                api_num = str(n.get("number", "")).replace("+", "").replace(" ", "").strip()
                if clean != api_num:
                    continue

                _otp_field = str(n.get("otp") or "").strip()
                _msg_field = (
                    n.get("message") or n.get("sms") or
                    n.get("raw_sms") or n.get("text") or
                    n.get("body") or n.get("content") or ""
                ).strip()

                if re.fullmatch(r"0+", _otp_field) or not _otp_field:
                    raw_otp = _msg_field or _otp_field
                else:
                    raw_otp = _otp_field or _msg_field

                otp = extract_otp(raw_otp)
                if not otp or re.fullmatch(r"0+", otp):
                    if _msg_field and _msg_field != raw_otp:
                        fb = extract_otp(_msg_field)
                        if fb and not re.fullmatch(r"0+", fb):
                            otp, raw_otp = fb, _msg_field

                if otp and otp not in st["otp_lines"]:
                    st["otp_lines"].append(otp)
                    # Extend deadline for THIS number only
                    st["deadline"] = max(st["deadline"], time.time() + OTP_EXTEND_SECONDS)

                    country_result = n.get("country", "").strip() or country_r
                    flag_result    = get_flag_by_iso(country_result)
                    detected_app   = detect_app_from_message(raw_otp, app)
                    # raw SMS এ keyword না থাকলে user এর app context ব্যবহার করো
                    if detected_app == "FACEBOOK" and app in ("INSTAGRAM", "TIKTOK", "SNAPCHAT", "WHATSAPP", "TELEGRAM"):
                        detected_app = app
                    if bot:
                        # ── Inbox notification — নতুন design ──
                        await send_otp_to_inbox(bot, chat_id, clean, otp, detected_app, country_result, flag_result, raw_otp, panel)
                        # ── Channel post ──
                        await send_otp_to_channel(bot, clean, otp, detected_app, country_result, flag_result, raw_otp, panel)
        except Exception as e:
            logger.error(f"poll_number({clean}) error: {e}")

    async def _run():
        global_elapsed = 0
        while not all_done():
            if user_data.get(user_id, {}).get("auto_otp_cancel"):
                for st in num_state.values():
                    st["done"] = True
                break

            # Poll all active numbers in parallel
            active = [c for c, st in num_state.items() if not st["done"]]
            await asyncio.gather(*[poll_number(c) for c in active])

            # Check deadlines per number (independent)
            for clean, st in num_state.items():
                if not st["done"] and time.time() >= st["deadline"]:
                    st["done"] = True
                    logger.info(f"⌛ S2 OTP timeout for +{clean}")

            # Update UI
            if any_otp_received():
                await update_msg(build_otp_msg())
            else:
                await update_msg(build_waiting_msg())

            if all_done():
                break

            interval = OTP_POLL_FAST if global_elapsed < 60 else OTP_POLL_SLOW
            await asyncio.sleep(interval)
            global_elapsed += interval

        # Return all number sessions
        sessions = user_data[user_id].get("number_sessions", [])
        for sess in sessions:
            if sess and sess.get("token"):
                if False:  # S1 removed
                    await s2_pool.return_number_session(sess)
                else:
                    await s2_pool.return_number_session(sess)
        user_data[user_id]["number_sessions"] = []
        user_data[user_id]["number_session"]  = None
        user_data[user_id]["otp_running"]     = False
        user_data[user_id]["otp_active"]      = False

    wrapper = asyncio.create_task(_run())
    add_otp_task(user_id, wrapper)

# ══════════════════════════════════════════════════════════
#              A1 — ZENEX NUMBER + OTP FLOW
# ══════════════════════════════════════════════════════════

def after_number_inline_a1(num_list, service_name, otp_status=None):
    """A1 number card inline keyboard — S3 মতো number box."""
    if otp_status is None:
        otp_status = {}
    rows = []
    colors = ["success", "primary", "success"]
    for i, num in enumerate(num_list):
        clean = str(num).replace("+", "").strip()
        has_otp = otp_status.get(clean, False)
        label = f"📋 {clean} ✅" if has_otp else f"📋 {clean}"
        color = colors[i % len(colors)]
        rows.append([InlineKeyboardButton(
            label,
            copy_text=CopyTextButton(text=clean),
            api_kwargs={"style": color}
        )])
    rows.append([InlineKeyboardButton("🔄 Change Numbers", callback_data="a1_change_numbers", api_kwargs={"style": "primary"})])
    rows.append([InlineKeyboardButton("🌍 Change Range", callback_data="select_panel_A1_fb", api_kwargs={"style": "primary"})])
    _ch_link = OTP_CHANNEL_LINK or OTP_CHANNEL_JOIN_LINK or ""
    if _ch_link:
        rows.append([InlineKeyboardButton("📢 OTP Channel", url=_ch_link, api_kwargs={"style": "success"})])
    return InlineKeyboardMarkup(rows)


async def do_get_number_a1(message, user_id, bot=None):
    """A1 (ZENEX) — S3 style card + number box + OTP polling. Facebook=2 numbers, Instagram=1 number."""
    init_user(user_id)
    range_val = user_data[user_id].get("range", "")
    service_name = user_data[user_id].get("a1_service", "Facebook New Account")
    zenex_service = user_data[user_id].get("a1_zenex_service", "Facebook")
    chat_id = message.chat.id

    # Instagram = 1 number, others = 2 numbers
    is_instagram = "instagram" in zenex_service.lower() or "instagram" in service_name.lower()
    fetch_count = 1 if is_instagram else 2

    if not range_val:
        await bot.send_message(chat_id, "❌ Range select করা হয়নি!")
        return

    if not ZENEX_API_KEY:
        await bot.send_message(chat_id, "❌ ZENEX API key সেট করা নেই! Railway ENV এ ZENEX_API_KEY add করুন।")
        return

    # existing message থাকলে সেটা use করো, নতুন message পাঠাবো না
    existing_msg_id = user_msg.get(chat_id)
    if existing_msg_id:
        class _FakeMsg:
            def __init__(self, mid): self.message_id = mid
        loading_msg = _FakeMsg(existing_msg_id)
    else:
        loading_msg = await bot.send_message(chat_id, "⏳ Searching Number...")
        await asyncio.sleep(1)
        try:
            await bot.edit_message_text("📡 Connecting Server...", chat_id=chat_id, message_id=loading_msg.message_id)
            await asyncio.sleep(1)
        except Exception:
            pass
        user_msg[chat_id] = loading_msg.message_id
    results = await asyncio.gather(
        *[zenex_get_number(range_val) for _ in range(fetch_count)],
        return_exceptions=True
    )

    numbers = []
    for r in results:
        if isinstance(r, dict) and r.get("full_number"):
            full = r.get("full_number", "")
            if full and full not in [n.get("full_number") for n in numbers]:
                numbers.append(r)

    if not numbers:
        try:
            await bot.edit_message_text("❌ Number পাওয়া যায়নি! আবার try করুন।", chat_id=chat_id, message_id=loading_msg.message_id)
        except Exception:
            pass
        return

    country = numbers[0].get("country", "Unknown")
    flag = get_flag_by_iso(country)
    num_list = [n.get("full_number", "") for n in numbers]

    user_data[user_id]["a1_numbers"] = num_list
    user_data[user_id]["a1_otp_received"] = {}

    # ── S3 মতো card text ──
    CIRCLE = {0: "①", 1: "②", 2: "③"}
    card_text = (
        f"✅ <b>Numbers Assigned!</b>\n\n"
        f"<b>Service:</b> {service_name} [A1]\n"
        f"🌍 <b>Country:</b> {flag} {country}\n"
        f"⏳ <b>Reserved:</b> 20 min\n\n"
        f"📩 OTPs forwarded automatically."
    )

    otp_status = {}
    try:
        await bot.edit_message_text(
            card_text,
            chat_id=chat_id,
            message_id=loading_msg.message_id,
            parse_mode="HTML",
            reply_markup=after_number_inline_a1(num_list, service_name, otp_status)
        )
        card_msg_id = loading_msg.message_id
    except Exception as e:
        logger.warning(f"A1 card edit failed, keeping existing: {e}")
        card_msg_id = loading_msg.message_id

    # ── OTP polling — 20 min ──
    OTP_BASE_TIMEOUT = 20 * 60
    OTP_EXTEND_SECONDS = 5 * 60
    deadline = {num: time.time() + OTP_BASE_TIMEOUT for num in num_list}
    otp_received = {}  # num -> [otp_code, ...]

    async def _update_card():
        """Card update with OTP status."""
        try:
            await bot.edit_message_text(
                card_text,
                chat_id=chat_id,
                message_id=card_msg_id,
                parse_mode="HTML",
                reply_markup=after_number_inline_a1(num_list, service_name, otp_status)
            )
        except Exception:
            pass

    async def poll_a1():
        while True:
            now = time.time()
            all_done = True
            for num_data in numbers:
                full = num_data.get("full_number", "")
                clean = full.replace("+", "").strip()
                if now >= deadline.get(full, 0) and full in otp_received:
                    continue
                if now >= deadline.get(full, 0):
                    continue
                all_done = False
                entry = await zenex_poll_otp(full)
                if entry:
                    otp_text = entry.get("otp", "") or entry.get("message", "") or ""
                    otp_code = extract_otp(otp_text) or otp_text[:10]
                    if otp_code and otp_code not in otp_received.get(full, []):
                        otp_received.setdefault(full, []).append(otp_code)
                        otp_status[clean] = True
                        deadline[full] = max(deadline.get(full, 0), time.time() + OTP_EXTEND_SECONDS)

                        # ── Inbox notification — নতুন design ──
                        try:
                            c_flag = get_flag_by_iso(entry.get("country", country))
                            c_name = entry.get("country", country)
                            await send_otp_to_inbox(bot, chat_id, full, otp_code, service_name, c_name, c_flag, otp_text, "A1")
                        except Exception as e:
                            logger.error(f"A1 inbox send error: {e}")

                        # ── Channel OTP post — send_otp_to_channel মতো ──
                        try:
                            c_flag = get_flag_by_iso(entry.get("country", country))
                            c_name = entry.get("country", country)
                            await send_otp_to_channel(bot, full, otp_code, service_name, c_name, c_flag, otp_text, "A1")
                        except Exception as e:
                            logger.error(f"A1 channel send error: {e}")

                        # ── Card update ──
                        await _update_card()

            if all_done or all(time.time() >= deadline.get(n.get("full_number", ""), 0) for n in numbers):
                break
            await asyncio.sleep(5)

    asyncio.create_task(poll_a1())

async def do_get_number(message, user_id, count=1, user_name="User", bot=None):
    """
    Dual-number fetch for S2.
    Fetches 2 numbers in parallel using different sessions.
    Same range, same country.
    """
    init_user(user_id)
    range_val = user_data[user_id].get("range")
    app       = user_data[user_id].get("app", "FACEBOOK")
    panel     = user_data[user_id].get("panel", "S2")
    chat_id   = message.chat.id

    if not range_val:
        await message.reply_text(
            "❌ Range select করা হয়নি!\n\n🏠 Start → Service → Country → Range",
            reply_markup=main_keyboard(user_id)
        )
        return

    user_data[user_id]["last_number"]    = None
    user_data[user_id]["numbers"]        = []
    user_data[user_id]["number_sessions"] = []

    # ── Reuse existing card — delete করা হবে না, edit করা হবে ──
    # user_msg[chat_id] থাকলে সেই message_id টাই রাখো।
    # auto_otp_multi ওই message_id edit করে কার্ড আপডেট করবে।
    # নতুন message পাঠানো বা delete করা হবে না।

    # ── Fetch numbers: Instagram → 1 number, others (Facebook etc.) → 2 numbers ──
    api_fn = api_get_number_s2
    fetch_count = 1 if app == "INSTAGRAM" else 2
    results = await asyncio.gather(
        *[api_fn(range_val, app) for _ in range(fetch_count)],
        return_exceptions=True
    )

    numbers  = []
    sessions = []
    country_r = ""
    pool = s2_pool

    for res in results:
        if isinstance(res, Exception):
            continue
        data, sess = res
        if data.get("meta", {}).get("code") == 200:
            nd = data["data"]
            num = (
                nd.get("number") or nd.get("num") or
                nd.get("phone")  or nd.get("mobile") or ""
            )
            if num and num not in numbers:
                numbers.append(num)
                sessions.append(sess)
                if not country_r:
                    country_r = nd.get("country", "") or user_data[user_id].get("country", "")
            else:
                # duplicate number — return session
                if sess:
                    await pool.return_number_session(sess)
        else:
            # failed fetch — return unused session
            if sess:
                await pool.return_number_session(sess)

    if numbers:
        user_data[user_id]["numbers"]         = numbers
        user_data[user_id]["last_number"]     = numbers[0]
        user_data[user_id]["number_sessions"] = sessions
        user_data[user_id]["number_session"]  = sessions[0] if sessions else None
        user_data[user_id]["auto_otp_cancel"] = False
        user_data[user_id]["country_r"]       = country_r
        asyncio.create_task(auto_otp_multi(message, numbers, user_id, range_val, bot=bot))
        return

    # ── All fetches failed — auto next range ──
    country = user_data[user_id].get("country", "")
    tried_ranges = user_data[user_id].setdefault("tried_ranges", set())
    tried_ranges.add(range_val)

    if country:
        if app == "INSTAGRAM":
            all_ranges = await get_instagram_ranges(country, panel=panel)
        else:
            all_ranges = await get_all_ranges_for_country(app, country, panel=panel)
        next_range = None
        for r in all_ranges:
            rv = r.get("range", "")
            if rv and rv not in tried_ranges:
                next_range = rv
                break

        if next_range:
            user_data[user_id]["range"] = next_range
            await message.reply_text(
                f"⚠️ Range <code>{range_val}</code> শেষ!\n⏳ Next range try হচ্ছে: <code>{next_range}</code>...",
                parse_mode="HTML"
            )
            await do_get_number(message, user_id, bot=bot)
            return
        else:
            user_data[user_id]["tried_ranges"] = set()
            await message.reply_text(
                f"❌ <b>{country}</b> তে সব range শেষ!\n\nঅন্য country select করুন।",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🌍 Change Country", callback_data="back_app", api_kwargs={"style": "primary"})]
                ])
            )
            return

    # Generic error from last result
    last_data = results[-1][0] if results and not isinstance(results[-1], Exception) else {}
    err_msg = last_data.get("message") or last_data.get("error") or "Number পাওয়া যায়নি"
    await message.reply_text(f"❌ {err_msg}", reply_markup=main_keyboard(user_id))

# ══════════════════════════════════════════════════════════
#              S3 — OTP POLLING (CR API)
# ══════════════════════════════════════════════════════════

async def _s3_send_with_retry(bot, chat_id, text, parse_mode=None, reply_markup=None, max_retries=3):
    """Telegram send with auto-retry — silent fail নেই, log থাকবে।"""
    import hashlib as _hl
    for attempt in range(1, max_retries + 1):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup
            )
            return True
        except Exception as e:
            err = str(e).lower()
            if "retry after" in err or "flood" in err:
                wait = int(re.search(r'\d+', str(e)).group() or 5)
                logger.warning(f"S3 flood wait {wait}s (attempt {attempt})")
                await asyncio.sleep(wait + 1)
            elif attempt < max_retries:
                logger.warning(f"S3 send fail attempt {attempt}: {e}")
                await asyncio.sleep(2 * attempt)
            else:
                logger.error(f"S3 send FINAL FAIL after {max_retries} attempts: {e}")
                return False
    return False

# ══════════════════════════════════════════════════════════
#              SHARK OTP POLL — S4/v1
# ══════════════════════════════════════════════════════════

async def _poll_shark_otps(context):
    """Supabase shark_otps থেকে নতুন OTP নিয়ে v1 session user দের notify করো।"""
    global _shark_otp_seen
    try:
        import hashlib
        shark_rows = await fetch_shark_otps_from_supabase()
        if not shark_rows:
            return

        # OTP channel keyboard
        _s4_kb_buttons = []
        _s4_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
        if _s4_ch_link and len(_s4_ch_link) > 10:
            _s4_kb_buttons.append(InlineKeyboardButton("📢 Main Channel", url=_s4_ch_link, api_kwargs={"style": "primary"}))
        if NUMBER_BOT_LINK:
            _s4_kb_buttons.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
        otp_channel_keyboard = InlineKeyboardMarkup([_s4_kb_buttons]) if _s4_kb_buttons else None

        sent_count = 0
        for row in shark_rows:
            uid_key = row.get("unique_id", "")
            if not uid_key or uid_key in _shark_otp_seen:
                continue

            number  = str(row.get("number", "")).strip()
            message = str(row.get("message", "")).strip()
            otp_code = str(row.get("otp", "")).strip()
            dt       = str(row.get("dt", "")).strip()

            if not number or not message:
                continue

            _shark_otp_seen.add(uid_key)

            # Cache size limit
            if len(_shark_otp_seen) > 5000:
                _shark_otp_seen = set(list(_shark_otp_seen)[-2500:])

            country = extract_country_code_from_number(number)
            flag = COUNTRY_FLAGS_CODE.get(country, "🌍")
            hidden = hide_number(number)
            country_name = COUNTRY_NAMES_CODE.get(country, "Unknown")
            detected_app = detect_app_from_message(message, default_app="FACEBOOK")
            detected_app_cap = detected_app.capitalize()

            # ── S3 v1 OTP Post — real OTP না থাকলে channel post করবো না ──
            if not otp_code:
                logger.info(f"S3v1 skip channel post — no OTP for {hidden}")
                continue

            await send_otp_to_channel(context.bot, number, otp_code, detected_app_cap, country_name, flag, message, "S3v1")
            sent_count += 1

            # User inbox — v1 session user দের notify
            matched_users = s3_find_users_by_number(number)
            for uid in matched_users:
                try:
                    session = s3_get_session(int(uid))
                    pool_key = session.get("pool_key", "") if session else ""
                    # শুধু v1 pool এর user দের Shark OTP দাও
                    if not is_shark_pool(pool_key):
                        continue

                    # নতুন inbox design
                    header = f"{flag} {country_name} {detected_app_cap} (S3) → 🟢"
                    inbox_text = header

                    kb_inbox = []
                    kb_inbox.append([InlineKeyboardButton(
                        f"📱 Phone : {number}",
                        copy_text=CopyTextButton(text=str(number)),
                        api_kwargs={"style": "primary"}
                    )])
                    btn_row = []
                    if message:
                        btn_row.append(InlineKeyboardButton(
                            "📋 Full SMS",
                            copy_text=CopyTextButton(text=message[:256]),
                            api_kwargs={"style": "success"}
                        ))
                    if otp_code:
                        btn_row.append(InlineKeyboardButton(
                            f"📋 {otp_code}",
                            copy_text=CopyTextButton(text=otp_code),
                            api_kwargs={"style": "success"}
                        ))
                    if btn_row:
                        kb_inbox.append(btn_row)
                    inbox_kb = InlineKeyboardMarkup(kb_inbox)

                    await _s3_send_with_retry(
                        context.bot, int(uid), inbox_text,
                        reply_markup=inbox_kb
                    )
                except Exception as e:
                    logger.error(f"Shark inbox error [{uid}]: {e}")

        if sent_count:
            logger.info(f"🦈 Shark poll done: {sent_count} OTPs sent to channel")

    except Exception as e:
        logger.error(f"_poll_shark_otps error: {e}")

# S3 OTP processing lock — async race condition এড়াতে
_s3_poll_lock = asyncio.Lock()

async def poll_otps_s3(context):
    # Race condition fix: একসাথে দুইটা poll চলবে না
    if _s3_poll_lock.locked():
        logger.debug("S3 poll skipped — previous poll still running")
        return

    async with _s3_poll_lock:
        try:
            import hashlib
            if len(otp_cache) > 5000:
                # পুরো clear না করে পুরানো half সরাই (recent OTP হারাবে না)
                keys = list(otp_cache.keys())
                for k in keys[:2500]:
                    otp_cache.pop(k, None)
                logger.info("S3 OTP cache trimmed to ~2500")

            # Clean expired sessions (30 min)
            for uid in list(s3_user_sessions.keys()):
                session = s3_user_sessions.get(uid, {})
                try:
                    session_time = datetime.fromisoformat(session.get("assigned_time", ""))
                    if datetime.now() - session_time > timedelta(minutes=30):
                        s3_user_sessions.pop(uid, None)
                except Exception:
                    pass

            # OTP channel keyboard
            _s3_kb_buttons = []
            _s3_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
            if _s3_ch_link and len(_s3_ch_link) > 10:
                _s3_kb_buttons.append(InlineKeyboardButton("📢 Main Channel", url=_s3_ch_link, api_kwargs={"style": "primary"}))
            if NUMBER_BOT_LINK:
                _s3_kb_buttons.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
            otp_channel_keyboard = InlineKeyboardMarkup([_s3_kb_buttons]) if _s3_kb_buttons else None

            cr_otps = await fetch_cr_api_otps()
            fetched_count = len(cr_otps)
            sent_count = skipped_count = 0
            logger.info(f"S3 poll: fetched {fetched_count} OTPs from CR API")

            for otp_data in cr_otps:
                try:
                    number = otp_data.get("num", "").strip()
                    message = otp_data.get("message", "").strip()
                    dt = otp_data.get("dt", "").strip()
                    if not number or not message or not dt:
                        logger.debug(f"S3 skip — missing field: num={number!r} dt={dt!r}")
                        skipped_count += 1
                        continue

                    # Improved duplicate detection: number + dt + full message hash
                    msg_hash = hashlib.md5(message.encode()).hexdigest()[:12]
                    cache_key = f"s3:{number}:{dt}:{msg_hash}"
                    if cache_key in otp_cache:
                        skipped_count += 1
                        continue

                    # BOT_START_TIME এর আগের OTP skip (cache এ mark করো)
                    try:
                        otp_dt = datetime.strptime(dt[:19], "%Y-%m-%d %H:%M:%S")
                        if otp_dt < BOT_START_TIME:
                            otp_cache[cache_key] = True
                            skipped_count += 1
                            continue
                    except Exception:
                        pass

                    # Cache তে mark করো BEFORE send (duplicate race condition fix)
                    otp_cache[cache_key] = True

                    otp_code = extract_otp(message)
                    country = extract_country_code_from_number(number)
                    flag = COUNTRY_FLAGS_CODE.get(country, "🌍")
                    hidden = hide_number(number)
                    country_name = COUNTRY_NAMES_CODE.get(country, "Unknown")
                    detected_app = detect_app_from_message(message, default_app="FACEBOOK")
                    detected_app_cap = detected_app.capitalize()

                    logger.info(f"S3 OTP processing: num={hidden} otp={otp_code or 'N/A'} app={detected_app_cap} dt={dt}")

                    # ── S3 OTP Post — real OTP না থাকলে channel post করবো না ──
                    if not otp_code:
                        logger.info(f"S3 skip channel post — no OTP extracted for {hidden}")
                        continue

                    await send_otp_to_channel(context.bot, number, otp_code, detected_app_cap, country_name, flag, message, "S3")
                    sent_count += 1

                    # User inbox notification — নতুন design
                    matched_users = s3_find_users_by_number(number)
                    for uid in matched_users:
                        try:
                            session = s3_get_session(int(uid))
                            pool_key = session.get("pool_key", "") if session else ""
                            # panel label
                            _s3_panel = "S3"
                            _s3_app = detected_app_cap
                            _s3_country_name = country_name
                            _s3_flag = flag

                            header = f"{_s3_flag} {_s3_country_name} {_s3_app} ({_s3_panel}) → 🟢"
                            inbox_text = header

                            kb_inbox = []
                            kb_inbox.append([InlineKeyboardButton(
                                f"📱 Phone : {number}",
                                copy_text=CopyTextButton(text=str(number)),
                                api_kwargs={"style": "primary"}
                            )])
                            btn_row = []
                            if message:
                                btn_row.append(InlineKeyboardButton(
                                    "📋 Full SMS",
                                    copy_text=CopyTextButton(text=message[:256]),
                                    api_kwargs={"style": "success"}
                                ))
                            if otp_code:
                                btn_row.append(InlineKeyboardButton(
                                    f"📋 {otp_code}",
                                    copy_text=CopyTextButton(text=otp_code),
                                    api_kwargs={"style": "success"}
                                ))
                            if btn_row:
                                kb_inbox.append(btn_row)
                            inbox_kb = InlineKeyboardMarkup(kb_inbox)

                            inbox_sent = await _s3_send_with_retry(
                                context.bot, int(uid), inbox_text,
                                reply_markup=inbox_kb
                            )
                            if not inbox_sent:
                                logger.error(f"S3 Inbox FAILED [{uid}] for {hidden}")
                        except Exception as e:
                            logger.error(f"S3 Inbox error [{uid}]: {e}")

                except Exception as e:
                    logger.error(f"S3 OTP process error: {e}")

            logger.info(f"S3 poll done: fetched={fetched_count} sent={sent_count} skipped={skipped_count}")

            # ══════════════════════════════
            #  SHARK PANEL OTP POLL (S4/v1)
            # ══════════════════════════════
            await _poll_shark_otps(context)

        except Exception as e:
            logger.error(f"poll_otps_s3 error: {e}")

# ══════════════════════════════════════════════════════════
#              LIVE SMS POST (S1/S2 Channel)
# ══════════════════════════════════════════════════════════

LAST_POST_MESSAGE_ID = None
LAST_POST_TEXT = ""
_job_is_running = False

# ── S1/S2 Range Post Rate Control ──
# Max 4 posts per minute total: S1=2, S2=2
RANGE_POST_MAX_PER_PANEL = 2          # S1 max 2, S2 max 2
RANGE_POST_DELAY_BETWEEN = 15         # 15 seconds between each post (flood safe)

async def job_post_live_sms(context):
    global _posted_sms_ids, _job_is_running, LAST_POST_MESSAGE_ID, LAST_POST_TEXT
    if _job_is_running:
        return
    _job_is_running = True
    try:
        bot = context.bot
        try:
            s2_logs = await asyncio.wait_for(
                get_console_logs_s2(force=True),
                timeout=45
            )
            if isinstance(s2_logs, Exception): s2_logs = []
        except asyncio.TimeoutError:
            logger.error("⚠️ S2 logs timeout")
            return

        from datetime import timezone, timedelta as _td
        now_bd = datetime.now(timezone(_td(hours=6)))

        panel_label = "S2"
        panel_post_count = 0
        for log in (s2_logs or []):
            if panel_post_count >= RANGE_POST_MAX_PER_PANEL:
                break
            app = (log.get("app_name") or log.get("app") or "").replace("*", "").strip().upper()
            if app not in ("FACEBOOK", "WHATSAPP", "INSTAGRAM"):
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

            # ── Range display: always ends with XXX ──
            clean_base = range_val.upper().replace("XXX","").replace("X","").strip()
            display_range = clean_base + "XXX"
            svc_emoji = "💬" if app == "WHATSAPP" else ("📸" if app == "INSTAGRAM" else "📘")
            svc_label = "WhatsApp" if app == "WHATSAPP" else ("Instagram" if app == "INSTAGRAM" else "Facebook")

            # ── v10 format: service tag + raw SMS + separator ──
            clean_sms = raw_sms.replace("<#>", "").strip() if raw_sms else ""
            text = (
                f"{flag} {escape_mdv2(country)}\n\n"
                f"📞 `{escape_mdv2(display_range)}`\n"
                f"🔐 `{escape_mdv2(otp)}`\n"
                f"{svc_emoji} Service: {escape_mdv2(svc_label)} \\| {escape_mdv2(panel_label)}\n"
                f"{escape_mdv2('────────────')}\n"
                f"📩"
            )
            if clean_sms:
                quoted = "\n".join(f">{escape_mdv2(line)}" for line in clean_sms.splitlines() if line.strip())
                text += f"\n{quoted}"

            # ── Keyboard: COPY OTP box + Main Channel + Number Bot ──
            kb_rows = []
            kb_rows.append([InlineKeyboardButton(
                "🔑 COPY OTP",
                copy_text=CopyTextButton(text=otp),
                api_kwargs={"style": "success"}
            )])
            _kb_btm = []
            _main_ch_link = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
            if _main_ch_link and len(_main_ch_link) > 10:
                _kb_btm.append(InlineKeyboardButton("📢 Main Channel", url=_main_ch_link, api_kwargs={"style": "primary"}))
            if NUMBER_BOT_LINK:
                _kb_btm.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
            if _kb_btm:
                kb_rows.append(_kb_btm)
            keyboard = InlineKeyboardMarkup(kb_rows) if kb_rows else None
            try:
                await safe_send_message(
                    bot,
                    chat_id=RANGE_CHANNEL_ID,
                    text=text,
                    parse_mode="MarkdownV2",
                    reply_markup=keyboard
                )
                panel_post_count += 1
                logger.info(f"📤 Range post sent: {panel_label} | {display_range} | count={panel_post_count}/{RANGE_POST_MAX_PER_PANEL}")
                await asyncio.sleep(RANGE_POST_DELAY_BETWEEN)
            except Exception as e:
                logger.error(f"Live SMS post error: {e}")

    finally:
        _job_is_running = False

async def zenex_get_recent_sms_by_range(rng_prefix):
    """ZENEX numsuccess/info থেকে range অনুযায়ী raw SMS খোঁজো।"""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{ZENEX_BASE_URL}/v1/numsuccess/info",
                headers=get_zenex_headers()
            )
        if res.status_code == 200:
            data = res.json()
            otps = data.get("data", {}).get("otps", [])
            clean_prefix = rng_prefix.upper().replace("XXX", "").replace("X", "").strip()
            for entry in otps:
                num = str(entry.get("number", "")).lstrip("+")
                if num.startswith(clean_prefix):
                    msg = (entry.get("message") or entry.get("sms") or entry.get("otp") or "").strip()
                    if msg:
                        return msg
    except Exception as e:
        logger.error(f"zenex_get_recent_sms_by_range error: {e}")
    return ""

async def job_a1_range_post(context):
    """A1 (ZENEX) active ranges channel এ post করে।"""
    global _posted_sms_ids
    if not ZENEX_API_KEY or not RANGE_CHANNEL_ID:
        logger.warning("A1 range post skipped: no API key or channel ID")
        return
    try:
        ranges = await zenex_get_active_ranges()
        logger.info(f"A1 range post: {len(ranges)} active ranges found")
        bot = context.bot
        from datetime import timezone, timedelta as _tdz
        now_bd = datetime.now(timezone(_tdz(hours=6)))
        post_count = 0

        for r in ranges:
            if post_count >= 3:
                break
            rng = r.get("range", "").strip()
            service = r.get("service", "").strip()
            if not rng or not service:
                continue
            slot = now_bd.strftime('%Y-%m-%d %H:') + str(now_bd.minute // 5 * 5).zfill(2)
            unique_id = f"a1_{rng}_{slot}"
            if unique_id in _posted_sms_ids:
                logger.info(f"A1 range post: skip duplicate {unique_id}")
                continue

            # ── OTP length by service ──
            svc_lower = service.lower()
            if "instagram" in svc_lower:
                _otp_len = 6
            elif "pc clone" in svc_lower or "clone" in svc_lower:
                _otp_len = random.choice([6, 7, 8])
            elif "new account" in svc_lower or "facebook" in svc_lower:
                _otp_len = 5
            else:
                _otp_len = 6
            otp = ''.join([str(random.randint(0, 9)) for _ in range(_otp_len)])

            # ── Range display ──
            code  = a2_extract_country_code(rng)
            clean_base = re.sub(r'X+$', '', rng.upper().strip())
            code = clean_base[:3] if len(clean_base) >= 3 else clean_base[:2]
            range_flag = COUNTRY_FLAGS_CODE.get(code, COUNTRY_FLAGS_CODE.get(code[:2], "🌍"))
            country_name = COUNTRY_NAMES_CODE.get(code, COUNTRY_NAMES_CODE.get(code[:2], code[:2].upper()))

            # ── Service label + emoji ──
            svc_emoji = "📸" if "instagram" in svc_lower else ("💬" if "whatsapp" in svc_lower else "📘")
            svc_label = "Instagram" if "instagram" in svc_lower else ("WhatsApp" if "whatsapp" in svc_lower else "Facebook")

            # ── Post format ──
            display_range = clean_base + "XXX"
            text = (
                f"{range_flag} {escape_mdv2(country_name)}\n\n"
                f"{svc_emoji} Service: {escape_mdv2(svc_label)} \\[A1\\]\n\n"
                f"📞 `{escape_mdv2(display_range)}`\n"
                f"🔐 `{escape_mdv2(otp)}`"
            )

            # ── Keyboard: OTP copy box + Main Channel + Number Bot ──
            kb_rows = []
            kb_rows.append([InlineKeyboardButton(
                otp,
                copy_text=CopyTextButton(text=otp),
                api_kwargs={"style": "success"}
            )])
            bottom_row = []
            _main_ch_link = OTP_CHANNEL_LINK or OTP_CHANNEL_JOIN_LINK or ""
            if _main_ch_link and len(_main_ch_link) > 10:
                bottom_row.append(InlineKeyboardButton("📢 Main Channel", url=_main_ch_link, api_kwargs={"style": "primary"}))
            if NUMBER_BOT_LINK:
                bottom_row.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
            if bottom_row:
                kb_rows.append(bottom_row)
            keyboard = InlineKeyboardMarkup(kb_rows)
            try:
                await safe_send_message(
                    bot,
                    chat_id=RANGE_CHANNEL_ID,
                    text=text,
                    parse_mode="MarkdownV2",
                    reply_markup=keyboard
                )
                _posted_sms_ids.add(unique_id)
                post_count += 1
                logger.info(f"A1 range post: posted {rng} [{service}]")
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"A1 range post send error [{rng}]: {e}")
        logger.info(f"A1 range post done: {post_count} posted")
    except Exception as e:
        logger.error(f"job_a1_range_post error: {e}")

async def job_whatsapp_range_post(context):
    """WhatsApp range post — S2 + A1. 5 min পরপর, 2টা range।"""
    global _posted_sms_ids
    if not RANGE_CHANNEL_ID:
        return
    try:
        bot = context.bot
        from datetime import timezone, timedelta as _td
        now_bd = datetime.now(timezone(_td(hours=6)))
        slot = now_bd.strftime('%Y-%m-%d %H:') + str(now_bd.minute // 5 * 5).zfill(2)
        post_count = 0
        WA_MAX = 2
        panel_label = "S2"

        # ── S2 WhatsApp ranges ──
        try:
            s2_logs = await asyncio.wait_for(get_console_logs_s2(force=True), timeout=30)
            if isinstance(s2_logs, Exception): s2_logs = []
        except Exception:
            s2_logs = []

        for log in (s2_logs or []):
            if post_count >= WA_MAX:
                break
            app_name = (log.get("app_name") or log.get("app") or "").strip().upper()
            if app_name != "WHATSAPP":
                continue
            range_val = log.get("range", "").strip()
            if not range_val:
                continue
            unique_id = f"wa_{range_val}_{panel_label}_{slot}"
            if unique_id in _posted_sms_ids:
                continue

            country = log.get("country", "").strip() or "Unknown"
            flag = get_flag_by_iso(country)
            raw_sms = (log.get("message") or log.get("sms") or "").strip()
            _fl = raw_sms.splitlines()[0] if raw_sms else ""
            _sg = re.findall(r'(?<![A-Za-z0-9])\*{2,}(?![A-Za-z0-9])', _fl)
            otp_len = max(5, min(8, sum(len(g) for g in _sg) if _sg else 6))
            otp = ''.join([str(random.randint(0, 9)) for _ in range(otp_len)])
            display_range = range_val + "XXX" if not range_val.upper().endswith('X') else range_val

            wa_header = f"{flag} {country} WA {display_range} [{panel_label}]"
            wa_text = escape_mdv2(wa_header)
            wa_kb_rows = []
            wa_kb_rows.append([InlineKeyboardButton("🔑 COPY OTP", copy_text=CopyTextButton(text=otp), api_kwargs={"style": "success"})])
            _wa_btm = []
            _wa_ch_lnk = OTP_CHANNEL_LINK or MAIN_CHANNEL_LINK or JOIN_CHANNEL_LINK or ""
            if _wa_ch_lnk and len(_wa_ch_lnk) > 10:
                _wa_btm.append(InlineKeyboardButton("📢 Main Channel", url=_wa_ch_lnk, api_kwargs={"style": "primary"}))
            if NUMBER_BOT_LINK:
                _wa_btm.append(InlineKeyboardButton("🤖 Number Bot", url=NUMBER_BOT_LINK, api_kwargs={"style": "danger"}))
            if _wa_btm:
                wa_kb_rows.append(_wa_btm)
            wa_keyboard = InlineKeyboardMarkup(wa_kb_rows)
            try:
                await safe_send_message(bot, chat_id=RANGE_CHANNEL_ID, text=wa_text, parse_mode="MarkdownV2", reply_markup=wa_keyboard)
                _posted_sms_ids.add(unique_id)
                post_count += 1
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"WA range post error [{panel_label}]: {e}")

    except Exception as e:
        logger.error(f"job_whatsapp_range_post error: {e}")


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
    except Exception as e:
        logger.warning(f"safe_edit failed: {e}")
        try:
            await query.message.reply_text(text, **kwargs)
        except Exception as e2:
            logger.error(f"safe_edit reply also failed: {e2}")

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
            keyboard_buttons = [
                [InlineKeyboardButton("🔗 Main Channel", url=JOIN_CHANNEL_LINK, api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("🔗 OTP Channel", url=OTP_CHANNEL_JOIN_LINK, api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("🔗 Backup Channel", url=BACKUP_CHANNEL_LINK, api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("✅ Verify", callback_data="verify_join", api_kwargs={"style": "primary"})],
            ]
            await update.message.reply_text(
                "🚦 Access Locked!\n\nসব channel join করুন\nতারপর Verify করুন।",
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
        f"👋 Welcome *•.¸♡ {user.first_name} ♡¸.•*!\n\n"
        f"Use the menu below to get started."
    )

    pool = get_numbers_pool()
    fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
    ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
    inline_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🆕 Facebook A1", callback_data="select_panel_A1_fb", api_kwargs={"style": "success"})],
        [InlineKeyboardButton("⚡ Facebook A2", callback_data="select_panel_A2_fb", api_kwargs={"style": "success"})],
        [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2",    api_kwargs={"style": "success"})],
        [InlineKeyboardButton(f"🔴 Facebook S3 ({fb_count})", callback_data="s3app:fb", api_kwargs={"style": "danger"})],
        [InlineKeyboardButton(f"📸 Instagram S3 ({ig_count})", callback_data="s3app:ig", api_kwargs={"style": "danger"})],
        [InlineKeyboardButton("💬 WhatsApp",    callback_data="select_panel_WA",    api_kwargs={"style": "primary"})],
        [InlineKeyboardButton("✈️ Telegram",    callback_data="select_panel_TG",    api_kwargs={"style": "primary"})],
    ])

    await context.bot.send_message(
        chat_id=chat_id,
        text="⌨️",
        reply_markup=main_keyboard(user_id)
    )

    await context.bot.send_message(
        chat_id=chat_id,
        text=welcome_text,
        parse_mode="Markdown",
    )

    new_msg = await context.bot.send_message(
        chat_id=chat_id,
        text="📱 *Please select a service:*",
        parse_mode="Markdown",
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

    # শুধু private chat এ respond করবে
    if update.message.chat.type != "private":
        return

    user = update.effective_user
    user_id = user.id
    text = update.message.text.strip()
    chat_id = update.message.chat.id
    user_name = user.first_name or "User"
    init_user(user_id)

    # Admin broadcast handling (text via copy_message)
    waiting = user_data.get(user_id, {}).get("waiting_for")
    if waiting == "broadcast" and user_id == ADMIN_ID:
        user_data[user_id]["waiting_for"] = None
        await _do_broadcast(update, context, update.message)
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
        panel = user_data[user_id].get("panel", "S2")
        app = user_data[user_id].get("app", "FACEBOOK")
        try:
            await update.message.delete()
        except Exception:
            pass
        if panel == "A1":
            await do_get_number_a1(update.message, user_id, bot=context.bot)
        elif panel == "A2":
            await do_get_number_a2(update.message, user_id, bot=context.bot)
        else:
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

    if text in ("📲 Get Number", "Get Number"):
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
            pool = get_numbers_pool()
            fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
            ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
            inline_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("🆕 Facebook A1", callback_data="select_panel_A1_fb", api_kwargs={"style": "success"})],
                [InlineKeyboardButton("⚡ Facebook A2", callback_data="select_panel_A2_fb", api_kwargs={"style": "success"})],
                [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2", api_kwargs={"style": "success"})],
                [InlineKeyboardButton(f"🔴 Facebook S3 ({fb_count})", callback_data="s3app:fb", api_kwargs={"style": "danger"})],
                [InlineKeyboardButton(f"📸 Instagram S3 ({ig_count})", callback_data="s3app:ig", api_kwargs={"style": "danger"})],
                [InlineKeyboardButton("💬 WhatsApp", callback_data="select_panel_WA", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("✈️ Telegram",  callback_data="select_panel_TG",  api_kwargs={"style": "primary"})],
            ])
            new_msg = await context.bot.send_message(
                chat_id=chat_id,
                text="📱 *Please select a service:*",
                parse_mode="Markdown",
                reply_markup=inline_kb
            )
            user_kb_msg[user_id] = new_msg.message_id
            user_msg[chat_id] = new_msg.message_id
        finally:
            processing_users.discard(user_id)
        return

    if text in ("📡 Custom Range", "Custom Range"):
        panel = user_data[user_id].get("panel", "S2")
        if panel == "S3":
            await update.message.reply_text("❌ S3 তে Custom Range নেই। S2 select করুন।", reply_markup=main_keyboard(user_id))
            return
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["waiting_for"] = "custom_range"
        app = user_data[user_id].get("app", "FACEBOOK")
        await update.message.reply_text(
            f"📡 Custom Range লিখুন:\n\n🖥 Panel: {panel}\n📱 App: {app}\n\nউদাহরণ: 23762155XXX",
            reply_markup=main_keyboard(user_id)
        )
        return

    if text in ("📦 My Numbers", "📋 My Numbers"):
        panel = user_data[user_id].get("panel", "S2")
        if panel == "S3":
            session = s3_get_session(user_id)
            if session:
                nums = session.get("numbers", [session.get("number")] if session.get("number") else [])
                pool_key = session['pool_key']
                code, service, variant = parse_pool_key(pool_key)
                country_name = COUNTRY_NAMES_CODE.get(code, "Unknown")
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                service_label = "FACEBOOK" if service == "fb" else "INSTAGRAM"
                card_text = (
                    f"✅ <b>Numbers Assigned!</b>\n\n"
                    f"<b>Service:</b> {service_label}\n"
                    f"🌍 <b>Country:</b> {flag} {country_name}\n"
                    f"⏳ <b>Reserved:</b> 30 min\n\n"
                    f"📩 OTPs forwarded automatically."
                )
                await update.message.reply_text(
                    card_text,
                    parse_mode="HTML",
                    reply_markup=after_number_inline_s3(pool_key, nums)
                )
            else:
                await update.message.reply_text("❌ No active numbers in S3", reply_markup=main_keyboard(user_id))
        else:
            last_number = str(user_data[user_id].get("last_number", "")).replace("+", "").strip()
            if not last_number:
                await update.message.reply_text("❌ কোনো number নেওয়া হয়নি।", reply_markup=main_keyboard(user_id))
                return
            await update.message.reply_text("⏳ Loading...")
            if False:  # S1 removed
                data = await api_get_info_s2(search=last_number)
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
        await update.message.reply_text("⏳ Live OTP Traffic লোড হচ্ছে...", reply_markup=main_keyboard(user_id))
        try:
            from datetime import timezone, timedelta as _td

            now_ts = time.time()
            window_24h = 86400  # 24 hours in seconds

            # ── Helper: parse log time → unix timestamp ──
            def parse_log_time(t_str):
                if not t_str:
                    return None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S"):
                    try:
                        return datetime.strptime(str(t_str)[:19], fmt).timestamp()
                    except Exception:
                        pass
                return None

            app_counts = {}
            country_counts = {}

            # ── S2 OTP count (24h) ──
            s2_logs = await get_console_logs_s2(force=True)
            s2_otp = 0

            for log in s2_logs:
                t = parse_log_time(log.get("time") or log.get("dt") or log.get("created_at") or "")
                if t and (now_ts - t) > window_24h:
                    continue
                app = log.get("app_name", "").replace("*", "").strip().upper()
                country = log.get("country", "").strip()
                s2_otp += 1
                if app:
                    app_counts[app] = app_counts.get(app, 0) + 1
                if country:
                    country_counts[country] = country_counts.get(country, 0) + 1

            # ── S3 OTP count (24h) — otp_cache থেকে ──
            # cache key format: s3:{number}:{dt}:{msg_hash}
            s3_otp = 0
            for cache_key in list(otp_cache.keys()):
                if not cache_key.startswith("s3:"):
                    continue
                parts = cache_key.split(":", 3)
                # parts[0]=s3, parts[1]=number, parts[2]=dt, parts[3]=hash
                if len(parts) < 3:
                    continue
                dt_str = parts[2]
                t = parse_log_time(dt_str)
                if t is None:
                    # timestamp parse হয়নি — count করো (bot চালু থেকে এসেছে)
                    s3_otp += 1
                    continue
                if (now_ts - t) <= window_24h:
                    s3_otp += 1

            # ── Build message ──
            bd_now = datetime.now(timezone(_td(hours=6))).strftime("%I:%M %p")

            msg = "🚦 LIVE OTP TRAFFIC (24H)\n\n"

            # App statistics
            if app_counts:
                sorted_apps = sorted(app_counts.items(), key=lambda x: x[1], reverse=True)
                for app_name, cnt in sorted_apps:
                    emoji = APP_EMOJIS.get(app_name, "📱")
                    display = app_name.capitalize()
                    msg += f"{emoji} {display}: {cnt} OTP\n"
                msg += "\n"

            # Panel OTP counts
            msg += f"🟢 S2 OTP: {s2_otp}\n"
            msg += f"🔴 S3 OTP: {s3_otp}\n"

            # Country statistics
            if country_counts:
                msg += "\n🌍 COUNTRY OTP STATS\n\n"
                sorted_countries = sorted(country_counts.items(), key=lambda x: x[1], reverse=True)
                for country, cnt in sorted_countries[:10]:
                    flag = get_flag_by_iso(country)
                    msg += f"{flag} {country}: {cnt} OTP\n"

            msg += f"\n🕐 Last Updated: {bd_now}"

            await update.message.reply_text(msg, reply_markup=main_keyboard(user_id))
        except Exception as e:
            logger.error(f"Live Traffic error: {e}")
            await update.message.reply_text("❌ Data load error.", reply_markup=main_keyboard(user_id))
        return

    if text == "✈️ Telegram":
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["waiting_for"] = None
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False
        user_data[user_id]["app"] = "TELEGRAM"
        user_data[user_id]["panel"] = "A2"
        await cleanup_s1s2_panel(context.bot, user_id)
        await context.bot.send_message(chat_id=chat_id, text="⏳ লোড হচ্ছে...")
        ranges = a2_get_cached_ranges()
        if not ranges:
            ranges = await a2_get_active_ranges(force=True)
        tg_ranges = [r for r in (ranges or []) if r.get("service", "").upper() in ("TELEGRAM", "TG")]
        if not tg_ranges:
            tg_ranges = ranges or []
        if not tg_ranges:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Telegram এ এখন কোনো active range নেই।\n\nকিছুক্ষণ পর আবার try করুন।",
                reply_markup=main_keyboard(user_id)
            )
            return
        seen = set()
        buttons = []
        for r in tg_ranges[:25]:
            rng = r.get("range", "")
            rid = r.get("rid", rng)
            if not rng or rng in seen:
                continue
            seen.add(rng)
            code = a2_extract_country_code(rng)
            flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
            buttons.append([InlineKeyboardButton(
                f"{flag} {rng}",
                callback_data=f"tg_range:{rid}",
                api_kwargs={"style": "primary"}
            )])
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
        new_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="✈️ *Telegram*\n\n📡 Range select করুন:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        user_msg[chat_id] = new_msg.message_id
        user_kb_msg[user_id] = new_msg.message_id
        return

    if text in ("🆘 Support", "🛟 Support Admin"):
        await update.message.reply_text(
            "🆘 Support এর জন্য নিচের button এ click করুন:",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🆘 Support Admin", url=SUPPORT_ADMIN_LINK, api_kwargs={"style": "primary"})
            ]])
        )
        # Restore persistent keyboard
        await context.bot.send_message(
            chat_id=chat_id, text="⌨️", reply_markup=main_keyboard(user_id)
        )
        return

    if text == "👤 Profile":
        panel = user_data[user_id].get("panel", "S2")
        app   = user_data[user_id].get("app", "FACEBOOK")
        rng   = user_data[user_id].get("range", "—")
        name  = user_data[user_id].get("name", "User")
        msg = (
            f"👤 <b>Profile</b>\n\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"👋 Name: {name}\n"
            f"🖥 Panel: {panel}\n"
            f"📱 App: {app}\n"
            f"📡 Range: {rng}"
        )
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=main_keyboard(user_id))
        return

    if text == "📦 Bulk Number":
        if not has_get100_access(user_id):
            await update.message.reply_text("❌ Bulk Number access নেই।", reply_markup=main_keyboard(user_id))
        else:
            panel = user_data[user_id].get("panel", "S2")
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

    # Filename format: 91.txt or 91_s2.txt or 91_v1.txt (Shark Panel)
    is_shark_upload = "_v1" in filename.lower().replace("-", "_")
    country_match = re.match(r'(\d+(?:[_-](?:s\d+|v1))?)', filename, re.IGNORECASE)
    if not country_match:
        await update.message.reply_text(
            "❌ *Invalid filename!*\n\nFormat:\n`91.txt` — S3 normal\n`91_v1.txt` — Shark Panel",
            parse_mode="Markdown"
        )
        return

    base_pool_key = country_match.group(1).lower().replace("-", "_")
    new_numbers = [
        line.strip().lstrip('+')
        for line in text.split('\n')
        if line.strip() and len(line.strip()) >= 7
    ]

    if not new_numbers:
        await update.message.reply_text("❌ File empty or invalid format!")
        return

    # Save file info for service selection
    context.user_data["pending_numbers"] = new_numbers
    context.user_data["pending_pool_key"] = base_pool_key

    await update.message.reply_text(
        f"📁 File received: `{filename}`\n"
        f"📊 Numbers found: `{len(new_numbers)}`\n"
        f"{'🦈 Shark Panel (v1)' if is_shark_upload else '🔴 S3 (CR API)'}\n\n"
        f"কোন service এর জন্য add করবো?",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📘 Facebook", callback_data="pool_service_fb", api_kwargs={"style": "primary"})],
            [InlineKeyboardButton("📸 Instagram", callback_data="pool_service_ig", api_kwargs={"style": "primary"})],
            [InlineKeyboardButton("📘📸 Facebook + Instagram (Both)", callback_data="pool_service_both", api_kwargs={"style": "primary"})],
        ])
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
    total_users = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
    pool = get_numbers_pool()
    total_s3 = sum(len(v) for v in pool.values())
    fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
    ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
    s3_pools = len(pool)
    msg = (
        "╔═══════════════════════╗\n"
        "║     👑 ADMIN PANEL      ║\n"
        "╚═══════════════════════╝\n\n"
        f"👥 *Total Users:* `{total_users}`\n"
        f"📦 *Bulk Mode:* `{get100_status}`\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📡 *Session Status*\n"
        f"  🟢 S2: `{s2_pool.number_sessions.qsize()}` number | `{s2_pool.otp_sessions.qsize()}` otp\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "🔴 *S3 Pool Status*\n"
        f"  📘 Facebook: `{fb_count}` numbers\n"
        f"  📸 Instagram: `{ig_count}` numbers\n"
        f"  🗂 Total Pools: `{s3_pools}` | Numbers: `{total_s3}`\n\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=admin_keyboard_unified())

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
        f"🟢 S2 Pool: {s2_pool.number_sessions.qsize()}/18 number sessions\n"
        f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n━━━━━━━━━━━━━━━━━━"
    )

async def cmd_apistatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    s2_sess = await s2_pool._login_once()
    s2_ok = "✅" if s2_sess.get("token") else "❌"
    # CR API check
    cr_otps = await fetch_cr_api_otps()
    cr_ok = "✅" if cr_otps is not None else "❌"
    await update.message.reply_text(
        f"━━━━━━━━━━━━━━━━━━\n🔗 API STATUS\n━━━━━━━━━━━━━━━━━━\n\n"
        f"🟢 S2 (X.Mint): {s2_ok}\n"
        f"  Number slots: {s2_pool.number_sessions.qsize()}/20\n"
        f"  OTP slots: {s2_pool.otp_sessions.qsize()}/12\n\n"
        f"🔴 S3 (CR API): {cr_ok}\n"
        f"  Recent OTPs: {len(cr_otps)}\n\n━━━━━━━━━━━━━━━━━━"
    )

# ══════════════════════════════════════════════════════════
#              BROADCAST HELPER (copy_message — all media)
# ══════════════════════════════════════════════════════════

async def _do_broadcast(update, context, source_message):
    """
    Broadcasts source_message to all users via copy_message.
    Supports: text, photo, video, document, sticker, animation/GIF, caption.
    Does NOT touch any other system.
    """
    from_chat_id = source_message.chat_id
    message_id   = source_message.message_id

    all_users = list(set(
        list(user_data.keys()) + [int(u) for u in s3_get_all_users()]
    ))
    total   = len(all_users)
    success = 0
    failed  = 0

    status_msg = await update.message.reply_text(
        f"📢 *Broadcasting...*\n\n"
        f"👥 Total users: `{total}`\n"
        f"⏳ Please wait...",
        parse_mode="Markdown"
    )

    for uid in all_users:
        try:
            await context.bot.copy_message(
                chat_id=int(uid),
                from_chat_id=from_chat_id,
                message_id=message_id
            )
            success += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)

    try:
        await status_msg.edit_text(
            f"✅ *Broadcast Done!*\n\n"
            f"👥 Total: `{total}`\n"
            f"✅ Sent: `{success}`\n"
            f"❌ Failed: `{failed}`",
            parse_mode="Markdown"
        )
    except Exception:
        await update.message.reply_text(
            f"✅ *Broadcast Done!*\n\n"
            f"👥 Total: `{total}`\n"
            f"✅ Sent: `{success}`\n"
            f"❌ Failed: `{failed}`",
            parse_mode="Markdown"
        )


async def handle_broadcast_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles photo / video / document / sticker / animation broadcast.
    Triggers only when admin has waiting_for == 'broadcast'.
    Does NOT interfere with any other handler or system.
    """
    if not update.message:
        return
    if update.message.chat.type != "private":
        return

    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        return

    waiting = user_data.get(user_id, {}).get("waiting_for")
    if waiting != "broadcast":
        return

    user_data[user_id]["waiting_for"] = None
    await _do_broadcast(update, context, update.message)


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
    await s2_pool.refresh_all()
    await update.message.reply_text(
        f"✅ Refresh Done!\n"
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

    # ── Pool service select (txt upload) ──
    if data in ("pool_service_fb", "pool_service_ig", "pool_service_both"):
        if user_id != ADMIN_ID:
            return
        new_numbers = context.user_data.get("pending_numbers", [])
        base_pool_key = context.user_data.get("pending_pool_key", "")
        if not new_numbers or not base_pool_key:
            await query.message.edit_text("❌ Session expired. File আবার পাঠান।")
            return

        service_map = {
            "pool_service_fb": [("fb", "📘 Facebook")],
            "pool_service_ig": [("ig", "📸 Instagram")],
            "pool_service_both": [("fb", "📘 Facebook"), ("ig", "📸 Instagram")],
        }
        services = service_map[data]
        result_text = f"✅ *Upload Complete!*\n\n"

        broadcast_parts = []
        for suffix, label in services:
            # v1 file: base_pool_key = "95_v1" → pool_key = "95_v1_fb"
            # normal:  base_pool_key = "95"    → pool_key = "95_fb"
            pool_key = f"{base_pool_key}_{suffix}"
            is_shark = is_shark_pool(pool_key)
            added, skipped = await add_numbers_to_pool(context.bot, pool_key, new_numbers)
            result_text += (
                f"{label} Pool: `{pool_key}`\n"
                f"✅ Added: `{added}` | ⏭ Skipped: `{skipped}`\n"
                f"📱 Total: `{count_numbers(pool_key)}`\n\n"
            )
            if added > 0:
                code = base_pool_key.split("_")[0]
                country_name = COUNTRY_NAMES_CODE.get(code, code)
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                service_label = "Facebook" if suffix == "fb" else "Instagram"
                broadcast_parts.append((flag, country_name, service_label, added))

        context.user_data.pop("pending_numbers", None)
        context.user_data.pop("pending_pool_key", None)
        await query.message.edit_text(result_text, parse_mode="Markdown")

        # Auto broadcast to all S3 users
        if broadcast_parts:
            all_users = list(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
            for flag, country_name, service_label, added in broadcast_parts:
                bc_msg = (
                    f"🆕 *New Numbers Available!*\n\n"
                    f"{flag} *{country_name} {service_label}*\n"
                    f"📱 `{added}` numbers added\n\n"
                    f"⚡ Get yours now → /start"
                )
                sent = failed = 0
                for uid in all_users:
                    try:
                        await context.bot.send_message(
                            chat_id=int(uid),
                            text=bc_msg,
                            parse_mode="Markdown"
                        )
                        sent += 1
                    except Exception:
                        failed += 1
                    await asyncio.sleep(0.05)
                await context.bot.send_message(
                    chat_id=ADMIN_ID,
                    text=f"📢 *Broadcast Done!*\n✅ Sent: `{sent}` | ❌ Failed: `{failed}`",
                    parse_mode="Markdown"
                )
        return

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
            # Restore persistent keyboard
            await context.bot.send_message(
                chat_id=chat_id, text="⌨️", reply_markup=main_keyboard(user_id)
            )
            inline_kb = await panel_select_inline()
            new_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Verified! Welcome, {user_name}!\n\n⚙️ Select Service:",
                reply_markup=inline_kb
            )
            user_msg[chat_id] = new_msg.message_id
        else:
            keyboard_buttons = [
                [InlineKeyboardButton("🔗 Main Channel", url=JOIN_CHANNEL_LINK, api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("🔗 OTP Channel", url=OTP_CHANNEL_JOIN_LINK, api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("🔗 Backup Channel", url=BACKUP_CHANNEL_LINK, api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("✅ Verify", callback_data="verify_join", api_kwargs={"style": "primary"})],
            ]
            await safe_edit(query, "🚦 এখনো join করা হয়নি।\n\nসব channel join করুন\nতারপর Verify করুন।",
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

    # ── Instagram panel select ──
    if data.startswith("ig_panel_"):
        panel = data.replace("ig_panel_", "")
        user_data[user_id]["panel"] = panel
        user_data[user_id]["app"] = "INSTAGRAM"
        user_data[user_id]["country"] = None
        user_data[user_id]["range"] = None
        await safe_edit(query, "⏳ Instagram country লোড হচ্ছে...")
        countries = await get_instagram_countries(panel=panel)
        if not countries:
            await safe_edit(query,
                f"❌ {panel} তে এখন কোনো Instagram range নেই।\nকিছুক্ষণ পর আবার try করুন।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"select_panel_{panel}", api_kwargs={"style": "primary"})]]))
            return
        buttons = []
        row = []
        for i, country in enumerate(countries[:20]):
            flag = get_flag_by_iso(country)
            row.append(InlineKeyboardButton(
                f"{flag} {country}",
                callback_data=f"ig_country_{panel}_{country}",
                api_kwargs={"style": "primary"}
            ))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"select_panel_{panel}", api_kwargs={"style": "primary"})])
        await safe_edit(query,
            f"📸 Instagram {panel}\n\n🌍 Country select করুন:",
            reply_markup=InlineKeyboardMarkup(buttons))
        return

    # ── Instagram country → range select ──
    if data.startswith("ig_country_"):
        parts = data.replace("ig_country_", "").split("_", 1)
        panel = parts[0]
        country = parts[1] if len(parts) > 1 else ""
        user_data[user_id]["panel"] = panel
        user_data[user_id]["app"] = "INSTAGRAM"
        user_data[user_id]["country"] = country
        await safe_edit(query, "⏳ Instagram range লোড হচ্ছে...")
        ranges = await get_instagram_ranges(country, panel=panel)
        if not ranges:
            await safe_edit(query,
                f"❌ {country} তে কোনো Instagram range নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"ig_panel_{panel}", api_kwargs={"style": "primary"})]]))
            return
        flag = get_flag_by_iso(country)
        await safe_edit(query,
            f"📸 Instagram {panel} — {flag} {country}\n\n📡 Range select করুন:",
            reply_markup=range_select_inline(ranges, "INSTAGRAM", country))
        return

    # ── Panel select ──
    # ══════════════════════════════════════════════
    #  NEW SERVICE SELECT — Facebook / Instagram / S3
    # ══════════════════════════════════════════════
    if data.startswith("service:"):
        svc = data.split("service:", 1)[1]

        if svc == "facebook":
            user_data[user_id]["app"] = "FACEBOOK"
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            pool = get_numbers_pool()
            fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))

            # সব source থেকে country collect করো — parallel fetch
            country_map = {}  # country_name → [panels]

            async def _fetch_a1_fb():
                if not ZENEX_API_KEY: return []
                try: return await zenex_get_active_ranges(service="Facebook New Account")
                except Exception: return []

            async def _fetch_a2_fb():
                if not A2_API_KEY: return []
                try: return a2_get_cached_ranges() or await a2_get_active_ranges(force=True)
                except Exception: return []

            s2_countries, a1_ranges, a2_ranges_all = await asyncio.gather(
                get_countries_for_app("FACEBOOK", panel="S2"),
                _fetch_a1_fb(),
                _fetch_a2_fb(),
            )

            # S2 countries
            for c in (s2_countries or []):
                if c not in country_map: country_map[c] = []
                country_map[c].append("S2")

            # A1 countries (ZENEX)
            for r in (a1_ranges or []):
                rng = r.get("range", "")
                code = rng[:2] if rng else ""
                cname = COUNTRY_NAMES_CODE.get(code, "")
                if cname and cname not in country_map: country_map[cname] = []
                if cname: country_map[cname].append("A1")

            # A2 countries
            fb_a2 = [r for r in (a2_ranges_all or []) if r.get("service","").upper() in ("FACEBOOK","FB")]
            for r in fb_a2:
                code = a2_extract_country_code(r.get("range",""))
                cname = COUNTRY_NAMES_CODE.get(code, "")
                if cname and cname not in country_map: country_map[cname] = []
                if cname: country_map[cname].append("A2")

            if not country_map and fb_count == 0:
                await safe_edit(query, "❌ Facebook এ এখন কোনো number নেই।\n\nকিছুক্ষণ পর আবার try করুন.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]]))
                return

            buttons = []
            for cname in sorted(country_map.keys())[:24]:
                code = next((k for k,v in COUNTRY_NAMES_CODE.items() if v == cname), "")
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                buttons.append(InlineKeyboardButton(f"{flag} {cname}", callback_data=f"fb_country:{cname}", api_kwargs={"style": "primary"}))

            kb_rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
            if fb_count > 0:
                kb_rows.append([InlineKeyboardButton(f"🔴 Saved Numbers ({fb_count})", callback_data="s3app:fb", api_kwargs={"style": "danger"})])
            kb_rows.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
            await safe_edit(query, "📘 *Facebook*\n\n🌍 Country select করুন:", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb_rows))
            return

        elif svc == "instagram":
            user_data[user_id]["app"] = "INSTAGRAM"
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            pool = get_numbers_pool()
            ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))

            country_map = {}

            async def _fetch_a1_ig_svc():
                if not ZENEX_API_KEY: return []
                try: return await zenex_get_active_ranges(service="Instagram Account")
                except Exception: return []

            s2_ig, a1_ig = await asyncio.gather(
                get_instagram_countries(panel="S2"),
                _fetch_a1_ig_svc(),
            )

            for c in (s2_ig or []):
                if c not in country_map: country_map[c] = []
                country_map[c].append("S2")

            for r in (a1_ig or []):
                rng = r.get("range", "")
                code = rng[:2] if rng else ""
                cname = COUNTRY_NAMES_CODE.get(code, "")
                if cname and cname not in country_map: country_map[cname] = []
                if cname: country_map[cname].append("A1")

            if not country_map and ig_count == 0:
                await safe_edit(query, "❌ Instagram এ এখন কোনো number নেই।\n\nকিছুক্ষণ পর আবার try করুন.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]]))
                return

            buttons = []
            for cname in sorted(country_map.keys())[:24]:
                code = next((k for k,v in COUNTRY_NAMES_CODE.items() if v == cname), "")
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                buttons.append(InlineKeyboardButton(f"{flag} {cname}", callback_data=f"ig_country:{cname}", api_kwargs={"style": "primary"}))

            kb_rows = [buttons[i:i+2] for i in range(0, len(buttons), 2)]
            if ig_count > 0:
                kb_rows.append([InlineKeyboardButton(f"🔴 Saved Numbers ({ig_count})", callback_data="s3app:ig", api_kwargs={"style": "danger"})])
            kb_rows.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
            await safe_edit(query, "📸 *Instagram*\n\n🌍 Country select করুন:", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb_rows))
            return

        elif svc == "s3":
            pool = get_numbers_pool()
            fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
            ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
            rows = []
            if fb_count > 0:
                rows.append([InlineKeyboardButton(f"📘 Facebook ({fb_count})", callback_data="s3app:fb", api_kwargs={"style": "primary"})])
            if ig_count > 0:
                rows.append([InlineKeyboardButton(f"📸 Instagram ({ig_count})", callback_data="s3app:ig", api_kwargs={"style": "primary"})])
            if not rows:
                await safe_edit(query, "❌ Saved Numbers খালি।",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]]))
                return
            rows.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
            await safe_edit(query, "🔴 *Saved Numbers*\n\nSelect করুন:", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(rows))
            return

    # ── Facebook country select → Range/panel auto ──
    if data.startswith("fb_country:"):
        cname = data.split("fb_country:", 1)[1]
        user_data[user_id]["country"] = cname
        user_data[user_id]["app"] = "FACEBOOK"
        await safe_edit(query, "⏳ Range লোড হচ্ছে...")
        code = next((k for k,v in COUNTRY_NAMES_CODE.items() if v == cname), "")
        flag = COUNTRY_FLAGS_CODE.get(code, "🌍")

        range_buttons = []

        async def _fetch_s2_fb_ranges():
            return await get_all_ranges_for_country("FACEBOOK", cname, panel="S2")

        async def _fetch_a2_fb_ranges():
            if not A2_API_KEY: return []
            try: return a2_get_cached_ranges() or await a2_get_active_ranges(force=True)
            except Exception: return []

        async def _fetch_a1_fb_ranges():
            if not ZENEX_API_KEY: return []
            try: return await zenex_get_active_ranges(service="Facebook New Account")
            except Exception: return []

        s2_ranges, a2_all, a1_ranges = await asyncio.gather(
            _fetch_s2_fb_ranges(),
            _fetch_a2_fb_ranges(),
            _fetch_a1_fb_ranges(),
        )

        for r in (s2_ranges or [])[:10]:
            rng = r if isinstance(r, str) else r.get("range", "")
            if rng:
                range_buttons.append(InlineKeyboardButton(f"📞 {rng}", callback_data=f"auto_range:S2:{rng}", api_kwargs={"style": "primary"}))

        # A2 ranges
        fb_a2 = [r for r in (a2_all or []) if r.get("service","").upper() in ("FACEBOOK","FB")]
        for r in fb_a2:
            if a2_extract_country_code(r.get("range","")) == code:
                rng = r.get("range","")
                rid = r.get("rid", rng)
                if rng:
                    range_buttons.append(InlineKeyboardButton(f"📞 {rng}", callback_data=f"auto_range:A2:{rid}", api_kwargs={"style": "primary"}))

        # A1 ranges
        for r in (a1_ranges or []):
            rng = r.get("range","")
            if rng and (rng.startswith(code) or a2_extract_country_code(rng) == code):
                range_buttons.append(InlineKeyboardButton(f"📞 {rng}", callback_data=f"auto_range:A1:{rng}", api_kwargs={"style": "primary"}))

        if not range_buttons:
            await safe_edit(query, f"❌ {flag} {cname} তে এখন কোনো range নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="service:facebook", api_kwargs={"style": "primary"})]]))
            return

        kb_rows = [range_buttons[i:i+1] for i in range(0, min(len(range_buttons), 15))]
        kb_rows.append([InlineKeyboardButton("◀️ Back", callback_data="service:facebook", api_kwargs={"style": "primary"})])
        await safe_edit(query, f"{flag} *{cname} — Range select করুন:*", parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    # ── Instagram country select → Range/panel auto ──
    if data.startswith("ig_country:"):
        cname = data.split("ig_country:", 1)[1]
        user_data[user_id]["country"] = cname
        user_data[user_id]["app"] = "INSTAGRAM"
        await safe_edit(query, "⏳ Range লোড হচ্ছে...")
        code = next((k for k,v in COUNTRY_NAMES_CODE.items() if v == cname), "")
        flag = COUNTRY_FLAGS_CODE.get(code, "🌍")

        range_buttons = []

        async def _fetch_s2_ig_ranges():
            return await get_instagram_ranges(cname, panel="S2")

        async def _fetch_a1_ig_ranges():
            if not ZENEX_API_KEY: return []
            try: return await zenex_get_active_ranges(service="Instagram Account")
            except Exception: return []

        s2_ranges, a1_ig = await asyncio.gather(
            _fetch_s2_ig_ranges(),
            _fetch_a1_ig_ranges(),
        )

        for r in (s2_ranges or [])[:10]:
            rng = r if isinstance(r, str) else r.get("range","")
            if rng:
                range_buttons.append(InlineKeyboardButton(f"📞 {rng}", callback_data=f"auto_range:S2:{rng}", api_kwargs={"style": "primary"}))

        for r in (a1_ig or []):
            rng = r.get("range","")
            if rng and a2_extract_country_code(rng) == code:
                range_buttons.append(InlineKeyboardButton(f"📞 {rng}", callback_data=f"auto_range:A1:{rng}", api_kwargs={"style": "primary"}))

        if not range_buttons:
            await safe_edit(query, f"❌ {flag} {cname} তে এখন কোনো range নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="service:instagram", api_kwargs={"style": "primary"})]]))
            return

        kb_rows = [range_buttons[i:i+1] for i in range(0, min(len(range_buttons), 15))]
        kb_rows.append([InlineKeyboardButton("◀️ Back", callback_data="service:instagram", api_kwargs={"style": "primary"})])
        await safe_edit(query, f"{flag} *{cname} — Range select করুন:*", parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    # ── Auto range select → panel auto decide করে number দাও ──
    if data.startswith("auto_range:"):
        parts = data.split(":", 2)
        if len(parts) == 3:
            _, panel_auto, range_val = parts
            user_data[user_id]["range"] = range_val
            user_data[user_id]["panel"] = panel_auto
            user_msg[query.message.chat.id] = query.message.message_id
            if panel_auto == "A2":
                asyncio.create_task(do_get_number_a2(query.message, user_id, bot=context.bot))
            elif panel_auto == "A1":
                asyncio.create_task(do_get_number_a1(query.message, user_id, bot=context.bot))
            else:
                asyncio.create_task(do_get_number(query.message, user_id, bot=context.bot))
        return

    if data.startswith("select_panel_") and data not in ("select_panel_A2_fb", "select_panel_S2", "select_panel_TG"):
        panel = data.replace("select_panel_", "")
        user_data[user_id]["panel"] = panel
        user_data[user_id]["country"] = None
        user_data[user_id]["range"] = None

        if panel == "S3":
            # S3 — আগে App choice দেখাও
            pool = get_numbers_pool()
            fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
            ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
            await safe_edit(query,
                "📱 *Please select a service:*",
                parse_mode="Markdown",
                reply_markup=await panel_select_inline()
            )
        elif panel == "A1_fb":
            # A1 Facebook — ZENEX active ranges দেখাও
            user_data[user_id]["panel"] = "A1"
            user_data[user_id]["app"] = "FACEBOOK"
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            # A1 sub-service select
            await safe_edit(query,
                "🆕 *Facebook A1 (New)*\n\nService select করুন:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📘 Facebook New Account", callback_data="a1_service:Facebook New Account", api_kwargs={"style": "primary"})],
                    [InlineKeyboardButton("💻 Facebook PC Clone", callback_data="a1_service:Facebook PC Clone", api_kwargs={"style": "primary"})],
                    [InlineKeyboardButton("📸 Instagram Account", callback_data="a1_service:Instagram Account", api_kwargs={"style": "primary"})],
                    [InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})],
                ])
            )
        elif panel == "WA":
            # WhatsApp — শুধু A1 + A2, S2 বাদ
            app_name = "WHATSAPP"
            user_data[user_id]["app"] = app_name
            user_data[user_id]["panel"] = "A2"
            wa_buttons = []
            if ZENEX_API_KEY:
                wa_buttons.append([InlineKeyboardButton("🆕 WhatsApp A1", callback_data="wa_source:a1", api_kwargs={"style": "success"})])
            if A2_API_KEY:
                wa_buttons.append([InlineKeyboardButton("⚡ WhatsApp A2", callback_data="wa_source:a2", api_kwargs={"style": "success"})])
            wa_buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
            await safe_edit(query,
                "💬 *WhatsApp*\n\nPanel select করুন:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(wa_buttons)
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
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]])
                )
                return
            country_list = [{"country": c, "panel": panel} for c in countries]
            await safe_edit(query,
                f"📘 Facebook {panel}\n\n🌍 Country select করুন:",
                reply_markup=country_select_inline(country_list, app_name)
            )
        return

    # ── WhatsApp source select (A1 / S1 / S2) ──
    if data.startswith("wa_source:"):
        source = data.split("wa_source:", 1)[1]
        app_name = "WHATSAPP"
        user_data[user_id]["app"] = app_name
        if source == "a1":
            # WhatsApp A1 — ZENEX range list
            user_data[user_id]["panel"] = "A1"
            user_data[user_id]["a1_service"] = "WhatsApp"
            user_data[user_id]["a1_zenex_service"] = "Whatsapp"
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            ranges = await zenex_get_active_ranges(service="Whatsapp")
            if not ranges:
                await safe_edit(query,
                    "❌ WhatsApp A1 তে এখন কোনো active range নেই।",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="select_panel_WA", api_kwargs={"style": "primary"})]])
                )
                return
            buttons = []
            for r in ranges[:20]:
                rng = r.get("range", "")
                hits = r.get("hits", 0)
                if not rng:
                    continue
                code = rng[:2].upper()
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                buttons.append([InlineKeyboardButton(
                    f"{flag} {rng} (hits: {hits})",
                    callback_data=f"a1_range:{rng}",
                    api_kwargs={"style": "primary"}
                )])
            buttons.append([InlineKeyboardButton("◀️ Back", callback_data="select_panel_WA", api_kwargs={"style": "primary"})])
            await safe_edit(query,
                "💬 *A1 WhatsApp*\n\n📡 Range select করুন:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        elif source == "a2":
            # WhatsApp A2 — VOLTX range list
            user_data[user_id]["panel"] = "A2"
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            a2_all = a2_get_cached_ranges()
            if not a2_all:
                a2_all = await a2_get_active_ranges(force=True)
            ranges = [r for r in a2_all if r.get("service", "").upper() in ("WHATSAPP", "WA")]
            if not ranges:
                await safe_edit(query,
                    "❌ WhatsApp A2 তে এখন কোনো active range নেই।",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="select_panel_WA", api_kwargs={"style": "primary"})]])
                )
                return
            buttons = []
            seen = set()
            for r in ranges[:20]:
                rng = r.get("range", "")
                rid = r.get("rid", rng)
                if not rng or rng in seen:
                    continue
                seen.add(rng)
                code = a2_extract_country_code(rng)
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                buttons.append([InlineKeyboardButton(
                    f"{flag} {rng}",
                    callback_data=f"a2_range:{rid}",
                    api_kwargs={"style": "primary"}
                )])
            buttons.append([InlineKeyboardButton("◀️ Back", callback_data="select_panel_WA", api_kwargs={"style": "primary"})])
            await safe_edit(query,
                "💬 *A2 WhatsApp*\n\n📡 Range select করুন:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
        elif source in ("s2",):
            # WhatsApp S1+S2 combined
            await safe_edit(query, "⏳ লোড হচ্ছে...")
            countries_s2 = await get_countries_for_app(app_name, panel="S2")
            country_list = []
            for c in (countries_s2_extra or []):
                country_list.append({"country": c, "panel": "S2"})
            for c in (countries_s2 or []):
                if not any(x["country"] == c and x["panel"] == "S2" for x in country_list):
                    country_list.append({"country": c, "panel": "S2"})
            if not country_list:
                await safe_edit(query,
                    "❌ WhatsApp S2 তে এখন কোনো active range নেই।",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="select_panel_WA", api_kwargs={"style": "primary"})]])
                )
                return
            user_data[user_id]["panel"] = country_list[0]["panel"]
            await safe_edit(query,
                "💬 WhatsApp\n\n🌍 Country select করুন:",
                reply_markup=country_select_inline(country_list, app_name)
            )
        return

    # ── A1 Service select → Range list ──
    if data.startswith("a1_service:"):
        service_name = data.split("a1_service:", 1)[1]
        # Map display name to ZENEX service keyword
        zenex_service_map = {
            "Facebook New Account": "Facebook",
            "Facebook PC Clone": "Facebook",
            "Instagram Account": "Instagram",
        }
        zenex_kw = zenex_service_map.get(service_name, "Facebook")
        user_data[user_id]["a1_service"] = service_name
        user_data[user_id]["a1_zenex_service"] = zenex_kw
        user_data[user_id]["panel"] = "A1"
        await safe_edit(query, "⏳ Range লোড হচ্ছে...")
        ranges = await zenex_get_active_ranges(service=zenex_kw)
        if not ranges:
            await safe_edit(query,
                f"❌ {service_name} এ এখন কোনো active range নেই।\n\nকিছুক্ষণ পর try করুন।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="select_panel_A1_fb", api_kwargs={"style": "primary"})]])
            )
            return
        # Build range buttons with hits indicator
        buttons = []
        for r in ranges[:12]:
            rng = r.get("range", "")
            hits = r.get("hits", 0)
            dot = "🟢" if hits >= 100 else ("🟡" if hits >= 20 else "🔴")
            buttons.append([InlineKeyboardButton(
                f"{dot} {rng} ({hits})",
                callback_data=f"a1_range:{rng}",
                api_kwargs={"style": "primary"}
            )])
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="select_panel_A1_fb", api_kwargs={"style": "primary"})])
        await safe_edit(query,
            f"🆕 *{service_name}*\n\n👇 Range select করুন:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    # ── A1 Change Numbers ──
    if data == "a1_change_numbers":
        range_val = user_data[user_id].get("range", "")
        if not range_val:
            await safe_edit(query, "❌ Range নেই। আবার শুরু করুন।")
            return
        # existing message id সেট করো যাতে do_get_number_a1 নতুন message না পাঠায়
        user_msg[query.message.chat.id] = query.message.message_id
        asyncio.create_task(do_get_number_a1(query.message, user_id, bot=context.bot))
        return

    # ── A1 Range select → Get 2 numbers ──
    if data.startswith("a1_range:"):
        range_val = data.split("a1_range:", 1)[1]
        user_data[user_id]["range"] = range_val
        user_data[user_id]["panel"] = "A1"
        asyncio.create_task(do_get_number_a1(query.message, user_id, bot=context.bot))
        return

    # ── S2 Panel select (Facebook) ──
    if data == "select_panel_S2":
        user_data[user_id].update({"app": "FACEBOOK", "panel": "S2", "country": None, "range": None})
        await safe_edit(query, "⏳ লোড হচ্ছে...")
        countries = await get_countries_for_app("FACEBOOK", panel="S2")
        if not countries:
            await safe_edit(query, "❌ S2 তে এখন কোনো active country নেই।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]]))
            return
        country_list = [{"country": c, "panel": "S2"} for c in countries]
        await safe_edit(query, "📗 Facebook S2\n\n🌍 Country select করুন:",
            reply_markup=country_select_inline(country_list, "FACEBOOK"))
        return

    # ── A2 Panel select → Country list ──
    if data == "select_panel_TG":
        user_data[user_id]["panel"] = "A2"
        user_data[user_id]["app"] = "TELEGRAM"
        await safe_edit(query, "⏳ লোড হচ্ছে...")
        ranges = a2_get_cached_ranges()
        if not ranges:
            ranges = await a2_get_active_ranges(force=True)
        tg_ranges = [r for r in (ranges or []) if r.get("service", "").upper() in ("TELEGRAM", "TG")]
        if not tg_ranges:
            tg_ranges = ranges or []
        if not tg_ranges:
            await safe_edit(query,
                "❌ Telegram এ এখন কোনো active range নেই।\n\nকিছুক্ষণ পর try করুন।",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]])
            )
            return
        seen = set()
        buttons = []
        for r in tg_ranges[:25]:
            rng = r.get("range", "")
            rid = r.get("rid", rng)
            if not rng or rng in seen:
                continue
            seen.add(rng)
            code = a2_extract_country_code(rng)
            flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
            buttons.append([InlineKeyboardButton(
                f"{flag} {rng}",
                callback_data=f"tg_range:{rid}",
                api_kwargs={"style": "primary"}
            )])
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
        await safe_edit(query,
            "✈️ *Telegram*\n\n📡 Range select করুন:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    if data == "select_panel_A2_fb":
        user_data[user_id]["panel"] = "A2"
        user_data[user_id]["app"]   = "FACEBOOK"
        await safe_edit(query, "⏳ লোড হচ্ছে...")
        ranges = a2_get_cached_ranges()
        if not ranges:
            ranges = await a2_get_active_ranges(force=True)
        logger.info(f"A2 panel select: {len(ranges)} total ranges | services: {list(set(r.get('service','') for r in ranges))} | user {user_id}")
        if not ranges:
            await safe_edit(query,
                "❌ A2 তে এখন কোনো active range নেই।\n\nকিছুক্ষণ পর try করুন।",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})
                ]])
            )
            return
        # শুধু Facebook service এর range দেখাও
        fb_ranges = [r for r in ranges if r.get("service", "").upper() in ("FACEBOOK", "FB")]
        if not fb_ranges:
            fb_ranges = ranges  # Facebook না থাকলে সব fallback
        seen_countries = {}  # code → (flag, name, [ranges])
        for r in fb_ranges:
            rng = r.get("range", "")
            if not rng:
                continue
            code = a2_extract_country_code(rng)
            if code not in seen_countries:
                flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
                name = COUNTRY_NAMES_CODE.get(code, code)
                seen_countries[code] = {"flag": flag, "name": name, "ranges": []}
            seen_countries[code]["ranges"].append(r.get("rid", rng))
        if not seen_countries:
            await safe_edit(query,
                "❌ A2 তে এখন কোনো active country নেই।\n\nকিছুক্ষণ পর try করুন।",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})
                ]])
            )
            return
        buttons = []
        for code, info in list(seen_countries.items())[:30]:
            range_count = len(info["ranges"])
            buttons.append([InlineKeyboardButton(
                f"{info['flag']} {info['name']} ({range_count})",
                callback_data=f"a2_country:{code}",
                api_kwargs={"style": "primary"}
            )])
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
        await safe_edit(query,
            f"🌍 *Country select করুন ({len(seen_countries)} available):*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    # ── A2 Country select → Range list ──
    if data.startswith("a2_country:"):
        selected_code = data.split("a2_country:", 1)[1]
        user_data[user_id]["country"] = COUNTRY_NAMES_CODE.get(selected_code, selected_code)
        await safe_edit(query, "⏳ Range লোড হচ্ছে...")
        ranges = a2_get_cached_ranges()
        if not ranges:
            ranges = await a2_get_active_ranges(force=True)
        # এই country এর Facebook ranges
        fb_only = [r for r in ranges if r.get("service", "").upper() in ("FACEBOOK", "FB")]
        if not fb_only:
            fb_only = ranges
        country_ranges = [r for r in fb_only if a2_extract_country_code(r.get("range", "")) == selected_code]
        if not country_ranges:
            await safe_edit(query,
                "❌ এই country তে এখন কোনো active range নেই।",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("◀️ Back", callback_data="select_panel_A2_fb", api_kwargs={"style": "primary"})
                ]])
            )
            return
        flag = COUNTRY_FLAGS_CODE.get(selected_code, "🌍")
        country_name = COUNTRY_NAMES_CODE.get(selected_code, selected_code)
        buttons = []
        seen = set()
        for r in country_ranges[:20]:
            rng = r.get("range", "")
            rid = r.get("rid", rng)
            if not rng or rng in seen:
                continue
            seen.add(rng)
            buttons.append([InlineKeyboardButton(
                f"📡 {rng}",
                callback_data=f"a2_range:{rid}",
                api_kwargs={"style": "primary"}
            )])
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="select_panel_A2_fb", api_kwargs={"style": "primary"})])
        await safe_edit(query,
            f"{flag} *{country_name} — Range select করুন ({len(seen)} available):*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

    if data.startswith("a2_range:"):
        rid_val = data.split("a2_range:", 1)[1]
        user_data[user_id]["range"] = rid_val
        user_data[user_id]["panel"] = "A2"
        _country_code = a2_extract_country_code(rid_val)
        user_data[user_id]["country"] = COUNTRY_NAMES_CODE.get(_country_code, _country_code)
        user_msg[query.message.chat.id] = query.message.message_id
        asyncio.create_task(do_get_number_a2(query.message, user_id, bot=context.bot))
        return

    if data.startswith("tg_range:"):
        rid_val = data.split("tg_range:", 1)[1]
        user_data[user_id]["range"] = rid_val
        user_data[user_id]["panel"] = "A2"
        user_data[user_id]["app"] = "TELEGRAM"
        _country_code = a2_extract_country_code(rid_val)
        user_data[user_id]["country"] = COUNTRY_NAMES_CODE.get(_country_code, _country_code)
        user_msg[query.message.chat.id] = query.message.message_id
        asyncio.create_task(do_get_number_a2(query.message, user_id, bot=context.bot))
        return

    # ── A2 Change Numbers ──
    if data == "a2_change_numbers":
        range_val = user_data[user_id].get("range", "")
        if not range_val:
            await safe_edit(query, "❌ Range নেই। আবার শুরু করুন।")
            return
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["auto_otp_cancel"] = False
        user_msg[query.message.chat.id] = query.message.message_id
        asyncio.create_task(do_get_number_a2(query.message, user_id, bot=context.bot))
        return

    # ── Back to panel select ──
    if data == "back_app":
        # ✅ পুরানো OTP task cancel করো, session return করো
        cancel_all_otp_tasks(user_id)
        old_session = user_data[user_id].get("number_session")
        if old_session and old_session.get("token"):
            panel = user_data[user_id].get("panel", "S2")
            if False:  # S1 removed
                await s2_pool.return_number_session(old_session)
            elif panel == "S2":
                await s2_pool.return_number_session(old_session)
        user_data[user_id]["number_session"] = None
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False
        user_data[user_id]["auto_otp_cancel"] = True
        user_data[user_id]["range"] = None
        user_data[user_id]["country"] = None
        inline_kb = await panel_select_inline()
        await safe_edit(query, "⚙️ Select Service:", reply_markup=inline_kb)
        return


    if data.startswith("app_s2_"):
        app_name = data.replace("app_s2_", "")
        user_data[user_id].update({"app": app_name, "panel": "S2", "country": None, "range": None})
        await safe_edit(query, "⏳ লোড হচ্ছে...")
        countries = await get_countries_for_app(app_name, panel="S2")
        if not countries:
            await safe_edit(query, "❌ এখন কোনো active country নেই.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})]]))
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
            [InlineKeyboardButton(get_button_label(pk), callback_data=f"s3getcountry:{pk}", api_kwargs={"style": "primary"})]
            for pk in sorted(pool.keys())
        ]
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})])
        await safe_edit(query, "🔴 Facebook S3\n\n🌍 Country select করুন:",
            reply_markup=InlineKeyboardMarkup(buttons))
        return

    # ── Country select (S1/S2) ──
    if data.startswith("country_"):
        raw = data.replace("country_", "")
        if raw.startswith("S2_"):
            panel = raw[:2]
            country = raw[3:]
            user_data[user_id]["panel"] = panel
        else:
            country = raw
            panel = user_data[user_id].get("panel", "S2")
        app_name = user_data[user_id].get("app", "FACEBOOK")
        user_data[user_id]["country"] = country
        user_data[user_id]["range"] = None
        await safe_edit(query, "⏳ Range লোড হচ্ছে...")
        ranges = await get_all_ranges_for_country(app_name, country, panel=panel)
        if not ranges:
            await safe_edit(query, f"❌ {country} তে কোনো range নেই.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"back_country_{app_name}", api_kwargs={"style": "primary"})]]))
            return
        flag = get_flag_by_iso(country)
        await safe_edit(query,
            f"{flag} {country}\n\n📡 Range select করুন:",
            reply_markup=range_select_inline(ranges, app_name, country)
        )
        return

    if data.startswith("back_country_"):
        app_name = data.replace("back_country_", "")
        panel = user_data[user_id].get("panel", "S2")
        user_data[user_id]["country"] = None
        await safe_edit(query, "⏳ Loading...")
        if app_name == "INSTAGRAM":
            countries = await get_instagram_countries(panel=panel)
            if not countries:
                await safe_edit(query, "❌ কোনো Instagram country নেই।",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back", callback_data=f"select_panel_{panel}", api_kwargs={"style": "primary"})]]))
                return
            buttons = []
            row = []
            for country in countries[:20]:
                flag = get_flag_by_iso(country)
                row.append(InlineKeyboardButton(f"{flag} {country}", callback_data=f"ig_country_{panel}_{country}", api_kwargs={"style": "primary"}))
                if len(row) == 2:
                    buttons.append(row)
                    row = []
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton("◀️ Back", callback_data=f"select_panel_{panel}", api_kwargs={"style": "primary"})])
            await safe_edit(query, f"📸 Instagram {panel}\n\n🌍 Country select করুন:", reply_markup=InlineKeyboardMarkup(buttons))
        else:
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
        user_data[user_id]["tried_ranges"] = set()  # নতুন range select হলে reset
        # ✅ পুরানো session return করো
        old_session = user_data[user_id].get("number_session")
        if old_session and old_session.get("token"):
            panel = user_data[user_id].get("panel", "S2")
            if False:  # S1 removed
                await s2_pool.return_number_session(old_session)
            elif panel == "S2":
                await s2_pool.return_number_session(old_session)
        user_data[user_id]["number_session"] = None
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["auto_otp_cancel"] = False
        user_data[user_id]["otp_active"] = False
        user_data[user_id]["otp_running"] = False
        # ── Card delete করা হবে না — same message edit করো ──
        chat_id = query.message.chat.id
        try:
            await query.message.edit_text("⏳ Searching Number...", reply_markup=None)
            await asyncio.sleep(1)
            await query.message.edit_text("📡 Connecting Server...", reply_markup=None)
            await asyncio.sleep(1)
        except Exception:
            pass
        user_msg[chat_id] = query.message.message_id
        await do_get_number(query.message, user_id, bot=context.bot)
        return

    # ── New Number (S1/S2) ──
    if data.startswith("new_number_"):
        range_val = data.replace("new_number_", "")
        user_data[user_id]["range"] = range_val
        panel = user_data[user_id].get("panel", "S2")
        pool  = s2_pool

        # ── Return all dual sessions ──
        old_sessions = user_data[user_id].get("number_sessions", [])
        for sess in old_sessions:
            if sess and sess.get("token"):
                await pool.return_number_session(sess)
        # compat: return single session too if set
        old_single = user_data[user_id].get("number_session")
        if old_single and old_single.get("token") and old_single not in old_sessions:
            await pool.return_number_session(old_single)

        user_data[user_id]["number_session"]  = None
        user_data[user_id]["number_sessions"] = []
        user_data[user_id]["numbers"]         = []
        cancel_all_otp_tasks(user_id)
        user_data[user_id]["auto_otp_cancel"] = False
        user_data[user_id]["otp_active"]      = False
        user_data[user_id]["otp_running"]     = False
        user_data[user_id]["last_number"]     = None

        # Same card edit — loading text দেখাও, নতুন message নয়
        chat_id = query.message.chat.id
        try:
            await query.message.edit_text("⏳ Searching Number...", reply_markup=None)
            await asyncio.sleep(1)
            await query.message.edit_text("📡 Connecting Server...", reply_markup=None)
            await asyncio.sleep(1)
        except Exception:
            pass
        # existing message id সেট করো যাতে do_get_number নতুন message না পাঠায়
        user_msg[chat_id] = query.message.message_id
        await do_get_number(query.message, user_id, bot=context.bot)
        return

    # ══════════════════
    #  S3 CALLBACKS
    # ══════════════════

    # S3 App select → country list
    if data.startswith("s3app:"):
        service = data.split(":", 1)[1]  # "fb" or "ig"
        user_data[user_id]["s3_service"] = service
        pool = get_numbers_pool()
        service_label = "Facebook" if service == "fb" else "Instagram"
        service_icon = "🔵" if service == "fb" else "📸"

        # এই service এর সব pool_key filter করো
        matching_pools = {k: v for k, v in pool.items() if k.endswith(f"_{service}")}
        if not matching_pools:
            await safe_edit(query,
                f"❌ {service_label} S3 তে এখন কোনো number নেই!\n\nAdmin কে জানান।"
            )
            return

        buttons = []
        for pk in sorted(matching_pools.keys()):
            parts = pk.split("_")
            code = parts[0]
            variant = parts[1].upper() if len(parts) == 3 else None
            flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
            name = COUNTRY_NAMES_CODE.get(code, code)
            count = len(matching_pools[pk])
            label = f"{flag} {name} {variant} ({count})" if variant else f"{flag} {name} ({count})"
            buttons.append([InlineKeyboardButton(
                label,
                callback_data=f"s3getcountry:{pk}",
                api_kwargs={"style": "primary"}
            )])
        buttons.append([InlineKeyboardButton("◀️ Back", callback_data="select_panel_S3", api_kwargs={"style": "primary"})])

        await safe_edit(query,
            f"{service_icon} *{service_label} S3*\n\n🌍 Country select করুন:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons)
        )
        return

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

        # ৩টা number একসাথে নাও
        count = min(3, len(numbers))
        selected = random.sample(numbers, count)
        for n in selected:
            if n in numbers:
                numbers.remove(n)
        numbers_pool[pool_key] = numbers
        for n in selected:
            asyncio.create_task(mark_number_assigned(n, user_id, pool_key))

        s3_set_session(user_id, selected, pool_key)
        asyncio.create_task(_save_users_s3(context.bot))

        code, service, variant = parse_pool_key(pool_key)
        country_name = COUNTRY_NAMES_CODE.get(code, "Unknown")
        flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
        service_label = "FACEBOOK" if service == "fb" else "INSTAGRAM"

        card_text = (
            f"✅ <b>Numbers Assigned!</b>\n\n"
            f"<b>Service:</b> {service_label}\n"
            f"🌍 <b>Country:</b> {flag} {country_name}\n"
            f"⏳ <b>Reserved:</b> 30 min\n\n"
            f"📩 OTPs forwarded automatically."
        )
        await query.message.edit_text(
            card_text,
            parse_mode="HTML",
            reply_markup=after_number_inline_s3(pool_key, selected)
        )
        return

    if data.startswith("s3change:"):
        if is_rate_limited(user_id):
            await query.answer("⏳ একটু ধীরে!", show_alert=True)
            return
        pool_key = data.split(":", 1)[1]
        session = s3_get_session(user_id)
        if session:
            old_numbers = session.get("numbers", [session.get("number")] if session.get("number") else [])
            for n in old_numbers:
                await remove_number_from_pool(context.bot, pool_key, n)

        numbers = get_pool_numbers(pool_key)
        if not numbers:
            await safe_edit(query,
                "❌ *No Numbers Available*\n\n📭 এই country তে এখন number নেই",
                parse_mode="Markdown"
            )
            return

        count = min(3, len(numbers))
        selected = random.sample(numbers, count)
        for n in selected:
            if n in numbers:
                numbers.remove(n)
        numbers_pool[pool_key] = numbers
        asyncio.create_task(_save_numbers(context.bot))
        s3_set_session(user_id, selected, pool_key)
        asyncio.create_task(_save_users_s3(context.bot))

        code, service, variant = parse_pool_key(pool_key)
        country_name = COUNTRY_NAMES_CODE.get(code, "Unknown")
        flag = COUNTRY_FLAGS_CODE.get(code, "🌍")
        service_label = "FACEBOOK" if service == "fb" else "INSTAGRAM"

        card_text = (
            f"✅ <b>Numbers Assigned!</b>\n\n"
            f"<b>Service:</b> {service_label}\n"
            f"🌍 <b>Country:</b> {flag} {country_name}\n"
            f"⏳ <b>Reserved:</b> 30 min\n\n"
            f"📩 OTPs forwarded automatically."
        )
        await query.message.edit_text(
            card_text,
            parse_mode="HTML",
            reply_markup=after_number_inline_s3(pool_key, selected)
        )
        return

    if data == "s3changecountry":
        pool = get_numbers_pool()
        if not pool:
            await query.answer("❌ No countries available", show_alert=True)
            return
        fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
        ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
        await safe_edit(query,
            "📱 *Please select a service:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🆕 Facebook A1", callback_data="select_panel_A1_fb", api_kwargs={"style": "success"})],
                [InlineKeyboardButton("⚡ Facebook A2", callback_data="select_panel_A2_fb", api_kwargs={"style": "success"})],
                [InlineKeyboardButton("📗 Facebook S2", callback_data="select_panel_S2", api_kwargs={"style": "success"})],
                [InlineKeyboardButton(f"🔴 Facebook S3 ({fb_count})", callback_data="s3app:fb", api_kwargs={"style": "danger"})],
                [InlineKeyboardButton(f"📸 Instagram S3 ({ig_count})", callback_data="s3app:ig", api_kwargs={"style": "danger"})],
                [InlineKeyboardButton("💬 WhatsApp", callback_data="select_panel_WA", api_kwargs={"style": "primary"})],
                [InlineKeyboardButton("◀️ Back", callback_data="back_app", api_kwargs={"style": "primary"})],
            ])
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
        global GET100_ENABLED

        _back_btn = [[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]]

        if action == "back":
            get100_status = "✅ ON" if GET100_ENABLED else "❌ OFF"
            total_users = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
            pool = get_numbers_pool()
            total_s3 = sum(len(v) for v in pool.values())
            fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
            ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
            s3_pools = len(pool)
            msg = (
                "╔═══════════════════════╗\n"
                "║     👑 ADMIN PANEL      ║\n"
                "╚═══════════════════════╝\n\n"
                f"👥 *Total Users:* `{total_users}`\n"
                f"📦 *Bulk Mode:* `{get100_status}`\n\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "📡 *Session Status*\n"
                f"  🟢 S2: `{s2_pool.number_sessions.qsize()}` number | `{s2_pool.otp_sessions.qsize()}` otp\n\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "🔴 *S3 Pool Status*\n"
                f"  📘 Facebook: `{fb_count}` numbers\n"
                f"  📸 Instagram: `{ig_count}` numbers\n"
                f"  🗂 Total Pools: `{s3_pools}` | Numbers: `{total_s3}`\n\n"
                "━━━━━━━━━━━━━━━━━━"
            )
            await safe_edit(query, msg, parse_mode="Markdown", reply_markup=admin_keyboard_unified())
            return

        if action == "bulk_on":
            GET100_ENABLED = True
            await query.answer("✅ Bulk ON")
            await safe_edit(query, "✅ *Bulk Number Mode: ON*\n\nসব user এখন Bulk Number নিতে পারবে।",
                parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(_back_btn))
            return
        if action == "bulk_off":
            GET100_ENABLED = False
            await query.answer("❌ Bulk OFF")
            await safe_edit(query, "❌ *Bulk Number Mode: OFF*\n\nBulk Number বন্ধ করা হয়েছে।",
                parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(_back_btn))
            return
        if action == "allusers":
            total = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
            msg = f"👥 *Total Users: {total}*\n\n"
            for uid, uinfo in list(user_data.items())[:15]:
                msg += f"• `{uid}` — {uinfo.get('name','?')} | {uinfo.get('panel','?')}\n"
            await safe_edit(query, msg, parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(_back_btn))
            return
        if action == "stats_s12":
            pool = get_numbers_pool()
            total_s3 = sum(len(v) for v in pool.values())
            total_users = len(set(list(user_data.keys()) + [int(u) for u in s3_get_all_users()]))
            await safe_edit(query,
                f"📊 *BOT STATS*\n\n"
                f"👥 Users: `{total_users}`\n"
                f"📦 Bulk: `{'ON' if GET100_ENABLED else 'OFF'}`\n"
                f"🟢 S2 Sessions: `{s2_pool.number_sessions.qsize()}`\n"
                f"🔴 S3 Numbers: `{total_s3}`\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(_back_btn)
            )
            return
        if action == "apistatus":
            s2_sess = await s2_pool._login_once()
            s2_ok = "✅" if s2_sess.get("token") else "❌"
            cr_otps = await fetch_cr_api_otps()
            cr_ok = "✅" if cr_otps is not None else "❌"
            await safe_edit(query,
                f"🔗 *API STATUS*\n\n"
                f"🟢 S2 (X.Mint): {s2_ok}\n"
                f"🔴 S3 (CR API): {cr_ok} ({len(cr_otps)} OTPs)\n\n"
                f"S2 Number slots: `{s2_pool.number_sessions.qsize()}`/20",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(_back_btn)
            )
            return

        if action == "s2panel":
            s2_sess = await s2_pool._login_once()
            s2_ok = "✅ Online" if s2_sess.get("token") else "❌ Offline"
            s2_logs = await get_console_logs_s2(force=True)
            cached_token, _ = await _s2_load_token_cache()
            cache_status = "✅ Cached" if cached_token else "❌ No cache"
            await safe_edit(query,
                f"🟢 *S2 Panel — X.Mint*\n\n"
                f"Status: {s2_ok}\n"
                f"Number Sessions: `{s2_pool.number_sessions.qsize()}`/40\n"
                f"OTP Sessions: `{s2_pool.otp_sessions.qsize()}`/10\n"
                f"Total Sessions: `{len(s2_pool.all_sessions)}`\n"
                f"Supabase Token Cache: {cache_status}\n\n"
                f"📋 Console Logs: `{len(s2_logs)}` entries\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh S2 Sessions", callback_data="admin_refresh_s2", api_kwargs={"style": "success"})],
                    *_back_btn
                ])
            )
            return

        if action == "a1panel":
            a1_ok = "✅ Online" if ZENEX_API_KEY else "❌ API Key নেই"
            try:
                ranges = await zenex_get_active_ranges()
                range_count = len(ranges)
            except Exception:
                range_count = 0
            await safe_edit(query,
                f"🆕 *A1 Panel — ZENEX Network*\n\n"
                f"Status: {a1_ok}\n"
                f"Active Ranges: `{range_count}`\n"
                f"Range Post: ✅ Every 2 min\n\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh A1 Ranges", callback_data="admin_refresh_a1", api_kwargs={"style": "success"})],
                    *_back_btn
                ])
            )
            return

        if action == "refresh_a1":
            global _zenex_ranges_cache
            _zenex_ranges_cache = {"data": [], "time": 0}
            try:
                ranges = await zenex_get_active_ranges()
                range_count = len(ranges)
            except Exception:
                range_count = 0
            await safe_edit(query,
                f"✅ *A1 Ranges Refreshed!*\n\n"
                f"Active Ranges: `{range_count}`\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀️ A1 Panel", callback_data="admin_a1panel", api_kwargs={"style": "primary"})],
                    *_back_btn
                ])
            )
            return

        if action == "a2panel":
            a2_ok = "✅ Online" if A2_API_KEY else "❌ API Key নেই"
            try:
                a2_ranges = await a2_get_active_ranges()
                a2_range_count = len(a2_ranges)
            except Exception:
                a2_range_count = 0
            await safe_edit(query,
                f"⚡ *A2 Panel — VOLTX SMS*\n\n"
                f"Status: {a2_ok}\n"
                f"Active Ranges: `{a2_range_count}`\n"
                f"Range Post: ✅ Every 2 min\n\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Refresh A2 Ranges", callback_data="admin_refresh_a2", api_kwargs={"style": "success"})],
                    *_back_btn
                ])
            )
            return

        if action == "refresh_a2":
            global _a2_ranges_cache
            _a2_ranges_cache = {"data": [], "time": 0}
            try:
                a2_ranges = await a2_get_active_ranges()
                a2_range_count = len(a2_ranges)
            except Exception:
                a2_range_count = 0
            await safe_edit(query,
                f"✅ *A2 Ranges Refreshed!*\n\n"
                f"Active Ranges: `{a2_range_count}`\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀️ A2 Panel", callback_data="admin_a2panel", api_kwargs={"style": "primary"})],
                    *_back_btn
                ])
            )
            return

        if action == "refresh_s2":
            await query.answer("🔄 Refreshing S2...")
            await s2_pool.refresh_all()
            await safe_edit(query,
                f"✅ *S2 Sessions Refreshed!*\n\n"
                f"Number: `{s2_pool.number_sessions.qsize()}`/40\n"
                f"OTP: `{s2_pool.otp_sessions.qsize()}`/10",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("◀️ S2 Panel", callback_data="admin_s2panel", api_kwargs={"style": "primary"})],
                    *_back_btn
                ])
            )
            return

        if action == "refresh":
            await query.answer("🔄 Refreshing all sessions...")
            await s2_pool.refresh_all()
            await safe_edit(query,
                f"✅ *All Sessions Refreshed!*\n\n"
                f"🟢 S2 Number: `{s2_pool.number_sessions.qsize()}`/40\n"
                f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(_back_btn)
            )
            return
        if action == "broadcast_s12":
            user_data[ADMIN_ID]["waiting_for"] = "broadcast"
            await safe_edit(query, "📢 *Broadcast*\n\nMessage লিখুন — সব user কে পাঠানো হবে:", parse_mode="Markdown")
            return
        if action == "stop":
            await query.answer("🛑 Bot বন্ধ হচ্ছে...")
            await safe_edit(query, "🛑 *Bot stopped by admin.*", parse_mode="Markdown")
            asyncio.get_event_loop().stop()
            return

        if action == "restart":
            await query.answer("♻️ Restarting...")
            await safe_edit(query, "♻️ *Bot restarting...*\n\nকিছুক্ষণ পরে আবার চালু হবে।", parse_mode="Markdown")
            import sys, os
            await asyncio.sleep(1)
            os.execv(sys.executable, [sys.executable] + sys.argv)
            return

        if action == "clearcache":
            otp_cache.clear()
            _join_cache.clear()
            await query.answer("🧹 Cache cleared!")
            await safe_edit(query, "🧹 *Cache Cleared!*\n\notp_cache এবং join_cache পরিষ্কার হয়েছে।",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(_back_btn))
            return

        if action == "livetraffic":
            try:
                from datetime import timezone, timedelta as _td
                now_ts = time.time()
                window_24h = 86400
                def parse_log_time(t_str):
                    if not t_str:
                        return None
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y %H:%M:%S"):
                        try:
                            return datetime.strptime(str(t_str)[:19], fmt).timestamp()
                        except Exception:
                            pass
                    return None
                s2_logs = await get_console_logs_s2(force=True)
                s2_otp = sum(1 for log in s2_logs if True)
                s3_otp = sum(
                    1 for k in otp_cache if k.startswith("s3:")
                )
                pool = get_numbers_pool()
                fb_count = sum(len(v) for k, v in pool.items() if k.endswith("_fb"))
                ig_count = sum(len(v) for k, v in pool.items() if k.endswith("_ig"))
                bd_now = datetime.now(timezone(_td(hours=6))).strftime("%I:%M %p")
                msg = (
                    f"🚦 *Live Traffic*\n\n"
                    f"🟢 S2 OTP: `{s2_otp}`\n"
                    f"🔴 S3 OTP: `{s3_otp}`\n\n"
                    f"📘 FB Numbers: `{fb_count}`\n"
                    f"📸 IG Numbers: `{ig_count}`\n\n"
                    f"🕐 {bd_now}"
                )
                await safe_edit(query, msg, parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(_back_btn))
            except Exception as e:
                await safe_edit(query, f"❌ Error: {e}",
                    reply_markup=InlineKeyboardMarkup(_back_btn))
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
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]])
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
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]])
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
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]])
            )
            return

        if action == "delete":
            pool = get_numbers_pool()
            if not pool:
                await safe_edit(query, "❌ No pools available",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]]))
                return
            buttons = [
                [InlineKeyboardButton(f"🗑 {get_button_label(pk)} ({len(pool[pk])})", callback_data=f"s3deletepool:{pk}", api_kwargs={"style": "danger"})]
                for pk in sorted(pool.keys())
            ]
            buttons.append([InlineKeyboardButton("✏️ Delete Single Number", callback_data="s3deletesingle", api_kwargs={"style": "danger"})])
            buttons.append([InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})])
            await safe_edit(query,
                "🗑️ *Delete Numbers (S3)*\n\nPool delete করতে pool এ ক্লিক করো:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            return

        if action == "settings":
            await safe_edit(query, "⚙️ *S3 Settings*\n\nComing soon...", parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]]))
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
                [InlineKeyboardButton("✅ হ্যাঁ Delete করো", callback_data=f"s3confirmdeletepool:{pool_key}", api_kwargs={"style": "success"})],
                [InlineKeyboardButton("❌ Cancel", callback_data="s3admin_delete", api_kwargs={"style": "danger"})],
                [InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})],
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
            # Supabase থেকে pool_key এর সব number permanently DELETE
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.delete(
                        f"{SUPABASE_URL}/rest/v1/s3_numbers",
                        params={"pool_key": f"eq.{pool_key}"},
                        headers=_sb_headers()
                    )
                logger.info(f"🗑 Pool permanent delete: {pool_key} | {count} numbers | status={resp.status_code}")
            except Exception as e:
                logger.error(f"Pool delete Supabase error: {e}")
            await safe_edit(query,
                f"✅ *Pool Permanently Deleted!*\n\n🌍 Pool: `{pool_key}`\n🗑 Removed: `{count}` numbers\n⚠️ Redeploy এর পরেও ফিরে আসবে না",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀️ Back to Admin", callback_data="admin_back", api_kwargs={"style": "primary"})]])
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
    if update.message.chat.type != "private":
        return
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
        otps = await fetch_cr_api_otps()
        preloaded = 0
        for otp_data in otps:
            number = otp_data.get("num", "").strip()
            message = otp_data.get("message", "").strip()
            dt = otp_data.get("dt", "").strip()
            import hashlib
            msg_hash = hashlib.md5(message.encode()).hexdigest()[:8]
            cache_key = f"s3:{number}:{dt}:{msg_hash}"
            otp_cache[cache_key] = True
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
                user_data[uid].setdefault("panel", u.get("panel", "S2"))
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

    # Start session pools — S1 আগে, তারপর S2
    async def _init_pools():
        try:
            await s2_pool.initialize()
            logger.info("✅ S2 pool init complete")
        except Exception as e:
            logger.error(f"S2 pool init error: {e}")

    asyncio.create_task(_init_pools())

    # A1 (ZENEX) startup log
    if ZENEX_API_KEY:
        try:
            a1_ranges = await zenex_get_active_ranges()
            logger.info(f"✅ A1 (ZENEX) ready! Active ranges: {len(a1_ranges)}")
        except Exception as e:
            logger.warning(f"⚠️ A1 (ZENEX) startup check failed: {e}")
    else:
        logger.warning("⚠️ A1 (ZENEX) — ZENEX_API_KEY not set, A1 panel disabled.")

    # A2 (VOLTX) startup log
    if A2_API_KEY:
        try:
            a2_ranges = await a2_get_active_ranges(force=True)
            logger.info(f"✅ A2 (VOLTX) ready! Active ranges: {len(a2_ranges)}")
        except Exception as e:
            logger.warning(f"⚠️ A2 (VOLTX) startup check failed: {e}")
    else:
        logger.warning("⚠️ A2 (VOLTX) — A2_API_KEY not set, A2 panel disabled.")

    logger.info("✅ 4-Panel Bot started! S2 + S3 + A1 + A2 active.")

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
    if any(x in err for x in [
        "message is not modified",
        "bad request",
        "message to edit not found",
        "query is too old",
        "query id is invalid",
    ]):
        return
    logger.error(f"Exception: {context.error}")

# ══════════════════════════════════════════════════════════
#              MAIN
# ══════════════════════════════════════════════════════════

def main():
    logger.info("🚀 Starting 4-Panel Combined Bot (S2 + S3 + A1 + A2)...")

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

    # Callback
    app.add_handler(CallbackQueryHandler(callback_handler))

    # TXT file upload (S3 number add)
    app.add_handler(MessageHandler(filters.Document.FileExtension("txt"), handle_txt_file))

    # Broadcast media handler (photo/video/document/sticker/animation)
    _broadcast_media_filter = (
        filters.PHOTO | filters.VIDEO | filters.ANIMATION |
        filters.Sticker.ALL | filters.Document.ALL
    ) & filters.ChatType.PRIVATE
    app.add_handler(MessageHandler(_broadcast_media_filter, handle_broadcast_media))

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

    app.job_queue.run_repeating(job_post_live_sms, interval=120, first=60)
    if ZENEX_API_KEY:
        pass  # A1 range post disabled
    if A2_API_KEY:
        app.job_queue.run_repeating(job_a2_range_post, interval=120, first=105)

    # WhatsApp range post — 5 min পরপর, S1+S2+A1
    app.job_queue.run_repeating(job_whatsapp_range_post, interval=300, first=120)

    # Background auto cleanup — every 30 minutes
    app.job_queue.run_repeating(job_auto_cleanup, interval=1800, first=300)

    # Midnight reset — Supabase posted_sms clear
    import datetime as _dt
    from datetime import timezone, timedelta as _td
    bd_now = _dt.datetime.now(timezone(_td(hours=6)))
    midnight_bd = bd_now.replace(hour=0, minute=0, second=0, microsecond=0) + _td(days=1)
    seconds_until_midnight = (midnight_bd - bd_now).total_seconds()
    app.job_queue.run_repeating(job_midnight_reset, interval=86400, first=seconds_until_midnight)

    app.run_polling(drop_pending_updates=True, timeout=30)

if __name__ == "__main__":
    main()

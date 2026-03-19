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

# STEXSMS CONFIG
STEXSMS_EMAIL = "shuvosrb86@gmail.com"
STEXSMS_PASSWORD = "Superdry168"
STEXSMS_BASE_URL = "https://stexsms.com/mapi/v1"

# X.MINT CONFIG
XMINT_EMAIL = "aboos7008@gmail.com"
XMINT_PASSWORD = "Siam12345678@"
XMINT_BASE_URL = "https://x.mnitnetwork.com/mapi/v1"
XMINT_MAUTTOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1aWQiOiJNX1RWUTMyUU5ZSyIsInJvbGUiOiJ1c2VyIiwiYWNjZXNzX3BhdGgiOlsiL2Rhc2hib2FyZCJdLCJleHBpcnkiOjE3NzQwMDc0NDEsImNyZWF0ZWQiOjE3NzM5MjEwNDEsIjJvbzkiOiJNc0giLCJleHAiOjE3NzQwMDc0NDEsImlhdCI6MTc3MzkyMTA0MSwic3ViIjoiTV9UVlEzMlFOWUsifQ.wOGqOeaMcrKz1nKa1F7YCdW9IepGbtM6WIXZ-rw96d4"

ADMIN_ID = 1984916365
CHANNEL_USERNAME = "@alwaysrvice24hours"
CHANNEL_LINK = "https://t.me/alwaysrvice24hours"
OTP_CHANNEL_ID = -1002625886518

GET100_ENABLED = False
GET100_USERS = set()

logging.basicConfig(level=logging.INFO)

user_data = {}

# =============================================
#         APP EMOJIS & COUNTRIES
# =============================================

APP_EMOJIS = {
    "FACEBOOK": "📘", "INSTAGRAM": "📸", "TIKTOK": "🎵",
    "SNAPCHAT": "👻", "TWITTER": "🐦", "GOOGLE": "🔍",
    "WHATSAPP": "💬", "TELEGRAM": "✈️", "CHATGPT": "🤖",
    "SHEIN": "👗", "TWILIO": "📞", "TWVERIFY": "✅",
}

COUNTRY_FLAGS = {
    "CM": "🇨🇲", "VN": "🇻🇳", "PK": "🇵🇰", "TZ": "🇹🇿",
    "TJ": "🇹🇯", "TG": "🇹🇬", "NG": "🇳🇬", "GH": "🇬🇭",
    "KE": "🇰🇪", "BD": "🇧🇩", "IN": "🇮🇳", "PH": "🇵🇭",
    "ID": "🇮🇩", "MM": "🇲🇲", "KH": "🇰🇭", "ET": "🇪🇹",
    "US": "🇺🇸", "CA": "🇨🇦", "AU": "🇦🇺", "NZ": "🇳🇿",
    "JP": "🇯🇵", "KR": "🇰🇷", "CN": "🇨🇳", "BR": "🇧🇷",
    "CI": "🇨🇮", "TG": "🇹🇬",
}

ALL_APPS = [
    "FACEBOOK", "INSTAGRAM", "TIKTOK", "SNAPCHAT",
    "TWITTER", "GOOGLE", "WHATSAPP", "TELEGRAM",
    "CHATGPT", "SHEIN", "TWILIO", "TWVERIFY"
]

# =============================================
#         SESSION POOL SYSTEM (StexSMS)
# =============================================

SESSION_POOL_SIZE = 100
NUMBER_GET_SLOTS = 50
OTP_CHECK_SLOTS = 50

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
            logging.info("🔄 StexSMS Session pool initialize হচ্ছে...")
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
            logging.info(f"✅ StexSMS Session pool ready! Number: {number_count}, OTP: {otp_count}")

    async def _login_once(self):
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                res = await client.post(
                    f"{STEXSMS_BASE_URL}/mauth/login",
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
            logging.error(f"StexSMS Login error: {e}")
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
        logging.info("🔄 StexSMS Session pool refresh হচ্ছে...")
        self.initialized = False
        while not self.number_sessions.empty():
            self.number_sessions.get_nowait()
        while not self.otp_sessions.empty():
            self.otp_sessions.get_nowait()
        self.all_sessions.clear()
        await self.initialize()

session_pool = SessionPool()

# =============================================
#         HELPER FUNCTIONS
# =============================================

def get_flag(country):
    return COUNTRY_FLAGS.get(country, "🌍")

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

def get_stexsms_headers(token, session):
    return {
        'User-Agent': "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        'Accept': "application/json, text/plain, */*",
        'Content-Type': "application/json",
        'mauthtoken': token,
        'Cookie': f"mauthtoken={token}; session_token={session}"
    }

def get_xmint_headers():
    return {
        'User-Agent': "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        'Accept': "application/json, text/plain, */*",
        'Content-Type': "application/json",
        'Cookie': f"mautToken={XMINT_MAUTTOKEN}"
    }

# =============================================
#         STEXSMS API FUNCTIONS
# =============================================

async def stexsms_get_number(range_val, app_name="FACEBOOK"):
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
                f"{STEXSMS_BASE_URL}/mdashboard/getnum/number",
                json=payload,
                headers=get_stexsms_headers(token, sess)
            )
        return res.json()
    except Exception as e:
        logging.error(f"StexSMS get_number error: {e}")
        return {"error": str(e)}
    finally:
        await session_pool.return_number_session(session)

async def stexsms_check_otp(number):
    session = await session_pool.get_otp_session()
    try:
        token = session.get("token")
        sess = session.get("session")
        if not token:
            return {"error": "Login failed"}
        clean_search = number.replace("+", "").strip()
        today = datetime.now().strftime("%Y-%m-%d")
        params = {"date": today, "page": 1, "search": clean_search, "status": ""}
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{STEXSMS_BASE_URL}/mdashboard/getnum/info",
                params=params,
                headers=get_stexsms_headers(token, sess)
            )
        return res.json()
    except Exception as e:
        return {"error": str(e)}
    finally:
        await session_pool.return_otp_session(session)

# =============================================
#         X.MINT API FUNCTIONS
# =============================================

async def xmint_get_number(range_val, app_name="FACEBOOK"):
    clean_range = ''.join(c for c in range_val.upper() if c.isdigit() or c == 'X')
    if not clean_range:
        return {"error": "Invalid range"}
    
    payload = {
        "range": clean_range,
        "is_national": False,
        "remove_plus": True
    }
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            res = await client.post(
                f"{XMINT_BASE_URL}/mdashboard/getnum/number",
                json=payload,
                headers=get_xmint_headers()
            )
        return res.json()
    except Exception as e:
        logging.error(f"X.Mint get_number error: {e}")
        return {"error": str(e)}

async def xmint_check_otp(number):
    try:
        clean_search = number.replace("+", "").strip()
        today = datetime.now().strftime("%Y-%m-%d")
        params = {"date": today, "page": 1, "search": clean_search, "status": ""}
        async with httpx.AsyncClient(timeout=15) as client:
            res = await client.get(
                f"{XMINT_BASE_URL}/mdashboard/getnum/info",
                params=params,
                headers=get_xmint_headers()
            )
        return res.json()
    except Exception as e:
        logging.error(f"X.Mint check_otp error: {e}")
        return {"error": str(e)}

# =============================================
#         MENUS
# =============================================

def main_keyboard(user_id=None):
    buttons = [
        [KeyboardButton("🏠 Start"), KeyboardButton("🎯 Custom Range")],
        [KeyboardButton("📋 My Numbers"), KeyboardButton("📦 Bulk Number")],
        [KeyboardButton("👑 Admin Panel")]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)

def app_keyboard_dual():
    """S1 + S2 সহ app buttons"""
    buttons = []
    for app in ALL_APPS:
        emoji = APP_EMOJIS.get(app, "📱")
        buttons.append([
            InlineKeyboardButton(f"{emoji} {app} S1", callback_data=f"app_s1_{app}"),
            InlineKeyboardButton(f"{emoji} {app} S2", callback_data=f"app_s2_{app}")
        ])
    return InlineKeyboardMarkup(buttons)

def range_keyboard():
    buttons = [
        [InlineKeyboardButton("🎯 Custom Range", callback_data="custom_range")],
        [InlineKeyboardButton("❌ Cancel", callback_data="cancel")]
    ]
    return InlineKeyboardMarkup(buttons)

def admin_keyboard():
    buttons = [
        [InlineKeyboardButton("✅ Bulk ON", callback_data="bulk_on"), 
         InlineKeyboardButton("❌ Bulk OFF", callback_data="bulk_off")],
        [InlineKeyboardButton("👥 Users", callback_data="admin_users"), 
         InlineKeyboardButton("📊 Stats", callback_data="admin_stats")]
    ]
    return InlineKeyboardMarkup(buttons)

# =============================================
#         COMMANDS
# =============================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    user_name = user.first_name or "User"
    init_user(user_id)
    user_data[user_id]["name"] = user_name
    
    joined = await check_joined(user_id, context.bot)
    if not joined:
        await update.message.reply_text(
            "⚠️ Channel Join করুন!\n\nJoin করে /start দিন।",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Channel Join করুন", url=CHANNEL_LINK)
            ]])
        )
        return

    await update.message.reply_text(
        f"👋 Welcome back, {user_name}!\n\n"
        f"📱 NUMBER PANEL OTP BOT (Dual Panel)\n\n"
        f"🔗 API Status: ✅ Connected\n\n"
        f"📍 কিভাবে ব্যবহার করবেন:\n"
        f"Service → Select Panel (S1/S2) → Range → Number → OTP\n\n"
        f"🎯 নিচে service select করুন:",
        reply_markup=app_keyboard_dual()
    )

async def check_joined(user_id, bot):
    try:
        member = await bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except:
        return False

async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ বাতিল করা হয়েছে।", reply_markup=main_keyboard())

async def cmd_mynum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📋 Feature coming soon...", reply_markup=main_keyboard())

async def cmd_allusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        msg = f"👥 Total Users: {len(user_data)}\n\n"
        for uid, uinfo in list(user_data.items())[:15]:
            msg += f"• {uid}  —  {uinfo.get('name','?')}\n"
        await update.message.reply_text(msg)

async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id == ADMIN_ID:
        await update.message.reply_text(
            f"📊 BOT STATS\n\n"
            f"👥 Users: {len(user_data)}\n"
            f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        )

# =============================================
#         CALLBACK HANDLER
# =============================================

async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    data = query.data
    init_user(user_id)

    if data.startswith("app_s1_"):
        app = data.replace("app_s1_", "")
        user_data[user_id]["app"] = app
        user_data[user_id]["panel"] = "S1"
        await query.edit_message_text(
            f"📌 {app} (StexSMS - S1) selected\n\n"
            f"🎯 Range লিখুন (উদাহরণ: 880XXXXXX):",
            reply_markup=range_keyboard()
        )
        user_data[user_id]["waiting_for"] = "custom_range"

    elif data.startswith("app_s2_"):
        app = data.replace("app_s2_", "")
        user_data[user_id]["app"] = app
        user_data[user_id]["panel"] = "S2"
        await query.edit_message_text(
            f"📌 {app} (X.Mint - S2) selected\n\n"
            f"🎯 Range লিখুন (উদাহরণ: 225XXXXXX):",
            reply_markup=range_keyboard()
        )
        user_data[user_id]["waiting_for"] = "custom_range"

    elif data == "custom_range":
        await query.edit_message_text(
            "📡 Range লিখুন:\n\nউদাহরণ: 880XXXXXX বা 225XXXXXX",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel", callback_data="cancel")
            ]])
        )

    elif data == "cancel":
        await query.edit_message_text("❌ বাতিল করা হয়েছে।", reply_markup=InlineKeyboardMarkup([]))
        await query.message.reply_text("🏠 Main menu:", reply_markup=app_keyboard_dual())

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

    if waiting == "custom_range":
        user_data[user_id]["waiting_for"] = None
        user_data[user_id]["range"] = text
        
        panel = user_data[user_id].get("panel", "S1")
        app = user_data[user_id].get("app", "FACEBOOK")
        
        await update.message.reply_text(
            f"⏳ {panel} থেকে {app} number নেওয়া হচ্ছে...",
            reply_markup=main_keyboard(user_id)
        )
        
        if panel == "S1":
            result = await stexsms_get_number(text, app)
        else:
            result = await xmint_get_number(text, app)
        
        if result.get("meta", {}).get("code") == 200 or result.get("data"):
            number_data = result.get("data", {})
            number = number_data.get("number") or number_data.get("copy") or "N/A"
            country = number_data.get("country", "N/A")
            
            await update.message.reply_text(
                f"✅ Number পেয়েছি!\n\n"
                f"📞 Number: `{number}`\n"
                f"🌍 Country: {country}\n"
                f"📱 Panel: {panel}\n\n"
                f"🔍 OTP আসার অপেক্ষায়...",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"❌ Number পাওয়া যায়নি!\n\n"
                f"Error: {result.get('message', 'Unknown error')}",
                reply_markup=app_keyboard_dual()
            )
        return

    if text == "📋 My Numbers":
        await cmd_mynum(update, context)
        return

    if text == "👑 Admin Panel":
        if user_id == ADMIN_ID:
            await update.message.reply_text(
                f"👑 ADMIN PANEL\n\n"
                f"👥 Users: {len(user_data)}\n"
                f"📊 Status: Active",
                reply_markup=admin_keyboard()
            )
        else:
            await update.message.reply_text("❌ Admin access নেই।")
        return

# =============================================
#              MAIN
# =============================================

async def post_init(application):
    await session_pool.initialize()
    logging.info("✅ Dual Panel Bot initialized!")

async def post_shutdown(application):
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("✅ All tasks cancelled cleanly.")

if __name__ == "__main__":
    app = ApplicationBuilder().token(BOT_TOKEN).read_timeout(30).write_timeout(30).connect_timeout(30).post_init(post_init).post_shutdown(post_shutdown).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler("mynum", cmd_mynum))
    app.add_handler(CommandHandler("allusers", cmd_allusers))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CallbackQueryHandler(callback_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    print("✅ Dual Panel Bot is running...")
    app.run_polling(drop_pending_updates=True, timeout=30)

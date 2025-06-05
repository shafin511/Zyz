#!/usr/bin/env python3
"""
Telegram Referral Bot for Render Hosting
"""

import logging
import os
import asyncio
from datetime import datetime
from uuid import uuid4
import re
import sys

# If using .env for local development, uncomment and ensure python-dotenv is installed
# from dotenv import load_dotenv
# load_dotenv() # Call this before accessing os.getenv if you use .env locally

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Import telegram libraries
try:
    from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
    from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, CallbackQueryHandler
    from telegram.constants import ParseMode
    from telegram.error import TelegramError
    logger.info("Telegram libraries imported successfully")
except ImportError as e:
    logger.error(f"Failed to import telegram libraries: {e}")
    sys.exit(1)

# Import Supabase
try:
    from supabase import create_client, Client
    logger.info("Supabase library imported successfully")
except ImportError as e:
    logger.error(f"Failed to import Supabase: {e}")
    sys.exit(1)

# --- Configuration ---
# Values will be taken from environment variables if set, otherwise defaults are used.
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7838133414:AAGtEKtAw0wBZp4jwhpyhEivGm-68l2dmNI")
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://lqmkqgerzohlqvaibvqr.supabase.co")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxxbWtxZ2Vyem9obHF2YWlidnFyIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0ODA4NzM1MywiZXhwIjoyMDYzNjYzMzUzfQ.XNWoVTXGo32wprguG-U3oLHqnAelySFi2ITYLx26sJA")
BOT_USERNAME = os.getenv("BOT_USERNAME", "GetPaidBD_bot")
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "6809399141")
SUPPORT_USERNAME_CONFIG = os.getenv("SUPPORT_USERNAME", "ashfaqalshafin01") # Used in support_command

LOGO_URL = "https://ygpicvrjboljjzijfibg.supabase.co/storage/v1/object/public/giveme/1749142333775_1748699182505.webp"
REFER_IMAGE_URL = "https://ygpicvrjboljjzijfibg.supabase.co/storage/v1/object/public/giveme/1749142417533_refer-and-earn-concept-business-partnership-strategy-illustration-vector.jpg"

# Validate essential configuration (even with defaults, good to check if they are valid)
missing_configs = []
if not TELEGRAM_BOT_TOKEN: missing_configs.append("TELEGRAM_BOT_TOKEN (or its default)")
if not SUPABASE_URL: missing_configs.append("SUPABASE_URL (or its default)")
if not SUPABASE_SERVICE_KEY: missing_configs.append("SUPABASE_SERVICE_KEY (or its default)")
if not BOT_USERNAME: missing_configs.append("BOT_USERNAME (or its default)")
if not ADMIN_IDS_STR: missing_configs.append("ADMIN_IDS (or its default)")

if missing_configs:
    logger.error(f"CRITICAL ERROR: Missing configuration values for: {', '.join(missing_configs)}. Bot cannot start.")
    sys.exit(1)

try:
    ADMIN_IDS = [int(admin_id.strip()) for admin_id in ADMIN_IDS_STR.split(',') if admin_id.strip()]
    if not ADMIN_IDS:
        logger.error("CRITICAL ERROR: ADMIN_IDS string is set but resulted in an empty list (e.g., was it just a comma or empty?).")
        sys.exit(1)
    logger.info(f"Admin IDs loaded: {ADMIN_IDS}")
except ValueError:
    logger.error("CRITICAL ERROR: ADMIN_IDS string contains non-integer values.")
    sys.exit(1)
except Exception as e:
    logger.error(f"CRITICAL ERROR: Could not parse ADMIN_IDS: {e}")
    sys.exit(1)

JOINING_BONUS = 50
REFERRAL_BONUS = 10
MIN_WITHDRAWAL_AMOUNT = 500

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    supabase.table("users").select("id", count="exact").limit(1).execute()
    logger.info("Supabase connection established and tested successfully")
except Exception as e:
    logger.error(f"CRITICAL ERROR: Failed to connect to Supabase or test connection: {e}")
    logger.error("Please check your SUPABASE_URL and SUPABASE_SERVICE_KEY values (environment or defaults).")
    sys.exit(1)

ENTER_FULL_NAME, CHOOSE_METHOD, ENTER_NUMBER, ENTER_AMOUNT, CONFIRM_WITHDRAWAL = range(5)
BROADCAST_MESSAGE, CONFIRM_BROADCAST_SEND = range(5, 7)

# --- Database helper functions ---
def get_user(user_id: int):
    try:
        response = supabase.table("users").select("*").eq("id", user_id).maybe_single().execute()
        return response.data
    except Exception as e:
        logger.error(f"Error fetching user {user_id}: {e}", exc_info=True)
        return None

def create_user(user_id: int, name: str, referred_by: int = None):
    try:
        user_data = {
            "id": user_id, "name": name, "balance": JOINING_BONUS,
            "ref_by": referred_by, "referrals": 0, "withdraws": 0
        }
        response = supabase.table("users").insert(user_data).execute()
        if response.data:
            logger.info(f"Created new user {user_id} ({name})")
            return response.data[0]
        return None
    except Exception as e:
        if hasattr(e, 'code') and e.code == '23505': 
             logger.warning(f"User {user_id} already exists (Postgres code 23505).")
             return get_user(user_id)
        if (hasattr(e, 'message') and "duplicate key value violates unique constraint" in e.message) or \
           ("duplicate key value violates unique constraint" in str(e)):
            logger.warning(f"User {user_id} already exists.")
            return get_user(user_id)
        logger.error(f"Error creating user {user_id}: {e}", exc_info=True)
        return None

def update_user_balance(user_id: int, amount_change: int, operation: str = "add"):
    try:
        user = get_user(user_id)
        if not user:
            logger.warning(f"Balance update attempt for non-existent user {user_id}")
            return False
        current_balance = user.get('balance', 0)
        if operation == "add": new_balance = current_balance + amount_change
        elif operation == "subtract":
            if current_balance < amount_change: return "insufficient_funds"
            new_balance = current_balance - amount_change
        else:
            logger.error(f"Invalid balance operation '{operation}' for user {user_id}")
            return False
        supabase.table("users").update({"balance": new_balance}).eq("id", user_id).execute()
        logger.info(f"User {user_id} balance: {current_balance}৳ -> {new_balance}৳ ({operation})")
        return True
    except Exception as e:
        logger.error(f"Error updating balance for user {user_id}: {e}", exc_info=True)
        return False

def increment_referral_count(user_id: int):
    try:
        user = get_user(user_id)
        if user:
            new_count = user.get('referrals', 0) + 1
            supabase.table("users").update({"referrals": new_count}).eq("id", user_id).execute()
            logger.info(f"User {user_id} referral count updated to {new_count}")
    except Exception as e:
        logger.error(f"Error incrementing referral count for user {user_id}: {e}", exc_info=True)

def record_withdrawal(user_id: int, full_name: str, amount: int, method: str, account_number: str):
    try:
        wd_data = {
            "user_id": user_id, "full_name": full_name, "amount": amount,
            "method": method, "account_number": account_number,
            "status": "pending", "request_id": str(uuid4())
        }
        response = supabase.table("withdrawals").insert(wd_data).execute()
        if response.data:
            logger.info(f"Withdrawal recorded: User {user_id}, Amt {amount}৳, ID: {wd_data['request_id']}")
            return response.data[0]
        return None
    except Exception as e:
        logger.error(f"Error recording withdrawal: {e}", exc_info=True)
        return None

def get_all_user_ids():
    try:
        response = supabase.table("users").select("id").execute()
        return [user['id'] for user in response.data] if response.data else []
    except Exception as e:
        logger.error(f"Error fetching all user IDs: {e}", exc_info=True)
        return []

# --- Keyboard and Menu Functions ---
def main_menu_keyboard():
    keyboard = [
        [KeyboardButton("💰 My Balance"), KeyboardButton("🔗 Refer a Friend")],
        [KeyboardButton("💸 Withdraw Funds"), KeyboardButton("📊 My Stats")],
        [KeyboardButton("📋 Rules & Terms"), KeyboardButton("📋 Withdraw History")],
        [KeyboardButton("📋 Withdraw Guide"), KeyboardButton("📞 Support")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

async def send_main_menu_text(chat_id: int, context: ContextTypes.DEFAULT_TYPE, message_text: str):
    """Helper to send a text-based main menu, typically after an action."""
    try:
        await context.bot.send_message(
            chat_id=chat_id,
            text=message_text,
            reply_markup=main_menu_keyboard(),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error in send_main_menu_text to {chat_id}: {e}")


# --- Command Handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = user.id
    name = user.full_name
    existing_user = get_user(user_id)
    
    welcome_caption = f"🌟 <b>আসসালামু আলাইকুম, {name}!</b>\n\n" # Using user's name

    if existing_user:
        balance = existing_user.get('balance', 0)
        referrals = existing_user.get('referrals', 0)
        welcome_caption += (
            f"🔥 <b>ReferEarnBD-তে স্বাগতম ফিরে!</b>\n\n"
            f"💰 আপনার বর্তমান ব্যালেন্স: <b>{balance}৳</b>\n"
            f"👥 আপনি রেফার করেছেন: <b>{referrals} জন</b>\n\n"
            f"✨ আরো বেশি রেফার করে আপনার আয় বাড়ান!"
        )
    else:
        referred_by_id = None
        if context.args and len(context.args) > 0:
            try:
                p_ref_id_str = context.args[0]
                if p_ref_id_str.isdigit():
                    p_ref_id = int(p_ref_id_str)
                    if p_ref_id != user_id:
                        if get_user(p_ref_id): referred_by_id = p_ref_id
                        else: logger.warning(f"Referrer ID {p_ref_id} not found for user {user_id}.")
                    else: logger.warning(f"User {user_id} tried to refer self.")
                else: logger.warning(f"Invalid ref ID format: {p_ref_id_str}")
            except Exception as e: logger.error(f"Error processing ref ID: {e}", exc_info=True)

        new_user = create_user(user_id, name, referred_by_id)
        if new_user:
            welcome_caption += (
                f"💎 <b>ReferEarnBD-তে আপনাকে স্বাগতম! 🎉</b>\n\n"
                f"🎁 <b>বিশেষ জয়েনিং বোনাস: {JOINING_BONUS}৳</b> ✅\n\n"
                f"🚀 <b>আয় শুরু করুন:</b>\n"
                f"• বন্ধুদের আমন্ত্রণ জানান\n"
                f"• প্রতি রেফারেলে <b>{REFERRAL_BONUS}৳</b>\n"
                f"• <b>{MIN_WITHDRAWAL_AMOUNT}৳</b> হলেই টাকা তুলুন!\n\n"
            )
            if referred_by_id:
                update_user_balance(referred_by_id, REFERRAL_BONUS, "add")
                increment_referral_count(referred_by_id)
                ref_data = get_user(referred_by_id)
                r_name = f"User ID <code>{referred_by_id}</code>"
                if ref_data and ref_data.get('name'): r_name = ref_data.get('name')
                welcome_caption += f"🎯 আপনি {r_name} এর মাধ্যমে জয়েন করেছেন। রেফারার <b>{REFERRAL_BONUS}৳</b> বোনাস পেয়েছেন! 🙏\n\n"
        else:
            await update.message.reply_text("⚠️ Account creation failed. Try /start or contact support.")
            return
    
    try:
        await update.message.reply_photo(photo=LOGO_URL, caption=welcome_caption, parse_mode=ParseMode.HTML)
    except TelegramError as e:
        logger.error(f"Failed to send start photo: {e}. Sending text only.")
        await update.message.reply_text(welcome_caption, parse_mode=ParseMode.HTML)

    await update.message.reply_text("📌 Main Menu:", reply_markup=main_menu_keyboard(), parse_mode=ParseMode.HTML)


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    if user_data:
        bal = user_data.get('balance', 0); refs = user_data.get('referrals', 0)
        needed_amt = max(0, MIN_WITHDRAWAL_AMOUNT - bal)
        needed_refs = (needed_amt + REFERRAL_BONUS - 1) // REFERRAL_BONUS if REFERRAL_BONUS > 0 and needed_amt > 0 else 0
        
        msg = (f"💎 <b>Account Overview</b>\n\n💰 <b>Balance:</b> <code>{bal}৳</code>\n👥 <b>Referrals:</b> <code>{refs}</code>\n\n")
        if bal >= MIN_WITHDRAWAL_AMOUNT:
            msg += f"🎉 <b>Congrats!</b> You can withdraw.\n💸 Max: <b>{bal}৳</b>\n\n📌 Click '💸 Withdraw Funds'."
        else:
            msg += f"🎯 <b>To Withdraw:</b>\n💵 Need: <b>{needed_amt}৳</b>\n"
            if REFERRAL_BONUS > 0: msg += f"👨‍👩‍👧‍👦 Refer: Approx. <b>{needed_refs}</b>\n\n"
            else: msg += "\n"
            msg += f"🚀 Earn <b>{REFERRAL_BONUS}৳</b> per referral!"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    else: await update.message.reply_text("❌ User data not found. Use /start.")


async def refer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not BOT_USERNAME:
        logger.error("BOT_USERNAME not set for referral link.")
        await update.message.reply_text("❌ Error: Cannot generate referral link.")
        return
    link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    user_data = get_user(user_id)
    
    caption = ""
    if user_data:
        refs = user_data.get('referrals', 0)
        earned = refs * REFERRAL_BONUS
        caption = (
            f"🔥 <b>রেফার করে আয় করুন!</b>\n\n"
            f"💰 <b>প্রতি রেফারেলে: {REFERRAL_BONUS}৳</b>\n"
            f"👥 <b>মোট রেফারেল: {refs} জন</b>\n"
            f"💵 <b>মোট আয়: {earned}৳</b>\n\n"
            f"🎯 <b>আপনার রেফারেল লিঙ্ক:</b>\n<code>{link}</code>\n(ক্লিক করলে কপি হবে)\n\n"
            f"📱 বন্ধুদের শেয়ার করুন আর আয় বাড়ান!"
        )
    else:
        caption = (
            f"🔗 <b>রেফার করে আয় করুন!</b>\n\n"
            f"আপনার লিঙ্ক: <code>https://t.me/{BOT_USERNAME}?start={user_id}</code>\n\n"
            f"প্রতি রেফারেলে <b>{REFERRAL_BONUS}৳</b>!"
        )
    
    try:
        await update.message.reply_photo(photo=REFER_IMAGE_URL, caption=caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except TelegramError as e:
        logger.error(f"Failed to send refer photo: {e}. Sending text.")
        await update.message.reply_text(caption, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    if user_data:
        bal, refs = user_data.get('balance', 0), user_data.get('referrals', 0)
        total_wd = 0
        try:
            resp = supabase.table("withdrawals").select("amount").eq("user_id", user_data['id']).eq("status", "approved").execute()
            if resp.data: total_wd = sum(item['amount'] for item in resp.data)
        except Exception as e: logger.error(f"Error fetching withdrawn for {user_data['id']}: {e}")
        
        ref_earn = refs * REFERRAL_BONUS
        prog_stat = "✅ <b>অভিনন্দন! টাকা তুলতে পারবেন!</b>" if bal >= MIN_WITHDRAWAL_AMOUNT else \
                    f"🎯 টাকা তুলতে আরো <b>{MIN_WITHDRAWAL_AMOUNT - bal}৳</b> প্রয়োজন" + \
                    (f" (আনুমানিক <b>{( (MIN_WITHDRAWAL_AMOUNT - bal) + REFERRAL_BONUS - 1) // REFERRAL_BONUS if REFERRAL_BONUS > 0 else 0}</b> রেফার)" if REFERRAL_BONUS > 0 else "")
        
        msg = (f"📈 <b>পারফরম্যান্স রিপোর্ট</b>\n\n💎 <b>সামারি:</b>\n┣ 💰 ব্যালেন্স: <code>{bal}৳</code>\n"
               f"┣ 👥 রেফারেল: <code>{refs} জন</code>\n┣ 💵 রেফার আয়: <code>{ref_earn}৳</code>\n"
               f"┗ 💸 মোট উইথড্র: <code>{total_wd}৳</code>\n\n🎯 <b>উইথড্র স্ট্যাটাস:</b>\n{prog_stat}\n\n")
        if REFERRAL_BONUS > 0: msg += f"📊 <b>আয়ের সম্ভাবনা ({REFERRAL_BONUS}৳/রেফার):</b> ...\n\n" # Simplified
        msg += "🚀 যত বেশি রেফার, তত বেশি আয়!"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    else: await update.message.reply_text("❌ Stats unavailable. /start first.")

async def support_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    support_user = SUPPORT_USERNAME_CONFIG # Use the globally configured one
    msg = (f"📞 <b>সাপোর্ট</b>\n\n📱 <b>Telegram:</b> @{support_user}\n"
           f"🕒 সময়: সকাল ৯টা - রাত ১০টা (শনি-বৃহঃ)\n\n"
           f"⚡ User ID <code>{update.effective_user.id}</code> সহ জানান।")
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def rules_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rules = (f"📋 <b>নিয়মাবলী</b>\n\n🎯 <b>রেফারেল: {REFERRAL_BONUS}৳/সফল রেফার...\n"
             f"💸 <b>উইথড্র:</b> সর্বনিম্ন {MIN_WITHDRAWAL_AMOUNT}৳...\n...") # Simplified
    await update.message.reply_text(rules, parse_mode=ParseMode.HTML)

async def withdrawal_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        resp = supabase.table("withdrawals").select("*").eq("user_id", user_id).order("requested_at", desc=True).limit(10).execute()
        wds = resp.data
        if not wds:
            await update.message.reply_text("📋 No withdrawal history.")
            return
        hist_txt = "📋 <b>উইথড্র ইতিহাস (১০টি)</b>\n\n"
        for r in wds:
            s_emoji = {"pending":"⏳","approved":"✅","rejected":"❌"}.get(r['status'],"❓")
            s_txt = {"pending":"অপেক্ষমাণ","approved":"অনুমোদিত","rejected":"বাতিল"}.get(r['status'],r['status'].capitalize())
            req_at = datetime.fromisoformat(str(r['requested_at']).replace('Z','+00:00')).strftime("%d%b%y %I:%M%p") if r.get('requested_at') else "N/A"
            hist_txt += (f"<b>ID:</b><code>{r.get('request_id','N/A')[:8]}</code> {s_emoji}{s_txt}\n"
                         f"<b>Amt:</b>{r.get('amount','N/A')}৳ <b>To:</b><code>{r.get('account_number','N/A')}</code> ({r.get('method','').title()})\n"
                         f"<b>Date:</b>{req_at}\n---\n")
        await update.message.reply_text(hist_txt, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Error fetching WD history for {user_id}: {e}")
        await update.message.reply_text("❌ Error loading history.")

async def withdrawal_guide_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    guide = (f"📋 <b>উইথড্র গাইড</b>\n\n<b>ধাপ ১:</b> ব্যালেন্স চেক ({MIN_WITHDRAWAL_AMOUNT}৳ Minimum)...\n...") # Simplified
    await update.message.reply_text(guide, parse_mode=ParseMode.HTML)

async def send_withdrawal_status_update_to_user(app: Application, user_id: int, status: str, amount: int, req_id: str, reason: str = ""):
    msg = ""
    if status.lower() == "approved": msg = f"🎉 অভিনন্দন! আপনার {amount}৳ উইথড্র ({req_id}) অনুমোদিত হয়েছে।"
    elif status.lower() == "rejected": msg = f"⚠️ দুঃখিত, আপনার {amount}৳ উইথড্র ({req_id}) বাতিল হয়েছে। কারণ: {reason or 'N/A'}"
    elif status.lower() == "completed": msg = f"💸 আপনার {amount}৳ উইথড্র ({req_id}) সম্পন্ন হয়েছে!"
    else: return False
    try:
        await app.bot.send_message(user_id, msg, parse_mode=ParseMode.HTML)
        logger.info(f"WD status '{status}' update to {user_id} for {req_id}")
        return True
    except Exception as e:
        logger.error(f"Error sending WD status to {user_id}: {e}")
        return False

# --- Admin Functions ---
def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user and update.effective_user.id in ADMIN_IDS:
            return await func(update, context, *args, **kwargs)
        if update.message: await update.message.reply_text("❌ Admin only.")
        elif update.callback_query: await update.callback_query.answer("❌ Admin only.", show_alert=True)
        return ConversationHandler.END # Important for ConversationHandler fallbacks
    return wrapper

@admin_only
async def start_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📢 <b>Broadcast:</b> Send message (text/photo/video).\n/cancel to abort.", parse_mode=ParseMode.HTML)
    return BROADCAST_MESSAGE

async def receive_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    txt, p_id, v_id, cap, ent, cap_ent = None,None,None,None,None,None # text, photo_id, video_id, caption, entities, caption_entities
    if msg.text: txt, ent = msg.text, msg.entities
    elif msg.photo: p_id, cap, cap_ent = msg.photo[-1].file_id, msg.caption, msg.caption_entities
    elif msg.video: v_id, cap, cap_ent = msg.video.file_id, msg.caption, msg.caption_entities
    else:
        await msg.reply_text("❌ Unsupported type for broadcast. Use text, photo, or video. /cancel to retry.")
        return BROADCAST_MESSAGE
    
    context.user_data.update({'bt':txt,'bpi':p_id,'bvi':v_id,'bc':cap,'be':ent,'bce':cap_ent})
    kbd = [[InlineKeyboardButton("✅ Send Broadcast",cb_data="confirm_bcast"), InlineKeyboardButton("❌ Cancel Broadcast",cb_data="cancel_bcast")]]
    reply_markup=InlineKeyboardMarkup(kbd)
    preview_header = "📢 <b>Broadcast Preview:</b>\n\nDo you want to send this message to all users?\n-------------------------------------\n"
    
    if txt: await msg.reply_text(preview_header+txt, entities=ent, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    elif p_id: await msg.reply_photo(p_id,caption=preview_header+(cap or ""),caption_entities=cap_ent,reply_markup=reply_markup,parse_mode=ParseMode.HTML)
    elif v_id: await msg.reply_video(v_id,caption=preview_header+(cap or ""),caption_entities=cap_ent,reply_markup=reply_markup,parse_mode=ParseMode.HTML)
    return CONFIRM_BROADCAST_SEND

async def confirm_broadcast_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    if query.data == "cancel_bcast":
        await query.edit_message_text("❌ Broadcast cancelled.")
        context.user_data.clear(); return ConversationHandler.END
    
    await query.edit_message_text("🚀 Sending broadcast... This may take a while.")
    all_uids = get_all_user_ids()
    if not all_uids:
        await context.bot.send_message(query.from_user.id, "No users found to broadcast the message to.")
        context.user_data.clear(); return ConversationHandler.END
        
    ud = context.user_data # Shorthand for user_data
    txt,p_id,v_id,cap,ent,cap_ent = ud.get('bt'),ud.get('bpi'),ud.get('bvi'),ud.get('bc'),ud.get('be'),ud.get('bce')
    sent_count,failed_count,blocked_count = 0,0,0; delay_between_msgs = 0.1 # 10 messages per second max

    for user_id_target in all_uids:
        try:
            if txt: await context.bot.send_message(user_id_target,txt,entities=ent,parse_mode=None if ent else ParseMode.HTML)
            elif p_id: await context.bot.send_photo(user_id_target,p_id,caption=cap,caption_entities=cap_ent,parse_mode=None if cap_ent else ParseMode.HTML)
            elif v_id: await context.bot.send_video(user_id_target,v_id,caption=cap,caption_entities=cap_ent,parse_mode=None if cap_ent else ParseMode.HTML)
            sent_count+=1
        except TelegramError as e:
            if "blocked" in str(e).lower() or "deactivated" in str(e).lower() or "not found" in str(e).lower(): blocked_count+=1
            else: failed_count+=1; logger.error(f"Broadcast TelegramError to {user_id_target}: {e}")
        except Exception as e: failed_count+=1; logger.error(f"Broadcast Exception to {user_id_target}: {e}",exc_info=True)
        finally: await asyncio.sleep(delay_between_msgs) # Rate limiting
            
    summary_msg = (f"✅ <b>Broadcast Complete!</b>\n\n"
                   f"🎯 Targeted Users: {len(all_uids)}\n"
                   f"✔️ Successfully Sent: {sent_count}\n"
                   f"❌ Failed to Send: {failed_count}\n"
                   f"🚫 Blocked/Deactivated/Not Found: {blocked_count}")
    await context.bot.send_message(query.from_user.id, summary_msg, parse_mode=ParseMode.HTML)
    context.user_data.clear(); return ConversationHandler.END

async def broadcast_cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE): # Renamed to avoid conflict
    await update.message.reply_text("❌ Broadcast process cancelled by command.")
    context.user_data.clear(); return ConversationHandler.END

# --- Withdrawal Conversation Handlers ---
async def start_withdrawal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = get_user(update.effective_user.id)
    if not user_data:
        await update.message.reply_text("User not found. Please /start first.")
        return ConversationHandler.END
    bal = user_data.get('balance', 0)
    if bal < MIN_WITHDRAWAL_AMOUNT:
        await update.message.reply_text(f"❌ আপনার ব্যালেন্স কম! ({bal}৳), টাকা তুলতে আরো {MIN_WITHDRAWAL_AMOUNT - bal}৳ প্রয়োজন।",parse_mode=ParseMode.HTML)
        return ConversationHandler.END
    context.user_data.clear() # Fresh start for withdrawal
    await update.message.reply_text(f"💸 উইথড্র প্রক্রিয়া শুরু হচ্ছে...\n\nআপনার <b>পূর্ণ নাম</b> লিখুন (যেমনটি মোবাইল ব্যাংকিং একাউন্টে আছে):\n\nবাতিল করতে /cancelwithdrawal টাইপ করুন.",parse_mode=ParseMode.HTML,
                                   reply_markup=ReplyKeyboardMarkup([["/cancelwithdrawal"]],resize_keyboard=True,one_time_keyboard=True))
    return ENTER_FULL_NAME

async def enter_full_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not (3 <= len(name) <= 60 and re.match(r'^[a-zA-Z\u0980-\u09FF\s.\'-]+$', name)): # Allows Bangla, Eng, space, dot, hyphen, apostrophe
        await update.message.reply_text("❌ অবৈধ নাম। অনুগ্রহ করে ৩-৬০ অক্ষরের মধ্যে সঠিক নাম লিখুন।",parse_mode=ParseMode.HTML)
        return ENTER_FULL_NAME
    context.user_data['wd_name'] = name
    kbd = [[InlineKeyboardButton("📱 বিকাশ",cb_data="wdm_bkash"),InlineKeyboardButton("💳 নগদ",cb_data="wdm_nagad"), InlineKeyboardButton("🚀 রকেট",cb_data="wdm_rocket")],[InlineKeyboardButton("❌ বাতিল করুন",cb_data="wd_cancel_conv")]]
    await update.message.reply_text(f"✅ আপনার নাম:<b>{name}</b>।\n\nএবার আপনার পেমেন্ট পদ্ধতি বেছে নিন:",reply_markup=InlineKeyboardMarkup(kbd),parse_mode=ParseMode.HTML)
    return CHOOSE_METHOD

async def choose_payment_method_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer()
    if query.data == "wd_cancel_conv":
        await query.edit_message_text("❌ উইথড্র প্রক্রিয়া বাতিল করা হয়েছে।")
        await send_main_menu_text(query.from_user.id, context, "প্রধান মেনু।")
        context.user_data.clear(); return ConversationHandler.END
    
    method = query.data.split('_')[-1] # e.g. wdm_bkash -> bkash
    context.user_data['wd_method'] = method
    method_display_names = {"bkash":"বিকাশ","nagad":"নগদ","rocket":"রকেট"}
    await query.edit_message_text(f"✅ আপনি <b>{method_display_names.get(method, method.title())}</b> বেছে নিয়েছেন।\n\nআপনার <b>{method_display_names.get(method, method.title())} একাউন্ট নম্বর</b> লিখুন:",parse_mode=ParseMode.HTML)
    return ENTER_NUMBER

async def enter_account_number(update: Update, context: ContextTypes.DEFAULT_TYPE):
    acc_num = update.message.text.strip()
    # Basic validation for Bangladeshi mobile numbers (11 digits, or 12 for Rocket)
    if not re.match(r'^01[3-9]\d{8}(\d?)$', acc_num): # Allows 11 or 12 digits (last digit optional)
        await update.message.reply_text("❌ ভুল নম্বর ফরম্যাট। ১১ বা ১২ ডিজিটের সঠিক মোবাইল নম্বর দিন।",parse_mode=ParseMode.HTML)
        return ENTER_NUMBER
    context.user_data['wd_acc_num'] = acc_num
    current_balance = get_user(update.effective_user.id).get('balance',0) # Get current balance
    await update.message.reply_text(f"✅ আপনার একাউন্ট নম্বর:<code>{acc_num}</code>।\n\nকত টাকা তুলতে চান? (সর্বনিম্ন:{MIN_WITHDRAWAL_AMOUNT}৳, আপনার আছে:{current_balance}৳)",parse_mode=ParseMode.HTML)
    return ENTER_AMOUNT

async def enter_withdrawal_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try: amt_requested = int(update.message.text.strip())
    except ValueError: 
        await update.message.reply_text("❌ অবৈধ পরিমাণ। সংখ্যায় লিখুন।",parse_mode=ParseMode.HTML); return ENTER_AMOUNT
    
    current_balance = get_user(update.effective_user.id).get('balance',0)
    if not (MIN_WITHDRAWAL_AMOUNT <= amt_requested <= current_balance and amt_requested > 0):
        err_text = "❌ পরিমাণ সঠিক নয়।\n"
        if amt_requested < MIN_WITHDRAWAL_AMOUNT: err_text += f"সর্বনিম্ন {MIN_WITHDRAWAL_AMOUNT}৳ তুলতে হবে।\n"
        if amt_requested > current_balance: err_text += f"আপনার ব্যালেন্সের ({current_balance}৳) চেয়ে বেশি তুলতে পারবেন না।\n"
        if amt_requested <= 0 : err_text += "টাকার পরিমাণ অবশ্যই শূন্যের বেশি হতে হবে।"
        await update.message.reply_text(err_text,parse_mode=ParseMode.HTML); return ENTER_AMOUNT
    
    context.user_data['wd_amount'] = amt_requested
    ud = context.user_data # shorthand
    method_display_names = {"bkash":"বিকাশ","nagad":"নগদ","rocket":"রকেট"}
    confirm_text = (f"🔍 <b>আপনার তথ্য যাচাই করুন:</b>\n\n"
                    f"👤 নাম: {ud['wd_name']}\n"
                    f"📱 পেমেন্ট পদ্ধতি: {method_display_names.get(ud['wd_method'], ud['wd_method'].title())}\n"
                    f"📞 একাউন্ট নম্বর: <code>{ud['wd_acc_num']}</code>\n"
                    f"💰 টাকার পরিমাণ: <b>{amt_requested}৳</b>\n\n"
                    f"⚠️ <b>গুরুত্বপূর্ণ:</b> উপরের তথ্যগুলো সঠিক কি? ভুল তথ্যের জন্য পেমেন্ট সমস্যা হতে পারে।")
    kbd = [[InlineKeyboardButton("✅ নিশ্চিত করুন এবং রিকোয়েস্ট পাঠান",cb_data="wd_final_confirm")],[InlineKeyboardButton("❌ বাতিল করুন",cb_data="wd_cancel_conv")]]
    await update.message.reply_text(confirm_text,reply_markup=InlineKeyboardMarkup(kbd),parse_mode=ParseMode.HTML)
    return CONFIRM_WITHDRAWAL

async def confirm_withdrawal_request_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; await query.answer(); user_id = query.from_user.id
    if query.data == "wd_cancel_conv":
        await query.edit_message_text("❌ উইথড্র প্রক্রিয়া বাতিল করা হয়েছে।")
        await send_main_menu_text(user_id, context, "প্রধান মেনু।")
        context.user_data.clear(); return ConversationHandler.END

    ud = context.user_data
    name,method,acc,amt = ud.get('wd_name'),ud.get('wd_method'),ud.get('wd_acc_num'),ud.get('wd_amount')
    if not all([name,method,acc,amt is not None]):
        logger.error(f"Withdrawal Confirm Error: User {user_id}, missing data in context: {ud}")
        await query.edit_message_text("❌ একটি অভ্যন্তরীণ ত্রুটি ঘটেছে (WDC-MDATA)। অনুগ্রহ করে সাপোর্টে যোগাযোগ করুন।"); 
        context.user_data.clear(); return ConversationHandler.END
    
    # Re-check balance just before processing
    user_current_data = get_user(user_id)
    if not user_current_data or user_current_data.get('balance',0) < amt:
        await query.edit_message_text("❌ দুঃখিত, আপনার ব্যালেন্স এখন এই পরিমাণ টাকা তোলার জন্য পর্যাপ্ত নয় (WDC-LBAL)।"); 
        context.user_data.clear(); return ConversationHandler.END

    balance_update_result = update_user_balance(user_id, amt, "subtract")
    if balance_update_result != True:
        err_msg = "❌ ব্যালেন্স আপডেট করতে সমস্যা হয়েছে (WDC-UBAL)।"
        if balance_update_result == "insufficient_funds": err_msg = "❌ অপর্যাপ্ত ব্যালেন্স (WDC-IFUND)।" # Should be caught above
        await query.edit_message_text(err_msg); 
        context.user_data.clear(); return ConversationHandler.END

    withdrawal_record = record_withdrawal(user_id,name,amt,method,acc)
    if withdrawal_record and withdrawal_record.get('request_id'):
        req_id = withdrawal_record['request_id']
        await query.edit_message_text(f"✅ আপনার উইথড্র রিকোয়েস্ট (ID: <code>{req_id}</code>) সফলভাবে জমা হয়েছে। এটি ২৪-৭২ ঘন্টার মধ্যে প্রসেস করা হবে।",parse_mode=ParseMode.HTML)
        # Admin Notification
        admin_notification = f"🔔 নতুন উইথড্র রিকোয়েস্ট!\nUser: {name} (<code>{user_id}</code>)\nAmount: {amt}৳\nMethod: {method.title()}\nAcc: <code>{acc}</code>\nReq ID: <code>{req_id}</code>"
        for admin_tg_id in ADMIN_IDS:
            try: await context.bot.send_message(admin_tg_id, admin_notification, parse_mode=ParseMode.HTML)
            except Exception as e_admin: logger.error(f"Failed to notify admin {admin_tg_id}: {e_admin}")
    else: # Withdrawal recording failed, CRITICAL: REFUND BALANCE
        logger.error(f"CRITICAL: Balance for user {user_id} (amount {amt}) was deducted BUT withdrawal recording FAILED. Attempting to refund.")
        refund_success = update_user_balance(user_id, amt, "add") # Attempt to refund
        refund_msg = "আপনার টাকা আপনার মূল ব্যালেন্সে ফেরত দেওয়া হয়েছে।" if refund_success else "ব্যালেন্স স্বয়ংক্রিয়ভাবে ফেরত দিতেও সমস্যা হয়েছে! অবিলম্বে সাপোর্টে যোগাযোগ করুন।"
        await query.edit_message_text(f"❌ উইথড্র রিকোয়েস্ট জমা দিতে গুরুতর সমস্যা হয়েছে (WDC-RECFAIL)।\n{refund_msg}",parse_mode=ParseMode.HTML)
    
    context.user_data.clear()
    await send_main_menu_text(user_id, context, "আপনার উইথড্র রিকোয়েস্ট প্রসেস করা হয়েছে।") # Follow-up message with menu
    return ConversationHandler.END

async def cancel_withdrawal_command(update: Update, context: ContextTypes.DEFAULT_TYPE): # Renamed to avoid conflict
    await update.message.reply_text("❌ উইথড্র প্রক্রিয়া বাতিল করা হয়েছে।", reply_markup=main_menu_keyboard())
    context.user_data.clear(); return ConversationHandler.END

# --- General Message Handler & Error Handler ---
async def handle_general_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text: return # Ignore non-text messages
    txt = update.message.text.strip()
    # Check main menu button presses
    if txt == "💰 My Balance": await balance_command(update, context)
    elif txt == "🔗 Refer a Friend": await refer_command(update, context)
    elif txt == "📊 My Stats": await stats_command(update, context)
    elif txt == "📞 Support": await support_command(update, context)
    elif txt == "📋 Rules & Terms": await rules_command(update, context)
    elif txt == "📋 Withdraw History": await withdrawal_history_command(update, context)
    elif txt == "📋 Withdraw Guide": await withdrawal_guide_command(update, context)
    # "💸 Withdraw Funds" is handled by its ConversationHandler entry point
    else:
        # If text doesn't match any known command or button, send main menu
        await update.message.reply_text("Sorry, I didn't understand that. Please use the buttons from the menu.", reply_markup=main_menu_keyboard())


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update: {update}\nError: {context.error}", exc_info=context.error)

# --- Main Bot Execution ---
def main():
    try:
        if not TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN.split(':')) != 2:
            logger.critical("TELEGRAM_BOT_TOKEN seems invalid or is missing. Bot cannot start.")
            sys.exit(1)
        
        app_builder = Application.builder().token(TELEGRAM_BOT_TOKEN)
        
        # Optional: Configure HTTPX client for more control over bot's outgoing requests
        # import httpx
        # custom_httpx_client = httpx.AsyncClient(
        #     timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=5.0), # connect, read, write, pool timeouts
        #     limits=httpx.Limits(max_connections=100, max_keepalive_connections=20) # Example limits
        # )
        # app_builder.httpx_client(custom_httpx_client)

        application = app_builder.build()

        # Withdrawal Conversation Handler
        wd_conv = ConversationHandler(
            entry_points=[MessageHandler(filters.Regex("^💸 Withdraw Funds$"), start_withdrawal)],
            states={
                ENTER_FULL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_full_name)],
                CHOOSE_METHOD: [CallbackQueryHandler(choose_payment_method_callback, pattern="^wdm_.*$|^wd_cancel_conv$")],
                ENTER_NUMBER: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_account_number)],
                ENTER_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_withdrawal_amount)],
                CONFIRM_WITHDRAWAL: [CallbackQueryHandler(confirm_withdrawal_request_callback, pattern="^wd_final_confirm$|^wd_cancel_conv$")]
            },
            fallbacks=[CommandHandler("cancelwithdrawal", cancel_withdrawal_command),
                       CallbackQueryHandler(cancel_withdrawal_command, pattern="^wd_cancel_conv$"), # If user clicks cancel during CBQ
                       MessageHandler(filters.Regex("^(💰 My Balance|🔗 Refer a Friend|📊 My Stats|📞 Support|📋 Rules & Terms|📋 Withdraw History|📋 Withdraw Guide)$"), cancel_withdrawal_command),
                       MessageHandler(filters.COMMAND, cancel_withdrawal_command) # Any other command during convo
                      ],
            per_message=False, allow_reentry=True
        )
        
        # Broadcast Conversation Handler
        bc_conv = ConversationHandler(
            entry_points=[CommandHandler("broadcast", start_broadcast)], # admin_only is applied to start_broadcast
            states={
                BROADCAST_MESSAGE: [MessageHandler(filters.TEXT|filters.PHOTO|filters.VIDEO & ~filters.COMMAND, receive_broadcast_message)],
                CONFIRM_BROADCAST_SEND: [CallbackQueryHandler(confirm_broadcast_send_callback, pattern="^confirm_bcast$|^cancel_bcast$")]
            },
            fallbacks=[CommandHandler("cancel", broadcast_cancel_command), # Admin can type /cancel
                       CallbackQueryHandler(broadcast_cancel_command, pattern="^cancel_bcast$") # Admin clicks cancel button
                      ],
            per_message=False, allow_reentry=True
        )
        
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(wd_conv)
        application.add_handler(bc_conv)
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_general_messages))
        application.add_error_handler(error_handler)

        logger.info("Bot starting polling...")
        application.run_polling(
            allowed_updates=Update.ALL_TYPES, 
            drop_pending_updates=True, 
            timeout=60 # Long polling timeout
        )
    except Exception as e:
        logger.critical(f"CRITICAL ERROR in main bot setup or run_polling: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    logger.info("--- Starting Telegram Referral Bot ---")
    # If you use .env locally for testing, ensure these lines are active:
    # from dotenv import load_dotenv
    # load_dotenv()
    main()
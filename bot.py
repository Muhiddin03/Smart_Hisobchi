import logging
import os
import re
import asyncio
import io
import random
import psycopg2
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from fpdf import FPDF

# ───────────────────────────────────────────
# SOZLAMALAR
# ───────────────────────────────────────────
API_TOKEN = os.environ.get('API_TOKEN', '8435215607:AAHaJ3guIwMJimnMoTBAzMaPBCBb2ShYeEw')
ADMIN_ID  = int(os.environ.get('ADMIN_ID', '1680174090'))
DATABASE_URL = os.environ.get('DATABASE_URL')

logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp  = Dispatcher(bot, storage=MemoryStorage())

UZB = timezone(timedelta(hours=5))

def now_uzb():
    return datetime.now(UZB)

# ───────────────────────────────────────────
# POSTGRESQL — ULANISH VA JADVALLAR
# ───────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id       BIGINT PRIMARY KEY,
            real_name     TEXT,
            username      TEXT,
            phone         TEXT,
            reg_date      TIMESTAMPTZ,
            last_activity TIMESTAMPTZ,
            sub_end_date  TIMESTAMPTZ,
            is_active     INTEGER DEFAULT 1
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id       SERIAL PRIMARY KEY,
            user_id  BIGINT,
            type     TEXT,
            amount   REAL,
            category TEXT,
            date     TIMESTAMPTZ,
            currency TEXT DEFAULT 'so''m'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id      SERIAL PRIMARY KEY,
            user_id BIGINT,
            name    TEXT,
            type    TEXT,
            UNIQUE(user_id, name, type)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("PostgreSQL jadvallari tayyor ✅")

def db_fetchone(query, params=()):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(query, params)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def db_fetchall(query, params=()):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def db_execute(query, params=()):
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

def db_execute_many(queries_params):
    conn = get_conn()
    cur  = conn.cursor()
    for query, params in queries_params:
        cur.execute(query, params)
    conn.commit()
    cur.close()
    conn.close()

def _to_dt(val):
    if val is None:
        return None
    if isinstance(val, str):
        dt = datetime.fromisoformat(val)
    else:
        dt = val
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UZB)
    return dt

# ───────────────────────────────────────────
# STATES
# ───────────────────────────────────────────
class Reg(StatesGroup):
    name  = State()
    phone = State()

class Fin(StatesGroup):
    preview       = State()
    edit_amount   = State()
    edit_category = State()
    edit_date     = State()

class CatEdit(StatesGroup):
    new_name = State()

# ───────────────────────────────────────────
# YORDAMCHI FUNKSIYALAR
# ───────────────────────────────────────────
BLOCKED_MSG = (
    "🔒 *Kirish taqiqlandi!*\n\n"
    "Sizning obuna muddatingiz tugagan yoki hisobingiz bloklangan. 🚫\n\n"
    "Davom etish uchun obunani yangilang:\n"
    "💰 To'lov: *10 000 so'm* (1 oy)\n"
    "👨‍💻 Murojaat: @Karimjonov_M77\n\n"
    "_To'lovdan so'ng barcha funksiyalar va tariх qayta tiklanadi._"
)

def smart_suffx():
    return random.choice([
        "Yana qanday ma'lumot kiritmoqchisiz? Men tayyorman! 😊",
        "Keyingi operatsiyani kiritishingiz mumkin. ✨",
        "Moliya hisob-kitobini davom ettiramizmi? ✍️",
        "Boshqa xarajat yoki daromad bormi? 💰",
        "Hisob-kitobni aniq yuritish — baylik garovidir! 🚀",
        "Siz bilan ishlash zavqli! Yana nima qo'shamiz? 💎",
    ])

def smart_parse(text: str):
    """Matndan summa, kategoriya, valyuta ajratib oladi."""
    raw  = text.lower().strip()
    raw  = re.sub(r'(?<=\d)\s+(?=\d)', '', raw)
    m    = re.search(r"(\d+(?:\.\d+)?)\s*(k|ming|mln|million|m|\$|usd|dollar)?", raw)
    if not m:
        return None, "Boshqa", "so'm", now_uzb().isoformat()

    num    = float(m.group(1))
    suffix = (m.group(2) or "").lower()
    mult   = {'k': 1000, 'ming': 1000, 'mln': 1_000_000, 'million': 1_000_000, 'm': 1_000_000}

    if suffix in ('$', 'usd', 'dollar'):
        amount   = num
        currency = "$"
    else:
        amount   = num * mult.get(suffix, 1)
        currency = "so'm"

    rest = raw.replace(m.group(0), "").strip()
    category = rest.capitalize() if rest else "Boshqa"
    return amount, category, currency, now_uzb().isoformat()

def main_menu():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add("📊 STATISTIKA",          "📂 KATEGORIYALAR")
    kb.add("📄 PDF HISOBOT",         "❓ YORDAM")
    kb.add("🧹 CHATNI TOZALASH",     "🗑 MA'LUMOTLARNI O'CHIRISH")
    return kb

async def get_name(uid: int) -> str:
    row = db_fetchone("SELECT real_name FROM users WHERE user_id=%s", (uid,))
    return row[0] if row else "Foydalanuvchi"

async def touch(uid: int):
    db_execute(
        "UPDATE users SET last_activity=%s WHERE user_id=%s",
        (now_uzb().isoformat(), uid)
    )

async def registered(uid: int) -> bool:
    if uid == ADMIN_ID:
        return True
    row = db_fetchone(
        "SELECT phone FROM users WHERE user_id=%s AND phone IS NOT NULL", (uid,)
    )
    return bool(row)

async def subscribed(uid: int) -> bool:
    """True → botdan foydalanish mumkin."""
    if uid == ADMIN_ID:
        return True
    row = db_fetchone(
        "SELECT reg_date, sub_end_date, is_active FROM users WHERE user_id=%s", (uid,)
    )
    if not row:
        return False
    reg_date_val, sub_end_val, is_active = row
    if is_active == 0:
        return False
    reg_dt = _to_dt(reg_date_val)
    n = now_uzb()
    if n < reg_dt + timedelta(days=1):
        return True
    if sub_end_val:
        sub_dt = _to_dt(sub_end_val)
        if n < sub_dt:
            return True
    return False

# ───────────────────────────────────────────
# FON — ESLATMALAR
# ───────────────────────────────────────────
async def notifier():
    while True:
        await asyncio.sleep(60)
        n = now_uzb()
        rows = db_fetchall(
            "SELECT user_id, real_name, reg_date, sub_end_date, last_activity, phone "
            "FROM users WHERE is_active=1"
        )

        for uid, name, reg_s, sub_s, last_s, phone in rows:
            try:
                reg_dt  = _to_dt(reg_s)
                last_dt = _to_dt(last_s)
                sub_dt  = _to_dt(sub_s)

                if phone and n > last_dt + timedelta(hours=24):
                    msgs = [
                        "Moliyalaringiz hisobini unutib qo'ymadingizmi? 🧐",
                        "Bugungi xarajatlarni kiritish vaqti keldi. 💰",
                        "Moliya intizomi baraka garovidir. 📈",
                        "Kichik xarajatlar ham hisobda bo'lishi kerak. 📉",
                        "Pulingiz qayerga ketayotganini bilmoqchimisiz? 🔎",
                    ]
                    await bot.send_message(uid,
                        f"Assalomu alaykum, {name}! 👋\n\n{random.choice(msgs)}")
                    await touch(uid)

                free_end = reg_dt + timedelta(days=1)
                if not sub_dt:
                    if free_end - timedelta(hours=2) < n < free_end - timedelta(hours=1, minutes=58):
                        await bot.send_message(uid,
                            "⚠️ *Sinov muddati tugashiga 2 soat qoldi!*\n\n"
                            "Obuna: *10 000 so'm / oy*\n👨‍💻 @Karimjonov_M77",
                            parse_mode="Markdown")
                    elif free_end - timedelta(hours=1) < n < free_end - timedelta(minutes=58):
                        await bot.send_message(uid,
                            "🚨 *Muddatingiz tugashiga 1 soat qoldi!*\n\n"
                            "Hoziroq bog'laning: @Karimjonov_M77",
                            parse_mode="Markdown")

                if sub_dt:
                    if sub_dt - timedelta(days=1) < n < sub_dt - timedelta(hours=23, minutes=58):
                        await bot.send_message(uid,
                            "⚠️ *Obunangiz tugashiga 1 kun qoldi!*\n\n"
                            "Uzaytirish uchun: @Karimjonov_M77",
                            parse_mode="Markdown")
                    elif sub_dt - timedelta(hours=1) < n < sub_dt - timedelta(minutes=58):
                        await bot.send_message(uid,
                            "🚨 *Obunangiz tugashiga 1 soat qoldi!*\n\n"
                            "Hoziroq uzaytiring: @Karimjonov_M77",
                            parse_mode="Markdown")
            except Exception:
                continue

# ───────────────────────────────────────────
# RO'YXATDAN O'TISH
# ───────────────────────────────────────────
@dp.message_handler(commands=['start'], state="*")
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.finish()
    row = db_fetchone(
        "SELECT phone FROM users WHERE user_id=%s", (msg.from_user.id,)
    )

    if not row or not row[0]:
        await msg.answer(
            "👋 *Assalomu alaykum! \"Smart Hisobchi\" botiga xush kelibsiz!*\n\n"
            "Daromad va xarajatlaringizni tizimlashtirish uchun yaratilgan bot.\n\n"
            "*Boshlash uchun ismingizni kiriting:*",
            parse_mode="Markdown"
        )
        await Reg.name.set()
        return

    if not await subscribed(msg.from_user.id):
        return await msg.answer(BLOCKED_MSG, parse_mode="Markdown")

    name = await get_name(msg.from_user.id)
    await touch(msg.from_user.id)
    await msg.answer(
        f"Xush ko'rdik, *{name}*! ✨\n\nBugun qanday moliyaviy amallarni bajaramiz?",
        reply_markup=main_menu(), parse_mode="Markdown"
    )

@dp.message_handler(state=Reg.name)
async def reg_name(msg: types.Message, state: FSMContext):
    await state.update_data(real_name=msg.text)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True).add(
        types.KeyboardButton("📞 Telefon raqamni yuborish", request_contact=True)
    )
    await msg.answer(
        f"Tanishganimdan xursandman, *{msg.text}*! 😊\n\nTelefon raqamingizni yuboring:",
        reply_markup=kb, parse_mode="Markdown"
    )
    await Reg.phone.set()

@dp.message_handler(content_types=['contact'], state=Reg.phone)
async def reg_phone(msg: types.Message, state: FSMContext):
    data = await state.get_data()
    n    = now_uzb().isoformat()
    db_execute(
        "INSERT INTO users (user_id, real_name, username, phone, reg_date, last_activity, is_active) "
        "VALUES (%s,%s,%s,%s,%s,%s,1) "
        "ON CONFLICT (user_id) DO UPDATE SET "
        "real_name=EXCLUDED.real_name, username=EXCLUDED.username, "
        "phone=EXCLUDED.phone, reg_date=EXCLUDED.reg_date, last_activity=EXCLUDED.last_activity",
        (msg.from_user.id, data['real_name'],
         msg.from_user.username, msg.contact.phone_number, n, n)
    )
    await state.finish()
    await msg.answer(
        "✅ *Muvaffaqiyatli ro'yxatdan o'tdingiz!*\n\nBarcha imkoniyatlar menyuda ochildi. 👇",
        reply_markup=main_menu(), parse_mode="Markdown"
    )

# ───────────────────────────────────────────
# MOLIYAVIY MA'LUMOT KIRITISH
# ───────────────────────────────────────────
@dp.message_handler(
    lambda m: any(ch.isdigit() for ch in m.text),
    state=[None]
)
async def process_fin(msg: types.Message, state: FSMContext):
    uid = msg.from_user.id

    if not await registered(uid):
        return await msg.answer(
            "⚠️ Avval ro'yxatdan o'ting! /start bosing."
        )

    if not await subscribed(uid):
        return await msg.answer(BLOCKED_MSG, parse_mode="Markdown")

    amount, category, currency, date_iso = smart_parse(msg.text)
    if not amount:
        return

    name = await get_name(uid)
    await state.update_data(
        amount=amount, category=category,
        currency=currency, date_time=date_iso
    )

    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("📥 KIRIM",          callback_data="t_KIRIM"),
        types.InlineKeyboardButton("📤 CHIQIM",         callback_data="t_CHIQIM"),
        types.InlineKeyboardButton("📝 O'ZGARTIRISH",   callback_data="edit_menu"),
        types.InlineKeyboardButton("❌ BEKOR",          callback_data="cancel"),
    )
    d_str = datetime.fromisoformat(date_iso).strftime('%d.%m.%Y %H:%M')
    await Fin.preview.set()
    await msg.answer(
        f"{name}, ushbu ma'lumotni saqlaymi?\n\n"
        f"💵 Summa: *{amount:,.0f} {currency}*\n"
        f"🏷 Kategoriya: *{category}*\n"
        f"📅 Sana: *{d_str}*",
        reply_markup=kb, parse_mode="Markdown"
    )

# Kategoriya tanlash
@dp.callback_query_handler(lambda c: c.data.startswith('t_'), state="*")
async def choose_cat(cb: types.CallbackQuery, state: FSMContext):
    t_type = cb.data.split('_', 1)[1]
    await state.update_data(current_type=t_type)
    data = await state.get_data()
    uid  = cb.from_user.id

    existing = db_fetchall(
        "SELECT DISTINCT name FROM categories WHERE user_id=%s AND type=%s",
        (uid, t_type)
    )

    kb = types.InlineKeyboardMarkup(row_width=2)
    for (cat_name,) in existing:
        kb.insert(types.InlineKeyboardButton(cat_name, callback_data=f"save_{cat_name}"))
    kb.add(types.InlineKeyboardButton(
        f"✨ Yangi: {data['category']}", callback_data=f"save_{data['category']}"
    ))
    await cb.message.edit_text(
        f"*{t_type}* uchun kategoriya tanlang:",
        reply_markup=kb, parse_mode="Markdown"
    )
    await cb.answer()

# Saqlash
@dp.callback_query_handler(lambda c: c.data.startswith('save_'), state="*")
async def save_fin(cb: types.CallbackQuery, state: FSMContext):
    cat   = cb.data[5:]
    data  = await state.get_data()
    uid   = cb.from_user.id
    name  = await get_name(uid)
    ttype = data.get('current_type', 'KIRIM')
    curr  = data.get('currency', "so'm")
    amt   = data.get('amount', 0)
    dt    = data.get('date_time', now_uzb().isoformat())

    db_execute_many([
        (
            "INSERT INTO transactions (user_id, type, amount, category, date, currency) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (uid, ttype, amt, cat, dt, curr)
        ),
        (
            "INSERT INTO categories (user_id, name, type) VALUES (%s,%s,%s) "
            "ON CONFLICT (user_id, name, type) DO NOTHING",
            (uid, cat, ttype)
        ),
    ])
    await touch(uid)
    await state.finish()

    lbl  = "Daromad (Kirim)"   if ttype == "KIRIM" else "Xarajat (Chiqim)"
    tip  = ("Daromad qo'shildi. Moliyaviy intizomingizga gap yo'q!"
            if ttype == "KIRIM"
            else "Xarajat qayd etildi. Pullar hisobini bilish — boylik garovidir!")

    await cb.message.edit_text(
        "✅ *Muvaffaqiyatli saqlandi!*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 *Foydalanuvchi:* {name}\n"
        f"📈 *Operatsiya:* {lbl}\n"
        f"💰 *Summa:* {amt:,.0f} {curr}\n"
        f"📅 *Kategoriya:* {cat}\n"
        f"⏰ *Vaqt:* {now_uzb().strftime('%d.%m.%Y %H:%M')}\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"✨ {tip}\n\n{smart_suffx()}",
        parse_mode="Markdown"
    )
    await cb.answer()

# ───────────────────────────────────────────
# O'ZGARTIRISH MENYUSI
# ───────────────────────────────────────────
@dp.callback_query_handler(lambda c: c.data == "edit_menu", state="*")
async def edit_menu(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    amt  = data.get('amount', 0)
    curr = data.get('currency', "so'm")
    cat  = data.get('category', 'Boshqa')
    try:
        d_str = datetime.fromisoformat(data.get('date_time', now_uzb().isoformat())).strftime('%d.%m.%Y %H:%M')
    except Exception:
        d_str = now_uzb().strftime('%d.%m.%Y %H:%M')

    kb = types.InlineKeyboardMarkup(row_width=1).add(
        types.InlineKeyboardButton("💵 Summani o'zgartirish",     callback_data="ed_amount"),
        types.InlineKeyboardButton("🏷 Kategoriyani o'zgartirish", callback_data="ed_cat"),
        types.InlineKeyboardButton("📅 Sanani o'zgartirish",       callback_data="ed_date"),
        types.InlineKeyboardButton("⬅️ Orqaga",                    callback_data="ed_back"),
        types.InlineKeyboardButton("❌ Bekor qilish",              callback_data="cancel"),
    )
    await cb.message.edit_text(
        f"Joriy ma'lumotlar:\n"
        f"💵 Summa: *{amt:,.0f} {curr}*\n"
        f"🏷 Kategoriya: *{cat}*\n"
        f"📅 Sana: *{d_str}*\n\n"
        f"Qaysi ma'lumotni o'zgartirmoqchisiz?",
        reply_markup=kb, parse_mode="Markdown"
    )
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data == "ed_amount", state="*")
async def ed_amount_ask(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    amt  = data.get('amount', 0)
    curr = data.get('currency', "so'm")
    await cb.message.edit_text(
        f"Joriy summa: *{amt:,.0f} {curr}*\n\n"
        f"Yangi summani kiriting:\n_(masalan: 50000, 20k, 100$)_",
        parse_mode="Markdown"
    )
    await Fin.edit_amount.set()
    await cb.answer()

@dp.message_handler(state=Fin.edit_amount)
async def ed_amount_get(msg: types.Message, state: FSMContext):
    amount, _, currency, _ = smart_parse(msg.text)
    if not amount:
        return await msg.answer(
            "❌ Noto'g'ri format. Masalan: *50000*, *20k*, *100$*",
            parse_mode="Markdown"
        )
    await state.update_data(amount=amount, currency=currency)
    await show_preview(msg, state)

@dp.callback_query_handler(lambda c: c.data == "ed_cat", state="*")
async def ed_cat_ask(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cat  = data.get('category', 'Boshqa')
    await cb.message.edit_text(
        f"Joriy kategoriya: *{cat}*\n\nYangi kategoriya nomini kiriting:",
        parse_mode="Markdown"
    )
    await Fin.edit_category.set()
    await cb.answer()

@dp.message_handler(state=Fin.edit_category)
async def ed_cat_get(msg: types.Message, state: FSMContext):
    await state.update_data(category=msg.text.strip().capitalize())
    await show_preview(msg, state)

@dp.callback_query_handler(lambda c: c.data == "ed_date", state="*")
async def ed_date_ask(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    try:
        d_str = datetime.fromisoformat(data.get('date_time', now_uzb().isoformat())).strftime('%d.%m.%Y %H:%M')
    except Exception:
        d_str = now_uzb().strftime('%d.%m.%Y %H:%M')
    await cb.message.edit_text(
        f"Joriy sana: *{d_str}*\n\n"
        f"Yangi sanani kiriting:\n"
        f"• `25.03.2025 14:30`\n"
        f"• `2025-03-25 14:30`\n"
        f"• `25.03.2025` _(faqat sana)_",
        parse_mode="Markdown"
    )
    await Fin.edit_date.set()
    await cb.answer()

@dp.message_handler(state=Fin.edit_date)
async def ed_date_get(msg: types.Message, state: FSMContext):
    raw = msg.text.strip()
    dt  = None
    formats = [
        "%d.%m.%Y %H:%M",
        "%Y-%m-%d %H:%M",
        "%d.%m.%Y",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            break
        except ValueError:
            continue

    if dt is None:
        return await msg.answer(
            "❌ Noto'g'ri format. Masalan:\n"
            "`25.03.2025 14:30` yoki `2025-03-25 14:30`",
            parse_mode="Markdown"
        )

    dt_aware = dt.replace(tzinfo=UZB)
    await state.update_data(date_time=dt_aware.isoformat())
    await show_preview(msg, state)

@dp.callback_query_handler(lambda c: c.data == "ed_back", state="*")
async def ed_back(cb: types.CallbackQuery, state: FSMContext):
    await show_preview_cb(cb, state)
    await cb.answer()

async def show_preview(msg: types.Message, state: FSMContext):
    data  = await state.get_data()
    name  = await get_name(msg.from_user.id)
    amt   = data.get('amount', 0)
    curr  = data.get('currency', "so'm")
    cat   = data.get('category', 'Boshqa')
    try:
        d_str = datetime.fromisoformat(data.get('date_time', now_uzb().isoformat())).strftime('%d.%m.%Y %H:%M')
    except Exception:
        d_str = now_uzb().strftime('%d.%m.%Y %H:%M')

    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("📥 KIRIM",        callback_data="t_KIRIM"),
        types.InlineKeyboardButton("📤 CHIQIM",       callback_data="t_CHIQIM"),
        types.InlineKeyboardButton("📝 O'zgartirish", callback_data="edit_menu"),
        types.InlineKeyboardButton("❌ Bekor",        callback_data="cancel"),
    )
    await Fin.preview.set()
    await msg.answer(
        f"{name}, yangilangan ma'lumot:\n\n"
        f"💵 Summa: *{amt:,.0f} {curr}*\n"
        f"🏷 Kategoriya: *{cat}*\n"
        f"📅 Sana: *{d_str}*\n\n"
        f"Turi tanlang yoki yana o'zgartiring:",
        reply_markup=kb, parse_mode="Markdown"
    )

async def show_preview_cb(cb: types.CallbackQuery, state: FSMContext):
    data  = await state.get_data()
    name  = await get_name(cb.from_user.id)
    amt   = data.get('amount', 0)
    curr  = data.get('currency', "so'm")
    cat   = data.get('category', 'Boshqa')
    try:
        d_str = datetime.fromisoformat(data.get('date_time', now_uzb().isoformat())).strftime('%d.%m.%Y %H:%M')
    except Exception:
        d_str = now_uzb().strftime('%d.%m.%Y %H:%M')

    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("📥 KIRIM",        callback_data="t_KIRIM"),
        types.InlineKeyboardButton("📤 CHIQIM",       callback_data="t_CHIQIM"),
        types.InlineKeyboardButton("📝 O'zgartirish", callback_data="edit_menu"),
        types.InlineKeyboardButton("❌ Bekor",        callback_data="cancel"),
    )
    await Fin.preview.set()
    await cb.message.edit_text(
        f"{name}, yangilangan ma'lumot:\n\n"
        f"💵 Summa: *{amt:,.0f} {curr}*\n"
        f"🏷 Kategoriya: *{cat}*\n"
        f"📅 Sana: *{d_str}*\n\n"
        f"Turi tanlang yoki yana o'zgartiring:",
        reply_markup=kb, parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data == "cancel", state="*")
async def cancel_op(cb: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await cb.message.edit_text(f"Amal bekor qilindi. {smart_suffx()}")
    await cb.answer()

# ───────────────────────────────────────────
# STATISTIKA
# ───────────────────────────────────────────
@dp.message_handler(lambda m: m.text == "📊 STATISTIKA")
async def stats_menu(msg: types.Message):
    uid = msg.from_user.id
    if not await registered(uid): return
    if not await subscribed(uid):
        return await msg.answer(BLOCKED_MSG, parse_mode="Markdown")
    await touch(uid)
    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("📆 Bugun",    callback_data="st_bugun"),
        types.InlineKeyboardButton("📅 Shu oy",   callback_data="st_oy"),
        types.InlineKeyboardButton("📊 Oyma-oy",  callback_data="st_oymaoy"),
        types.InlineKeyboardButton("🌍 Umumiy",   callback_data="st_all"),
    )
    await msg.answer("Qaysi davrning natijasini ko'rmoqchisiz? 😊", reply_markup=kb)

def _collect_stats(uid, start_date, end_date=None):
    if end_date:
        rows = db_fetchall(
            "SELECT type, currency, SUM(amount) FROM transactions "
            "WHERE user_id=%s AND date>=%s AND date<=%s GROUP BY type, currency",
            (uid, start_date, end_date)
        )
        cats = db_fetchall(
            "SELECT type, category, currency, SUM(amount) FROM transactions "
            "WHERE user_id=%s AND date>=%s AND date<=%s GROUP BY type,category,currency ORDER BY type,SUM(amount) DESC",
            (uid, start_date, end_date)
        )
    else:
        rows = db_fetchall(
            "SELECT type, currency, SUM(amount) FROM transactions "
            "WHERE user_id=%s AND date>=%s GROUP BY type, currency",
            (uid, start_date)
        )
        cats = db_fetchall(
            "SELECT type, category, currency, SUM(amount) FROM transactions "
            "WHERE user_id=%s AND date>=%s GROUP BY type,category,currency ORDER BY type,SUM(amount) DESC",
            (uid, start_date)
        )

    s = {"KIRIM": {"so'm": 0.0, "$": 0.0}, "CHIQIM": {"so'm": 0.0, "$": 0.0}}
    for ttype, curr, total in rows:
        key = "so'm" if "so'm" in (curr or "").strip().lower() else "$"
        if ttype in s:
            s[ttype][key] += (total or 0)
    return s, cats

def _build_stat_text(title, period_str, s, cats):
    sn, dn = "so'm", "$"
    txt  = f"📊 *{title}*\n🗓 {period_str}\n\n"
    txt += f"🟢 *KIRIM:*\n ├ So'm: {s['KIRIM'][sn]:,.0f}\n └ Dollar: {s['KIRIM'][dn]:,.0f} $\n\n"
    txt += f"🔴 *CHIQIM:*\n ├ So'm: {s['CHIQIM'][sn]:,.0f}\n └ Dollar: {s['CHIQIM'][dn]:,.0f} $\n\n"
    txt += (f"💎 *SOF FOYDA:*\n"
            f" ├ So'm: {s['KIRIM'][sn]-s['CHIQIM'][sn]:,.0f}\n"
            f" └ Dollar: {s['KIRIM'][dn]-s['CHIQIM'][dn]:,.0f} $\n\n")
    txt += "*📂 KATEGORIYALAR:*\n"
    if cats:
        cur_t = None
        for ttype, cat, curr, amt in cats:
            if cur_t != ttype:
                cur_t = ttype
                icon  = "🟢" if ttype == "KIRIM" else "🔴"
                txt  += f"\n{icon} *{ttype}:*\n"
            txt += f"  • {cat}: {amt:,.0f} {curr}\n"
    else:
        txt += "Ma'lumot yo'q"
    return txt

async def _send_stat(uid: int, title: str, period_str: str, s: dict, cats: list, name: str):
    txt = _build_stat_text(title, period_str, s, cats)
    has_data = any(v > 0 for t in s.values() for v in t.values())
    if has_data:
        k_val  = s['KIRIM']["so'm"]  + s['KIRIM']["$"]  * 12800
        ch_val = s['CHIQIM']["so'm"] + s['CHIQIM']["$"] * 12800
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.pie(
            [max(k_val, 0.1), max(ch_val, 0.1)],
            labels=['Kirim', 'Chiqim'],
            colors=["#02B40E", "#C10C0C"],
            autopct='%1.1f%%', startangle=90
        )
        ax.set_title(f"{name} — {title}")
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        plt.close(fig)
        await bot.send_photo(uid, photo=buf,
                             caption=f"{txt}\n\n{smart_suffx()}",
                             parse_mode="Markdown")
    else:
        await bot.send_message(uid,
                               f"Hozircha ma'lumotlar mavjud emas. 😊\n\n{smart_suffx()}")

@dp.callback_query_handler(lambda c: c.data.startswith('st_'))
async def show_stats(cb: types.CallbackQuery):
    mode = cb.data[3:]
    uid  = cb.from_user.id
    name = await get_name(uid)
    n    = now_uzb()
    await cb.answer()

    if mode == "bugun":
        start = n.replace(hour=0,  minute=0,  second=0,  microsecond=0).isoformat()
        end   = n.replace(hour=23, minute=59, second=59, microsecond=999999).isoformat()
        s, cats = _collect_stats(uid, start, end)
        await _send_stat(uid, "Bugungi Hisobot", n.strftime('%d.%m.%Y'), s, cats, name)

    elif mode == "oy":
        start = n.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        s, cats = _collect_stats(uid, start)
        await _send_stat(uid, "Oylik Hisobot", n.strftime('%Y yil %B'), s, cats, name)

    elif mode == "oymaoy":
        rows = db_fetchall(
            "SELECT TO_CHAR(date, 'YYYY-MM') as mo, type, currency, SUM(amount) "
            "FROM transactions WHERE user_id=%s "
            "GROUP BY TO_CHAR(date, 'YYYY-MM'), type, currency ORDER BY mo DESC",
            (uid,)
        )
        if not rows:
            return await bot.send_message(uid, f"Ma'lumot yo'q. 😊\n\n{smart_suffx()}")
        txt = "📊 *Oyma-Oy Hisobot*\n"
        cur_mo = None
        for mo, ttype, curr, total in rows:
            if cur_mo != mo:
                cur_mo = mo
                try:
                    mn = datetime.strptime(mo, '%Y-%m').strftime('%B %Y')
                except Exception:
                    mn = mo
                txt += f"\n*{mn}:*\n"
            icon = "🟢" if ttype == "KIRIM" else "🔴"
            txt += f"  {icon} {ttype}: {total:,.0f} {curr}\n"
        await bot.send_message(uid, txt + f"\n\n{smart_suffx()}", parse_mode="Markdown")

    elif mode == "all":
        row = db_fetchone("SELECT reg_date FROM users WHERE user_id=%s", (uid,))
        start = _to_dt(row[0]).isoformat() if row and row[0] else "2000-01-01T00:00:00+05:00"
        s, cats = _collect_stats(uid, start)
        start_dt = _to_dt(row[0]) if row and row[0] else _to_dt("2000-01-01T00:00:00")
        period = f"{start_dt.strftime('%d.%m.%Y')} — {n.strftime('%d.%m.%Y')}"
        await _send_stat(uid, "Umumiy Hisobot", period, s, cats, name)

# ───────────────────────────────────────────
# KATEGORIYALAR — YANGILANGAN (tur tanlash + tahrirlash/o'chirish)
# ───────────────────────────────────────────
@dp.message_handler(lambda m: m.text == "📂 KATEGORIYALAR")
async def cats_list(msg: types.Message):
    uid = msg.from_user.id
    if not await registered(uid): return
    await touch(uid)
    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("🟢 KIRIM kategoriyalari",  callback_data="cattype_KIRIM"),
        types.InlineKeyboardButton("🔴 CHIQIM kategoriyalari", callback_data="cattype_CHIQIM"),
    )
    await msg.answer("Qaysi turdagi kategoriyalarni ko'rmoqchisiz?", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith('cattype_'))
async def cats_by_type(cb: types.CallbackQuery):
    ttype = cb.data[8:]
    uid   = cb.from_user.id
    await _show_cat_list(cb.message, uid, ttype, edit=True)
    await cb.answer()


async def _show_cat_list(msg: types.Message, uid: int, ttype: str, edit: bool = False):
    """Tanlangan turdagi kategoriyalar ro'yxatini inline tugmalar bilan ko'rsatadi."""
    rows = db_fetchall(
        "SELECT DISTINCT category FROM transactions WHERE user_id=%s AND type=%s ORDER BY category",
        (uid, ttype)
    )

    icon = "🟢" if ttype == "KIRIM" else "🔴"
    if not rows:
        text = f"{icon} *{ttype}* bo'yicha kategoriyalar yo'q."
        kb   = types.InlineKeyboardMarkup().add(
            types.InlineKeyboardButton("⬅️ Orqaga", callback_data="cats_back")
        )
        if edit:
            await msg.edit_text(text, reply_markup=kb, parse_mode="Markdown")
        else:
            await msg.answer(text, reply_markup=kb, parse_mode="Markdown")
        return

    kb = types.InlineKeyboardMarkup(row_width=1)
    for (cat,) in rows:
        kb.add(types.InlineKeyboardButton(
            f"📁 {cat}", callback_data=f"vc_{ttype}_{cat}"
        ))
    kb.add(types.InlineKeyboardButton("⬅️ Orqaga", callback_data="cats_back"))

    text = f"{icon} *{ttype} kategoriyalari:*\nBatafsil ko'rish yoki boshqarish uchun tanlang."
    if edit:
        await msg.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await msg.answer(text, reply_markup=kb, parse_mode="Markdown")


@dp.callback_query_handler(lambda c: c.data == "cats_back")
async def cats_back(cb: types.CallbackQuery):
    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("🟢 KIRIM kategoriyalari",  callback_data="cattype_KIRIM"),
        types.InlineKeyboardButton("🔴 CHIQIM kategoriyalari", callback_data="cattype_CHIQIM"),
    )
    await cb.message.edit_text("Qaysi turdagi kategoriyalarni ko'rmoqchisiz?", reply_markup=kb)
    await cb.answer()


# ── Kategoriya tarixi + boshqaruv tugmalari ──
@dp.callback_query_handler(lambda c: c.data.startswith('vc_'))
async def view_cat(cb: types.CallbackQuery):
    parts = cb.data.split('_', 2)
    if len(parts) < 3:
        await cb.answer()
        return
    ttype = parts[1]
    cat   = parts[2]
    uid   = cb.from_user.id

    rows = db_fetchall(
        "SELECT date, amount, currency FROM transactions "
        "WHERE user_id=%s AND category=%s AND type=%s ORDER BY date DESC",
        (uid, cat, ttype)
    )

    icon = "🟢" if ttype == "KIRIM" else "🔴"
    txt  = f"{icon} *{ttype} — {cat}*\n\n"
    if rows:
        tot = 0
        for dt, amt, curr in rows:
            dt_obj = _to_dt(dt)
            txt += f"  • {dt_obj.strftime('%d.%m.%Y %H:%M')} | {amt:,.0f} {curr}\n"
            tot += (amt or 0)
        txt += f"\n*Jami: {tot:,.0f}*"
    else:
        txt += "Ma'lumot yo'q"

    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("✏️ Nomini tahrirlash",  callback_data=f"catedit_{ttype}_{cat}"),
        types.InlineKeyboardButton("🗑 O'chirish",          callback_data=f"catdel_{ttype}_{cat}"),
        types.InlineKeyboardButton("⬅️ Orqaga",             callback_data=f"cattype_{ttype}"),
    )
    await cb.message.edit_text(txt + f"\n\n{smart_suffx()}", reply_markup=kb, parse_mode="Markdown")
    await cb.answer()


# ── Kategoriya nomini tahrirlash ──
@dp.callback_query_handler(lambda c: c.data.startswith('catedit_'))
async def cat_edit_ask(cb: types.CallbackQuery, state: FSMContext):
    parts = cb.data.split('_', 2)
    ttype = parts[1]
    cat   = parts[2]
    await state.update_data(edit_cat_old=cat, edit_cat_type=ttype)
    await cb.message.edit_text(
        f"✏️ *\"{cat}\"* kategoriyasining yangi nomini kiriting:",
        parse_mode="Markdown"
    )
    await CatEdit.new_name.set()
    await cb.answer()


@dp.message_handler(state=CatEdit.new_name)
async def cat_edit_save(msg: types.Message, state: FSMContext):
    data     = await state.get_data()
    old_name = data.get('edit_cat_old', '')
    ttype    = data.get('edit_cat_type', '')
    new_name = msg.text.strip().capitalize()
    uid      = msg.from_user.id

    if not new_name:
        return await msg.answer("❌ Ism bo'sh bo'lishi mumkin emas.")

    db_execute_many([
        (
            "UPDATE transactions SET category=%s WHERE user_id=%s AND category=%s AND type=%s",
            (new_name, uid, old_name, ttype)
        ),
        (
            "UPDATE categories SET name=%s WHERE user_id=%s AND name=%s AND type=%s",
            (new_name, uid, old_name, ttype)
        ),
    ])
    await state.finish()

    icon = "🟢" if ttype == "KIRIM" else "🔴"
    kb = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("⬅️ Kategoriyalarga qaytish", callback_data=f"cattype_{ttype}")
    )
    await msg.answer(
        f"✅ {icon} *{ttype}* kategoriyasi:\n"
        f"*\"{old_name}\"* → *\"{new_name}\"* ga o'zgartirildi!",
        reply_markup=kb, parse_mode="Markdown"
    )


# ── Kategoriyani o'chirish (tasdiqlash) ──
@dp.callback_query_handler(lambda c: c.data.startswith('catdel_'))
async def cat_del_confirm(cb: types.CallbackQuery):
    parts = cb.data.split('_', 2)
    ttype = parts[1]
    cat   = parts[2]
    uid   = cb.from_user.id

    count = db_fetchone(
        "SELECT COUNT(*) FROM transactions WHERE user_id=%s AND category=%s AND type=%s",
        (uid, cat, ttype)
    )[0]

    icon = "🟢" if ttype == "KIRIM" else "🔴"
    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("✅ HA, O'CHIRILSIN", callback_data=f"catdelok_{ttype}_{cat}"),
        types.InlineKeyboardButton("❌ Bekor",           callback_data=f"vc_{ttype}_{cat}"),
    )
    await cb.message.edit_text(
        f"⚠️ *DIQQAT!*\n\n"
        f"{icon} *{ttype} — {cat}*\n\n"
        f"Bu kategoriyaga tegishli *{count} ta* tranzaksiya ham o'chib ketadi!\n\n"
        f"Davom etasizmi?",
        reply_markup=kb, parse_mode="Markdown"
    )
    await cb.answer()


# ── Kategoriyani o'chirish (bajarish) ──
@dp.callback_query_handler(lambda c: c.data.startswith('catdelok_'))
async def cat_del_do(cb: types.CallbackQuery):
    parts = cb.data.split('_', 2)
    ttype = parts[1]
    cat   = parts[2]
    uid   = cb.from_user.id

    db_execute_many([
        (
            "DELETE FROM transactions WHERE user_id=%s AND category=%s AND type=%s",
            (uid, cat, ttype)
        ),
        (
            "DELETE FROM categories WHERE user_id=%s AND name=%s AND type=%s",
            (uid, cat, ttype)
        ),
    ])

    icon = "🟢" if ttype == "KIRIM" else "🔴"
    kb = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("⬅️ Kategoriyalarga qaytish", callback_data=f"cattype_{ttype}")
    )
    await cb.message.edit_text(
        f"🗑 {icon} *{ttype} — {cat}* kategoriyasi va unga tegishli barcha tranzaksiyalar o'chirildi!",
        reply_markup=kb, parse_mode="Markdown"
    )
    await cb.answer()


# ───────────────────────────────────────────
# CHAT TOZALASH
# ───────────────────────────────────────────
@dp.message_handler(lambda m: m.text == "🧹 CHATNI TOZALASH")
async def clear_chat(msg: types.Message):
    if not await registered(msg.from_user.id): return
    await touch(msg.from_user.id)
    for i in range(msg.message_id, msg.message_id - 50, -1):
        try:
            await bot.delete_message(msg.chat.id, i)
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await msg.answer(f"Chat tozalandi! 😊\n\n{smart_suffx()}", reply_markup=main_menu())

# ───────────────────────────────────────────
# MA'LUMOTLARNI O'CHIRISH
# ───────────────────────────────────────────
@dp.message_handler(lambda m: m.text == "🗑 MA'LUMOTLARNI O'CHIRISH")
async def reset_ask(msg: types.Message):
    if not await registered(msg.from_user.id): return
    kb = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("✅ HA, O'CHIRILSIN", callback_data="confirm_reset"),
        types.InlineKeyboardButton("❌ YO'Q, BEKOR",     callback_data="cancel"),
    )
    await msg.answer(
        "⚠️ *DIQQAT!*\n\n"
        "Barcha xarajat, daromad va kategoriyalar o'chiriladi.\n"
        "Profil va obuna muddati saqlanib qoladi.\n\n"
        "Bu amalni ortga qaytarib bo'lmaydi. Rozimisiz?",
        reply_markup=kb, parse_mode="Markdown"
    )

@dp.callback_query_handler(lambda c: c.data == "confirm_reset")
async def reset_confirm(cb: types.CallbackQuery):
    uid = cb.from_user.id
    db_execute_many([
        ("DELETE FROM transactions WHERE user_id=%s", (uid,)),
        ("DELETE FROM categories  WHERE user_id=%s",  (uid,)),
    ])
    await cb.message.edit_text(
        "🗑 *Barcha operatsiyalar va kategoriyalar o'chirildi!*\n"
        "Endi noldan boshlashingiz mumkin. 😊",
        parse_mode="Markdown"
    )
    await cb.answer()

# ───────────────────────────────────────────
# PDF HISOBOT (foydalanuvchi uchun)
# ───────────────────────────────────────────
@dp.message_handler(lambda m: m.text == "📄 PDF HISOBOT")
async def pdf_menu(msg: types.Message):
    uid = msg.from_user.id
    if not await registered(uid): return
    if not await subscribed(uid):
        return await msg.answer(BLOCKED_MSG, parse_mode="Markdown")
    await touch(uid)
    kb = types.InlineKeyboardMarkup(row_width=1).add(
        types.InlineKeyboardButton("📅 Shu oy",          callback_data="pdf_oy"),
        types.InlineKeyboardButton("🌍 Barcha (Oyma-Oy)", callback_data="pdf_all"),
    )
    await msg.answer("Qaysi davrning hisobotini yuklab olmoqchisiz?", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('pdf_'))
async def user_pdf(cb: types.CallbackQuery):
    mode = cb.data[4:]
    uid  = cb.from_user.id
    await cb.answer()
    await bot.send_message(uid, "Hisobotingizni tayyorlayapman... ⏳")
    if mode == "oy":
        await make_pdf_monthly(uid, send_to=uid)
    else:
        await make_pdf_all(uid, send_to=uid)
    await bot.send_message(uid, smart_suffx())

# ───────────────────────────────────────────
# PDF YARATISH FUNKSIYALARI
# ───────────────────────────────────────────
async def make_pdf_monthly(user_id: int, send_to: int):
    n      = now_uzb()
    start  = n.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
    rows   = db_fetchall(
        "SELECT date, category, type, amount, currency "
        "FROM transactions WHERE user_id=%s AND date>=%s ORDER BY date DESC",
        (user_id, start)
    )
    urow   = db_fetchone("SELECT real_name FROM users WHERE user_id=%s", (user_id,))
    uname  = urow[0] if urow else str(user_id)

    if not rows:
        return await bot.send_message(send_to,
            f"👤 {uname}\n\nShu oyda hozircha ma'lumotlar yo'q.")

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, "OYLIK MOLIYAVIY HISOBOT", ln=1, align='C')
    pdf.set_font("Arial", '', 11)
    pdf.cell(0, 8, f"Foydalanuvchi: {uname}", ln=1, align='C')
    pdf.cell(0, 8, n.strftime('%B %Y'), ln=1, align='C')
    pdf.ln(8)

    pdf.set_font("Arial", 'B', 10)
    for col, w in [("Sana", 35), ("Kategoriya", 50), ("Turi", 25), ("Summa", 35), ("Valyuta", 20)]:
        pdf.cell(w, 10, col, 1)
    pdf.ln()

    tk_s = tk_d = tch_s = tch_d = 0.0
    pdf.set_font("Arial", '', 9)
    for dt, cat, ttype, amt, curr in rows:
        dt_str = _to_dt(dt).strftime('%Y-%m-%d')
        pdf.cell(35, 8, dt_str,         1)
        pdf.cell(50, 8, str(cat)[:22],  1)
        pdf.cell(25, 8, str(ttype),     1)
        pdf.cell(35, 8, f"{amt:,.0f}",  1)
        pdf.cell(20, 8, str(curr),      1)
        pdf.ln()
        if ttype == "KIRIM":
            if curr == "$": tk_d += (amt or 0)
            else:           tk_s += (amt or 0)
        else:
            if curr == "$": tch_d += (amt or 0)
            else:           tch_s += (amt or 0)

    pdf.ln(8)
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(0, 8, "UMUMIY NATIJA:", ln=1)
    pdf.set_font("Arial", '', 9)
    pdf.cell(0, 7, f"Jami Kirim:  {tk_s:,.0f} so'm  |  {tk_d:,.0f} $", ln=1)
    pdf.cell(0, 7, f"Jami Chiqim: {tch_s:,.0f} so'm  |  {tch_d:,.0f} $", ln=1)
    pdf.cell(0, 7, f"Sof Foyda:   {tk_s-tch_s:,.0f} so'm  |  {tk_d-tch_d:,.0f} $", ln=1)

    fname = f"/tmp/oylik_{user_id}_{random.randint(1000,9999)}.pdf"
    try:
        pdf.output(fname)
        with open(fname, 'rb') as f:
            await bot.send_document(send_to, f,
                caption=f"📄 *{uname}* — Oylik hisobot ({n.strftime('%B %Y')})",
                parse_mode="Markdown")
    finally:
        if os.path.exists(fname):
            os.remove(fname)

async def make_pdf_all(user_id: int, send_to: int):
    rows  = db_fetchall(
        "SELECT TO_CHAR(date, 'YYYY-MM') as mo, date, category, type, amount, currency "
        "FROM transactions WHERE user_id=%s ORDER BY date DESC",
        (user_id,)
    )
    urow  = db_fetchone("SELECT real_name FROM users WHERE user_id=%s", (user_id,))
    uname = urow[0] if urow else str(user_id)

    if not rows:
        return await bot.send_message(send_to,
            f"👤 {uname}\n\nHozircha ma'lumotlar yo'q.")

    months: dict = {}
    for mo, dt, cat, ttype, amt, curr in rows:
        months.setdefault(mo, []).append((dt, cat, ttype, amt, curr))

    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(0, 10, "TO'LIQ MOLIYAVIY HISOBOT", ln=1, align='C')
    pdf.set_font("Arial", '', 10)
    pdf.cell(0, 8, f"Foydalanuvchi: {uname}", ln=1, align='C')
    pdf.ln(5)

    all_ks = all_kd = all_chs = all_chd = 0.0

    for mo in sorted(months.keys(), reverse=True):
        try:
            mn = datetime.strptime(mo, '%Y-%m').strftime('%B %Y')
        except Exception:
            mn = mo

        pdf.set_font("Arial", 'B', 11)
        pdf.cell(0, 8, mn, ln=1)

        pdf.set_font("Arial", 'B', 9)
        for col, w in [("Sana",30),("Kategoriya",42),("Turi",20),("Summa",30),("Valyuta",20)]:
            pdf.cell(w, 8, col, 1)
        pdf.ln()

        mo_ks = mo_kd = mo_chs = mo_chd = 0.0
        pdf.set_font("Arial", '', 8)
        for dt, cat, ttype, amt, curr in months[mo]:
            dt_str = _to_dt(dt).strftime('%Y-%m-%d')
            pdf.cell(30, 7, dt_str,        1)
            pdf.cell(42, 7, str(cat)[:20], 1)
            pdf.cell(20, 7, str(ttype),    1)
            pdf.cell(30, 7, f"{amt:,.0f}", 1)
            pdf.cell(20, 7, str(curr),     1)
            pdf.ln()
            if ttype == "KIRIM":
                if curr == "$": mo_kd  += (amt or 0)
                else:           mo_ks  += (amt or 0)
            else:
                if curr == "$": mo_chd += (amt or 0)
                else:           mo_chs += (amt or 0)

        all_ks  += mo_ks;  all_kd  += mo_kd
        all_chs += mo_chs; all_chd += mo_chd

        pdf.set_font("Arial", '', 8)
        pdf.cell(0, 6, f"Oy kirim:  {mo_ks:,.0f} so'm | {mo_kd:,.0f} $", ln=1)
        pdf.cell(0, 6, f"Oy chiqim: {mo_chs:,.0f} so'm | {mo_chd:,.0f} $", ln=1)
        pdf.ln(3)

    pdf.ln(5)
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(0, 8, "JAMI UMUMIY NATIJA:", ln=1)
    pdf.set_font("Arial", '', 9)
    pdf.cell(0, 7, f"Jami Kirim:  {all_ks:,.0f} so'm  |  {all_kd:,.0f} $", ln=1)
    pdf.cell(0, 7, f"Jami Chiqim: {all_chs:,.0f} so'm  |  {all_chd:,.0f} $", ln=1)
    pdf.cell(0, 7, f"Sof Foyda:   {all_ks-all_chs:,.0f} so'm  |  {all_kd-all_chd:,.0f} $", ln=1)

    fname = f"/tmp/toliq_{user_id}_{random.randint(1000,9999)}.pdf"
    try:
        pdf.output(fname)
        with open(fname, 'rb') as f:
            await bot.send_document(send_to, f,
                caption=f"📄 *{uname}* — To'liq hisobot",
                parse_mode="Markdown")
    finally:
        if os.path.exists(fname):
            os.remove(fname)

# ───────────────────────────────────────────
# YORDAM
# ───────────────────────────────────────────
@dp.message_handler(lambda m: m.text == "❓ YORDAM")
async def help_cmd(msg: types.Message):
    if not await registered(msg.from_user.id): return
    await touch(msg.from_user.id)
    await msg.answer(
        "📖 *Botdan foydalanish qo'llanmasi*\n\n"
        "1️⃣ *Oddiy yozish:*\n"
        "👉 `50000 bozor` — summa + kategoriya\n\n"
        "2️⃣ *Qisqartmalar:*\n"
        "👉 `20k tushlik` (k = 1 000)\n"
        "👉 `5mln ijara` (mln = 1 000 000)\n"
        "👉 `100$ kiyim` yoki `50 usd xizmat`\n\n"
        "3️⃣ *Tugmalar:*\n"
        "📊 Statistika — kunlik/oylik/umumiy balans\n"
        "📄 PDF Hisobot — jadval shaklida yuklab olish\n"
        "📂 Kategoriyalar — KIRIM/CHIQIM bo'yicha ko'rish,\n"
        "   tahrirlash va o'chirish imkoniyati\n\n"
        "Savollar bo'lsa: @Karimjonov_M77 😇",
        parse_mode="Markdown"
    )

# ───────────────────────────────────────────
# ADMIN PANEL
# ───────────────────────────────────────────
@dp.message_handler(commands=['panel'])
async def admin_panel(msg: types.Message):
    if msg.from_user.id != ADMIN_ID: return
    await _send_admin_panel(msg.chat.id)

async def _send_admin_panel(chat_id: int):
    n = now_uzb()
    rows = db_fetchall(
        "SELECT user_id, real_name, username, phone, reg_date, sub_end_date, is_active "
        "FROM users"
    )

    yangi, faol, tugagan, chala = [], [], [], []
    for uid, rname, uname, tel, reg_s, sub_s, act in rows:
        reg_dt = _to_dt(reg_s)
        sub_dt = _to_dt(sub_s)

        st_icon = "✅" if act == 1 else "🚫"
        label   = f"{st_icon} {rname or '?'} | @{uname or '?'} | {tel or '?'}"

        if not tel:                                chala.append((uid, label))
        elif n < reg_dt + timedelta(days=1):       yangi.append((uid, label))
        elif sub_dt and n < sub_dt:                faol.append((uid, label))
        else:                                      tugagan.append((uid, label))

    txt = (
        f"👑 *ADMIN PANEL*\n\n"
        f"🆕 Yangi (trial): {len(yangi)} ta\n"
        f"💎 Obuna faol:    {len(faol)} ta\n"
        f"⌛ Muddati o'tgan: {len(tugagan)} ta\n"
        f"📝 Ro'yxatdan o'tmagan: {len(chala)} ta\n\n"
        f"Foydalanuvchini tanlang:"
    )
    kb = types.InlineKeyboardMarkup(row_width=1)
    for uid, label in yangi + faol + tugagan + chala:
        kb.add(types.InlineKeyboardButton(label, callback_data=f"ap_{uid}"))
    await bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query_handler(lambda c: c.data.startswith('ap_') and c.data[3:].isdigit())
async def ap_user_card(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    uid  = int(cb.data[3:])
    await _show_user_card(cb.message, uid, edit=True)
    await cb.answer()

async def _show_user_card(msg: types.Message, uid: int, edit: bool = False):
    u = db_fetchone("SELECT * FROM users WHERE user_id=%s", (uid,))
    if not u: return
    reg_dt = _to_dt(u[4])
    kunlar = (now_uzb() - reg_dt).days

    sub_s  = u[6]
    if sub_s:
        sub_dt = _to_dt(sub_s)
        left   = sub_dt - now_uzb()
        if left.total_seconds() > 0:
            sub_str = f"✅ {sub_dt.strftime('%d.%m.%Y')} ({left.days} kun qoldi)"
        else:
            sub_str = f"❌ Tugagan ({sub_dt.strftime('%d.%m.%Y')})"
    else:
        free_end = reg_dt + timedelta(days=1)
        left     = free_end - now_uzb()
        if left.total_seconds() > 0:
            sub_str = f"🕐 Trial ({int(left.total_seconds()//3600)} soat qoldi)"
        else:
            sub_str = "❌ Trial tugagan"

    tx_count = db_fetchone(
        "SELECT COUNT(*) FROM transactions WHERE user_id=%s", (uid,)
    )[0]

    text = (
        f"👤 *Foydalanuvchi ma'lumotlari*\n\n"
        f"📝 Ism:       `{u[1] or '—'}`\n"
        f"🆔 ID:        `{u[0]}`\n"
        f"🔗 Username:  @{u[2] or '—'}\n"
        f"📞 Tel:       `{u[3] or '—'}`\n"
        f"📅 A'zo:      {kunlar} kun oldin\n"
        f"⏳ Obuna:     {sub_str}\n"
        f"📊 Operatsiya: {tx_count} ta\n"
        f"🔘 Holat:     {'✅ Faol' if u[7]==1 else '🚫 Bloklangan'}"
    )
    kb = types.InlineKeyboardMarkup(row_width=2).add(
        types.InlineKeyboardButton("➕ 1 Oylik Obuna",        callback_data=f"sub_{uid}"),
        types.InlineKeyboardButton("📄 PDF (shu oy)",         callback_data=f"apdf_{uid}_oy"),
        types.InlineKeyboardButton("📋 PDF (barcha)",         callback_data=f"apdf_{uid}_all"),
        types.InlineKeyboardButton("🚫 Bloklash / Ochish",    callback_data=f"blk_{uid}"),
        types.InlineKeyboardButton("⬅️ Orqaga",               callback_data="ap_back"),
    )
    if edit:
        await msg.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    else:
        await msg.answer(text, reply_markup=kb, parse_mode="Markdown")

@dp.callback_query_handler(lambda c: c.data.startswith('sub_'))
async def ap_sub_confirm(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    uid = int(cb.data[4:])
    kb  = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("✅ HA, tasdiqlayman", callback_data=f"csub_{uid}"),
        types.InlineKeyboardButton("❌ Yo'q",             callback_data=f"ap_{uid}"),
    )
    await cb.message.edit_text(
        f"ID `{uid}` ga *1 oylik obuna* berishni tasdiqlaysizmi?",
        reply_markup=kb, parse_mode="Markdown"
    )
    await cb.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('csub_'))
async def ap_sub_do(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    uid     = int(cb.data[5:])
    new_end = (now_uzb() + timedelta(days=30)).isoformat()
    db_execute(
        "UPDATE users SET sub_end_date=%s, is_active=1 WHERE user_id=%s",
        (new_end, uid)
    )
    await cb.message.edit_text("✅ *Obuna muvaffaqiyatli yoqildi!*", parse_mode="Markdown")
    try:
        await bot.send_message(uid,
            "🎉 *Tabriklaymiz!* Admin obunangizni *30 kunga* faollashtirdi.\n"
            "Endi botdan to'liq foydalanishingiz mumkin! 🚀",
            parse_mode="Markdown")
    except Exception:
        pass
    await cb.answer("Obuna yoqildi!")

@dp.callback_query_handler(lambda c: c.data.startswith('blk_'))
async def ap_block(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    uid  = int(cb.data[4:])
    curr = db_fetchone("SELECT is_active FROM users WHERE user_id=%s", (uid,))[0]
    new  = 0 if curr == 1 else 1
    db_execute("UPDATE users SET is_active=%s WHERE user_id=%s", (new, uid))

    try:
        if new == 0:
            await bot.send_message(uid,
                "🚫 *Hisobingiz bloklandi.*\n\n"
                "Botdan foydalanish imkoniyatingiz vaqtincha to'xtatildi.\n"
                "To'lov yoki ma'lumot uchun: @Karimjonov_M77",
                parse_mode="Markdown")
        else:
            await bot.send_message(uid,
                "✅ *Hisobingiz qayta faollashtirildi!*\n\n"
                "Endi botdan foydalanishingiz mumkin. 😊",
                parse_mode="Markdown")
    except Exception:
        pass

    await cb.answer("Holat o'zgartirildi!")
    await _show_user_card(cb.message, uid, edit=True)

@dp.callback_query_handler(lambda c: c.data.startswith('apdf_'))
async def ap_pdf(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    parts   = cb.data.split('_')
    uid     = int(parts[1])
    mode    = parts[2]
    admin   = cb.from_user.id

    await cb.answer("PDF tayyorlanmoqda...")
    await bot.send_message(admin, "PDF tayyorlanmoqda... ⏳")

    if mode == "oy":
        await make_pdf_monthly(uid, send_to=admin)
    else:
        await make_pdf_all(uid, send_to=admin)

@dp.callback_query_handler(lambda c: c.data == "ap_back")
async def ap_back(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID: return
    await cb.message.delete()
    await _send_admin_panel(cb.message.chat.id)
    await cb.answer()

# ───────────────────────────────────────────
# ISHGA TUSHIRISH
# ───────────────────────────────────────────
if __name__ == '__main__':
    init_db()

    async def on_startup(dp):
        asyncio.create_task(notifier())
        logging.info("Bot ishga tushdi! Notifier faollashdi. ✅")

    while True:
        try:
            executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
        except Exception as e:
            logging.error(f"Xatolik: {e}")
            import time
            time.sleep(5)

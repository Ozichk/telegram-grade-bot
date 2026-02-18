import json
import os
import time
import threading
from collections import Counter
from typing import Dict, List, Tuple, Any, Optional

import telebot
from telebot import types
from openpyxl import load_workbook
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask

# ====== НАСТРОЙКИ ======
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment variables")

DATA_FILE = "users.json"
DEFAULT_TZ = "Europe/Berlin"
SEP = "||"  # разделитель для ключей Counter (чтобы JSON мог сохранить)

bot = telebot.TeleBot(BOT_TOKEN)

# ====== Flask (порт-заглушка для Render Web Service) ======
app = Flask(__name__)

@app.get("/")
def home():
    return "OK", 200

def run_web():
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_web, daemon=True).start()

# ====== Scheduler ======
scheduler = BackgroundScheduler(timezone=DEFAULT_TZ)
scheduler.start()
scheduled_jobs: Dict[int, str] = {}

# ----------------- Хранилище -----------------
def load_data() -> Dict[str, Any]:
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # если файл сломан/битый — начинаем заново
        return {}

def save_data(d: Dict[str, Any]) -> None:
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)

data = load_data()

def get_user(chat_id: int) -> Dict[str, Any]:
    u = data.get(str(chat_id))
    if not u:
        u = {
            "grades_counter": {},      # dict со строковыми ключами
            "last_overall": None,
            "last_averages": {},
            "reminder_enabled": False,
            "reminder_time": None,
            "awaiting_time": False,
        }
        data[str(chat_id)] = u
        save_data(data)
    return u

# ----------------- Excel -> оценки -----------------
def parse_excel_grades(file_path: str) -> List[Tuple[str, int]]:
    wb = load_workbook(file_path)
    sheet = wb.active

    items: List[Tuple[str, int]] = []
    for row in sheet.iter_rows(values_only=True):
        subject = row[0]
        if not subject or not isinstance(subject, str):
            continue
        subject = subject.strip()

        for cell in row[1:]:
            if isinstance(cell, (int, float)):
                items.append((subject, int(cell)))

    return items

def analyze_items(items: List[Tuple[str, int]]) -> Optional[Dict[str, Any]]:
    if not items:
        return None

    by_subject: Dict[str, List[int]] = {}
    for subj, grade in items:
        by_subject.setdefault(subj, []).append(grade)

    averages = {s: sum(vals) / len(vals) for s, vals in by_subject.items()}
    overall = sum(averages.values()) / len(averages)

    best = max(averages, key=averages.get)
    worst = min(averages, key=averages.get)

    return {"overall": overall, "best": best, "worst": worst, "averages": averages}

# ----------------- Counter, который можно сохранить в JSON -----------------
def make_counter(items: List[Tuple[str, int]]) -> Counter:
    c = Counter()
    for subj, grade in items:
        c[f"{subj}{SEP}{grade}"] += 1
    return c

def parse_counter_key(key: str) -> Tuple[str, int]:
    subj, grade = key.split(SEP, 1)
    return subj, int(grade)

def diff_new_grades(old: Counter, new: Counter) -> List[Tuple[str, int, int]]:
    added = []
    for key, new_count in new.items():
        old_count = old.get(key, 0)
        if new_count > old_count:
            subj, grade = parse_counter_key(key)
            added.append((subj, grade, new_count - old_count))
    added.sort(key=lambda x: (x[0], x[1]))
    return added

# ----------------- UI: inline кнопки -----------------
def menu_kb() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("📊 Общий анализ", callback_data="summary"),
        types.InlineKeyboardButton("📚 Подробный отчёт", callback_data="details"),
    )
    kb.add(
        types.InlineKeyboardButton("🔄 Обновить данные", callback_data="refresh"),
        types.InlineKeyboardButton("⏰ Напоминания", callback_data="reminders"),
    )
    return kb

def reminders_kb(enabled: bool) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    toggle_text = "⛔ Выкл напоминания" if enabled else "✅ Вкл напоминания"
    kb.add(types.InlineKeyboardButton(toggle_text, callback_data="rem_toggle"))

    kb.add(
        types.InlineKeyboardButton("08:00", callback_data="time_08:00"),
        types.InlineKeyboardButton("12:00", callback_data="time_12:00"),
        types.InlineKeyboardButton("18:00", callback_data="time_18:00"),
        types.InlineKeyboardButton("21:00", callback_data="time_21:00"),
    )
    kb.add(types.InlineKeyboardButton("✍️ Ввести своё время", callback_data="time_custom"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="back"))
    return kb

# ----------------- Напоминания -----------------
def reminder_job(chat_id: int):
    bot.send_message(chat_id, "⏰ Пора обновить оценки: отправь свежий Excel-файл (.xlsx).")

def schedule_user_reminder(chat_id: int, hhmm: str):
    old_job_id = scheduled_jobs.get(chat_id)
    if old_job_id:
        try:
            scheduler.remove_job(old_job_id)
        except Exception:
            pass

    hour, minute = hhmm.split(":")
    job_id = f"rem_{chat_id}"

    scheduler.add_job(
        reminder_job,
        trigger=CronTrigger(hour=int(hour), minute=int(minute)),
        args=[chat_id],
        id=job_id,
        replace_existing=True,
    )
    scheduled_jobs[chat_id] = job_id

def unschedule_user_reminder(chat_id: int):
    job_id = scheduled_jobs.get(chat_id)
    if job_id:
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass
        scheduled_jobs.pop(chat_id, None)

def restore_jobs_from_file():
    global data
    data = load_data()
    for chat_id_str, u in data.items():
        try:
            chat_id = int(chat_id_str)
        except ValueError:
            continue
        if u.get("reminder_enabled") and u.get("reminder_time"):
            schedule_user_reminder(chat_id, u["reminder_time"])

restore_jobs_from_file()

# ----------------- Команды -----------------
@bot.message_handler(commands=["start"])
def start(message):
    get_user(message.chat.id)
    bot.send_message(
        message.chat.id,
        "Привет! 👋\n"
        "Отправь Excel (.xlsx) с оценками — я сделаю анализ.\n"
        "Дальше управляй через кнопки.",
        reply_markup=menu_kb()
    )

# ----------------- Приём файла -----------------
@bot.message_handler(content_types=["document"])
def on_document(message):
    file_name = message.document.file_name or ""
    if not file_name.lower().endswith(".xlsx"):
        bot.send_message(message.chat.id, "Нужен файл формата .xlsx 🙂", reply_markup=menu_kb())
        return

    file_info = bot.get_file(message.document.file_id)
    raw = bot.download_file(file_info.file_path)

    tmp_name = f"{message.from_user.id}_{int(time.time())}.xlsx"
    with open(tmp_name, "wb") as f:
        f.write(raw)

    try:
        items = parse_excel_grades(tmp_name)
        rep = analyze_items(items)
        if not rep:
            bot.send_message(message.chat.id, "Не нашёл оценок в файле 😔", reply_markup=menu_kb())
            return

        u = get_user(message.chat.id)

        # старые данные (уже строковые ключи)
        old_counter = Counter(u.get("grades_counter", {}))
        new_counter = make_counter(items)
        added = diff_new_grades(old_counter, new_counter)

        # сохранить
        u["grades_counter"] = dict(new_counter)  # JSON-совместимо
        u["last_overall"] = rep["overall"]
        u["last_averages"] = rep["averages"]
        save_data(data)

        msg = "✅ Файл обработан.\n"
        if added:
            msg += "\n🔔 Найдены новые оценки:\n"
            lines = []
            for subj, grade, cnt in added[:30]:
                suffix = f" x{cnt}" if cnt > 1 else ""
                lines.append(f"• {subj}: {grade}{suffix}")
            msg += "\n".join(lines)
            if len(added) > 30:
                msg += f"\n…и ещё {len(added) - 30}"
        else:
            msg += "\nНовых оценок не обнаружено."

        bot.send_message(message.chat.id, msg, reply_markup=menu_kb())

    finally:
        try:
            os.remove(tmp_name)
        except Exception:
            pass

# ----------------- Callback кнопок -----------------
@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    chat_id = call.message.chat.id
    u = get_user(chat_id)

    if call.data == "summary":
        overall = u.get("last_overall")
        averages = u.get("last_averages", {})
        if overall is None or not averages:
            bot.answer_callback_query(call.id, "Сначала отправь Excel 🙂")
            return

        best = max(averages, key=averages.get)
        worst = min(averages, key=averages.get)

        text = (
            f"📊 Средний балл: {overall:.2f}\n"
            f"🏆 Лучший предмет: {best}\n"
            f"⚠ Самый слабый предмет: {worst}"
        )
        bot.send_message(chat_id, text, reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    if call.data == "details":
        averages = u.get("last_averages", {})
        if not averages:
            bot.answer_callback_query(call.id, "Сначала отправь Excel 🙂")
            return

        lines = ["📚 Отчёт по предметам:"]
        for subj, avg in sorted(averages.items(), key=lambda x: x[0]):
            lines.append(f"• {subj}: {avg:.2f}")
        bot.send_message(chat_id, "\n".join(lines), reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    if call.data == "refresh":
        bot.send_message(chat_id, "🔄 Ок! Пришли новый Excel-файл (.xlsx).", reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    if call.data == "reminders":
        enabled = bool(u.get("reminder_enabled"))
        t = u.get("reminder_time") or "не задано"
        text = f"⏰ Напоминания\nСтатус: {'включены ✅' if enabled else 'выключены ⛔'}\nВремя: {t}"
        bot.send_message(chat_id, text, reply_markup=reminders_kb(enabled))
        bot.answer_callback_query(call.id)
        return

    if call.data == "rem_toggle":
        u["reminder_enabled"] = not bool(u.get("reminder_enabled"))
        if not u["reminder_enabled"]:
            unschedule_user_reminder(chat_id)
        else:
            if u.get("reminder_time"):
                schedule_user_reminder(chat_id, u["reminder_time"])
        save_data(data)

        enabled = bool(u.get("reminder_enabled"))
        bot.send_message(chat_id, "Готово ✅", reply_markup=reminders_kb(enabled))
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("time_"):
        hhmm = call.data.replace("time_", "")
        u["reminder_time"] = hhmm
        if u.get("reminder_enabled"):
            schedule_user_reminder(chat_id, hhmm)
        save_data(data)

        enabled = bool(u.get("reminder_enabled"))
        bot.send_message(chat_id, f"✅ Время установлено: {hhmm}", reply_markup=reminders_kb(enabled))
        bot.answer_callback_query(call.id)
        return

    if call.data == "time_custom":
        u["awaiting_time"] = True
        save_data(data)
        bot.send_message(chat_id, "Напиши время в формате HH:MM (например 18:30).")
        bot.answer_callback_query(call.id)
        return

    if call.data == "back":
        bot.send_message(chat_id, "Меню:", reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    bot.answer_callback_query(call.id)

# ----------------- Ввод своего времени -----------------
@bot.message_handler(content_types=["text"])
def on_text(message):
    chat_id = message.chat.id
    u = get_user(chat_id)

    if u.get("awaiting_time"):
        txt = (message.text or "").strip()
        u["awaiting_time"] = False

        try:
            hh, mm = txt.split(":")
            hh_i, mm_i = int(hh), int(mm)
            if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59):
                raise ValueError
        except Exception:
            save_data(data)
            bot.send_message(chat_id, "❌ Неправильный формат. Пример: 18:30")
            return

        u["reminder_time"] = txt
        if u.get("reminder_enabled"):
            schedule_user_reminder(chat_id, txt)

        save_data(data)
        bot.send_message(chat_id, f"✅ Время установлено: {txt}", reply_markup=menu_kb())
        return

    bot.send_message(chat_id, "Выбери действие кнопками 👇", reply_markup=menu_kb())

print("Бот запущен...")
bot.infinity_polling()


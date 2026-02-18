import json
import os
import time
import threading
from collections import Counter
from typing import Dict, List, Tuple, Any, Optional

import requests
import telebot
import telebot.apihelper as apihelper
from telebot import types
from openpyxl import load_workbook
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask

# ================== НАСТРОЙКИ ==================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing in environment variables")

DATA_FILE = "users.json"
DEFAULT_TZ = "Europe/Berlin"
SEP = "||"  # разделитель для ключей Counter, чтобы JSON мог сохранить
HISTORY_LIMIT = 60  # сколько снимков хранить
PORT_DEFAULT = "10000"

# Telegram timeouts (чтобы меньше отваливалось)
apihelper.CONNECT_TIMEOUT = 10
apihelper.READ_TIMEOUT = 30

bot = telebot.TeleBot(BOT_TOKEN)

# ================== safe_send: чтобы бот не падал при сбое сети ==================
def safe_send(chat_id: int, text: str, reply_markup=None, tries: int = 3):
    for i in range(tries):
        try:
            return bot.send_message(chat_id, text, reply_markup=reply_markup)
        except (requests.exceptions.RequestException, ConnectionError):
            time.sleep(2 + i * 2)
        except Exception:
            time.sleep(1)
    return None


# ================== Flask (порт-заглушка для Render Web Service) ==================
app = Flask(__name__)

@app.get("/")
def home():
    return "OK", 200

def run_web():
    port = int(os.environ.get("PORT", PORT_DEFAULT))
    app.run(host="0.0.0.0", port=port)

threading.Thread(target=run_web, daemon=True).start()

# ================== Scheduler ==================
scheduler = BackgroundScheduler(timezone=DEFAULT_TZ)
scheduler.start()
scheduled_jobs: Dict[int, str] = {}

# ================== Хранилище ==================
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
            "history": [],             # список снимков: ts, overall, averages
        }
        data[str(chat_id)] = u
        save_data(data)
    else:
        # совместимость
        u.setdefault("grades_counter", {})
        u.setdefault("last_overall", None)
        u.setdefault("last_averages", {})
        u.setdefault("reminder_enabled", False)
        u.setdefault("reminder_time", None)
        u.setdefault("awaiting_time", False)
        u.setdefault("history", [])
    return u


# ================== Excel -> оценки ==================
def parse_excel_grades(file_path: str) -> List[Tuple[str, int]]:
    """
    col0 = предмет, дальше оценки и 'Н'. Берём только числа.
    Возвращаем список (предмет, оценка).
    """
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


# ================== Counter (JSON-safe) ==================
def make_counter(items: List[Tuple[str, int]]) -> Counter:
    """
    Храним ключами строки: "Предмет||5" -> количество.
    """
    c = Counter()
    for subj, grade in items:
        c[f"{subj}{SEP}{grade}"] += 1
    return c

def parse_counter_key(key: str) -> Tuple[str, int]:
    subj, grade = key.split(SEP, 1)
    return subj, int(grade)

def diff_new_grades(old: Counter, new: Counter) -> List[Tuple[str, int, int]]:
    """
    (предмет, оценка, сколько раз добавилось)
    """
    added = []
    for key, new_count in new.items():
        old_count = old.get(key, 0)
        if new_count > old_count:
            subj, grade = parse_counter_key(key)
            added.append((subj, grade, new_count - old_count))
    added.sort(key=lambda x: (x[0], x[1]))
    return added


# ================== UI: inline кнопки ==================
def menu_kb() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("📊 Общий анализ", callback_data="summary"),
        types.InlineKeyboardButton("📚 Подробный отчёт", callback_data="details"),
    )
    kb.add(
        types.InlineKeyboardButton("📈 Динамика", callback_data="trend"),
        types.InlineKeyboardButton("🔄 Обновить данные", callback_data="refresh"),
    )
    kb.add(types.InlineKeyboardButton("⏰ Напоминания", callback_data="reminders"))
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

def subjects_kb(subjects: List[str], page: int = 0, per_page: int = 8) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    subjects_sorted = sorted(subjects)
    start = page * per_page
    chunk = subjects_sorted[start:start + per_page]

    for s in chunk:
        kb.add(types.InlineKeyboardButton(s, callback_data=f"subj:{s}"))

    nav = []
    if page > 0:
        nav.append(types.InlineKeyboardButton("⬅️", callback_data=f"subjpage:{page-1}"))
    if start + per_page < len(subjects_sorted):
        nav.append(types.InlineKeyboardButton("➡️", callback_data=f"subjpage:{page+1}"))
    if nav:
        kb.row(*nav)

    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="trend"))
    return kb


# ================== Напоминания ==================
def reminder_job(chat_id: int):
    safe_send(chat_id, "⏰ Пора обновить оценки: отправь свежий Excel-файл (.xlsx).")

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


# ================== Команды ==================
@bot.message_handler(commands=["start"])
def start(message):
    get_user(message.chat.id)
    safe_send(
        message.chat.id,
        "Привет! 👋\n"
        "Отправь Excel (.xlsx) с оценками — я сделаю анализ.\n"
        "Управляй через кнопки ниже.",
        reply_markup=menu_kb()
    )


# ================== Приём файла ==================
@bot.message_handler(content_types=["document"])
def on_document(message):
    file_name = message.document.file_name or ""
    if not file_name.lower().endswith(".xlsx"):
        safe_send(message.chat.id, "Нужен файл формата .xlsx 🙂", reply_markup=menu_kb())
        return

    try:
        file_info = bot.get_file(message.document.file_id)
        raw = bot.download_file(file_info.file_path)
    except Exception:
        safe_send(message.chat.id, "Не получилось скачать файл. Попробуй ещё раз 🙂", reply_markup=menu_kb())
        return

    tmp_name = f"{message.from_user.id}_{int(time.time())}.xlsx"
    with open(tmp_name, "wb") as f:
        f.write(raw)

    try:
        items = parse_excel_grades(tmp_name)
        rep = analyze_items(items)
        if not rep:
            safe_send(message.chat.id, "Не нашёл оценок в файле 😔", reply_markup=menu_kb())
            return

        u = get_user(message.chat.id)

        old_counter = Counter(u.get("grades_counter", {}))
        new_counter = make_counter(items)
        added = diff_new_grades(old_counter, new_counter)

        # сохранить новое состояние
        u["grades_counter"] = dict(new_counter)  # JSON-safe
        u["last_overall"] = rep["overall"]
        u["last_averages"] = rep["averages"]

        # история (снимок)
        stamp = time.strftime("%Y-%m-%d %H:%M")
        u.setdefault("history", []).append({
            "ts": stamp,
            "overall": rep["overall"],
            "averages": rep["averages"],
        })
        u["history"] = u["history"][-HISTORY_LIMIT:]

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

        safe_send(message.chat.id, msg, reply_markup=menu_kb())

    finally:
        try:
            os.remove(tmp_name)
        except Exception:
            pass


# ================== Callback кнопок ==================
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
        safe_send(chat_id, text, reply_markup=menu_kb())
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
        safe_send(chat_id, "\n".join(lines), reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    if call.data == "refresh":
        safe_send(chat_id, "🔄 Ок! Пришли новый Excel-файл (.xlsx).", reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    if call.data == "reminders":
        enabled = bool(u.get("reminder_enabled"))
        t = u.get("reminder_time") or "не задано"
        text = (
            "⏰ Напоминания\n"
            f"Статус: {'включены ✅' if enabled else 'выключены ⛔'}\n"
            f"Время: {t}\n\n"
            "Выбери время кнопками или введи своё."
        )
        safe_send(chat_id, text, reply_markup=reminders_kb(enabled))
        bot.answer_callback_query(call.id)
        return

    if call.data == "rem_toggle":
        u["reminder_enabled"] = not bool(u.get("reminder_enabled"))

        if not u["reminder_enabled"]:
            unschedule_user_reminder(chat_id)
            save_data(data)
            safe_send(chat_id, "⛔ Напоминания выключены.", reply_markup=reminders_kb(False))
            bot.answer_callback_query(call.id)
            return

        # включили
        if not u.get("reminder_time"):
            save_data(data)
            safe_send(chat_id, "✅ Включил! Теперь выбери время 👇", reply_markup=reminders_kb(True))
            bot.answer_callback_query(call.id)
            return

        schedule_user_reminder(chat_id, u["reminder_time"])
        save_data(data)
        safe_send(chat_id, f"✅ Напоминания включены ({u['reminder_time']}).", reply_markup=reminders_kb(True))
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("time_"):
        hhmm = call.data.replace("time_", "")
        u["reminder_time"] = hhmm
        if u.get("reminder_enabled"):
            schedule_user_reminder(chat_id, hhmm)
        save_data(data)

        enabled = bool(u.get("reminder_enabled"))
        safe_send(chat_id, f"✅ Время установлено: {hhmm}", reply_markup=reminders_kb(enabled))
        bot.answer_callback_query(call.id)
        return

    if call.data == "time_custom":
        u["awaiting_time"] = True
        save_data(data)
        safe_send(chat_id, "Напиши время в формате HH:MM (например 18:30).")
        bot.answer_callback_query(call.id)
        return

    # ----- ДИНАМИКА -----
    if call.data == "trend":
        hist = u.get("history", [])
        if len(hist) < 2:
            bot.answer_callback_query(call.id, "Нужно минимум 2 выгрузки Excel 🙂")
            return

        lines = ["📈 Динамика среднего балла (последние 10):"]
        for h in hist[-10:]:
            lines.append(f"• {h['ts']}: {h['overall']:.2f}")

        delta = hist[-1]["overall"] - hist[-2]["overall"]
        if delta > 0:
            lines.append(f"\n✅ Стало лучше на +{delta:.2f}")
        elif delta < 0:
            lines.append(f"\n⚠️ Стало хуже на {delta:.2f}")
        else:
            lines.append("\n➖ Без изменений")

        last_av = u.get("last_averages", {})
        if last_av:
            lines.append("\nВыбери предмет для динамики 👇")
            safe_send(chat_id, "\n".join(lines), reply_markup=subjects_kb(list(last_av.keys()), page=0))
        else:
            safe_send(chat_id, "\n".join(lines), reply_markup=menu_kb())

        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("subjpage:"):
        last_av = u.get("last_averages", {})
        if not last_av:
            bot.answer_callback_query(call.id, "Нет данных. Сначала отправь Excel 🙂")
            return

        page = int(call.data.split(":", 1)[1])
        safe_send(chat_id, "Выбери предмет:", reply_markup=subjects_kb(list(last_av.keys()), page=page))
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("subj:"):
        subject = call.data.split(":", 1)[1]
        hist = u.get("history", [])
        if not hist:
            bot.answer_callback_query(call.id, "Нет истории. Сначала отправь Excel 🙂")
            return

        points = []
        for h in hist[-10:]:
            av = h.get("averages", {}).get(subject)
            if av is not None:
                points.append((h["ts"], float(av)))

        if len(points) < 2:
            safe_send(chat_id, f"По предмету «{subject}» пока мало данных (нужно минимум 2 выгрузки).", reply_markup=menu_kb())
            bot.answer_callback_query(call.id)
            return

        lines = [f"📘 Динамика по предмету: {subject} (последние 10)"]
        for ts, av in points:
            lines.append(f"• {ts}: {av:.2f}")

        delta = points[-1][1] - points[-2][1]
        if delta > 0:
            lines.append(f"\n✅ Улучшение: +{delta:.2f}")
        elif delta < 0:
            lines.append(f"\n⚠️ Ухудшение: {delta:.2f}")
        else:
            lines.append("\n➖ Без изменений")

        safe_send(chat_id, "\n".join(lines), reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    if call.data == "back":
        safe_send(chat_id, "Меню:", reply_markup=menu_kb())
        bot.answer_callback_query(call.id)
        return

    bot.answer_callback_query(call.id)


# ================== Ввод своего времени ==================
@bot.message_handler(content_types=["text"])
def on_text(message):
    chat_id = message.chat.id
    u = get_user(chat_id)

    if u.get("awaiting_time"):
        raw = (message.text or "").strip()
        u["awaiting_time"] = False

        try:
            parts = raw.split(":")
            if len(parts) != 2:
                raise ValueError
            hh_i = int(parts[0])
            mm_i = int(parts[1])
            if not (0 <= hh_i <= 23 and 0 <= mm_i <= 59):
                raise ValueError
        except Exception:
            save_data(data)
            safe_send(chat_id, "❌ Неправильный формат. Пример: 18:30")
            return

        hhmm = f"{hh_i:02d}:{mm_i:02d}"
        u["reminder_time"] = hhmm
        if u.get("reminder_enabled"):
            schedule_user_reminder(chat_id, hhmm)

        save_data(data)
        safe_send(chat_id, f"✅ Время установлено: {hhmm}", reply_markup=menu_kb())
        return

    safe_send(chat_id, "Выбери действие кнопками 👇", reply_markup=menu_kb())


print("Бот запущен...")
bot.infinity_polling(timeout=20, long_polling_timeout=20)
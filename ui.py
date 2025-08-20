# ui.py
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

KYIV_TZ = ZoneInfo("Europe/Kyiv")

def get_main_menu():
    """Постоянное меню"""
    keyboard = [
        ["📅 Мої записи", "📞 Підтримка"]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_support_message():
    return "📞 Контакт для зв'язку: +380XXXXXXXXX"

def get_my_appointments_message():
    return "Ви можете переглянути свої записи через адмінку або зараз."

def get_date_keyboard():
    """Клавиатура с 7 днями"""
    today = datetime.now(KYIV_TZ).date()
    buttons = []
    for i in range(7):
        d = today + timedelta(days=i)
        buttons.append([
            InlineKeyboardButton(
                d.strftime("%a %d.%m"),
                callback_data=f"reschedule_date_{d.strftime('%d.%m.%Y')}"
            )
        ])
    buttons.append([
        InlineKeyboardButton("❌ Відміна", callback_data="reschedule_cancel")
    ])
    return InlineKeyboardMarkup(buttons)

def get_free_slots_keyboard(slots: list, date_str: str):
    """Клавиатура со свободными слотами"""
    buttons = []
    for time_str in slots:
        buttons.append([
            InlineKeyboardButton(time_str, callback_data=f"reschedule_time_{time_str}")
        ])
    buttons.append([
        InlineKeyboardButton("⬅️ Назад", callback_data="reschedule_back_date"),
        InlineKeyboardButton("❌ Відміна", callback_data="reschedule_cancel")
    ])
    return InlineKeyboardMarkup(buttons)

def get_confirmation_keyboard(appointment_id: str):
    """Кнопки подтверждения/отмены/переноса"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment_id}"),
            InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment_id}")
        ],
        [
            InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment_id}")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_back_to_dates_button():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="reschedule_back_date")]])

def get_back_to_slots_button():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="reschedule_back_time")]])
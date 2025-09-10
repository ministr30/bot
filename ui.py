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

def get_support_message(phone_number: str = "+380XXXXXXXXX"):
    """Возвращает сообщение с контактной информацией"""
    return f"📞 Контакт для зв'язку: {phone_number}"

def format_appointments_message(client_name: str, appointments: list, services: dict) -> str:
    """Форматирует список записей клиента в красивое сообщение"""
    if not appointments:
        return "У вас немає майбутніх записів."

    msg_lines = [f"Ваші майбутні записи, {client_name}:\n"]
    for app in appointments:
        # Предполагаем, что данные приходят уже в правильном формате
        dt = datetime.fromisoformat(app['scheduled_at']).astimezone(KYIV_TZ)
        service_name = services.get(app['service_id'], "невідома послуга")
        status = "✅ Підтверджено" if app.get('client_confirmed') else "⏳ Очікує підтвердження"

        msg_lines.append(f"📅 {dt.strftime('%d.%m.%Y о %H:%M')}")
        msg_lines.append(f"   - Послуга: {service_name}")
        msg_lines.append(f"   - Статус: {status}\n")

    return "\n".join(msg_lines)

def get_my_appointments_message():
    """Устаревшая функция - используйте format_appointments_message()"""
    return "Ви можете переглянути свої записи через адмінку або зараз."

def get_date_keyboard():
    """Клавиатура с 7 днями (с украинскими днями недели)"""
    # Словарь для локализации дней недели
    days_ua = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]

    today = datetime.now(KYIV_TZ).date()
    buttons = []
    for i in range(7):
        d = today + timedelta(days=i)
        day_name = days_ua[d.weekday()]  # weekday() возвращает 0-6 (Пн-Вс)
        buttons.append([
            InlineKeyboardButton(
                f"{day_name} {d.strftime('%d.%m')}",
                callback_data=f"reschedule_date_{d.strftime('%d.%m.%Y')}"
            )
        ])
    buttons.append([
        InlineKeyboardButton("❌ Відміна", callback_data="reschedule_cancel")
    ])
    return InlineKeyboardMarkup(buttons)

def get_free_slots_keyboard(slots: list):
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
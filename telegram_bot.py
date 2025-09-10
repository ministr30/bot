#!/usr/bin/env python3
"""
Telegram-бот для подключения клиентов через номер телефона
Упрощенная версия без токенов и сложной логики
"""
import os
import re
import logging
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from supabase import create_client
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# Импортируем функцию извлечения имени
from name_extractor import extract_first_name
from apscheduler.triggers.cron import CronTrigger
from enum import Enum, auto

# Локальный модуль
from scheduler import TimeSlotScheduler

load_dotenv()

# Настройка логирования
import os
log_file_path = os.path.join(os.getcwd(), '..', 'bot.log')
print(f"📁 Путь к файлу логов: {log_file_path}")
print(f"📁 Текущая директория: {os.getcwd()}")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Настройка уровней логирования для сторонних библиотек
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('apscheduler').setLevel(logging.WARNING)
logging.getLogger('telegram.ext').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('supabase').setLevel(logging.WARNING)
logging.getLogger('postgrest').setLevel(logging.WARNING)
logging.getLogger('gotrue').setLevel(logging.WARNING)
logging.getLogger('realtime').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)



def normalize_phone(phone: str) -> str:
    """Нормализует номер телефона к формату +380..."""
    original_phone = phone
    # Убираем все пробелы, скобки, дефисы
    phone = re.sub(r'[\s\(\)\-]', '', phone)
    
    # Если номер уже в правильном формате +380...
    if phone.startswith('+380') and len(phone) == 13:
        print(f"📞 НОРМАЛИЗАЦИЯ ТЕЛЕФОНА: {original_phone} -> {phone} (уже нормализован)")
        return phone
    
    # Если номер начинается с 380 (без +)
    if phone.startswith('380') and len(phone) == 12:
        normalized = '+' + phone
        print(f"📞 НОРМАЛИЗАЦИЯ ТЕЛЕФОНА: {original_phone} -> {normalized}")
        return normalized
    
    # Если номер начинается с 0 (украинский мобильный)
    if phone.startswith('0') and len(phone) == 10:
        normalized = '+38' + phone
        print(f"📞 НОРМАЛИЗАЦИЯ ТЕЛЕФОНА: {original_phone} -> {normalized}")
        return normalized
    
    # Если номер начинается с 8 (украинский мобильный)
    if phone.startswith('8') and len(phone) == 11:
        normalized = '+3' + phone
        print(f"📞 НОРМАЛИЗАЦИЯ ТЕЛЕФОНА: {original_phone} -> {normalized}")
        return normalized
    
    # Если номер уже содержит +, но неправильный формат
    if phone.startswith('+'):
        # Убираем + и обрабатываем заново
        normalized = normalize_phone(phone[1:])
        print(f"📞 НОРМАЛИЗАЦИЯ ТЕЛЕФОНА: {original_phone} -> {normalized} (рекурсивно)")
        return normalized
    
    # Если ничего не подошло, возвращаем как есть
    print(f"📞 НОРМАЛИЗАЦИЯ ТЕЛЕФОНА: {original_phone} -> {phone} (неизменен)")
    return phone

def safe_parse_datetime(date_string: str | None) -> datetime | None:
    """Безопасно парсит datetime из строки ISO формата от Supabase."""
    if not date_string:
        return None
    try:
        # Обрабатываем дату перед парсингом
        processed_date = date_string.replace('Z', '+00:00')



        # Обрабатываем микросекунды - приводим к 6 цифрам
        import re

        # Паттерн для поиска даты с микросекундами
        # Группа 1: дата и время без микросекунд
        # Группа 2: микросекунды (может быть 1-9 цифр)
        # Группа 3: таймзона
        microsecond_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{1,9})([\+\-]\d{2}:\d{2})'

        match = re.search(microsecond_pattern, processed_date)
        if match:
            date_part = match.group(1)  # 2025-08-30T13:07:01
            microseconds = match.group(2)  # 8814
            timezone = match.group(3)     # +00:00

            # Нормализуем микросекунды до 6 цифр
            if len(microseconds) < 6:
                # Добавляем нули справа: 8814 -> 881400
                microseconds = microseconds.ljust(6, '0')
            elif len(microseconds) > 6:
                # Обрезаем до 6 цифр: 881400123 -> 881400
                microseconds = microseconds[:6]

            processed_date = f"{date_part}.{microseconds}{timezone}"

        result = datetime.fromisoformat(processed_date)
        return result

    except (ValueError, TypeError) as e:
        # В случае ошибки логируем и возвращаем None
        print(f"❌ Ошибка парсинга даты: '{date_string}' -> '{processed_date}' | Ошибка: {e}")
        logger.warning(f"⚠️ Не удалось распарсить дату: '{date_string}'. Ошибка: {e}")
        return None

def get_days_text(scheduled_date: date):
    today = datetime.now(ZoneInfo("Europe/Kyiv")).date()
    delta = (scheduled_date - today).days
    if delta == 0:
        return "сьогодні"
    elif delta == 1:
        return "завтра"
    else:
        # Украинские дни недели с предлогом "у"
        days_ua = {
            0: "у понеділок",
            1: "у вівторок",
            2: "у середу",
            3: "у четвер",
            4: "у п'ятницю",
            5: "у суботу",
            6: "у неділю"
        }
        return days_ua[scheduled_date.weekday()]

class BotState(Enum):
    NONE = auto()
    RESCHEDULE_DATE = auto()
    RESCHEDULE_TIME = auto()

class MassageReminderBot:
    def __init__(self, token, supabase_url, supabase_key, admin_chat_id):
        self.supabase = create_client(supabase_url, supabase_key)
        self.application = Application.builder().token(token).build()
        self.scheduler = AsyncIOScheduler()
        self.time_scheduler = TimeSlotScheduler(self.supabase)  # Управление слотами
        self.user_states = {}  # chat_id: {'state': BotState, ...}
        self.admin_chat_id = admin_chat_id # <-- Сохранили

        # Scheduler будет запущен после запуска polling

        self.setup_handlers()
        self.setup_scheduler()

    def setup_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("my_appointments", self.my_appointments_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(MessageHandler(filters.Regex("^📋 Мої записи$"), self.my_appointments_command))
        self.application.add_handler(MessageHandler(filters.Regex("^💬 Підтримка$"), self.support_command))
        self.application.add_handler(MessageHandler(filters.Regex("^🔄 Перенести запис$"), self.quick_reschedule_command))
        self.application.add_handler(MessageHandler(filters.Regex("^❌ Скасувати запис$"), self.quick_cancel_command))
        self.application.add_handler(MessageHandler(filters.Regex("^✅ Підтвердити запис$"), self.confirm_appointments_command))
        self.application.add_handler(MessageHandler(filters.Regex("^📊 Статистика$"), self.stats_command))
        self.application.add_handler(MessageHandler(filters.CONTACT, self.contact_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.name_handler))
        self.application.add_handler(CallbackQueryHandler(self.button_callback))

    def setup_scheduler(self):
        """Планировщик проверяет записи каждую минуту (ТЕСТИРОВАНИЕ)"""
        self.scheduler.add_job(
            self.check_and_send_reminders,
            CronTrigger(minute="*"),
            id="reminder_checker",
            replace_existing=True
        )
        print("⏰ Планировщик напоминаний настроен (каждую минуту - РЕЖИМ ТЕСТИРОВАНИЯ)")

    async def start_scheduler(self):
        """Запуск планировщика в контексте asyncio event loop"""
        try:
            self.scheduler.start()
            logger.info("✅ Планировщик успешно запущен")
        except Exception as e:
            logger.error(f"❌ Ошибка запуска планировщика: {e}")
            raise

    # ----- /start -----
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            chat_id = str(update.effective_chat.id)
            telegram_name = update.effective_chat.first_name or update.effective_chat.username or "Unknown"

            response = self.supabase.table("clients").select("name").eq("telegram_chat_id", chat_id).execute()
            main_keyboard = self.get_persistent_menu(chat_id)
            
            if response.data:
                full_name = response.data[0].get("name", "Клієнт")
                client_name = extract_first_name(full_name)
                client_id = response.data[0].get("id")
                print(f"🚀 КЛИЕНТ {client_name} ЗАПУСТИЛ БОТА (чат {chat_id})")
                print(f"✅ ПОЛЬЗОВАТЕЛЬ УЖЕ ЗАРЕГИСТРИРОВАН: {client_name} (чат {chat_id})")

                # Обновляем статистику активности пользователя
                await self.update_user_activity(chat_id, client_id, client_name, "start")

                await update.message.reply_text(
                    f"🎉 Привіт, {client_name}!\nВаш акаунт вже підключено до бота.\n\n"
                    "Тепер ви будете отримувати сповіщення про майбутні записи на масаж.\n\n"
                    "📝 Кнопки меню завжди доступні для швидких дій:\n"
                    "• 📋 Мої записи - перегляд та підтвердження\n"
                    "• ✅ Підтвердити запис - швидке підтвердження\n"
                    "• 🔄 Перенести запис - швидкий перенос\n"
                    "• ❌ Скасувати запис - швидке скасування\n"
                    "• 💬 Підтримка - зв'язатися з адміністратором",
                    reply_markup=main_keyboard
                )
            else:
                print(f"📱 НАЧАЛ РЕГИСТРАЦИЮ НОВЫЙ КЛИЕНТ {user_name} (чат {chat_id})")

                keyboard = [[KeyboardButton("📱 Поділитися номером", request_contact=True)]]
                reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
                await update.message.reply_text(
                    "👋 Ласкаво просимо до бота нагадувань про масаж!\n\n"
                    "Для підключення до бота, будь ласка, поділіться своїм номером телефону.",
                    reply_markup=reply_markup
                )
        except Exception as e:
            chat_id = str(update.effective_chat.id) if update.effective_chat else "unknown"
            user_name = update.effective_chat.first_name if update.effective_chat else "unknown"
            print(f"❌ ОШИБКА В START_COMMAND: пользователь {user_name} (чат {chat_id}) - {str(e)}")
            logger.error("❌ Ошибка в start_command", exc_info=True)
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Контакт -----
    async def contact_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            contact = update.message.contact
            phone = contact.phone_number
            chat_id = str(update.effective_chat.id)
            user_name = update.effective_chat.first_name or "Unknown"

            print(f"📱 ПОЛУЧЕН КОНТАКТ: {user_name}, телефон {phone}, ID чата {chat_id}")

            main_keyboard = self.get_persistent_menu(chat_id)
            
            client = await self.get_client_by_phone(phone)
            if client:
                print(f"🔗 ОБНОВЛЕНИЕ ЧАТА: существующий клиент {client.get('name', 'Unknown')} ({user_name}) привязан к чату {chat_id}")
                await self.update_client_telegram_id(client["id"], chat_id)
                await update.message.reply_text(
                    f"🎉 Привіт, {client.get('name', 'Клієнт')}!\nВаш акаунт підключено.\n\n"
                    "📝 Кнопки меню завжди доступні!",
                    reply_markup=main_keyboard
                )
            else:
                print(f"👤 НОВЫЙ КЛИЕНТ: начинаем процесс регистрации для телефона {phone}")
                context.user_data["pending_phone"] = phone
                context.user_data["pending_chat_id"] = chat_id
                await update.message.reply_text("✅ Новий клієнт!\n\nБудь ласка, введіть ваше ім'я:")
        except Exception as e:
            chat_id = str(update.effective_chat.id) if update.effective_chat else "unknown"
            user_name = update.effective_chat.first_name if update.effective_chat else "unknown"
            print(f"❌ ОШИБКА В CONTACT_HANDLER: пользователь {user_name} (чат {chat_id}) - {str(e)}")
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Имя -----
    async def name_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        message_text = update.message.text
        chat_id = str(update.effective_chat.id)
        user_name = update.effective_chat.first_name or "Unknown"

        if not context.user_data.get("pending_phone"):
            # Пользователь отправил произвольное сообщение, не находящееся в процессе регистрации
            print(f"💬 ПРОИЗВОЛЬНОЕ СООБЩЕНИЕ: '{message_text}' от {user_name} (ID чата: {chat_id}) - отправляю справку")

            await update.message.reply_text(
                "🤖 Привіт! Я — бот для запису на масаж.\n\n"
                "Я вмію тільки:\n"
                "📋 Показувати ваші записи\n"
                "🔄 Переносити записи\n"
                "❌ Скасовувати записи\n"
                "✅ Підтверджувати записи\n"
                "💬 Надати контакт для зв'язку\n\n"
                "📞 Якщо у вас є питання або потрібно щось інше — "
                "зателефонуйте: (096) 35-102-35\n\n"
                "📝 Використовуйте кнопки меню нижче для швидких дій!",
                reply_markup=self.get_persistent_menu(chat_id)
            )
            return
        try:
            name = update.message.text.strip()
            if len(name) < 2 or len(name) > 50 or re.search(r'[<>"\']', name):
                await update.message.reply_text("❌ Ім'я некоректне. Спробуйте ще раз:")
                return

            phone = context.user_data["pending_phone"]
            chat_id = context.user_data["pending_chat_id"]

            print(f"📝 РЕГИСТРАЦИЯ НОВОГО КЛИЕНТА: {name}, телефон {phone}, чат {chat_id}")

            await self.create_new_client(name, phone, chat_id)
            context.user_data.clear()
            
            # Provide persistent menu after successful registration
            main_keyboard = self.get_persistent_menu(chat_id)
            await update.message.reply_text(
                f"🎉 Відмінно, {name}!\nВи успішно підключені до бота.\n\n"
                "📝 Кнопки меню завжди доступні для швидких дій!",
                reply_markup=main_keyboard
            )
        except Exception as e:
            chat_id = str(update.effective_chat.id) if update.effective_chat else "unknown"
            user_name = update.effective_chat.first_name if update.effective_chat else "unknown"
            message_text = update.message.text if update.message else "unknown"
            print(f"❌ ОШИБКА В NAME_HANDLER: пользователь {user_name} (чат {chat_id}), сообщение '{message_text}' - {str(e)}")
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Callback -----
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            query = update.callback_query
            
            # Handle expired callback queries gracefully
            try:
                await query.answer()
            except Exception as callback_error:
                # If callback is expired, send a new message with fresh buttons
                if "Query is too old" in str(callback_error) or "query id is invalid" in str(callback_error):
                    await self.handle_expired_callback(query, update)
                    return
                else:
                    raise callback_error
            
            data = query.data
            chat_id = str(query.message.chat_id)

            # Логируем нажатие кнопки
            print(f"🔘 НАЖАТИЕ КНОПКИ: '{data}' от чата {chat_id}")

            # Обновляем статистику активности пользователя
            try:
                # Получаем информацию о клиенте для статистики
                client_resp = self.supabase.table("clients").select("id, name").eq("telegram_chat_id", chat_id).execute()
                if client_resp.data:
                    client_id = client_resp.data[0]['id']
                    full_name = client_resp.data[0]['name']
                    client_name = extract_first_name(full_name)
                    await self.update_user_activity(chat_id, client_id, client_name, f"button_{data[:20]}")  # Ограничиваем длину
            except Exception as stat_error:
                # Не прерываем основную логику из-за ошибки статистики
                print(f"⚠️ ОШИБКА СТАТИСТИКИ КНОПОК: {str(stat_error)}")

            user_state = self.user_states.setdefault(chat_id, {'state': BotState.NONE})

            if data == "reschedule_cancel":
                self.user_states[chat_id] = {'state': BotState.NONE}
                await query.edit_message_text("❌ Перенесення скасовано.")
                return
            if data == "reschedule_back_date":
                await self.show_reschedule_dates(query)
                user_state['state'] = BotState.RESCHEDULE_DATE
                return
            if data == "reschedule_back_time":
                date_str = user_state.get('selected_date')
                if date_str:
                    await self.show_free_slots(query, chat_id, date_str)
                    user_state['state'] = BotState.RESCHEDULE_TIME
                return

            elif data.startswith("confirm_appointment_"):
                # Обработка подтверждения перенесенной записи (более специфичное условие - должно быть первым!)
                appointment_id = data.replace("confirm_appointment_", "")
                await self.handle_appointment_confirmation(query, appointment_id)
            elif data.startswith("select_confirm_"):
                # Обработка выбора записи для подтверждения
                appointment_id = data.replace("select_confirm_", "")
                await self.show_confirm_confirmation(query, appointment_id)
            elif data.startswith("select_cancel_"):
                # Обработка выбора записи для отмены
                appointment_id = data.replace("select_cancel_", "")
                await self.show_cancel_confirmation(query, appointment_id)
            elif data.startswith("select_reschedule_"):
                # Обработка выбора записи для переноса
                appointment_id = data.replace("select_reschedule_", "")
                await self.show_reschedule_dates_for_appointment(query, appointment_id)
            elif data.startswith("confirm_"):
                await self.handle_confirmation(query, data.replace("confirm_", ""), "confirm")
            elif data.startswith("cancel_"):
                await self.handle_confirmation(query, data.replace("cancel_", ""), "cancel")
            elif data.startswith("reschedule_time_"):
                if user_state.get('state') == BotState.RESCHEDULE_TIME:
                    time_str = data.replace("reschedule_time_", "")
                    await self.handle_reschedule_time(query, context, chat_id, time_str)
                else:
                    await query.edit_message_text("❌ Невідома дія")
            elif data.startswith("reschedule_date_"):
                if user_state.get('state') == BotState.RESCHEDULE_DATE:
                    date_str = data.replace("reschedule_date_", "")
                    date_dt = datetime.strptime(date_str, "%d.%m.%Y").date()
                    if date_dt.weekday() == 6:
                        buttons = [[InlineKeyboardButton("ОК", callback_data="reschedule_back_date")]]
                        reply_markup = InlineKeyboardMarkup(buttons)
                        await query.edit_message_text("❌ Запис на неділю неможливий. Оберіть іншу дату.", reply_markup=reply_markup)
                        return
                    user_state['selected_date'] = date_str
                    user_state['state'] = BotState.RESCHEDULE_TIME
                    await self.show_free_slots(query, chat_id, date_str)
            elif data == "contact_admin":
                # Обработка запроса на связь с администратором
                await query.edit_message_text(
                    "📞 Зв'язатися з адміністратором:\n\n"
                    "📱 Телефон: (096) 35-102-35\n"
                    "💬 Пишіть або телефонуйте в будь-який час!",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]])
                )
            elif data == "back_to_main":
                # Возврат в главное меню
                await query.edit_message_text(
                    "Оберіть дію:",
                    reply_markup=self.get_persistent_menu(chat_id)
                )
            elif data.startswith("reschedule_"):
                appointment_id = data.replace("reschedule_", "")
                # Проверяем, является ли это ответом на уведомление администратора
                appointment_response = self.supabase.table("appointments").select("reschedule_source").eq("id", appointment_id).execute()
                if appointment_response.data and appointment_response.data[0].get('reschedule_source') == 'ADMIN':
                    # Это ответ на уведомление администратора - показываем сообщение
                    await query.edit_message_text("🔄 Для перенесення запису, будь ласка, зв'яжіться з адміністратором або використайте команду /reschedule.")
                    return
                
                user_state['appointment_id'] = appointment_id
                user_state['state'] = BotState.RESCHEDULE_DATE
                await self.show_reschedule_dates(query)
            else:
                await query.edit_message_text("❌ Невідома дія")
        except Exception as e:
            data_info = data if 'data' in locals() else "unknown"
            chat_id = str(update.effective_chat.id) if update.effective_chat else "unknown"
            user_name = update.effective_chat.first_name if update.effective_chat else "unknown"
            print(f"❌ ОШИБКА В BUTTON_CALLBACK: пользователь {user_name} (чат {chat_id}), кнопка '{data_info}' - {str(e)}")
            logger.error("❌ Ошибка в button_callback", exc_info=True)
            try:
                # Try to handle expired callbacks with fresh message
                if "Query is too old" in str(e) or "query id is invalid" in str(e):
                    await self.handle_expired_callback(update.callback_query, update)
                else:
                    await update.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                # If edit fails, send new message with persistent menu
                try:
                    await update.effective_chat.send_message(
                        "❌ Час дії кнопок минув. Скористайтеся меню нижче:",
                        reply_markup=self.get_persistent_menu(chat_id)
                    )
                except Exception:
                    pass

    async def handle_expired_callback(self, query, update):
        """Handle expired callback queries by providing alternative options"""
        try:
            chat_id = str(query.message.chat_id)
            
            # Try to extract appointment ID from the callback data if possible
            data = query.data if hasattr(query, 'data') else ''
            appointment_id = None
            
            if data.startswith("confirm_"):
                appointment_id = data.replace("confirm_", "")
                action_text = "підтвердити запис"
            elif data.startswith("cancel_"):
                appointment_id = data.replace("cancel_", "")
                action_text = "скасувати запис"
            elif data.startswith("reschedule_"):
                appointment_id = data.replace("reschedule_", "")
                action_text = "перенести запис"
            else:
                action_text = "виконати дію"
            
            # Send new message with fresh persistent buttons and instructions
            message = (
                f"⏰ Час дії кнопок минув.\n\n"
                f"Щоб {action_text}, скористайтеся кнопками меню нижче або командами:\n\n"
                f"📋 Мої записи - переглянути всі записи\n"
                f"🔄 Перенести запис - швидке перенесення\n"
                f"❌ Скасувати запис - швидке скасування"
            )
            
            # Add persistent reply keyboard
            persistent_keyboard = self.get_persistent_menu(chat_id)
            
            # If we have appointment ID, also add fresh inline buttons
            if appointment_id:
                # Create fresh inline buttons
                fresh_buttons = [
                    [
                        InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment_id}"),
                        InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment_id}")
                    ],
                    [
                        InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment_id}")
                    ]
                ]
                fresh_inline_markup = InlineKeyboardMarkup(fresh_buttons)
                
                await update.effective_chat.send_message(
                    message + "\n\nАбо скористайтеся свіжими кнопками:",
                    reply_markup=persistent_keyboard
                )
                
                # Send fresh inline buttons in separate message
                await update.effective_chat.send_message(
                    "🔄 Свіжі кнопки для швидких дій:",
                    reply_markup=fresh_inline_markup
                )
            else:
                await update.effective_chat.send_message(
                    message,
                    reply_markup=persistent_keyboard
                )
                
        except Exception as e:
            print(f"❌ Помилка при обробці застарілого callback: {e}")
            # Fallback - just send persistent menu
            try:
                await update.effective_chat.send_message(
                    "❌ Час дії кнопок минув. Скористайтеся меню:",
                    reply_markup=self.get_persistent_menu(chat_id)
                )
            except Exception:
                pass

    def get_persistent_menu(self, chat_id=None):
        """Get persistent reply keyboard menu"""
        # Базовое меню для всех пользователей
        menu_buttons = [
                ["📋 Мої записи", "💬 Підтримка"],
            ["🔄 Перенести запис", "❌ Скасувати запис"],
            ["✅ Підтвердити запис"]
        ]

        # Добавляем кнопку статистики для администратора
        if chat_id and str(chat_id) == str(self.admin_chat_id):
            menu_buttons.append(["📊 Статистика"])

        return ReplyKeyboardMarkup(
            menu_buttons,
            resize_keyboard=True,
            one_time_keyboard=False
        )

    # ----- Подтверждение/отмена -----
    async def handle_confirmation(self, query, appointment_id: str, action: str):
        try:
            # Получаем ID клиента для его имени
            appointment_resp = self.supabase.table("appointments").select("client_id").eq("id", appointment_id).execute()
            if not appointment_resp.data:
                await query.edit_message_text("❌ Запис не знайдено")
                return

            client_id = appointment_resp.data[0]['client_id']
            client_resp = self.supabase.table("clients").select("name").eq("id", client_id).execute()
            full_name = client_resp.data[0]['name'] if client_resp.data else "Клієнт"
            client_name = extract_first_name(full_name)

            # --- Загальна частина для отримання деталей запису ---
            # Ми отримуємо деталі запису ДО того, як вирішити, що робити далі.
            appointment_details_resp = self.supabase.table("appointments").select(
                "scheduled_at, service_id"
            ).eq("id", appointment_id).execute()

            service_name = "масаж"
            formatted_date = ""
            formatted_time = ""
            details_available = False

            if appointment_details_resp.data:
                appointment = appointment_details_resp.data[0]
                service_resp = self.supabase.table("services").select("name").eq("id", appointment['service_id']).execute()
                if service_resp.data:
                    service_name = service_resp.data[0]['name']

                dt_raw = safe_parse_datetime(appointment.get('scheduled_at'))
                if dt_raw:
                    scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
                    formatted_date = scheduled_at.strftime("%d.%m.%Y")
                    formatted_time = scheduled_at.strftime("%H:%M")
                    details_available = True # Флаг, что у нас есть все данные

            # --- Логика для CONFIRM (Подтверждение) ---
            if action == "confirm":
                self.supabase.table("appointments").update({
                    "client_confirmed": True,
                    "confirmation_time": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat()
                }).eq("id", appointment_id).execute()

                await self.send_confirmation_notification(query, appointment_id, client_name)

                # Формируем сообщение для админа
                admin_message = f"✅ Клієнт {client_name} підтвердив запис."
                if details_available:
                    admin_message = f"✅ Клієнт {client_name} підтвердив запис\n📅 {formatted_date} о {formatted_time}\n💆 {service_name}"

                await self.application.bot.send_message(chat_id=self.admin_chat_id, text=admin_message)

            # --- Логика для CANCEL (Отмена) ---
            elif action == "cancel":
                # Оновлюємо статус запису на скасований
                update_result = self.supabase.table("appointments").update({
                    "status": "cancelled",
                    "notes": f"Скасовано клієнтом через бота {datetime.now(ZoneInfo('Europe/Kyiv')).strftime('%d.%m.%Y %H:%M')}"
                }).eq("id", appointment_id).execute()

                # Проверяем, что обновление прошло успешно
                if update_result.data:
                    logger.info(f"✅ Статус запису {appointment_id} успішно оновлено на 'cancelled'")
                else:
                    logger.error(f"❌ Помилка оновлення статусу запису {appointment_id}")

                # Проверяем актуальный статус после обновления
                check_response = self.supabase.table("appointments").select("status, notes").eq("id", appointment_id).execute()
                if check_response.data:
                    actual_status = check_response.data[0]['status']
                    actual_notes = check_response.data[0].get('notes', '')
                    logger.info(f"📊 Актуальний статус запису {appointment_id}: {actual_status}")
                    logger.info(f"📝 Примітки до запису: {actual_notes}")

                    if actual_status != "cancelled":
                        logger.error(f"❌ Статус запису {appointment_id} не було оновлено! Поточний статус: {actual_status}")
                        # Спробуємо оновити статус ще раз
                        retry_result = self.supabase.table("appointments").update({
                            "status": "cancelled"
                        }).eq("id", appointment_id).execute()
                        if retry_result.data:
                            logger.info(f"✅ Повторне оновлення статусу запису {appointment_id} виконано успішно")
                        else:
                            logger.error(f"❌ Повторне оновлення статусу запису {appointment_id} не вдалося")
                    else:
                        logger.info(f"✅ Статус запису {appointment_id} коректно встановлено на 'cancelled'")
                else:
                    logger.error(f"❌ Не вдалося перевірити статус запису {appointment_id}")

                await self.send_cancellation_notification(query, appointment_id, client_name)

                # Формуємо повідомлення для адміна
                admin_message = f"❌ Клієнт {client_name} скасував запис."
                if details_available:
                    admin_message = f"❌ Клієнт {client_name} скасував запис\n📅 {formatted_date} о {formatted_time}\n💆 {service_name}"

                await self.application.bot.send_message(chat_id=self.admin_chat_id, text=admin_message)

        except Exception as e:
            logger.error("❌ Помилка в handle_confirmation", exc_info=True)
            try:
                await query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_reschedule_dates(self, query):
        today = datetime.now(ZoneInfo("Europe/Kyiv")).date()
        buttons = []
        for i in range(7):  # 7 дней вперёд
            d = today + timedelta(days=i)
            # Пропускаем воскресенье
            if d.weekday() == 6:
                continue
            buttons.append([
                InlineKeyboardButton(
                    d.strftime("%d.%m.%Y"),
                    callback_data=f"reschedule_date_{d.strftime('%d.%m.%Y')}"
                )
            ])
        buttons.append([
            InlineKeyboardButton("❌ Відміна", callback_data="reschedule_cancel")
        ])
        reply_markup = InlineKeyboardMarkup(buttons)
        await query.edit_message_text("Оберіть нову дату для запису:", reply_markup=reply_markup)

    async def show_free_slots(self, query, chat_id, date_str):
        try:
            await query.edit_message_text("⏳ Обробка...", reply_markup=None)
            import time
            t0 = time.time()
            date_dt = datetime.strptime(date_str, "%d.%m.%Y").date()

            if date_dt.weekday() == 6:
                await query.edit_message_text("❌ Запис на неділю неможливий. Оберіть іншу дату.")
                return

            user_state = self.user_states.get(chat_id, {})
            appointment_id = user_state.get('appointment_id')
            if not appointment_id:
                await query.edit_message_text("❌ Не вдалося отримати дані запису.")
                return

            old_appointment = self.supabase.table("appointments").select("service_id, appointment_type, total_duration").eq("id", appointment_id).execute().data
            if not old_appointment:
                await query.edit_message_text("❌ Запис не знайдено.")
                return

            appointment = old_appointment[0]
            appointment_type = appointment.get('appointment_type', 'single')

            # Определяем длительность в зависимости от типа записи
            if appointment_type == 'package':
                duration = appointment.get('total_duration', 60)
                free_slots = await self.time_scheduler.get_free_slots_for_package(date_dt, duration)
            else:
                service_id = appointment['service_id']
                service_resp = self.supabase.table("services").select("duration_minutes").eq("id", service_id).execute()
                duration = service_resp.data[0]['duration_minutes'] if service_resp.data else 60
                free_slots = await self.time_scheduler.get_free_slots(date_dt, duration)
            t1 = time.time()
            print(f"[PERF] Расчет свободных слотов занял {t1-t0:.2f} сек")

            if not free_slots:
                await query.edit_message_text("❌ Немає вільних слотів на цю дату.")
                return

            formatted_slots = [dt.strftime("%H:%M") for dt in free_slots]
            buttons = [[InlineKeyboardButton(t, callback_data=f"reschedule_time_{t}")] for t in formatted_slots]
            buttons.append([
                InlineKeyboardButton("⬅️ Назад", callback_data="reschedule_back_date"),
                InlineKeyboardButton("❌ Відміна", callback_data="reschedule_cancel")
            ])
            reply_markup = InlineKeyboardMarkup(buttons)
            await query.edit_message_text(f"Оберіть час для запису на {date_str}:", reply_markup=reply_markup)

        except Exception as e:
            logger.error("❌ Ошибка в show_free_slots", exc_info=True)
            try:
                await query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def handle_reschedule_time(self, query, context, chat_id, time_str):
        try:
            await query.edit_message_text("⏳ Обробка...", reply_markup=None)
            user_state = self.user_states.get(chat_id, {})
            appointment_id = user_state.get('appointment_id')
            date_str = user_state.get('selected_date')

            if not appointment_id or not date_str:
                await query.edit_message_text("❌ Дані для перенесення не знайдено.")
                return

            date_dt = datetime.strptime(date_str, "%d.%m.%Y").date()
            new_time = datetime.strptime(time_str, "%H:%M").time()
            new_dt_kyiv = datetime.combine(date_dt, new_time).replace(tzinfo=ZoneInfo("Europe/Kyiv"))
            new_dt_utc = new_dt_kyiv.astimezone(ZoneInfo("UTC"))

            old_appointment = self.supabase.table("appointments").select("*").eq("id", appointment_id).execute().data
            if not old_appointment:
                await query.edit_message_text("❌ Запис не знайдено.")
                return

            old = old_appointment[0]
            # Форматируем оригинальную дату и время
            old_dt_raw = safe_parse_datetime(old['scheduled_at'])
            if not old_dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={old.get('id')}")
                await query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
                return
            old_dt = old_dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            old_date_str = old_dt.strftime("%d.%m.%Y")
            old_time_str = old_dt.strftime("%H:%M")

            # Определяем длительность в зависимости от типа записи
            appointment_type = old.get('appointment_type', 'single')
            if appointment_type == 'package':
                duration = old.get('total_duration', 60)
                free_slots = await self.time_scheduler.get_free_slots_for_package(date_dt, duration)
            else:
                service_id = old['service_id']
                service_resp = self.supabase.table("services").select("duration_minutes").eq("id", service_id).execute()
                duration = service_resp.data[0]['duration_minutes'] if service_resp.data else 60
                free_slots = await self.time_scheduler.get_free_slots(date_dt, duration)
            allowed_times = [dt.strftime("%H:%M") for dt in free_slots]

            if time_str not in allowed_times:
                await query.edit_message_text("❌ Цей слот більше недоступний. Оберіть інший.")
                return

            # Получаем информацию об услуге для корректного ценообразования
            service_resp = self.supabase.table("services").select("duration_minutes, price, name").eq("id", service_id).execute()
            service_info = service_resp.data[0] if service_resp.data else None
            base_price = service_info['price'] if service_info else old['price']
            service_name = service_info['name'] if service_info else "масаж"
            
            # Определяем день недели исходной и новой записи для правильного ценообразования
            original_date = old_dt.date()
            original_is_sunday = original_date.weekday() == 6
            new_is_sunday = date_dt.weekday() == 6
            
            # Рассчитываем правильную цену
            if new_is_sunday:
                # Если переносим на воскресенье - добавляем надбавку к базовой цене
                final_price = base_price + 100.0
            elif original_is_sunday and not new_is_sunday:
                # Если переносим с воскресенья на будний день - возвращаем базовую цену
                final_price = base_price
            else:
                # В остальных случаях сохраняем текущую цену записи
                final_price = old['price']
            
            # Обрабатываем заметки: убираем все существующие сообщения о переносе
            old_notes = old.get('notes', '') or ''
            import re
            clean_notes = old_notes.strip()
            clean_notes = re.sub(r'ПЕРЕНЕСЕНО АДМИНИСТРАТОРОМ,?\s*', '', clean_notes, flags=re.IGNORECASE)
            clean_notes = re.sub(r'ПЕРЕНЕСЕНО КЛИЕНТОМ,?\s*', '', clean_notes, flags=re.IGNORECASE)
            clean_notes = re.sub(r'ПЕРЕНЕСЕНО КЛІЄНТОМ,?\s*', '', clean_notes, flags=re.IGNORECASE)
            clean_notes = re.sub(r'^,\s*', '', clean_notes, flags=re.IGNORECASE)  # Убираем начальные запятые
            clean_notes = re.sub(r',\s*$', '', clean_notes, flags=re.IGNORECASE)  # Убираем конечные запятые
            clean_notes = clean_notes.strip()
            
            # Используем только очищенные пользовательские заметки
            notes = clean_notes
            reschedule_source = "CLIENT"  # Новое поле для отслеживания источника переноса
            
            print(f"🔄 Telegram Bot: Обрабатываем клиентский перенос записи:")
            print(f"   - ID: {appointment_id}")
            print(f"   - Оригинальная дата: {original_date} (воскресенье: {original_is_sunday})")
            print(f"   - Новая дата: {date_dt} (воскресенье: {new_is_sunday})")
            print(f"   - Базовая цена услуги: {base_price}")
            print(f"   - Оригинальная цена записи: {old['price']}")
            print(f"   - Новая цена: {final_price}")
            print(f"   - Оригинальные заметки: '{old_notes}'")
            print(f"   - Очищенные заметки: '{clean_notes}'")
            print(f"   - Новые заметки: '{notes}'")
            print(f"   - Источник переноса: {reschedule_source}")
            # Обновляем существующую запись вместо создания новой
            update_data = {
                "scheduled_at": new_dt_utc.isoformat(),
                "notes": notes,
                "price": final_price,
                "status": "scheduled",
                "reschedule_source": reschedule_source,  # 🔽 Новое поле
                "client_confirmed": False,  # 🔽 Сбрасываем подтверждение при переносе
                "confirmation_time": None   # 🔽 Очищаем время подтверждения
            }
            self.supabase.table("appointments").update(update_data).eq("id", appointment_id).execute()

            # Получаем данные клиента
            client = self.supabase.table("clients").select("name").eq("id", old['client_id']).execute().data
            full_name = client[0]['name'] if client else "Клієнт"
            client_name = extract_first_name(full_name)
            
            print(f"🔄 Запись перенесена: {appointment_id}, статус подтверждения сброшен")
            
            # Отправляем объединенное уведомление клиенту о переносе с кнопками подтверждения
            new_date_str = date_str
            new_time_str = new_dt_kyiv.strftime("%H:%M")
            await self.send_client_reschedule_notification(
                query, appointment_id, client_name, 
                old_date_str, old_time_str, 
                new_date_str, new_time_str
            )
            
            # Уведомляем администратора
            await self.application.bot.send_message(
                chat_id=self.admin_chat_id,
                text=f"🔄 Клієнт {client_name} переніс запис\n💆 {service_name}\n📅 З {old_date_str} о {old_time_str} на {new_date_str} о {new_time_str}."
            )

            self.user_states[chat_id] = {'state': BotState.NONE}
        except Exception as e:
            logger.error("❌ Ошибка в handle_reschedule_time", exc_info=True)
            try:
                await query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Supabase helpers -----
    async def get_client_by_phone(self, phone: str):
        normalized_phone = normalize_phone(phone)
        response = self.supabase.table("clients").select("*").eq("phone", normalized_phone).execute()
        return response.data[0] if response.data else None

    async def create_new_client(self, name: str, phone: str, chat_id: str):
        normalized_phone = normalize_phone(phone)
        print(f"🆕 СОЗДАНИЕ КЛИЕНТА В БАЗЕ: {name}, {normalized_phone}, чат {chat_id}")

        data = {
            "name": name,
            "phone": normalized_phone,
            "telegram_chat_id": chat_id,
            "notification_opt_in": True,
            "timezone": "Europe/Kyiv",
            "telegram_linked_at": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat(),
            "created_at": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat()
        }
        response = self.supabase.table("clients").insert(data).execute()

        if response.data:
            client_id = response.data[0]['id']
            print(f"✅ КЛИЕНТ СОЗДАН: ID {client_id} для {name}")
        else:
            print(f"❌ ОШИБКА СОЗДАНИЯ КЛИЕНТА: {name}")

        return response.data[0] if response.data else None

    async def update_client_telegram_id(self, client_id: str, chat_id: str):
        data = {
            "telegram_chat_id": chat_id,
            "telegram_linked_at": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat(),
            "notification_opt_in": True
        }
        response = self.supabase.table("clients").update(data).eq("id", client_id).execute()
        return len(response.data) > 0

    # ----- Напоминания -----
    async def check_and_send_reminders(self):
        try:
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            
            # Добавляем защиту от одновременного запуска
            if hasattr(self, '_reminder_check_in_progress') and self._reminder_check_in_progress:
                print("⏰ ПРОВЕРКА НАПОМИНАНИЙ УЖЕ ВЫПОЛНЯЕТСЯ - ПРОПУСКАЮ")
                return
            
            self._reminder_check_in_progress = True
            print(f"🚀 НАЧИНАЮ ПРОВЕРКУ НАПОМИНАНИЙ: {now.strftime('%d.%m.%Y %H:%M:%S')}")
            
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours(now):
                print(f"⏰ ВНЕ РАБОЧЕГО ВРЕМЕНИ: проверка напоминаний пропускается")
                self._reminder_check_in_progress = False
                return
            
            # Проверяем новые записи (созданные ровно 2-2.25 часа назад)
            two_hours_ago = now - timedelta(hours=2)
            two_hours_fifteen_minutes_ago = now - timedelta(hours=2, minutes=15)
            print(f"🔍 ИЩУ НОВЫЕ ЗАПИСИ: проверяю записи, созданные с {two_hours_fifteen_minutes_ago.strftime('%H:%M')} до {two_hours_ago.strftime('%H:%M')}")

            # Получаем записи, созданные 2-2.25 часа назад (15-минутное окно для обработки)
            new_appointments_response = self.supabase.table("appointments").select(
                "id, created_at, appointment_type, total_duration"
            ).eq("status", "scheduled").gte("created_at", two_hours_fifteen_minutes_ago.isoformat()).lt("created_at", two_hours_ago.isoformat()).execute()

            # Фильтруем только те, для которых еще не отправлено уведомление
            filtered_appointments = []
            if new_appointments_response.data:
                for appointment in new_appointments_response.data:
                    # Проверяем, было ли уже отправлено уведомление
                    if await self.check_notification_sent(appointment['id'], "new_appointment"):
                        logger.debug(f"📭 Уведомление для записи {appointment['id']} уже отправлено")
                        continue

                    # Запись подходит для уведомления (создана 2 часа назад)
                    filtered_appointments.append(appointment)
                    logger.debug(f"📭 Запись {appointment['id']} подходит для уведомления (создана: {appointment['created_at']})")

            # Заменяем оригинальный ответ отфильтрованным
            new_appointments_response.data = filtered_appointments

            if new_appointments_response.data:
                logger.info(f"📭 Знайдено {len(new_appointments_response.data)} нових записів")
                for appointment in new_appointments_response.data:
                    logger.info(f"📭 Обрабатываем новую запись: {appointment['id']}, создана: {appointment['created_at']}")
                    await self.send_new_appointment_notification(appointment['id'])
            else:
                logger.info("📭 Нових записів немає")
            
            # Проверяем переносы записей администратором
            admin_reschedule_response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, reschedule_source, client_confirmed"
            ).eq("status", "scheduled").eq("reschedule_source", "ADMIN").eq("client_confirmed", False).execute()

            if admin_reschedule_response.data:
                print(f"🔄 Знайдено {len(admin_reschedule_response.data)} перенесених адміністратором записів")
                for appointment in admin_reschedule_response.data:
                    # Проверяем, нужно ли отправлять уведомление
                    if await self.should_send_admin_reschedule_notification(appointment['id'], appointment['scheduled_at']):
                        await self.send_admin_reschedule_notification(appointment['id'])
                    else:
                        print(f"⏰ Уведомление о переносе для записи {appointment['id']} уже отправлено")
            else:
                print("🔄 Перенесених адміністратором записів немає")
            
            # Проверяем 24-часовые напоминания (завтра)
            tomorrow_start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_end = tomorrow_start + timedelta(days=1)

            response_24h = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, appointment_type, total_duration"
            ).eq("status", "scheduled").gte("scheduled_at", tomorrow_start.isoformat()).lt("scheduled_at", tomorrow_end.isoformat()).execute()

            regular_24h_count = 0
            if response_24h.data:
                print(f"📭 Знайдено {len(response_24h.data)} записів для 24-годинних нагадувань")
                for appointment in response_24h.data:
                    # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убеждаемся что напоминание не отправлялось в последние 2 часа
                    # Это защитит от дублирования с отложенными уведомлениями
                    two_hours_ago = now - timedelta(hours=2)
                    recent_24h_logs = self.supabase.table("notification_logs").select(
                        "id"
                    ).eq("appointment_id", appointment['id']).eq("type", "24h").eq("status", "sent").gte("sent_at", two_hours_ago.isoformat()).execute()

                    if not recent_24h_logs.data:
                        await self.process_appointment_reminder(appointment, now, "24h")
                        regular_24h_count += 1
            else:
                print("📭 Завтра немає записів для 24-годинних нагадувань")

            # Проверяем 1-часовые напоминания (через час)
            one_hour_from_now = now + timedelta(hours=1)
            one_hour_start = one_hour_from_now.replace(second=0, microsecond=0)
            one_hour_end = one_hour_start + timedelta(minutes=15)  # 15-минутное окно

            response_1h = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, appointment_type, total_duration"
            ).eq("status", "scheduled").gte("scheduled_at", one_hour_start.isoformat()).lt("scheduled_at", one_hour_end.isoformat()).execute()

            regular_1h_count = 0
            if response_1h.data:
                print(f"📭 Знайдено {len(response_1h.data)} записів для 1-годинних нагадувань")
                for appointment in response_1h.data:
                    # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: убеждаемся что напоминание не отправлялось в последние 45 минут
                    # Это защитит от дублирования с отложенными уведомлениями
                    forty_five_minutes_ago = now - timedelta(minutes=45)
                    recent_1h_logs = self.supabase.table("notification_logs").select(
                        "id"
                    ).eq("appointment_id", appointment['id']).eq("type", "1h").eq("status", "sent").gte("sent_at", forty_five_minutes_ago.isoformat()).execute()

                    if not recent_1h_logs.data:
                        await self.process_appointment_reminder(appointment, now, "1h")
                        regular_1h_count += 1
            else:
                print("📭 Через годину немає записів для 1-годинних нагадувань")

            if regular_24h_count > 0 or regular_1h_count > 0:
                print(f"✅ Відправлено регулярних нагадувань: 24h={regular_24h_count}, 1h={regular_1h_count}")
            
            # Проверяем напоминания об отсутствии ответа (через 6 часов после 24-часового напоминания)
            six_hours_ago = now - timedelta(hours=6)
            no_response_appointments_response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, appointment_type, total_duration"
            ).eq("status", "scheduled").gte("scheduled_at", tomorrow_start.isoformat()).lt("scheduled_at", tomorrow_end.isoformat()).execute()

            if no_response_appointments_response.data:
                print(f"📭 Перевіряємо {len(no_response_appointments_response.data)} записів на відсутність відповіді")
                for appointment in no_response_appointments_response.data:
                    await self.check_no_response_reminder(appointment, now)
            else:
                print("📭 Немає записів для перевірки відсутності відповіді")
            
            # Проверяем и отправляем отложенные уведомления (если бот был неактивен в рабочее время)
            await self.send_delayed_notifications(now)

            print(f"✅ ПРОВЕРКА НАПОМИНАНИЙ ЗАВЕРШЕНА: {datetime.now(ZoneInfo('Europe/Kyiv')).strftime('%H:%M:%S')}")
                
        except Exception as e:
            print(f"❌ ПОМИЛКА ПРИ ПЕРЕВІРЦІ НАГАДУВАНЬ: {e}")
        finally:
            # Сбрасываем флаг выполнения
            self._reminder_check_in_progress = False

    async def process_appointment_reminder_no_response(self, appointment, now):
        """Обрабатывает напоминание об отсутствии ответа с использованием шаблона no_response"""
        try:
            client_response = self.supabase.table("clients").select(
                "name, telegram_chat_id, notification_opt_in"
            ).eq("id", appointment['client_id']).execute()
            if not client_response.data:
                return

            client = client_response.data[0]
            if not client.get('telegram_chat_id') or not client.get('notification_opt_in'):
                return

            service_response = self.supabase.table("services").select(
                "name, duration_minutes"
            ).eq("id", appointment['service_id']).execute()
            if not service_response.data:
                return

            service = service_response.data[0]
            appointment_data = {
                'id': appointment['id'],  # Изменено с appointment_id на id
                'appointment_id': appointment['id'],  # Оставляем для совместимости
                'scheduled_at': appointment['scheduled_at'],
                'client_name': extract_first_name(client['name']),
                'telegram_chat_id': client['telegram_chat_id'],
                'service_name': service['name'],
                'duration': service['duration_minutes'],
                'appointment_type': appointment.get('appointment_type', 'single'),
                'total_duration': appointment.get('total_duration')
            }
            await self.process_reminder_no_response(appointment_data, now)
        except Exception as e:
            print(f"❌ Помилка при обробці нагадування про відсутність відповіді: {e}")

    async def process_appointment_reminder(self, appointment, now, reminder_type="24h"):
        try:
            client_response = self.supabase.table("clients").select(
                "name, telegram_chat_id, notification_opt_in"
            ).eq("id", appointment['client_id']).execute()
            if not client_response.data:
                return

            client = client_response.data[0]
            if not client.get('telegram_chat_id') or not client.get('notification_opt_in'):
                return

            service_response = self.supabase.table("services").select(
                "name, duration_minutes"
            ).eq("id", appointment['service_id']).execute()
            if not service_response.data:
                return

            service = service_response.data[0]
            appointment_data = {
                'id': appointment['id'],  # Изменено с appointment_id на id
                'appointment_id': appointment['id'],  # Оставляем для совместимости
                'scheduled_at': appointment['scheduled_at'],
                'client_name': extract_first_name(client['name']),
                'telegram_chat_id': client['telegram_chat_id'],
                'duration': service['duration_minutes'],
                'appointment_type': appointment.get('appointment_type', 'single'),
                'total_duration': appointment.get('total_duration')
            }

            # Получаем время записи для отображения
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if dt_raw:
                scheduled_time = dt_raw.astimezone(ZoneInfo("Europe/Kyiv")).strftime('%d.%m.%Y %H:%M')
                print(f"⏰ ГОТОВЛЮ НАПОМИНАНИЕ: {reminder_type} для {extract_first_name(client['name'])} на {scheduled_time}")

            await self.process_reminder(appointment_data, now, reminder_type)
        except Exception as e:
            print(f"❌ ПОМИЛКА ПРИ ОБРОБЦІ НАГАДУВАННЯ: {e}")

    async def process_reminder_no_response(self, appointment, now):
        """Обрабатывает напоминание об отсутствии ответа с использованием шаблона no_response"""
        try:
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                print(f"⚠️ ПРОПУСКАЮ ЗАПИСЬ: некорректная дата ID={appointment.get('id')}")
                return False
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            chat_id = appointment['telegram_chat_id']
            client_name = extract_first_name(appointment['client_name'])
            
            # Проверяем, было ли уже отправлено напоминание об отсутствии ответа
            if await self.check_notification_sent(appointment['id'], "1h"):
                return
            
            scheduled_time = scheduled_at.strftime('%d.%m.%Y %H:%M')
            print(f"⏰ ГОТОВЛЮ НАПОМИНАНИЕ ОБ ОТСУТСТВИИ ОТВЕТА: для {client_name} на {scheduled_time}")
            
            success = await self.send_reminder_no_response(chat_id, client_name, appointment)
            if success:
                print(f"✅ УСПЕШНО ОТПРАВЛЕНО: напоминание об отсутствии ответа клиенту {client_name} на {scheduled_time}")
                await self.log_notification_sent(appointment['id'], "1h")
        except Exception as e:
            print(f"❌ process_reminder_no_response error: {e}")

    async def process_reminder(self, appointment, now, reminder_type="24h"):
        try:
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                return False
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            chat_id = appointment['telegram_chat_id']
            client_name = extract_first_name(appointment['client_name'])
            if await self.check_notification_sent(appointment['id'], reminder_type):
                return
            success = await self.send_reminder(chat_id, client_name, appointment, reminder_type)
            if success:
                dt_raw = safe_parse_datetime(appointment['scheduled_at'])
                scheduled_time = "неизвестное время"
                if dt_raw:
                    scheduled_time = dt_raw.astimezone(ZoneInfo("Europe/Kyiv")).strftime('%d.%m.%Y %H:%M')

                reminder_name = {
                    "24h": "24-ГОДИННОЕ НАПОМИНАНИЕ",
                    "1h": "1-ГОДИННОЕ НАПОМИНАНИЕ"
                }.get(reminder_type, f"НАПОМИНАНИЕ {reminder_type}")

                print(f"⏰ ОТПРАВИЛ НАПОМИНАНИЕ '{reminder_name}' КЛИЕНТУ {client_name} о записи на {scheduled_time}")
                await self.log_notification_sent(appointment['id'], reminder_type)
        except Exception as e:
            print(f"❌ process_reminder error: {e}")

    async def send_reminder_no_response(self, chat_id, client_name, appointment):
        """Отправляет напоминание об отсутствии ответа с использованием шаблона no_response"""
        try:
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours():
                print(f"⏰ Вне рабочего времени (8:00-21:00), уведомление об отсутствии ответа не отправляется")
                return False
            
            template_path = "templates/ua/reminder_no_response.txt"
            if not os.path.exists(template_path):
                print(f"❌ Шаблон {template_path} не знайдено")
                return False

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                return False
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            days_text = get_days_text(scheduled_at.date())

            # Получаем описание пакета если это пакет услуг
            package_description = ""
            appointment_type = appointment.get('appointment_type', 'single')
            if appointment_type == "package":
                try:
                    package_description = await self.get_package_description(appointment['id'])
                except Exception as e:
                    print(f"⚠️ Ошибка получения описания пакета: {e}")
                    package_description = "комплексний масаж"

            # Обрабатываем условные конструкции в шаблоне
            template = self.process_template_conditions(template, {
                'appointment_type': appointment_type,
                'package_description': package_description
            })

            message = template.format(
                client_name=client_name,
                date=formatted_date,
                time=formatted_time,
                duration=appointment.get('duration', ''),
                days_text=days_text,
                package_description=package_description
            )

            # Для напоминания об отсутствии ответа отправляем с кнопками для подтверждения
            keyboard = [
                [
                    InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment['id']}"),
                    InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment['id']}")
                ],
                [
                    InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment['id']}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.application.bot.send_message(chat_id=chat_id, text=message, reply_markup=reply_markup)
            
            return True
        except Exception as e:
            print(f"❌ Помилка відправки нагадування про відсутність відповіді: {e}")
            return False

    async def send_reminder(self, chat_id, client_name, appointment, reminder_type):
        try:
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours():
                print(f"⏰ Вне рабочего времени (8:00-21:00), уведомление {reminder_type} не отправляется")
                return False
            template_path = f"templates/ua/reminder_{reminder_type}.txt"
            if not os.path.exists(template_path):
                print(f"❌ Шаблон {template_path} не знайдено")
                return False

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                return False
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            days_text = get_days_text(scheduled_at.date())

            # Получаем описание пакета если это пакет услуг
            package_description = ""
            appointment_type = appointment.get('appointment_type', 'single')
            if appointment_type == "package":
                try:
                    package_description = await self.get_package_description(appointment['id'])
                except Exception as e:
                    print(f"⚠️ Ошибка получения описания пакета: {e}")
                    package_description = "комплексний масаж"

            # Обрабатываем условные конструкции в шаблоне
            template = self.process_template_conditions(template, {
                'appointment_type': appointment_type,
                'package_description': package_description
            })

            message = template.format(
                client_name=client_name,
                date=formatted_date,
                time=formatted_time,
                duration=appointment['duration'],
                days_text=days_text,
                package_description=package_description
            )

            # Для 24-часовых напоминаний добавляем интерактивные кнопки
            if reminder_type == "24h":
                # Добавляем пояснение о сроке действия кнопок
                extended_message = message + "\n\n" + (
                    "⏱️ Кнопки діють до кінця дня. Якщо вони перестануть працювати, скористайтеся меню:\n"
                    "📋 Мої записи - переглянути та підтвердити\n"
                    "🔄 Перенести запис - швидкий перенос\n"
                    "❌ Скасувати запис - швидке скасування"
                )
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment['id']}"),
                        InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment['id']}")
                    ],
                    [
                        InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment['id']}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Отправляем основное сообщение с кнопками
                await self.application.bot.send_message(chat_id=chat_id, text=extended_message, reply_markup=reply_markup)
            elif reminder_type == "no_response":
                # Для напоминания об отсутствии ответа отправляем с кнопками для подтверждения
                extended_message = message + "\n\n" + (
                    "⏱️ Кнопки діють обмежений час. Якщо вони перестануть працювати, скористайтеся меню:"
                )
                
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment['id']}"),
                        InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment['id']}")
                    ],
                    [
                        InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment['id']}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await self.application.bot.send_message(chat_id=chat_id, text=extended_message, reply_markup=reply_markup)
            else:
                # Для 1-часовых напоминаний отправляем без кнопок
                await self.application.bot.send_message(chat_id=chat_id, text=message)
            
            return True
        except Exception as e:
            print(f"❌ Помилка відправки нагадування: {e}")
            return False


    async def send_confirmation_notification(self, query, appointment_id: str, client_name: str):
        """Отправляет уведомление о подтверждении записи клиенту"""
        try:
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours():
                print(f"⏰ Вне рабочего времени (8:00-21:00), уведомление о подтверждении не отправляется")
                await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")
                return
            # Получаем данные подтвержденной записи
            appointment_response = self.supabase.table("appointments").select(
                "scheduled_at, service_id, appointment_type, total_duration"
            ).eq("id", appointment_id).execute()

            if not appointment_response.data:
                await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")
                return

            appointment = appointment_response.data[0]

            # Получаем данные услуги
            service_response = self.supabase.table("services").select(
                "name"
            ).eq("id", appointment['service_id']).execute()

            service_name = service_response.data[0]['name'] if service_response.data else "масаж"

            # Форматируем дату и время
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаємо запис з некоректною датою: id={appointment_id}")
                await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")
                return
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            
            # Читаем шаблон
            template_path = "templates/ua/confirmation.txt"
            if not os.path.exists(template_path):
                await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")
                return

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            # Получаем описание пакета если это пакет услуг
            package_description = ""
            appointment_type = appointment.get('appointment_type', 'single')
            if appointment_type == "package":
                try:
                    package_description = await self.get_package_description(appointment_id)
                except Exception as e:
                    print(f"⚠️ Ошибка получения описания пакета: {e}")
                    package_description = "комплексний масаж"

            # Обрабатываем условные конструкции в шаблоне
            template = self.process_template_conditions(template, {
                'appointment_type': appointment_type,
                'package_description': package_description
            })

            # Форматируем сообщение
            message = template.format(
                client_name=client_name,
                date=formatted_date,
                time=formatted_time,
                package_description=package_description
            )

            await query.edit_message_text(message)
            
        except Exception as e:
            logger.error("❌ Ошибка отправки уведомления о подтверждении", exc_info=True)
            await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")

    async def update_user_activity(self, chat_id: str, client_id: str = None, client_name: str = None, command: str = None):
        """Обновляет статистику активности пользователя"""
        try:
            # Вызываем функцию PostgreSQL для обновления статистики
            self.supabase.rpc('update_user_activity_stats', {
                'p_chat_id': chat_id,
                'p_client_id': client_id,
                'p_client_name': client_name,
                'p_command': command
            }).execute()
        except Exception as e:
            # Логируем ошибку, но не прерываем основную функциональность
            print(f"⚠️ ОШИБКА ОБНОВЛЕНИЯ СТАТИСТИКИ: {str(e)}")

    async def get_daily_stats(self, date=None):
        """Получает статистику пользователей за указанную дату"""
        try:
            if date is None:
                date = datetime.now(ZoneInfo("Europe/Kyiv")).date()

            # Получаем статистику за указанную дату
            stats_resp = self.supabase.table("user_activity_stats").select(
                "chat_id, client_name, first_activity_at, last_activity_at, interaction_count, commands_used"
            ).eq("activity_date", date.isoformat()).execute()

            if not stats_resp.data:
                return {
                    'total_users': 0,
                    'total_interactions': 0,
                    'users': []
                }

            total_users = len(stats_resp.data)
            total_interactions = sum(user['interaction_count'] for user in stats_resp.data)

            return {
                'total_users': total_users,
                'total_interactions': total_interactions,
                'users': stats_resp.data
            }

        except Exception as e:
            print(f"❌ ОШИБКА ПОЛУЧЕНИЯ СТАТИСТИКИ: {str(e)}")
            return None

    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает статистику пользователей за сегодня"""
        try:
            # Проверяем, является ли пользователь администратором
            chat_id = str(update.effective_chat.id)
            if chat_id != str(self.admin_chat_id):
                await update.message.reply_text("❌ У вас нет доступа к этой команде.")
                return

            # Получаем статистику за сегодня
            today = datetime.now(ZoneInfo("Europe/Kyiv")).date()
            yesterday = today - timedelta(days=1)

            today_stats = await self.get_daily_stats(today)
            yesterday_stats = await self.get_daily_stats(yesterday)

            if not today_stats:
                await update.message.reply_text("❌ Не удалось получить статистику.")
                return

            # Формируем сообщение
            message = f"📊 СТАТИСТИКА ПОЛЬЗОВАТЕЛЕЙ\n\n"

            message += f"📅 СЕГОДНЯ ({today.strftime('%d.%m.%Y')}):\n"
            message += f"👥 Активных пользователей: {today_stats['total_users']}\n"
            message += f"💬 Всего взаимодействий: {today_stats['total_interactions']}\n\n"

            if yesterday_stats:
                message += f"📅 ВЧЕРА ({yesterday.strftime('%d.%m.%Y')}):\n"
                message += f"👥 Активных пользователей: {yesterday_stats['total_users']}\n"
                message += f"💬 Всего взаимодействий: {yesterday_stats['total_interactions']}\n\n"

                # Сравнение с предыдущим днем
                users_diff = today_stats['total_users'] - yesterday_stats['total_users']
                interactions_diff = today_stats['total_interactions'] - yesterday_stats['total_interactions']

                message += f"📈 СРАВНЕНИЕ С ВЧЕРА:\n"
                message += f"👥 Пользователи: {users_diff:+d}\n"
                message += f"💬 Взаимодействия: {interactions_diff:+d}\n\n"

                # Краткая информация о топ-пользователях
                if today_stats['users']:
                    top_users = sorted(today_stats['users'], key=lambda x: x['interaction_count'], reverse=True)[:3]
                    message += f"🏆 ТОП-3 АКТИВНЫХ:\n"
                    for i, user in enumerate(top_users, 1):
                        name = user.get('client_name', 'Неизвестный')
                        interactions = user['interaction_count']
                        message += f"{i}. {name} - {interactions} взаимодействий\n"
                else:
                    message += f"📋 Сегодня активных пользователей: {today_stats['total_users']}\n"

            await update.message.reply_text(message)

        except Exception as e:
            print(f"❌ ОШИБКА В СТАТИСТИКЕ: {str(e)}")
            await update.message.reply_text("❌ Виникла помилка при отриманні статистики.")

    async def handle_appointment_confirmation(self, query, appointment_id: str):
        """Обрабатывает подтверждение перенесенной записи"""
        try:
            print(f"✅ КЛИЕНТ ПОДТВЕРДИЛ ПЕРЕНЕСЕННУЮ ЗАПИСЬ: ID {appointment_id}")

            # Получаем данные записи
            appointment_resp = self.supabase.table("appointments").select(
                "client_id, scheduled_at, service_id, status"
            ).eq("id", appointment_id).execute()

            if not appointment_resp.data:
                await query.edit_message_text("❌ Запис не знайдено")
                return

            appointment = appointment_resp.data[0]

            # Получаем данные клиента
            client_resp = self.supabase.table("clients").select("name").eq("id", appointment['client_id']).execute()
            full_name = client_resp.data[0]['name'] if client_resp.data else "Клієнт"
            client_name = extract_first_name(full_name)

            # Обновляем статус записи на "confirmed"
            self.supabase.table("appointments").update({
                "confirmation_status": "confirmed",
                "client_confirmed": True,
                "confirmation_time": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat()
            }).eq("id", appointment_id).execute()

            # Получаем детали записи для уведомления
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if dt_raw:
                dt = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
                formatted_date = dt.strftime("%d.%m.%Y")
                formatted_time = dt.strftime("%H:%M")

                print(f"✅ КЛИЕНТ {client_name} ПОДТВЕРДИЛ ЗАПИСЬ на {formatted_date} в {formatted_time}")

                await query.edit_message_text(
                    f"✅ Дякуємо, {client_name}!\n\n"
                    f"Ваша запис на {formatted_date} о {formatted_time} підтверджена.\n\n"
                    f"До зустрічі! 👋"
                )

                # Уведомляем администратора
                service_resp = self.supabase.table("services").select("name").eq("id", appointment['service_id']).execute()
                service_name = service_resp.data[0]['name'] if service_resp.data else "масаж"

                await self.application.bot.send_message(
                    chat_id=self.admin_chat_id,
                    text=f"✅ Клієнт {client_name} підтвердив перенесену запис\n"
                         f"💆 {service_name}\n"
                         f"📅 {formatted_date} о {formatted_time}"
                )
            else:
                await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")

        except Exception as e:
            print(f"❌ ОШИБКА ПОДТВЕРЖДЕНИЯ ЗАПИСИ {appointment_id}: {str(e)}")
            await query.edit_message_text("❌ Виникла помилка при підтвердженні. Спробуйте ще раз.")

    async def send_client_reschedule_notification(self, query, appointment_id: str, client_name: str, old_date: str, old_time: str, new_date: str, new_time: str):
        """Отправляет уведомление клиенту о переносе записи с кнопками подтверждения"""
        try:
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours():
                print(f"⏰ Вне рабочего времени (8:00-21:00), уведомление о переносе не отправляется")
                await query.edit_message_text(f"✅ Запис успішно перенесено на {new_date} о {new_time}.")
                return
            
            # Читаем шаблон
            template_path = "templates/ua/client_reschedule.txt"
            if not os.path.exists(template_path):
                await query.edit_message_text(f"✅ Запис успішно перенесено на {new_date} о {new_time}.")
                return

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            # Получаем данные записи для обработки пакетов
            appointment_response = self.supabase.table("appointments").select(
                "appointment_type, total_duration"
            ).eq("id", appointment_id).execute()

            package_description = ""
            appointment_type = "single"
            if appointment_response.data:
                appointment_type = appointment_response.data[0].get('appointment_type', 'single')
                if appointment_type == "package":
                    try:
                        package_description = await self.get_package_description(appointment_id)
                    except Exception as e:
                        print(f"⚠️ Ошибка получения описания пакета: {e}")
                        package_description = "комплексний масаж"

            # Обрабатываем условные конструкции в шаблоне
            template = self.process_template_conditions(template, {
                'appointment_type': appointment_type,
                'package_description': package_description
            })

            # Форматируем сообщение
            message = template.format(
                client_name=client_name,
                old_date=old_date,
                old_time=old_time,
                new_date=new_date,
                new_time=new_time,
                package_description=package_description
            )

            # Создаем кнопки подтверждения
            keyboard = [
                [InlineKeyboardButton(f"✅ Підтвердити {new_date} {new_time}", callback_data=f"confirm_appointment_{appointment_id}")],
                [InlineKeyboardButton("📞 Зв'язатися з адміністратором", callback_data="contact_admin")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(message, reply_markup=reply_markup)
            
        except Exception as e:
            logger.error("❌ Ошибка отправки уведомления о переносе", exc_info=True)
            await query.edit_message_text(f"✅ Запис успішно перенесено на {new_date} о {new_time}.")

    async def send_cancellation_notification(self, query, appointment_id: str, client_name: str):
        """Отправляет уведомление об отмене записи клиенту"""
        try:
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours():
                print(f"⏰ Вне рабочего времени (8:00-21:00), уведомление об отмене не отправляется")
                await query.edit_message_text(f"❌ Запис скасовано, {client_name}.")
                return
            # Получаем данные отмененной записи
            appointment_response = self.supabase.table("appointments").select(
                "scheduled_at, service_id, appointment_type, total_duration"
            ).eq("id", appointment_id).execute()
            
            if not appointment_response.data:
                await query.edit_message_text(f"❌ Запис скасовано, {client_name}.")
                return
                
            appointment = appointment_response.data[0]
            
            # Получаем данные услуги
            service_response = self.supabase.table("services").select(
                "name"
            ).eq("id", appointment['service_id']).execute()
            
            service_name = service_response.data[0]['name'] if service_response.data else "масаж"

            # Форматируем дату и время
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаємо запис з некоректною датою: id={appointment_id}")
                await query.edit_message_text(f"❌ Запис скасовано, {client_name}.")
                return
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            
            # Читаем шаблон
            template_path = "templates/ua/cancellation.txt"
            if not os.path.exists(template_path):
                await query.edit_message_text(f"❌ Запис скасовано, {client_name}.")
                return

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            # Получаем описание пакета если это пакет услуг
            package_description = ""
            appointment_type = appointment.get('appointment_type', 'single')
            if appointment_type == "package":
                try:
                    package_description = await self.get_package_description(appointment_id)
                except Exception as e:
                    print(f"⚠️ Ошибка получения описания пакета: {e}")
                    package_description = "комплексний масаж"

            # Обрабатываем условные конструкции в шаблоне
            template = self.process_template_conditions(template, {
                'appointment_type': appointment_type,
                'package_description': package_description
            })

            # Форматируем сообщение
            message = template.format(
                client_name=client_name,
                date=formatted_date,
                time=formatted_time,
                package_description=package_description
            )

            await query.edit_message_text(message)
            
        except Exception as e:
            logger.error("❌ Ошибка отправки уведомления об отмене", exc_info=True)
            await query.edit_message_text(f"❌ Запис отменено, {client_name}.")

    async def send_new_appointment_notification(self, appointment_id: str):
        """Отправляет уведомление о новой записи клиенту (через 2 часа после создания)"""
        try:
            print(f"📨 ОТПРАВЛЯЮ УВЕДОМЛЕНИЕ О НОВОЙ ЗАПИСИ: ID {appointment_id}")

            # Проверяем, было ли уже отправлено уведомление о новой записи
            if await self.check_notification_sent(appointment_id, "new_appointment"):
                print(f"✅ Уведомление о новой записи {appointment_id} уже было отправлено ранее")
                return True

            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            if not self.is_working_hours(now):
                print(f"⏰ ВНЕ РАБОЧЕГО ВРЕМЕНИ: уведомление о новой записи {appointment_id} не отправляется")
                return False
            # Получаем данные записи
            appointment_response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, appointment_type, total_duration"
            ).eq("id", appointment_id).execute()

            if not appointment_response.data:
                logger.error(f"❌ Запис {appointment_id} не знайдено")
                return False

            appointment = appointment_response.data[0]

            # Получаем данные клиента
            client_response = self.supabase.table("clients").select(
                "name, telegram_chat_id, notification_opt_in"
            ).eq("id", appointment['client_id']).execute()

            if not client_response.data:
                logger.error(f"❌ Клієнт для запису {appointment_id} не знайдено")
                return False

            client = client_response.data[0]

            # Проверяем, подключен ли клиент к боту и согласен ли на уведомления
            if not client.get('telegram_chat_id') or not client.get('notification_opt_in'):
                print(f"❌ Клієнт {client.get('name', 'Unknown')} не підключений до бота або не погодився на сповіщення")
                return False

            # Получаем данные об услуге/пакете
            appointment_type = appointment.get('appointment_type', 'single')

            if appointment_type == 'package':
                # Для пакетов получаем описание через функцию
                package_description = await self.get_package_description(appointment_id)
                if not package_description:
                    print(f"❌ Не удалось получить описание пакета для записи {appointment_id}")
                    return False
                service_info = {
                    'name': 'Комплексний масаж',
                    'package_description': package_description
                }
            else:
                # Для одиночных услуг получаем данные из таблицы services
                service_response = self.supabase.table("services").select(
                    "name, duration_minutes"
                ).eq("id", appointment['service_id']).execute()

                if not service_response.data:
                    print(f"❌ Послуга для запису {appointment_id} не знайдена")
                    return False

                service_info = service_response.data[0]
            
            # Проверяем, не попадает ли запись в 24-часовой интервал
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                return False
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            tomorrow_start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_end = tomorrow_start + timedelta(days=1)
            
            # ВРЕМЕННО ОТКЛЮЧЕНО ДЛЯ ТЕСТИРОВАНИЯ: проверка 24-часового интервала
            # Если запись попадает в 24-часовой интервал, не отправляем уведомление о новой записи
            # (вместо этого будет отправлено 24-часовое напоминание)
            logger.info(f"📅 Запись на: {scheduled_at.strftime('%d.%m.%Y %H:%M')}, 24h интервал: {tomorrow_start.strftime('%d.%m.%Y %H:%M')} - {tomorrow_end.strftime('%d.%m.%Y %H:%M')}")
            # if tomorrow_start <= scheduled_at < tomorrow_end:
            #     logger.info(f"📝 Запись {appointment_id} попадает в 24-часовой интервал, уведомление о новой записи не отправляется")
            #     return True
            
            # Проверяем, было ли уже отправлено 24-часовое уведомление для этой записи
            if await self.check_notification_sent(appointment_id, "24h"):
                client_name = extract_first_name(appointment['client_name'])
                scheduled_time = scheduled_at.strftime('%d.%m.%Y %H:%M')
                logger.info(f"📝 ДЛЯ КЛИЕНТА {client_name} на {scheduled_time} уже было отправлено 24-часовое уведомление")
                return True
            
            # Отправляем уведомление о новой записи
            template_path = "templates/ua/new_appointment.txt"
            if not os.path.exists(template_path):
                print(f"❌ Шаблон {template_path} не найден")
                return False

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")

            # Получаем описание пакета для условных конструкций
            package_description = service_info.get('package_description', '') if appointment_type == 'package' else ''

            # Обрабатываем условные конструкции в шаблоне
            template = self.process_template_conditions(template, {
                'appointment_type': appointment_type,
                'package_description': package_description
            })

            # Форматируем сообщение
            message = template.format(
                client_name=client['name'],
                date=formatted_date,
                time=formatted_time,
                package_description=package_description,
                duration=service_info.get('duration_minutes', '')
            )

            client_display_name = extract_first_name(client['name'])
            print(f"📤 ОТПРАВИЛ КЛИЕНТУ {client_display_name} УВЕДОМЛЕНИЕ О НОВОЙ ЗАПИСИ на {formatted_date} в {formatted_time}")
            await self.application.bot.send_message(chat_id=client['telegram_chat_id'], text=message)
            print(f"✅ КЛИЕНТ {client_display_name} ПОЛУЧИЛ УВЕДОМЛЕНИЕ О ЗАПИСИ")

            # Уведомляем администратора о новой записи
            try:
                if appointment_type == 'package':
                    admin_message = f"🆕 Нова запис від {client['name']}\n💆 Комплексний масаж: {service_info['package_description']}\n📅 {formatted_date} о {formatted_time}"
                else:
                    admin_message = f"🆕 Нова запис від {client['name']}\n💆 {service_info['name']}\n📅 {formatted_date} о {formatted_time}"

                await self.application.bot.send_message(
                    chat_id=self.admin_chat_id,
                    text=admin_message
                )
                logger.info("✅ Уведомление о новой записи отправлено админу")
            except Exception as admin_error:
                logger.error("❌ Ошибка отправки уведомления админу", exc_info=True)

            # Логируем отправку уведомления о новой записи
            await self.log_notification_sent(appointment_id, "new_appointment")

            logger.info(f"✅ Уведомление о новой записи отправлено клиенту {extract_first_name(client['name'])}")
            return True

        except Exception as e:
            logger.error("❌ Ошибка отправки уведомления о новой записи", exc_info=True)
            return False

    async def get_package_description(self, appointment_id: str) -> str:
        """Получает описание пакета услуг для записи"""
        try:
            # Получаем информацию о записи
            appointment_resp = self.supabase.table("appointments").select(
                "service_id, appointment_type"
            ).eq("id", appointment_id).execute()

            if not appointment_resp.data:
                print(f"⚠️ ЗАПИСЬ НЕ НАЙДЕНА: {appointment_id} (невозможно получить описание пакета)")
                return ""

            appointment = appointment_resp.data[0]
            appointment_type = appointment.get('appointment_type', 'single')

            # Если это не пакет, возвращаем пустую строку
            if appointment_type != 'package':
                return ""

            # Получаем информацию об услуге
            service_resp = self.supabase.table("services").select(
                "name"
            ).eq("id", appointment['service_id']).execute()

            if service_resp.data:
                service_name = service_resp.data[0]['name']
                # Формируем описание пакета на основе названия услуги
                if 'комплекс' in service_name.lower() or 'пакет' in service_name.lower():
                    print(f"📦 ПАКЕТ ОБРАБОТАН: {appointment_id} -> {service_name}")
                    return service_name
                else:
                    package_desc = f"комплексний масаж ({service_name})"
                    print(f"📦 ПАКЕТ ОБРАБОТАН: {appointment_id} -> {package_desc}")
                    return package_desc
            else:
                print(f"📦 ПАКЕТ ОБРАБОТАН: {appointment_id} -> комплексний масаж (услуга не найдена)")
                return "комплексний масаж"

        except Exception as e:
            print(f"❌ ОШИБКА ПОЛУЧЕНИЯ ОПИСАНИЯ ПАКЕТА: {appointment_id} - {str(e)}")
            return "комплексний масаж"

    def process_template_conditions(self, template: str, context: dict) -> str:
        """Обрабатывает условные конструкции в шаблонах"""
        import re
        
        # Паттерн для поиска {if:condition}...{else}...{/if} или {if:condition}...{/if}
        pattern = r'\{if:([^}]+)\}(.*?)(?:\{else\}(.*?))?\{/if\}'
        
        def replace_condition(match):
            condition = match.group(1).strip()
            if_content = match.group(2)
            else_content = match.group(3) if match.group(3) is not None else ""
            
            # Оцениваем условие
            try:
                # Простая обработка условий типа "appointment_type == 'package'"
                if "==" in condition:
                    var_name, expected_value = condition.split("==", 1)
                    var_name = var_name.strip()
                    expected_value = expected_value.strip().strip('"').strip("'")
                    
                    actual_value = context.get(var_name, "")
                    
                    if actual_value == expected_value:
                        return if_content
                    else:
                        return else_content
                else:
                    # Простая проверка на существование переменной
                    if context.get(condition.strip()):
                        return if_content
                    else:
                        return else_content
                        
            except Exception as e:
                print(f"⚠️ Ошибка обработки условия '{condition}': {e}")
                return else_content
        
        # Заменяем все условные конструкции
        processed_template = re.sub(pattern, replace_condition, template, flags=re.DOTALL)
        
        return processed_template

    async def send_admin_reschedule_notification(self, appointment_id: str):
        """Отправляет уведомление клиенту о переносе записи администратором"""
        try:
            # Проверяем, находимся ли в рабочем времени
            if not self.is_working_hours():
                print(f"⏰ Вне рабочего времени (8:00-21:00), уведомление о переносе администратором не отправляется")
                return False
                
            # Получаем данные записи
            appointment_response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, reschedule_source, client_confirmed"
            ).eq("id", appointment_id).execute()
            
            if not appointment_response.data:
                print(f"❌ Запись {appointment_id} не найдена")
                return False
                
            appointment = appointment_response.data[0]
            
            # Проверяем, что это действительно перенос администратором
            if appointment.get('reschedule_source') != 'ADMIN' or appointment.get('client_confirmed') == True:
                print(f"❌ Запись {appointment_id} не является переносом администратором или уже подтверждена")
                return False
            
            # Получаем данные клиента
            client_response = self.supabase.table("clients").select(
                "name, telegram_chat_id, notification_opt_in"
            ).eq("id", appointment['client_id']).execute()
            
            if not client_response.data:
                print(f"❌ Клиент для записи {appointment_id} не найден")
                return False
                
            client = client_response.data[0]
            
            # Проверяем, подключен ли клиент к боту и согласен ли на уведомления
            if not client.get('telegram_chat_id') or not client.get('notification_opt_in'):
                print(f"❌ Клієнт {client.get('name', 'Unknown')} не підключений до бота або не погодився на сповіщення")
                return False
            
            # Получаем данные услуги
            service_response = self.supabase.table("services").select(
                "name, duration_minutes"
            ).eq("id", appointment['service_id']).execute()
            
            if not service_response.data:
                print(f"❌ Послуга для запису {appointment_id} не знайдена")
                return False
                
            service = service_response.data[0]
            
            # Форматируем дату и время
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                return False
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            
            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            
            # Читаем шаблон для уведомления о переносе администратором
            template_path = "templates/ua/admin_reschedule.txt"
            if not os.path.exists(template_path):
                print(f"❌ Шаблон {template_path} не найден, используем стандартное сообщение")
                message = (
                    f"🔄 {extract_first_name(client['name'])}, ваш запис перенесено адміністратором!\n\n"
                    f"📅 Новий час: {formatted_date} о {formatted_time}\n\n"
                    f"Будь ласка, підтвердіть новий час запису або скасуйте його."
                )
            else:
                with open(template_path, 'r', encoding='utf-8') as f:
                    template = f.read()

                # Получаем описание пакета если это пакет услуг
                package_description = ""
                appointment_type = appointment.get('appointment_type', 'single')
                if appointment_type == "package":
                    try:
                        package_description = await self.get_package_description(appointment_id)
                    except Exception as e:
                        print(f"⚠️ Ошибка получения описания пакета: {e}")
                        package_description = "комплексний масаж"

                # Обрабатываем условные конструкции в шаблоне
                template = self.process_template_conditions(template, {
                    'appointment_type': appointment_type,
                    'package_description': package_description
                })

                # Форматируем сообщение
                try:
                    message = template.format(
                        client_name=extract_first_name(client['name']),
                        new_date=formatted_date,
                        new_time=formatted_time,
                        package_description=package_description
                    )
                    print(f"✅ ШАБЛОН ОБРАБОТАН: admin_reschedule.txt для клиента {extract_first_name(client['name'])}")
                except KeyError as e:
                    print(f"❌ ОШИБКА ШАБЛОНА: отсутствует переменная {e} в admin_reschedule.txt")
                    print(f"   Шаблон: {template[:100]}...")
                    raise e
            
            # Добавляем кнопки подтверждения
            keyboard = [
                [InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment_id}")],
                [InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment_id}")],
                [InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await self.application.bot.send_message(
                chat_id=client['telegram_chat_id'],
                text=message,
                reply_markup=reply_markup
            )

            # Логируем отправку уведомления о переносе администратором
            await self.log_notification_sent(appointment_id, "admin_reschedule")

            logger.info(f"✅ Уведомление о переносе администратором отправлено клиенту {extract_first_name(client['name'])}")
            return True
            
        except Exception as e:
            logger.error("❌ Ошибка отправки уведомления о переносе администратором", exc_info=True)
            return False

    async def send_delayed_notifications(self, now):
        """Отправляет уведомления, которые могли быть пропущены из-за нерабочего времени"""
        try:
            print("🔄 Перевіряємо відкладені нагадування...")

            # Проверяем 24-часовые напоминания, которые могли быть пропущены
            # НО только для записей, которые еще НЕ ПРОШЛИ
            tomorrow_start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_end = tomorrow_start + timedelta(days=1)

            delayed_24h_response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status, appointment_type, total_duration"
            ).eq("status", "scheduled").gte("scheduled_at", tomorrow_start.isoformat()).lt("scheduled_at", tomorrow_end.isoformat()).execute()

            delayed_24h_count = 0
            if delayed_24h_response.data:
                print(f"📭 Перевіряємо {len(delayed_24h_response.data)} записів для відкладених 24-годинних нагадувань")
                for appointment in delayed_24h_response.data:
                    # Проверяем, что запись еще не прошла
                    dt_raw = safe_parse_datetime(appointment['scheduled_at'])
                    if not dt_raw:
                        logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                        continue # пропустить эту запись
                    appointment_time = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

                    # Отправляем напоминание только если запись еще не прошла
                    if appointment_time > now:
                        # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: отправляем только если прошло достаточно времени с момента регулярной проверки
                        # Это предотвратит дублирование с регулярными напоминаниями
                        last_check_time = now - timedelta(hours=1)  # Предполагаем, что регулярная проверка была час назад

                        # Проверяем логи уведомлений за последний час
                        recent_logs = self.supabase.table("notification_logs").select(
                            "sent_at, type"
                        ).eq("appointment_id", appointment['id']).eq("type", "24h").eq("status", "sent").gte("sent_at", last_check_time.isoformat()).execute()

                        # Отправляем только если нет недавних логов этого типа
                        if not recent_logs.data:
                            # Проверяем, было ли уже отправлено 24-часовое напоминание
                            if not await self.check_notification_sent(appointment['id'], "24h"):
                                await self.process_appointment_reminder(appointment, now, "24h")
                                delayed_24h_count += 1
                    else:
                        print(f"⏰ Запис {appointment['id']} на {appointment_time} вже пройшов, пропускаємо 24-годинне нагадування")

            # Проверяем 1-часовые напоминания, которые могли быть пропущены
            # НО только для записей, которые еще НЕ ПРОШЛИ (должны быть в будущем)
            one_hour_from_now = now + timedelta(hours=1)
            one_hour_start = one_hour_from_now.replace(minute=0, second=0, microsecond=0)
            one_hour_end = one_hour_start + timedelta(hours=1)

            delayed_1h_response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status"
            ).eq("status", "scheduled").gte("scheduled_at", one_hour_start.isoformat()).lt("scheduled_at", one_hour_end.isoformat()).execute()

            delayed_1h_count = 0
            if delayed_1h_response.data:
                print(f"📭 Перевіряємо {len(delayed_1h_response.data)} записів для відкладених 1-годинних нагадувань")
                for appointment in delayed_1h_response.data:
                    # Проверяем, что запись еще не началась (время записи больше текущего времени)
                    dt_raw = safe_parse_datetime(appointment['scheduled_at'])
                    if not dt_raw:
                        logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                        continue # пропустить эту запись
                    appointment_time = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

                    # Отправляем напоминание только если запись еще не началась
                    if appointment_time > now:
                        # ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА: отправляем только если прошло достаточно времени с момента регулярной проверки
                        last_check_time = now - timedelta(minutes=30)  # Регулярная проверка могла быть 30 минут назад

                        # Проверяем логи уведомлений за последний 30 минут
                        recent_logs = self.supabase.table("notification_logs").select(
                            "sent_at, type"
                        ).eq("appointment_id", appointment['id']).eq("type", "1h").eq("status", "sent").gte("sent_at", last_check_time.isoformat()).execute()

                        # Отправляем только если нет недавних логов этого типа
                        if not recent_logs.data:
                            # Проверяем, было ли уже отправлено 1-часовое напоминание
                            if not await self.check_notification_sent(appointment['id'], "1h"):
                                await self.process_appointment_reminder(appointment, now, "1h")
                                delayed_1h_count += 1
                    else:
                        print(f"⏰ Запис {appointment['id']} на {appointment_time} вже пройшов, пропускаємо 1-годинне нагадування")

            if delayed_24h_count > 0 or delayed_1h_count > 0:
                print(f"✅ Відправлено відкладених нагадувань: 24h={delayed_24h_count}, 1h={delayed_1h_count}")
            else:
                print("📭 Відкладених нагадувань для відправки немає")
                        
        except Exception as e:
            print(f"❌ Помилка при відправці відкладених нагадувань: {e}")

    def is_working_hours(self, current_time=None):
        """Проверяет, находится ли текущее время в рабочем диапазоне (8:00-21:00)"""
        if current_time is None:
            current_time = datetime.now(ZoneInfo("Europe/Kyiv"))
        
        # Рабочие часы: с 8:00 до 21:00
        start_hour = 8
        end_hour = 21
        
        current_hour = current_time.hour
        
        return start_hour <= current_hour < end_hour

    async def check_no_response_reminder(self, appointment, now):
        """Проверяет, нужно ли отправить напоминание об отсутствии ответа"""
        try:
            appointment_id = appointment['id']

            # Получаем данные клиента по client_id
            client_response = self.supabase.table("clients").select("name").eq("id", appointment['client_id']).execute()
            client_name = "Клієнт"
            if client_response.data:
                client_name = extract_first_name(client_response.data[0]['name'])

            # Получаем время записи для читаемого лога
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            appointment_time = "неизвестное время"
            if dt_raw:
                appointment_time = dt_raw.astimezone(ZoneInfo("Europe/Kyiv")).strftime('%d.%m.%Y %H:%M')

            # Проверяем, было ли отправлено 24-часовое напоминание
            if not await self.check_notification_sent(appointment_id, "24h"):
                print(f"📝 24-ГОДИННОЕ НАПОМИНАНИЕ КЛИЕНТУ {client_name} на {appointment_time} еще не отправлялось")
                return

            # Проверяем, было ли уже отправлено напоминание об отсутствии ответа
            if await self.check_notification_sent(appointment_id, "1h"):
                print(f"📝 НАПОМИНАНИЕ ОБ ОТСУТСТВИИ ОТВЕТА КЛИЕНТУ {client_name} на {appointment_time} уже отправлялось")
                return
            
            # Проверяем, прошло ли 6 часов с момента отправки 24-часового напоминания
            notification_log_response = self.supabase.table("notification_logs").select(
                "sent_at"
            ).eq("appointment_id", appointment_id).eq("type", "24h").execute()
            
            if not notification_log_response.data:
                print(f"📝 ЛОГ 24-ГОДИННОГО НАПОМИНАНИЯ КЛИЕНТУ {client_name} на {appointment_time} не найден")
                return

            dt_raw = safe_parse_datetime(notification_log_response.data[0]['sent_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой уведомления: клиент={client_name}, время={appointment_time}")
                return
            notification_time = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

            time_since_24h_reminder = now - notification_time

            if time_since_24h_reminder < timedelta(hours=6):
                hours_passed = time_since_24h_reminder.total_seconds() / 3600
                print(f"📝 С МОМЕНТА 24-ГОДИННОГО НАПОМИНАНИЯ КЛИЕНТУ {client_name} на {appointment_time} прошло только {hours_passed:.1f} часов (минимум 6)")
                return

            # Проверяем, не прошло ли слишком много времени (больше 24 часов)
            if time_since_24h_reminder > timedelta(hours=24):
                hours_passed = time_since_24h_reminder.total_seconds() / 3600
                print(f"📝 С МОМЕНТА 24-ГОДИННОГО НАПОМИНАНИЯ КЛИЕНТУ {client_name} на {appointment_time} прошло {hours_passed:.1f} часов (>24), пропускаем")
                return
            
            # Проверяем, подтвердил ли клиент запись
            appointment_response = self.supabase.table("appointments").select(
                "client_confirmed"
            ).eq("id", appointment_id).execute()
            
            if appointment_response.data and appointment_response.data[0].get('client_confirmed'):
                print(f"📝 КЛИЕНТ {client_name} УЖЕ ПОДТВЕРДИЛ ЗАПИСЬ на {appointment_time}")
                return
            
            # Отправляем напоминание об отсутствии ответа с использованием шаблона no_response
            await self.process_appointment_reminder_no_response(appointment, now)
            
        except Exception as e:
            logger.error("❌ Ошибка проверки отсутствия ответа", exc_info=True)

    async def check_notification_sent(self, appointment_id: str, notification_type: str) -> bool:
        try:
            response = self.supabase.table("notification_logs") \
                .select("id") \
                .eq("appointment_id", appointment_id) \
                .eq("type", notification_type) \
                .eq("status", "sent") \
                .execute()
            return len(response.data) > 0
        except Exception as e:
            print("❌ check_notification_sent error:", e)
            return False

    async def log_notification_sent(self, appointment_id: str, notification_type: str):
        try:
            # Создаем время с ограниченными микросекундами для корректного ISO формата
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            # Ограничиваем микросекунды до 6 цифр для корректного ISO формата
            now = now.replace(microsecond=now.microsecond // 10 * 10)

            log_data = {
                "appointment_id": appointment_id,
                "type": notification_type,
                "sent_at": now.isoformat(),
                "status": "sent"
            }

            # Сначала проверяем, существует ли уже запись со статусом 'sent'
            existing = self.supabase.table("notification_logs") \
                .select("id") \
                .eq("appointment_id", appointment_id) \
                .eq("type", notification_type) \
                .eq("status", "sent") \
                .execute()

            if existing.data:
                # Если запись существует, обновляем время отправки
                self.supabase.table("notification_logs") \
                    .update({"sent_at": now.isoformat()}) \
                    .eq("appointment_id", appointment_id) \
                    .eq("type", notification_type) \
                    .eq("status", "sent") \
                    .execute()
            else:
                # Если записи нет, вставляем новую
                self.supabase.table("notification_logs").insert(log_data).execute()

            print(f"✅ Уведомление залогировано: {appointment_id} - {notification_type}")

        except Exception as e:
            print("❌ log_notification_sent error:", e)
            # В случае ошибки всё равно продолжаем работу
            # Уведомление уже отправлено, просто не смогли залогировать

    async def should_send_admin_reschedule_notification(self, appointment_id: str, scheduled_at: str) -> bool:
        """Проверяет, нужно ли отправлять уведомление о переносе администратором.
        Отправляет уведомление только один раз для каждого переноса.
        """
        try:
            # Проверяем, было ли уже отправлено уведомление для этого времени записи
            existing_notifications = self.supabase.table("notification_logs") \
                .select("id, sent_at") \
                .eq("appointment_id", appointment_id) \
                .eq("type", "admin_reschedule") \
                .eq("status", "sent") \
                .execute()

            if existing_notifications.data:
                # Есть отправленные уведомления, проверяем время последнего
                last_notification = existing_notifications.data[0]
                last_sent_time = safe_parse_datetime(last_notification['sent_at'])

                if last_sent_time:
                    # Получаем время последнего обновления записи
                    appointment_response = self.supabase.table("appointments") \
                        .select("updated_at") \
                        .eq("id", appointment_id) \
                        .execute()

                    if appointment_response.data:
                        updated_at = safe_parse_datetime(appointment_response.data[0].get('updated_at'))
                        if updated_at and updated_at > last_sent_time:
                            # Запись была обновлена после отправки последнего уведомления
                            print(f"🔄 ОТПРАВЛЯЮ НОВОЕ УВЕДОМЛЕНИЕ О ПЕРЕНОСЕ ЗАПИСИ {appointment_id} (запись обновлена)")
                            return True
                        else:
                            # Запись не обновлялась после отправки уведомления
                            print(f"⏰ УВЕДОМЛЕНИЕ О ПЕРЕНОСЕ ЗАПИСИ {appointment_id} УЖЕ ОТПРАВЛЕНО")
                            return False

            # Уведомлений еще не было, отправляем первое
            print(f"🔄 ОТПРАВЛЯЮ ПЕРВОЕ УВЕДОМЛЕНИЕ О ПЕРЕНОСЕ ЗАПИСИ {appointment_id}")
            return True

        except Exception as e:
            print(f"❌ should_send_admin_reschedule_notification error: {e}")
            return False  # В случае ошибки не отправляем дубликаты

    async def my_appointments_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            chat_id = str(update.effective_chat.id)
            telegram_name = update.effective_chat.first_name or update.effective_chat.username or "Unknown"

            client_resp = self.supabase.table("clients").select("id, name").eq("telegram_chat_id", chat_id).execute()
            if not client_resp.data:
                print(f"❌ КЛИЕНТ НЕ НАЙДЕН: {telegram_name}, чат {chat_id} не зарегистрирован")
                await update.message.reply_text("❌ Ваш акаунт не знайдено. Спочатку підключіть бота через /start.")
                return

            full_name = client_resp.data[0]['name']
            client_name = extract_first_name(full_name)
            client_id = client_resp.data[0]['id']
            print(f"📋 КЛИЕНТ {client_name} ЗАПРОСИЛ СПИСОК СВОИХ ЗАПИСЕЙ (чат {chat_id})")

            # Обновляем статистику активности пользователя
            await self.update_user_activity(chat_id, client_id, client_name, "my_appointments")
            now = datetime.now(ZoneInfo("Europe/Kyiv"))

            # Получаем только активные записи (scheduled) и прошедшие отмененные (для истории)
            active_appointments = self.supabase.table("appointments").select(
                "scheduled_at, service_id, status, client_confirmed"
            ).eq("client_id", client_id).eq("status", "scheduled").gte("scheduled_at", now.isoformat()).order("scheduled_at").execute().data or []

            # Получаем недавние отмененные записи (за последние 7 дней для истории)
            week_ago = now - timedelta(days=7)
            cancelled_appointments = self.supabase.table("appointments").select(
                "scheduled_at, service_id, status, client_confirmed"
            ).eq("client_id", client_id).eq("status", "cancelled").gte("scheduled_at", week_ago.isoformat()).order("scheduled_at").execute().data or []

            # Объединяем активные и недавние отмененные записи
            appointments = active_appointments + cancelled_appointments

            # Сортируем по дате
            appointments.sort(key=lambda x: x['scheduled_at'])
            if not appointments:
                print(f"📋 У КЛИЕНТА {client_name} НЕТ АКТИВНЫХ ЗАПИСЕЙ")
                await update.message.reply_text("У вас немає майбутніх або недавніх скасованих записів.")
                return

            print(f"📋 ПОКАЗЫВАЮ СПИСОК ЗАПИСЕЙ: {len(appointments)} записей для {client_name}")

            service_ids = list(set(a['service_id'] for a in appointments))
            services = self.supabase.table("services").select("id, name").in_("id", service_ids).execute().data
            service_map = {s['id']: s['name'] for s in services}
            status_map = {
                'scheduled': 'Заплановано',
                'completed': 'Завершено',
                'cancelled': 'Скасовано',
                'no_show': "Не з'явився"
            }
            msg = f"Ваші записи, {client_name}:\n\n"
            for a in appointments:
                dt_raw = safe_parse_datetime(a['scheduled_at'])
                if not dt_raw:
                    logger.warning(f"Пропускаем запись с некорректной датой: id={a.get('id')}")
                    continue # пропустить эту запись
                dt = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))

                # Определяем статус с учетом подтверждения клиента
                if a['status'] == 'scheduled' and a.get('client_confirmed') == True:
                    status = 'Ти вже підтвердив'
                else:
                    status = status_map.get(a['status'], a['status'])

                msg += f"📅 {dt.strftime('%d.%m.%Y %H:%M')} ({status})\n"
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error("❌ Ошибка в my_appointments_command", exc_info=True)
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def support_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Маєш запитання - дзвони (096) 35-102-35")

    async def quick_reschedule_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Быстрый доступ к переносу записи"""
        try:
            chat_id = str(update.effective_chat.id)
            telegram_name = update.effective_chat.first_name or update.effective_chat.username or "Unknown"

            client_resp = self.supabase.table("clients").select("id, name").eq("telegram_chat_id", chat_id).execute()
            if not client_resp.data:
                print(f"❌ КЛИЕНТ НЕ НАЙДЕН: {telegram_name}, чат {chat_id} не зарегистрирован")
                await update.message.reply_text("❌ Ваш акаунт не знайдено. Спочатку підключіть бота через /start.")
                return
            
            full_name = client_resp.data[0]['name']
            client_name = extract_first_name(full_name)
            client_id = client_resp.data[0]['id']
            print(f"🔄 КЛИЕНТ {client_name} ЗАПРОСИЛ ПЕРЕНОС ЗАПИСИ (чат {chat_id})")

            # Обновляем статистику активности пользователя
            await self.update_user_activity(chat_id, client_id, client_name, "reschedule")
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            
            # Получаем будущие записи клиента
            appointments = self.supabase.table("appointments").select(
                "id, scheduled_at, service_id, status"
            ).eq("client_id", client_id).eq("status", "scheduled").gte("scheduled_at", now.isoformat()).order("scheduled_at").execute().data
            
            if not appointments:
                await update.message.reply_text("У вас немає майбутніх записів для перенесення.")
                return
            
            # Если есть только одна запись, сразу показываем даты для переноса
            if len(appointments) == 1:
                appointment = appointments[0]
                await self.show_reschedule_dates_for_appointment(update, appointment['id'])
            else:
                # Если несколько записей, показываем список для выбора
                await self.show_appointments_for_reschedule(update, appointments)
                
        except Exception as e:
            logger.error("❌ Ошибка в quick_reschedule_command", exc_info=True)
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def quick_cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Быстрый доступ к отмене записи"""
        try:
            chat_id = str(update.effective_chat.id)
            telegram_name = update.effective_chat.first_name or update.effective_chat.username or "Unknown"

            client_resp = self.supabase.table("clients").select("id, name").eq("telegram_chat_id", chat_id).execute()
            if not client_resp.data:
                print(f"❌ КЛИЕНТ НЕ НАЙДЕН: {telegram_name}, чат {chat_id} не зарегистрирован")
                await update.message.reply_text("❌ Ваш акаунт не знайдено. Спочатку підключіть бота через /start.")
                return
            
            full_name = client_resp.data[0]['name']
            client_name = extract_first_name(full_name)
            client_id = client_resp.data[0]['id']
            print(f"❌ КЛИЕНТ {client_name} ЗАПРОСИЛ ОТМЕНУ ЗАПИСИ (чат {chat_id})")

            # Обновляем статистику активности пользователя
            await self.update_user_activity(chat_id, client_id, client_name, "cancel")
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            
            # Получаем будущие записи клиента
            appointments = self.supabase.table("appointments").select(
                "id, scheduled_at, service_id, status"
            ).eq("client_id", client_id).eq("status", "scheduled").gte("scheduled_at", now.isoformat()).order("scheduled_at").execute().data
            
            if not appointments:
                await update.message.reply_text("У вас немає майбутніх записів для скасування.")
                return
            
            # Если есть только одна запись, сразу предлагаем отменить
            if len(appointments) == 1:
                appointment = appointments[0]
                await self.show_cancel_confirmation(update, appointment['id'])
            else:
                # Если несколько записей, показываем список для выбора
                await self.show_appointments_for_cancel(update, appointments)
                
        except Exception as e:
            logger.error("❌ Ошибка в quick_cancel_command", exc_info=True)
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def confirm_appointments_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Быстрый доступ к подтверждению записей"""
        try:
            chat_id = str(update.effective_chat.id)
            telegram_name = update.effective_chat.first_name or update.effective_chat.username or "Unknown"

            client_resp = self.supabase.table("clients").select("id, name").eq("telegram_chat_id", chat_id).execute()
            if not client_resp.data:
                print(f"❌ КЛИЕНТ НЕ НАЙДЕН: {telegram_name}, чат {chat_id} не зарегистрирован")
                await update.message.reply_text("❌ Ваш акаунт не знайдено. Спочатку підключіть бота через /start.")
                return

            full_name = client_resp.data[0]['name']
            client_name = extract_first_name(full_name)
            client_id = client_resp.data[0]['id']
            print(f"✅ КЛИЕНТ {client_name} ЗАПРОСИЛ СПИСОК ЗАПИСЕЙ ДЛЯ ПОДТВЕРЖДЕНИЯ (чат {chat_id})")

            # Обновляем статистику активности пользователя
            await self.update_user_activity(chat_id, client_id, client_name, "confirm")
            now = datetime.now(ZoneInfo("Europe/Kyiv"))

            # Получаем неподтвержденные записи клиента (будущие)
            appointments = self.supabase.table("appointments").select(
                "id, scheduled_at, service_id, status, client_confirmed"
            ).eq("client_id", client_id).eq("status", "scheduled").eq("client_confirmed", False).gte("scheduled_at", now.isoformat()).order("scheduled_at").execute().data

            if not appointments:
                await update.message.reply_text("У вас немає записів, які потребують підтвердження.")
                return

            # Если есть только одна запись, сразу предлагаем подтвердить
            if len(appointments) == 1:
                appointment = appointments[0]
                await self.show_confirm_confirmation(update, appointment['id'])
            else:
                # Если несколько записей, показываем список для выбора
                await self.show_appointments_for_confirm(update, appointments)

        except Exception as e:
            logger.error("❌ Ошибка в confirm_appointments_command", exc_info=True)
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_confirm_confirmation(self, update_or_query, appointment_id: str):
        """Показывает подтверждение для подтверждения записи"""
        try:
            # Определяем, получили ли мы Update или CallbackQuery
            if hasattr(update_or_query, 'effective_chat'):
                # Это Update из команды
                chat_id = str(update_or_query.effective_chat.id)
                send_message = update_or_query.message.reply_text
            else:
                # Это CallbackQuery
                chat_id = str(update_or_query.effective_chat.id)
                send_message = update_or_query.edit_message_text

            # Получаем данные записи
            appointment_resp = self.supabase.table("appointments").select(
                "scheduled_at, service_id, client_id"
            ).eq("id", appointment_id).execute()

            if not appointment_resp.data:
                await send_message("❌ Запис не знайдено")
                return

            appointment = appointment_resp.data[0]

            # Получаем данные клиента
            client_resp = self.supabase.table("clients").select("name").eq("id", appointment['client_id']).execute()
            full_name = client_resp.data[0]['name'] if client_resp.data else "Клієнт"
            client_name = extract_first_name(full_name)

            # Получаем данные услуги
            service_resp = self.supabase.table("services").select("name").eq("id", appointment['service_id']).execute()
            service_name = service_resp.data[0]['name'] if service_resp.data else "масаж"

            # Форматируем дату и время
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                await send_message("❌ Помилка в даті запису")
                return
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")

            # Создаем сообщение с подтверждением
            message = (
                f"🔔 Підтвердіть запис:\n\n"
                f"📅 Дата: {formatted_date}\n"
                f"🕐 Час: {formatted_time}\n\n"
                f"Будь ласка, підтвердіть цю запис."
            )

            # Добавляем кнопки подтверждения
            keyboard = [
                [InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment_id}")],
                [InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment_id}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await send_message(message, reply_markup=reply_markup)

        except Exception as e:
            logger.error("❌ Ошибка в show_confirm_confirmation", exc_info=True)
            try:
                if hasattr(update_or_query, 'effective_chat'):
                    await update_or_query.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
                else:
                    await update_or_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_appointments_for_confirm(self, update_or_query, appointments):
        """Показывает список записей для подтверждения"""
        try:
            # Определяем, получили ли мы Update или CallbackQuery
            if hasattr(update_or_query, 'effective_chat'):
                # Это Update из команды
                send_message = update_or_query.message.reply_text
            else:
                # Это CallbackQuery
                send_message = update_or_query.edit_message_text

            if not appointments:
                await send_message("У вас немає записів для підтвердження.")
                return

            message = "Оберіть запис для підтвердження:\n\n"

            keyboard = []
            for appointment in appointments:
                # Получаем данные услуги
                service_resp = self.supabase.table("services").select("name").eq("id", appointment['service_id']).execute()
                service_name = service_resp.data[0]['name'] if service_resp.data else "масаж"

                # Форматируем дату и время
                dt_raw = safe_parse_datetime(appointment['scheduled_at'])
                if dt_raw:
                    dt = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
                    formatted_date = dt.strftime("%d.%m.%Y")
                    formatted_time = dt.strftime("%H:%M")
                    display_text = f"{formatted_date} {formatted_time}"

                    keyboard.append([
                        InlineKeyboardButton(display_text, callback_data=f"select_confirm_{appointment['id']}")
                    ])

            # Добавляем кнопку "Назад"
            keyboard.append([
                InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")
            ])

            reply_markup = InlineKeyboardMarkup(keyboard)
            await send_message(message, reply_markup=reply_markup)

        except Exception as e:
            logger.error("❌ Ошибка в show_appointments_for_confirm", exc_info=True)
            try:
                if hasattr(update_or_query, 'effective_chat'):
                    await update_or_query.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
                else:
                    await update_or_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_reschedule_dates_for_appointment(self, update_or_query, appointment_id: str):
        """Показывает даты для переноса конкретной записи"""
        try:
            # Определяем, получили ли мы Update или CallbackQuery
            if hasattr(update_or_query, 'effective_chat'):
                # Это Update из команды
                chat_id = str(update_or_query.effective_chat.id)
                send_message = update_or_query.message.reply_text
            elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                # Это Update с callback_query
                query = update_or_query.callback_query
                chat_id = str(query.message.chat.id)
                send_message = query.edit_message_text
            elif hasattr(update_or_query, 'message') and update_or_query.message:
                # Это CallbackQuery
                query = update_or_query
                chat_id = str(query.message.chat.id)
                send_message = query.edit_message_text
            else:
                print(f"❌ Неизвестный тип объекта: {type(update_or_query)}")
                return

            # Получаем данные записи
            appointment_response = self.supabase.table("appointments").select(
                "scheduled_at, service_id"
            ).eq("id", appointment_id).execute()

            if not appointment_response.data:
                await send_message("❌ Запис не знайдено.")
                return

            appointment = appointment_response.data[0]
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                await send_message("❌ Запис не знайдено.")
                return
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            
            # Показываем даты для переноса (7 дней, исключая воскресенье)
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            dates = []
            for i in range(1, 8):
                date = now.date() + timedelta(days=i)
                if date.weekday() != 6:  # Исключаем воскресенье
                    dates.append(date)
            
            if not dates:
                await send_message("❌ Немає доступних дат для перенесення.")
                return
            
            # Создаем кнопки с датами
            keyboard = []
            for date in dates:
                date_str = date.strftime("%d.%m.%Y")
                day_name = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб"][date.weekday()]
                keyboard.append([InlineKeyboardButton(f"{day_name}, {date_str}", callback_data=f"reschedule_date_{date_str}")])
            
            keyboard.append([InlineKeyboardButton("❌ Скасувати", callback_data="reschedule_cancel")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Устанавливаем состояние для переноса
            self.user_states[chat_id] = {
                'state': BotState.RESCHEDULE_DATE,
                'appointment_id': appointment_id
            }
            
            await send_message(
                f"🔄 Оберіть нову дату для перенесення запису:\n"
                f"📅 Поточна дата: {scheduled_at.strftime('%d.%m.%Y %H:%M')}",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error("❌ Ошибка в show_reschedule_dates_for_appointment", exc_info=True)
            # Пытаемся отправить сообщение об ошибке
            try:
                if hasattr(update_or_query, 'effective_chat'):
                    await update_or_query.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                    await update_or_query.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'message'):
                    await update_or_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_appointments_for_reschedule(self, update_or_query, appointments):
        """Показывает список записей для выбора переноса"""
        try:
            # Определяем, получили ли мы Update или CallbackQuery
            if hasattr(update_or_query, 'effective_chat'):
                # Это Update из команды
                send_message = update_or_query.message.reply_text
            elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                # Это Update с callback_query
                send_message = update_or_query.callback_query.edit_message_text
            elif hasattr(update_or_query, 'message') and update_or_query.message:
                # Это CallbackQuery
                send_message = update_or_query.edit_message_text
            else:
                print(f"❌ Неизвестный тип объекта: {type(update_or_query)}")
                return

            keyboard = []
            for appointment in appointments:
                dt_raw = safe_parse_datetime(appointment['scheduled_at'])
                if not dt_raw:
                    logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                    continue # пропустить эту запись
                scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
                
                date_str = scheduled_at.strftime("%d.%m.%Y %H:%M")
                keyboard.append([InlineKeyboardButton(f"📅 {date_str}", callback_data=f"select_reschedule_{appointment['id']}")])
            
            keyboard.append([InlineKeyboardButton("❌ Скасувати", callback_data="reschedule_cancel")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await send_message(
                "🔄 Оберіть запис для перенесення:",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error("❌ Ошибка в show_appointments_for_reschedule", exc_info=True)
            # Пытаемся отправить сообщение об ошибке
            try:
                if hasattr(update_or_query, 'effective_chat'):
                    await update_or_query.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                    await update_or_query.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'message'):
                    await update_or_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_cancel_confirmation(self, update_or_query, appointment_id: str):
        """Показывает подтверждение отмены записи"""
        try:
            # Определяем, получили ли мы Update или CallbackQuery
            if hasattr(update_or_query, 'effective_chat'):
                # Это Update из команды
                send_message = update_or_query.message.reply_text
            elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                # Это Update с callback_query
                query = update_or_query.callback_query
                send_message = query.edit_message_text
            elif hasattr(update_or_query, 'message') and update_or_query.message:
                # Это CallbackQuery
                query = update_or_query
                send_message = query.edit_message_text
            else:
                print(f"❌ Неизвестный тип объекта: {type(update_or_query)}")
                return
            
            # Получаем данные записи
            appointment_response = self.supabase.table("appointments").select(
                "scheduled_at, service_id"
            ).eq("id", appointment_id).execute()
            
            if not appointment_response.data:
                await send_message("❌ Запис не знайдено.")
                return
            
            appointment = appointment_response.data[0]
            dt_raw = safe_parse_datetime(appointment['scheduled_at'])
            if not dt_raw:
                logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                await send_message("❌ Запис не знайдено.")
                return
            scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
            
            keyboard = [
                [InlineKeyboardButton("✅ Підтвердити скасування", callback_data=f"cancel_{appointment_id}")],
                [InlineKeyboardButton("❌ Відмінити", callback_data="reschedule_cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await send_message(
                f"❌ Ви дійсно хочете скасувати запис?\n"
                f"📅 Дата: {scheduled_at.strftime('%d.%m.%Y %H:%M')}",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error("❌ Ошибка в show_cancel_confirmation", exc_info=True)
            # Пытаемся отправить сообщение об ошибке
            try:
                if hasattr(update_or_query, 'effective_chat'):
                    await update_or_query.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                    await update_or_query.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'message'):
                    await update_or_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def show_appointments_for_cancel(self, update_or_query, appointments):
        """Показывает список записей для выбора отмены"""
        try:
            # Определяем, получили ли мы Update или CallbackQuery
            if hasattr(update_or_query, 'effective_chat'):
                # Это Update из команды
                send_message = update_or_query.message.reply_text
            elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                # Это Update с callback_query
                send_message = update_or_query.callback_query.edit_message_text
            elif hasattr(update_or_query, 'message') and update_or_query.message:
                # Это CallbackQuery
                send_message = update_or_query.edit_message_text
            else:
                print(f"❌ Неизвестный тип объекта: {type(update_or_query)}")
                return

            keyboard = []
            for appointment in appointments:
                dt_raw = safe_parse_datetime(appointment['scheduled_at'])
                if not dt_raw:
                    logger.warning(f"Пропускаем запись с некорректной датой: id={appointment.get('id')}")
                    continue # пропустить эту запись
                scheduled_at = dt_raw.astimezone(ZoneInfo("Europe/Kyiv"))
                
                date_str = scheduled_at.strftime("%d.%m.%Y %H:%M")
                keyboard.append([InlineKeyboardButton(f"📅 {date_str}", callback_data=f"select_cancel_{appointment['id']}")])
            
            keyboard.append([InlineKeyboardButton("❌ Скасувати", callback_data="reschedule_cancel")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await send_message(
                "❌ Оберіть запис для скасування:",
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error("❌ Ошибка в show_appointments_for_cancel", exc_info=True)
            # Пытаемся отправить сообщение об ошибке
            try:
                if hasattr(update_or_query, 'effective_chat'):
                    await update_or_query.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'callback_query') and update_or_query.callback_query:
                    await update_or_query.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
                elif hasattr(update_or_query, 'message'):
                    await update_or_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    def run(self):
        logger.info("🚀 Запуск Telegram бота...")
        # Запускаем polling в фоне
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Запускаем scheduler в event loop
        loop.run_until_complete(self.start_scheduler())

        # Запускаем polling
        self.application.run_polling()

def main():
    logger.info("🚀 Запуск Telegram бота...")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    admin_chat_id = os.getenv("ADMIN_CHAT_ID") # <-- Добавили
    if not all([token, supabase_url, supabase_key, admin_chat_id]): # <-- Добавили
        logger.error("❌ Відсутні змінні середовища")
        return
    bot = MassageReminderBot(token, supabase_url, supabase_key, admin_chat_id) # <-- Передаем в конструктор
    bot.run()

if __name__ == "__main__":
    main()
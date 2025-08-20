#!/usr/bin/env python3
"""
Telegram-бот для подключения клиентов через номер телефона
Упрощенная версия без токенов и сложной логики
"""
import os
import re
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler
from supabase import create_client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from enum import Enum, auto

# Локальный модуль
from scheduler import TimeSlotScheduler

load_dotenv()

ADMIN_CHAT_ID = "790881459"

def normalize_phone(phone: str) -> str:
    """Нормализует номер телефона к формату +380..."""
    phone = re.sub(r'[\s\(\)\-]', '', phone)
    if phone.startswith('380') and not phone.startswith('+380'):
        phone = '+' + phone
    if phone.startswith('0'):
        phone = '+38' + phone
    return phone

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
    def __init__(self, token, supabase_url, supabase_key):
        self.supabase = create_client(supabase_url, supabase_key)
        self.application = Application.builder().token(token).build()
        self.scheduler = AsyncIOScheduler()
        self.time_scheduler = TimeSlotScheduler(self.supabase)  # Управление слотами
        self.user_states = {}  # chat_id: {'state': BotState, ...}
        self.setup_handlers()
        self.setup_scheduler()

    def setup_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("my_appointments", self.my_appointments_command))
        self.application.add_handler(MessageHandler(filters.Regex("^📋 Мої записи$"), self.my_appointments_command))
        self.application.add_handler(MessageHandler(filters.Regex("^💬 Підтримка$"), self.support_command))
        self.application.add_handler(MessageHandler(filters.CONTACT, self.contact_handler))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.name_handler))
        self.application.add_handler(CallbackQueryHandler(self.button_callback))

    def setup_scheduler(self):
        """Планировщик проверяет записи каждые 2 минуты"""
        self.scheduler.add_job(
            self.check_and_send_reminders,
            CronTrigger(minute="*/2"),
            id="reminder_checker",
            replace_existing=True
        )
        print("⏰ Планировщик напоминаний настроен (каждые 2 минуты)")

    # ----- /start -----
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            chat_id = str(update.effective_chat.id)
            response = self.supabase.table("clients").select("name").eq("telegram_chat_id", chat_id).execute()
            main_keyboard = ReplyKeyboardMarkup(
                [["📋 Мої записи", "💬 Підтримка"]],
                resize_keyboard=True
            )
            if response.data:
                client_name = response.data[0].get("name", "Клієнт")
                await update.message.reply_text(
                    f"🎉 Привіт, {client_name}!\nВаш акаунт вже підключено до бота.\n\n"
                    "Тепер ви будете отримувати сповіщення про майбутні записи на масаж.",
                    reply_markup=main_keyboard
                )
            else:
                keyboard = [[KeyboardButton("📱 Поділитися номером", request_contact=True)]]
                reply_markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
                await update.message.reply_text(
                    "👋 Ласкаво просимо до бота нагадувань про масаж!\n\n"
                    "Для підключення до бота, будь ласка, поділіться своїм номером телефону.",
                    reply_markup=reply_markup
                )
        except Exception as e:
            print(f"❌ Ошибка в start_command: {e}")
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
            main_keyboard = ReplyKeyboardMarkup(
                [["📋 Мої записи", "💬 Підтримка"]],
                resize_keyboard=True
            )
            client = await self.get_client_by_phone(phone)
            if client:
                await self.update_client_telegram_id(client["id"], chat_id)
                await update.message.reply_text(
                    f"🎉 Привіт, {client.get('name', 'Клієнт')}!\nВаш акаунт підключено.",
                    reply_markup=main_keyboard
                )
            else:
                context.user_data["pending_phone"] = phone
                context.user_data["pending_chat_id"] = chat_id
                await update.message.reply_text("✅ Новий клієнт!\n\nБудь ласка, введіть ваше ім'я:")
        except Exception as e:
            print(f"Помилка: {e}")
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Имя -----
    async def name_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.user_data.get("pending_phone"):
            return
        try:
            name = update.message.text.strip()
            if len(name) < 2 or len(name) > 50 or re.search(r'[<>"\']', name):
                await update.message.reply_text("❌ Ім'я некоректне. Спробуйте ще раз:")
                return

            phone = context.user_data["pending_phone"]
            chat_id = context.user_data["pending_chat_id"]
            await self.create_new_client(name, phone, chat_id)
            context.user_data.clear()
            await update.message.reply_text(
                f"🎉 Відмінно, {name}!\nВи успішно підключені до бота."
            )
        except Exception as e:
            print(f"Помилка: {e}")
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Callback -----
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            query = update.callback_query
            await query.answer()
            data = query.data
            chat_id = str(query.message.chat_id)
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

            if data.startswith("confirm_"):
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
                else:
                    await query.edit_message_text("❌ Невідома дія")
            elif data.startswith("reschedule_"):
                appointment_id = data.replace("reschedule_", "")
                user_state['appointment_id'] = appointment_id
                user_state['state'] = BotState.RESCHEDULE_DATE
                await self.show_reschedule_dates(query)
            else:
                await query.edit_message_text("❌ Невідома дія")
        except Exception as e:
            print(f"❌ Ошибка в button_callback: {e}")
            try:
                await update.callback_query.edit_message_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    # ----- Подтверждение/отмена -----
    async def handle_confirmation(self, query, appointment_id: str, action: str):
        try:
            response = self.supabase.table("appointments").select(
                "id, client_id, status"
            ).eq("id", appointment_id).execute()
            if not response.data:
                await query.edit_message_text("❌ Запис не знайдено")
                return

            appointment = response.data[0]
            client_response = self.supabase.table("clients").select("name").eq("id", appointment['client_id']).execute()
            client_name = client_response.data[0]['name'] if client_response.data else "Клієнт"

            if action == "confirm":
                self.supabase.table("appointments").update({
                    "client_confirmed": True,
                    "confirmation_time": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat()
                }).eq("id", appointment_id).execute()
                await query.edit_message_text(f"✅ Дякуємо, {client_name}! Ваша запис підтверджена.")
                await self.application.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"✅ Клієнт {client_name} підтвердив запис.")
            elif action == "cancel":
                self.supabase.table("appointments").update({"status": "cancelled"}).eq("id", appointment_id).execute()
                await query.edit_message_text(f"❌ Запис скасовано, {client_name}.")
        except Exception as e:
            print(f"❌ Помилка: {e}")
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

            old_appointment = self.supabase.table("appointments").select("service_id").eq("id", appointment_id).execute().data
            if not old_appointment:
                await query.edit_message_text("❌ Запис не знайдено.")
                return

            service_id = old_appointment[0]['service_id']
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
            print(f"❌ Ошибка в show_free_slots: {e}")
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
            old_dt = datetime.fromisoformat(old['scheduled_at'])
            if old_dt.tzinfo is None:
                old_dt = old_dt.replace(tzinfo=ZoneInfo("UTC"))
            old_dt_kyiv = old_dt.astimezone(ZoneInfo("Europe/Kyiv"))
            old_date_str = old_dt_kyiv.strftime("%d.%m.%Y")
            old_time_str = old_dt_kyiv.strftime("%H:%M")
            
            service_id = old['service_id']
            service_resp = self.supabase.table("services").select("duration_minutes").eq("id", service_id).execute()
            duration = service_resp.data[0]['duration_minutes'] if service_resp.data else 60

            free_slots = await self.time_scheduler.get_free_slots(date_dt, duration)
            allowed_times = [dt.strftime("%H:%M") for dt in free_slots]

            if time_str not in allowed_times:
                await query.edit_message_text("❌ Цей слот більше недоступний. Оберіть інший.")
                return

            notes = "ПЕРЕНЕСЕНО КЛІЄНТОМ"
            # Обновляем существующую запись вместо создания новой
            update_data = {
                "scheduled_at": new_dt_utc.isoformat(),
                "notes": notes,
                # ВАЖНО: оставляем статус "scheduled", чтобы слот считался занятым и для других клиентов
                "status": "scheduled"
            }
            self.supabase.table("appointments").update(update_data).eq("id", appointment_id).execute()

            await query.edit_message_text(f"✅ Запис успішно перенесено на {date_str} о {new_dt_kyiv.strftime('%H:%M')}!")

            client = self.supabase.table("clients").select("name").eq("id", old['client_id']).execute().data
            client_name = client[0]['name'] if client else "Клієнт"
            await self.application.bot.send_message(
                chat_id=ADMIN_CHAT_ID,
                text=f"🔄 Клієнт {client_name} переніс запис з {old_date_str} о {old_time_str} на {date_str} о {new_dt_kyiv.strftime('%H:%M')}."
            )

            self.user_states[chat_id] = {'state': BotState.NONE}
        except Exception as e:
            print(f"❌ Ошибка в handle_reschedule_time: {e}")
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
            tomorrow_start = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow_end = tomorrow_start + timedelta(days=1)

            response = self.supabase.table("appointments").select(
                "id, scheduled_at, client_id, service_id, status"
            ).eq("status", "scheduled").gte("scheduled_at", tomorrow_start.isoformat()).lt("scheduled_at", tomorrow_end.isoformat()).execute()

            if not response.data:
                print("📭 Завтра немає записів для нагадувань")
                return

            for appointment in response.data:
                await self.process_appointment_reminder(appointment, now)
        except Exception as e:
            print(f"❌ Помилка при перевірці нагадувань: {e}")

    async def process_appointment_reminder(self, appointment, now):
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
                'appointment_id': appointment['id'],
                'scheduled_at': appointment['scheduled_at'],
                'client_name': client['name'],
                'telegram_chat_id': client['telegram_chat_id'],
                'service_name': service['name'],
                'duration': service['duration_minutes']
            }
            await self.process_reminder(appointment_data, now)
        except Exception as e:
            print(f"❌ Помилка при обробці нагадування: {e}")

    async def process_reminder(self, appointment, now):
        try:
            scheduled_at = datetime.fromisoformat(appointment['scheduled_at'])
            # Если время в UTC, конвертируем его в Europe/Kyiv
            if scheduled_at.tzinfo is None or scheduled_at.tzinfo == ZoneInfo("UTC"):
                scheduled_at = scheduled_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Kyiv"))
            else:
                scheduled_at = scheduled_at.astimezone(ZoneInfo("Europe/Kyiv"))
            chat_id = appointment['telegram_chat_id']
            client_name = appointment['client_name']
            if await self.check_notification_sent(appointment['appointment_id'], "24h"):
                return
            success = await self.send_reminder(chat_id, client_name, appointment, "24h")
            if success:
                await self.log_notification_sent(appointment['appointment_id'], "24h")
        except Exception as e:
            print(f"❌ process_reminder error: {e}")

    async def send_reminder(self, chat_id, client_name, appointment, reminder_type):
        try:
            template_path = f"templates/ua/reminder_{reminder_type}.txt"
            if not os.path.exists(template_path):
                print(f"❌ Шаблон {template_path} не знайдено")
                return False

            with open(template_path, 'r', encoding='utf-8') as f:
                template = f.read()

            scheduled_at = datetime.fromisoformat(appointment['scheduled_at'])
            # Если время в UTC, конвертируем его в Europe/Kyiv
            if scheduled_at.tzinfo is None or scheduled_at.tzinfo == ZoneInfo("UTC"):
                scheduled_at = scheduled_at.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Europe/Kyiv"))
            else:
                scheduled_at = scheduled_at.astimezone(ZoneInfo("Europe/Kyiv"))

            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            days_text = get_days_text(scheduled_at.date())

            message = template.format(
                client_name=client_name,
                date=formatted_date,
                time=formatted_time,
                service_name=appointment['service_name'],
                duration=appointment['duration'],
                days_text=days_text
            )

            keyboard = [
                [
                    InlineKeyboardButton("✅ Підтвердити", callback_data=f"confirm_{appointment['appointment_id']}"),
                    InlineKeyboardButton("❌ Скасувати", callback_data=f"cancel_{appointment['appointment_id']}")
                ],
                [
                    InlineKeyboardButton("🔄 Перенести", callback_data=f"reschedule_{appointment['appointment_id']}")
                ]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await self.application.bot.send_message(chat_id=chat_id, text=message, reply_markup=reply_markup)
            return True
        except Exception as e:
            print(f"❌ Помилка відправки нагадування: {e}")
            return False

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
            log_data = {
                "appointment_id": appointment_id,
                "type": notification_type,
                "sent_at": datetime.now(ZoneInfo("Europe/Kyiv")).isoformat(),
                "status": "sent"
            }
            self.supabase.table("notification_logs").insert(log_data).execute()
        except Exception as e:
            print("❌ log_notification_sent error:", e)

    async def my_appointments_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            chat_id = str(update.effective_chat.id)
            client_resp = self.supabase.table("clients").select("id, name").eq("telegram_chat_id", chat_id).execute()
            if not client_resp.data:
                await update.message.reply_text("❌ Ваш акаунт не знайдено. Спочатку підключіть бота через /start.")
                return
            client_id = client_resp.data[0]['id']
            name = client_resp.data[0]['name']
            now = datetime.now(ZoneInfo("Europe/Kyiv"))
            appointments = self.supabase.table("appointments").select("scheduled_at, service_id, status").eq("client_id", client_id).gte("scheduled_at", now.isoformat()).order("scheduled_at").execute().data
            if not appointments:
                await update.message.reply_text("У вас немає майбутніх записів.")
                return
            service_ids = list(set(a['service_id'] for a in appointments))
            services = self.supabase.table("services").select("id, name").in_("id", service_ids).execute().data
            service_map = {s['id']: s['name'] for s in services}
            status_map = {
                'scheduled': 'Заплановано',
                'completed': 'Завершено',
                'cancelled': 'Скасовано',
                'no_show': "Не з'явився"
            }
            msg = f"Ваші майбутні записи, {name}:\n\n"
            for a in appointments:
                dt = datetime.fromisoformat(a['scheduled_at'])
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo("UTC"))
                dt = dt.astimezone(ZoneInfo("Europe/Kyiv"))
                status = status_map.get(a['status'], a['status'])
                msg += f"📅 {dt.strftime('%d.%m.%Y %H:%M')} ({status})\n"
            await update.message.reply_text(msg)
        except Exception as e:
            print(f"❌ Ошибка в my_appointments_command: {e}")
            try:
                await update.message.reply_text("❌ Виникла помилка. Спробуйте ще раз.")
            except Exception:
                pass

    async def support_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("Для підтримки звертайтесь: @ministr30 або телефонуйте адміністратору.")

    def run(self):
        print("🚀 Запуск Telegram бота...")
        self.scheduler.start()
        self.application.run_polling()

def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if not all([token, supabase_url, supabase_key]):
        print("❌ Відсутні змінні середовища")
        return
    bot = MassageReminderBot(token, supabase_url, supabase_key)
    bot.run()

if __name__ == "__main__":
    main()
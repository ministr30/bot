#!/usr/bin/env python3
"""
Скрипт для быстрого тестирования уведомлений
"""
import os
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ContextTypes
from supabase import create_client

load_dotenv()

class TestReminderBot:
    def __init__(self):
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        self.supabase = create_client(supabase_url, supabase_key)
        self.application = Application.builder().token(token).build()

    async def test_notification_for_vadim(self):
        """Тестирует отправку уведомления для клиента Вадим"""
        try:
            print("🔍 Ищем клиента Вадим...")
            
            # Ищем клиента по имени
            response = self.supabase.table("clients").select("*").ilike("name", "%вадим%").execute()
            
            if not response.data:
                print("❌ Клиент Вадим не найден")
                return
            
            client = response.data[0]
            print(f"✅ Найден клиент: {client['name']} (ID: {client['id']})")
            
            if not client.get('telegram_chat_id'):
                print("❌ У клиента нет telegram_chat_id")
                return
            
            print(f"📱 Telegram chat_id: {client['telegram_chat_id']}")
            
            # Создаем тестовую запись на завтра
            tomorrow = datetime.now(ZoneInfo("Europe/Kyiv")) + timedelta(days=1)
            test_appointment = {
                'id': 'test-appointment-' + datetime.now().strftime('%Y%m%d%H%M%S'),
                'scheduled_at': tomorrow.isoformat(),
                'client_name': client['name'],
                'telegram_chat_id': client['telegram_chat_id'],
                'service_name': 'Тестовый массаж',
                'duration': 60
            }
            
            print(f"📅 Создаем тестовую запись на {tomorrow.strftime('%d.%m.%Y %H:%M')}")
            
            # Отправляем тестовое уведомление
            await self.send_test_reminder(test_appointment)
            
        except Exception as e:
            print(f"❌ Ошибка при тестировании: {e}")

    async def send_test_reminder(self, appointment):
        """Отправляет тестовое уведомление"""
        try:
            # Форматируем время
            scheduled_at = datetime.fromisoformat(appointment['scheduled_at'])
            formatted_date = scheduled_at.strftime("%d.%m.%Y")
            formatted_time = scheduled_at.strftime("%H:%M")
            
            # Создаем сообщение
            message = f"""🧪 ТЕСТОВОЕ УВЕДОМЛЕНИЕ

Нагадування про масаж завтра!

Доброго дня, {appointment['client_name']}!

Нагадуємо, що завтра у вас заплановано масаж:
Дата: {formatted_date}
Час: {formatted_time}
Услуга: {appointment['service_name']}
Длительность: {appointment['duration']} мин

Будь ласка, підтвердіть свою присутність або повідомте про зміни.

Дякуємо, що обрали нас!"""
            
            # Создаем кнопки
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
            
            # Отправляем сообщение
            await self.application.bot.send_message(
                chat_id=appointment['telegram_chat_id'],
                text=message,
                reply_markup=reply_markup
            )
            
            print(f"✅ Тестовое уведомление отправлено клиенту {appointment['client_name']}")
            
        except Exception as e:
            print(f"❌ Ошибка при отправке тестового уведомления: {e}")

    async def check_vadim_appointments(self):
        """Проверяет записи клиента Вадим"""
        try:
            print("🔍 Проверяем записи клиента Вадим...")
            
            # Ищем клиента
            client_response = self.supabase.table("clients").select("*").ilike("name", "%вадим%").execute()
            
            if not client_response.data:
                print("❌ Клиент Вадим не найден")
                return
            
            client = client_response.data[0]
            client_id = client['id']
            
            # Ищем записи клиента
            appointments_response = self.supabase.table("appointments").select(
                "id, scheduled_at, status, client_confirmed"
            ).eq("client_id", client_id).execute()
            
            if not appointments_response.data:
                print("📭 У клиента Вадим нет записей")
                return
            
            print(f"📋 Найдено {len(appointments_response.data)} записей:")
            
            for appointment in appointments_response.data:
                scheduled_at = appointment['scheduled_at']
                # Убираем часовой пояс для отображения
                if scheduled_at.endswith('+00:00'):
                    scheduled_at = scheduled_at[:-6]
                
                scheduled_time = datetime.fromisoformat(scheduled_at)
                time_str = scheduled_time.strftime("%d.%m.%Y %H:%M")
                
                confirmed = "✅" if appointment.get('client_confirmed') else "❌"
                status = appointment['status']
                
                print(f"  {confirmed} {time_str} - {status}")
                
        except Exception as e:
            print(f"❌ Ошибка при проверке записей: {e}")

async def main():
    bot = TestReminderBot()
    
    print("🧪 ТЕСТИРОВАНИЕ УВЕДОМЛЕНИЙ")
    print("=" * 50)
    
    # Проверяем записи клиента Вадим
    await bot.check_vadim_appointments()
    
    print("\n" + "=" * 50)
    
    # Отправляем тестовое уведомление
    await bot.test_notification_for_vadim()

if __name__ == "__main__":
    asyncio.run(main())

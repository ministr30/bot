# scheduler.py
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Tuple

logger = logging.getLogger(__name__)

class TimeSlotScheduler:
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self._services_cache = None
        self.TIMEZONE = ZoneInfo("Europe/Kyiv")
        self.UTC = ZoneInfo("UTC")

    async def get_services(self):
        """Кэшированный запрос к услугам (id → duration_minutes)"""
        if self._services_cache is None:
            response = self.supabase.table("services").select("id, duration_minutes").execute()
            if not response.data:
                logger.warning("Failed to load services. Using default duration 60 min.")
                self._services_cache = {}
            else:
                self._services_cache = {s['id']: s['duration_minutes'] for s in response.data}
        return self._services_cache

    def _merge_intervals(self, intervals: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
        """Объединяет перекрывающиеся или соприкасающиеся интервалы"""
        if not intervals:
            return []
        sorted_intervals = sorted(intervals, key=lambda x: x[0])
        merged = [sorted_intervals[0]]
        for current in sorted_intervals[1:]:
            last = merged[-1]
            if current[0] <= last[1]:  # Пересечение или касание
                merged[-1] = (last[0], max(last[1], current[1]))
            else:
                merged.append(current)
        return merged

    def _ensure_tz(self, dt: datetime, field_name: str = "") -> datetime:
        """Приводит datetime к Europe/Kyiv, если у него нет таймзоны (считая исходную UTC)."""
        if dt.tzinfo is None:
            logger.warning(f"Time without timezone: {dt} ({field_name}) — treating as UTC")
            dt = dt.replace(tzinfo=self.UTC)
        return dt.astimezone(self.TIMEZONE)

    def _round_to_next_quarter_hour(self, dt: datetime) -> datetime:
        """Округляет время до ближайших 15 минут вверх."""
        minutes = dt.minute
        # Находим сколько минут нужно добавить до следующего 15-минутного интервала
        minutes_to_add = (15 - minutes % 15) % 15
        if minutes_to_add == 0:
            minutes_to_add = 15  # Если уже на 15-минутном интервале, переходим к следующему
        return dt + timedelta(minutes=minutes_to_add)

    async def get_busy_intervals(self, date: datetime.date, for_package_check: bool = False) -> List[Tuple[datetime, datetime]]:
        """
        Возвращает объединённые занятые интервалы на указанный день:
        - Записи (с учётом 15 мин перерыва после)
        - Блокировки времени (без перерыва)
        Все временные метки приводятся к Europe/Kyiv
        """
        start_dt = datetime.combine(date, datetime.min.time()).replace(tzinfo=self.TIMEZONE)
        end_dt = start_dt + timedelta(days=1)

        appointments = self.supabase.table("appointments") \
            .select("scheduled_at, service_id, status, appointment_type, total_duration") \
            .gte("scheduled_at", start_dt.isoformat()) \
            .lt("scheduled_at", end_dt.isoformat()) \
            .neq("status", "cancelled") \
            .execute().data or []

        time_blocks = self.supabase.table("time_blocks") \
            .select("start_date_time, end_date_time") \
            .lt("start_date_time", end_dt.isoformat()) \
            .gt("end_date_time", start_dt.isoformat()) \
            .execute().data or []

        raw_intervals = []
        services = await self.get_services()

        logger.debug(f"Processing busy intervals for {date.strftime('%d.%m.%Y')}")

        # Обрабатываем записи с перерывами
        for a in appointments:
            try:
                dt = datetime.fromisoformat(a['scheduled_at'])
                dt = self._ensure_tz(dt, "scheduled_at")

                # Определяем длительность в зависимости от типа записи
                appointment_type = a.get('appointment_type', 'single')
                if appointment_type == 'package':
                    # Для пакетов используем total_duration
                    duration = a.get('total_duration', 60)
                    duration_desc = f"package {duration} min"
                else:
                    # Для одиночных услуг используем длительность из services
                    service_id = a['service_id']
                    duration = services.get(service_id, 60)
                    duration_desc = f"service {duration} min"

                end_with_break = dt + timedelta(minutes=duration + 15)
                raw_intervals.append((dt, end_with_break))
                logger.debug(f"Appointment[{a.get('status','?')}, {appointment_type}]: {dt.strftime('%H:%M')} - {end_with_break.strftime('%H:%M')} ({duration_desc} + 15 min break)")
            except Exception as e:
                logger.error(f"Error processing appointment {a}", exc_info=True)

        # Обрабатываем блоки времени без перерывов
        for b in time_blocks:
            try:
                block_start = datetime.fromisoformat(b['start_date_time'])
                block_end = datetime.fromisoformat(b['end_date_time'])
                block_start = self._ensure_tz(block_start, "start_date_time")
                block_end = self._ensure_tz(block_end, "end_date_time")
                raw_intervals.append((block_start, block_end))
                logger.debug(f"Time block: {block_start.strftime('%H:%M')} - {block_end.strftime('%H:%M')}")
            except Exception as e:
                logger.error(f"Error processing time block {b}", exc_info=True)

        # Объединяем все интервалы в один отсортированный список
        merged = self._merge_intervals(raw_intervals)

        logger.debug(f"Merged busy intervals on {date.strftime('%d.%m.%Y')}:")
        for start, end in merged:
            logger.debug(f"  Busy: {start.strftime('%H:%M')} - {end.strftime('%H:%M')}")

        return merged

    async def get_free_slots(self, date: datetime.date, duration_minutes: int) -> List[datetime]:
        """
        Возвращает список доступных начал записи (в Europe/Kyiv)
        Учитывает:
        - Рабочее время 9:00–21:00
        - Воскресенье — недоступен
        - Длительность услуги
        - 15-минутный перерыв после
        - Занятые интервалы (записи + блокировки)
        - Для сегодняшней даты: текущее время + 15 мин буфер
        """
        return await self._get_free_slots_internal(date, duration_minutes, is_package=False)

    async def get_free_slots_for_package(self, date: datetime.date, total_duration: int) -> List[datetime]:
        """
        Возвращает список доступных начал для пакета услуг (в Europe/Kyiv)
        Учитывает:
        - Рабочее время 9:00–21:00
        - Воскресенье — недоступен
        - Общее время пакета (без дополнительных перерывов между услугами)
        - 15-минутный перерыв после всего пакета
        - Занятые интервалы (записи + блокировки)
        - Для сегодняшней даты: текущее время + 15 мин буфер
        """
        return await self._get_free_slots_internal(date, total_duration, is_package=True)

    async def _get_free_slots_internal(self, date: datetime.date, duration_minutes: int, is_package: bool = False) -> List[datetime]:
        """
        Возвращает список доступных начал записи (в Europe/Kyiv)
        Учитывает:
        - Рабочее время 9:00–21:00
        - Воскресенье — недоступен
        - Длительность услуги
        - 15-минутный перерыв после
        - Занятые интервалы (записи + блокировки)
        - Для сегодняшней даты: текущее время + 15 мин буфер
        """
        if date.weekday() == 6:  # Воскресенье
            logger.debug(f"{date.strftime('%d.%m.%Y')} - Sunday -> unavailable")
            return []

        work_start = datetime.combine(date, datetime.min.time()).replace(tzinfo=self.TIMEZONE, hour=9)  # 9:00
        work_end = datetime.combine(date, datetime.min.time()).replace(tzinfo=self.TIMEZONE, hour=21)  # 21:00

        # Для сегодняшней даты учитываем текущее время
        today = datetime.now(self.TIMEZONE).date()
        if date == today:
            current_time_kyiv = datetime.now(self.TIMEZONE)
            # Добавляем 15-минутный буфер для подготовки
            buffered_time = (current_time_kyiv + timedelta(minutes=15)).replace(second=0, microsecond=0)
            # Округляем до ближайших 15 минут вверх
            min_start_time = self._round_to_next_quarter_hour(buffered_time)
            work_start = max(work_start, min_start_time)
            logger.debug(f"Today: adjusted work_start to {work_start.strftime('%H:%M')} (rounded to next 15-min slot)")

        # Проверяем, что услуга/пакет помещается в рабочий день
        required_end = work_start + timedelta(minutes=duration_minutes + 15)
        if required_end > work_end:
            service_type = "package" if is_package else "service"
            logger.debug(f"{service_type.title()} {duration_minutes} min does not fit in workday")
            return []

        busy_intervals = await self.get_busy_intervals(date, for_package_check=is_package)
        free_slots = []

        service_type = "package" if is_package else "service"
        logger.debug(f"Searching for free slots for {service_type} {duration_minutes} min on {date.strftime('%d.%m.%Y')}")

        # Начинаем искать свободное время с начала рабочего дня (или adjusted work_start для сегодня)
        # Округляем начальное время до ближайших 15 минут вверх
        current_time = self._round_to_next_quarter_hour(work_start)

        # Проверяем окна до первого занятого слота и между ними
        for busy_start, busy_end in busy_intervals:
            logger.debug(f"Checking window: {current_time.strftime('%H:%M')} to {busy_start.strftime('%H:%M')}")

            # Ищем свободные слоты в промежутке от current_time до busy_start
            while current_time + timedelta(minutes=duration_minutes) <= busy_start:
                slot_end = current_time + timedelta(minutes=duration_minutes)
                if slot_end <= busy_start:  # Убеждаемся, что слот полностью помещается
                    free_slots.append(current_time)
                    logger.debug(f"  ✓ Free slot: {current_time.strftime('%H:%M')}")
                current_time += timedelta(minutes=15)

            # Перепрыгиваем через занятый интервал
            current_time = max(current_time, busy_end)
            # Округляем время после занятого интервала до ближайших 15 минут вверх
            current_time = self._round_to_next_quarter_hour(current_time)
            logger.debug(f"  → Jumping to: {current_time.strftime('%H:%M')}")

        # Проверяем оставшееся время после последнего занятого слота до конца дня
        logger.debug(f"Checking final window: {current_time.strftime('%H:%M')} to {work_end.strftime('%H:%M')}")
        while current_time + timedelta(minutes=duration_minutes) <= work_end:
            free_slots.append(current_time)
            logger.debug(f"  ✓ Free slot: {current_time.strftime('%H:%M')}")
            current_time += timedelta(minutes=15)

        service_type = "package" if is_package else "service"
        logger.debug(f"Found {len(free_slots)} free slots for {duration_minutes} min {service_type}")
        return free_slots
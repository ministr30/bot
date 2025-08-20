# scheduler.py
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Tuple

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
                print("Warning: Failed to load services. Using default duration 60 min.")
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

    def _ensure_tz_utc_default(self, dt: datetime, field_name: str = "") -> datetime:
        """Если нет зоны — считаем UTC и конвертируем в Europe/Kyiv (для appointments)"""
        if dt.tzinfo is None:
            print(f"Warning: Time without zone: {dt} ({field_name}) — treating as UTC")
            dt = dt.replace(tzinfo=self.UTC)
        return dt.astimezone(self.TIMEZONE)

    def _ensure_tz_local_default(self, dt: datetime, field_name: str = "") -> datetime:
        """Единообразно: если нет зоны — считаем UTC и конвертируем в Europe/Kyiv (для time_blocks)"""
        if dt.tzinfo is None:
            print(f"Warning: Time without zone: {dt} ({field_name}) — treating as UTC")
            dt = dt.replace(tzinfo=self.UTC)
        return dt.astimezone(self.TIMEZONE)

    async def get_busy_intervals(self, date: datetime.date) -> list:
        """
        Возвращает объединённые занятые интервалы на указанный день:
        - Записи (с учётом 15 мин перерыва после, тип 'appointment')
        - Блокировки времени (без перерыва, тип 'block')
        Все временные метки приводятся к Europe/Kyiv
        """
        start_dt = datetime.combine(date, datetime.min.time()).replace(tzinfo=self.TIMEZONE)
        end_dt = start_dt + timedelta(days=1)

        appointments = self.supabase.table("appointments") \
            .select("scheduled_at, service_id, status") \
            .gte("scheduled_at", start_dt.isoformat()) \
            .lt("scheduled_at", end_dt.isoformat()) \
            .in_("status", ["scheduled", "rescheduled"]) \
            .execute().data or []

        time_blocks = self.supabase.table("time_blocks") \
            .select("start_date_time, end_date_time") \
            .gte("start_date_time", start_dt.isoformat()) \
            .lt("end_date_time", end_dt.isoformat()) \
            .execute().data or []

        busy_intervals = []
        services = await self.get_services()

        print(f"\n[DEBUG] Processing busy intervals for {date.strftime('%d.%m.%Y')}")

        for a in appointments:
            try:
                dt = datetime.fromisoformat(a['scheduled_at'])
                dt = self._ensure_tz_utc_default(dt, "scheduled_at")
                service_id = a['service_id']
                duration = services.get(service_id, 60)
                end_time = dt + timedelta(minutes=duration)
                end_with_break = end_time + timedelta(minutes=15)
                busy_intervals.append(('appointment', dt, end_with_break))
                print(f"  Appointment[{a.get('status','?')}]: {dt.strftime('%H:%M')} - {end_with_break.strftime('%H:%M')} (service {duration} min + 15 min break)")
            except Exception as e:
                print(f"Error processing appointment {a}: {e}")

        for b in time_blocks:
            try:
                block_start = datetime.fromisoformat(b['start_date_time'])
                block_end = datetime.fromisoformat(b['end_date_time'])
                block_start = self._ensure_tz_local_default(block_start, "start_date_time")
                block_end = self._ensure_tz_local_default(block_end, "end_date_time")
                busy_intervals.append(('block', block_start, block_end))
                print(f"  Block: {block_start.strftime('%H:%M')} - {block_end.strftime('%H:%M')}")
            except Exception as e:
                print(f"Error processing block {b}: {e}")

        # Не объединяем интервалы разных типов!
        print(f"\n[DEBUG] All intervals:")
        for t, s, e in busy_intervals:
            print(f"  {t}: {s.strftime('%H:%M')} - {e.strftime('%H:%M')}")
        return busy_intervals

    async def get_free_slots(self, date: datetime.date, duration_minutes: int) -> list:
        """
        Возвращает список доступных начал записи (в Europe/Kyiv)
        Учитывает:
        - Рабочее время 9:00–21:00
        - Воскресенье — недоступен
        - Длительность услуги
        - 15-минутный перерыв после
        - Занятые интервалы (записи + блокировки)
        """
        if date.weekday() == 6:  # Воскресенье
            print(f"[DEBUG] {date.strftime('%d.%m.%Y')} - Sunday -> unavailable")
            return []

        work_start = datetime.combine(date, datetime.min.time()).replace(tzinfo=self.TIMEZONE) + timedelta(hours=9)  # 9:00
        work_end = work_start + timedelta(hours=12)  # 21:00

        required_end = work_start + timedelta(minutes=duration_minutes + 15)
        if required_end > work_end:
            print(f"[DEBUG] Service {duration_minutes} min does not fit in workday")
            return []

        busy_intervals = await self.get_busy_intervals(date)
        free_slots = []

        current_time = work_start
        slot_count = 0

        print(f"\n[DEBUG] Searching for free slots for service {duration_minutes} min:")

        while current_time + timedelta(minutes=duration_minutes) <= work_end:
            slot_start = current_time
            slot_end = slot_start + timedelta(minutes=duration_minutes)
            slot_end_with_break = slot_end + timedelta(minutes=15)

            is_free = True
            for busy_type, busy_start, busy_end in busy_intervals:
                # Проверяем пересечение слота (без учета перерыва после него) с занятыми интервалами
                if slot_start < busy_end and slot_end > busy_start:
                    is_free = False
                    break

            if is_free:
                free_slots.append(slot_start)
                print(f"  OK: {slot_start.strftime('%H:%M')} - {slot_end_with_break.strftime('%H:%M')}")
            else:
                print(f"  Busy:  {slot_start.strftime('%H:%M')} - {slot_end_with_break.strftime('%H:%M')}")

            current_time += timedelta(minutes=15)
            slot_count += 1
            if slot_count > 100:
                print("Warning: Too many slots - interrupting")
                break

        print(f"[DEBUG] Found {len(free_slots)} free slots")
        return free_slots
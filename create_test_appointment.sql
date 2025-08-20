-- Скрипт для создания тестовой записи для клиента Вадим
-- Запустите этот скрипт в Supabase SQL Editor

-- 1. Найдем клиента Вадим
SELECT id, name, phone, telegram_chat_id, notification_opt_in 
FROM clients 
WHERE name ILIKE '%вадим%';

-- 2. Создаем тестовую запись на завтра
-- Замените CLIENT_ID на ID клиента Вадим из результата выше
-- Замените SERVICE_ID на ID любой услуги из таблицы services

INSERT INTO appointments (
    id,
    client_id,
    service_id,
    scheduled_at,
    status,
    notes,
    client_generated_id,
    client_confirmed,
    created_at,
    updated_at
) VALUES (
    gen_random_uuid(),
    'CLIENT_ID', -- Замените на реальный ID клиента Вадим
    'SERVICE_ID', -- Замените на реальный ID услуги
    (CURRENT_DATE + INTERVAL '1 day' + INTERVAL '14:00:00')::timestamp, -- Завтра в 14:00
    'scheduled',
    'Тестовая запись для проверки уведомлений',
    'test-' || extract(epoch from now())::text,
    false,
    now(),
    now()
);

-- 3. Проверим созданную запись
SELECT 
    a.id,
    a.scheduled_at,
    a.status,
    a.client_confirmed,
    c.name as client_name,
    s.name as service_name
FROM appointments a
JOIN clients c ON a.client_id = c.id
JOIN services s ON a.service_id = s.id
WHERE c.name ILIKE '%вадим%'
ORDER BY a.scheduled_at DESC
LIMIT 5;

-- 4. Проверим логи уведомлений
SELECT * FROM notification_logs 
WHERE appointment_id IN (
    SELECT a.id 
    FROM appointments a 
    JOIN clients c ON a.client_id = c.id 
    WHERE c.name ILIKE '%вадим%'
)
ORDER BY created_at DESC;

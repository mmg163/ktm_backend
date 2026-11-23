-- V003__rejection_rules_examples.sql
-- Примеры правил отбраковки (ADR-0005 п. 2.3, 9): ВСЕ выключены (enabled = false), условия на операциях первой итерации.
-- Примеры - иллюстрация формата, не бизнес-критерии: включать только после согласования критериев с владельцами источников.
-- Идемпотентен: INSERT ... ON CONFLICT (rule_id) DO NOTHING; изменённый вручную пример повторный прогон не затирает.
-- Удалённый пример вернётся при следующем прогоне migrate: правило выключают (enabled = false), а не удаляют.

DO $$
BEGIN
    IF current_user <> 'ktm_migrator' THEN
        RAISE EXCEPTION 'migration must run as ktm_migrator, current role is %', current_user;
    END IF;
END
$$;

INSERT INTO ktm.rejection_rules (rule_id, name, description, source, condition, reason_code, enabled, priority, version)
VALUES
    ('skud-test-event',
     'СКУД: тестовое событие',
     'Пример: отбраковывает события СКУД с типом test_event.',
     'skud',
     '{"==": [{"var": "eventType"}, "test_event"]}'::jsonb,
     'test_event', false, 100, 1),
    ('skud-stale-event',
     'СКУД: событие старше 2020 года',
     'Пример: отбраковывает события СКУД с occurredAt раньше 2020-01-01T00:00:00Z.',
     'skud',
     '{"<": [{"var": "occurredAt"}, "2020-01-01T00:00:00Z"]}'::jsonb,
     'stale_event', false, 100, 1),
    ('cv-low-confidence',
     'Видео: низкая уверенность распознавания',
     'Пример: уверенность от 0 до 0.3 включительно (диапазон, обе границы включительно).',
     'cv',
     '{"<=": [0, {"var": "payload.confidence"}, 0.3]}'::jsonb,
     'low_confidence', false, 100, 1),
    ('cv-confidence-out-of-range',
     'Видео: уверенность вне диапазона 0..1',
     'Пример: уверенность меньше 0 или больше 1.',
     'cv',
     '{"or": [{"<": [{"var": "payload.confidence"}, 0]}, {">": [{"var": "payload.confidence"}, 1]}]}'::jsonb,
     'confidence_out_of_range', false, 100, 1),
    ('1c-do-draft-document',
     '1С:ДО: документ в статусе «черновик»',
     'Пример: отбраковывает события 1С:ДО со статусом draft.',
     '1c-do',
     '{"==": [{"var": "payload.status"}, "draft"]}'::jsonb,
     'draft_document', false, 100, 1),
    ('common-test-flag',
     'Общее: признак тестового сообщения',
     'Пример: отбраковывает события любого источника с payload.isTest = true.',
     '*',
     '{"==": [{"var": "payload.isTest"}, true]}'::jsonb,
     'test_flag', false, 100, 1)
ON CONFLICT (rule_id) DO NOTHING;

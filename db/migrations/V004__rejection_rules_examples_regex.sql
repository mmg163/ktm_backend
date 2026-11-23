-- V004__rejection_rules_examples_regex.sql
-- Примеры правил отбраковки с оператором regex (ADR-0005 п. 2.7): ОБА выключены (enabled = false).
-- Примеры - иллюстрация формата, не бизнес-критерии: включать только после согласования критериев с владельцами источников.
-- Шаблон выключенного правила при накате не проверяется: синтаксис и поддержку движком без возвратов (NonBacktracking) сервис
-- проверяет при включении правила (ошибка правила в логе и rules.invalid, остальные правила работают).
-- Идемпотентен: INSERT ... ON CONFLICT (rule_id) DO NOTHING; изменённый вручную пример повторный прогон не затирает.
-- Удалённый пример вернётся при следующем прогоне migrate: правило выключают (enabled = false), а не удаляют.
-- Обратная косая в JSON внутри обычной SQL-строки (без E'') остаётся как написана; в JSON обратная косая удваивается: "\\s" в условии - это \s в шаблоне.

DO $$
BEGIN
    IF current_user <> 'ktm_migrator' THEN
        RAISE EXCEPTION 'migration must run as ktm_migrator, current role is %', current_user;
    END IF;
END
$$;

INSERT INTO ktm.rejection_rules (rule_id, name, description, source, condition, reason_code, enabled, priority, version)
VALUES
    ('skud-test-device',
     'СКУД: тестовое устройство',
     'Пример regex: отбраковывает события СКУД, у которых deviceId начинается с TEST-.',
     'skud',
     '{"regex": [{"var": "deviceId"}, "^TEST-"]}'::jsonb,
     'test_device', false, 100, 1),
    ('skud-blank-person',
     'СКУД: пустой идентификатор человека',
     'Пример regex: externalPersonId состоит только из пробельных символов (в том числе пуст); \z - конец строки, в отличие от $ не допускает перевод строки в конце.',
     'skud',
     '{"regex": [{"var": "externalPersonId"}, "^\\s*\\z"]}'::jsonb,
     'blank_person', false, 100, 1)
ON CONFLICT (rule_id) DO NOTHING;

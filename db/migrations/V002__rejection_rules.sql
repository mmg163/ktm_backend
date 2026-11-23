-- V002__rejection_rules.sql
-- Правила отбраковки Data Saver: таблица ktm.rejection_rules (ADR-0005 п. 2.3, редакция 3).
-- Применяется под ролью ktm_migrator ПОСЛЕ V001; идемпотентен.
-- Без метакоманд psql: один файл для psql и для Npgsql.
-- Триггеров, аудита и NOTIFY нет (отложено, ADR-0005 приложение A): version увеличивает автор правки вручную.
-- Уже применённый файл не правится: изменение схемы - новый файл V00N.

DO $$
BEGIN
    IF current_user <> 'ktm_migrator' THEN
        RAISE EXCEPTION 'migration must run as ktm_migrator, current role is %', current_user;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS ktm.rejection_rules (
    rule_id      text    NOT NULL,
    name         text    NOT NULL,
    description  text    NULL,
    source       text    NOT NULL,                 -- 'skud' | 'cv' | '1c-do' | '*' (все источники); без умолчания
    condition    jsonb   NOT NULL,
    reason_code  text    NOT NULL,
    enabled      boolean NOT NULL,                 -- без умолчания: правило не включается молча
    priority     integer NOT NULL DEFAULT 100,
    version      integer NOT NULL DEFAULT 1,       -- при правке условия или причины его увеличивает автор

    CONSTRAINT pk_rejection_rules PRIMARY KEY (rule_id),
    CONSTRAINT ck_rejection_rules_id      CHECK (rule_id ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT ck_rejection_rules_name    CHECK (length(btrim(name)) BETWEEN 1 AND 200),
    CONSTRAINT ck_rejection_rules_source  CHECK (length(source) BETWEEN 1 AND 32),
    CONSTRAINT ck_rejection_rules_cond    CHECK (jsonb_typeof(condition) = 'object' AND pg_column_size(condition) <= 16384),
    CONSTRAINT ck_rejection_rules_reason  CHECK (reason_code ~ '^[a-z][a-z0-9_]{0,63}$'
                                                 AND reason_code NOT IN ('malformed_json', 'unknown_schema_version', 'invalid_schema')),
    CONSTRAINT ck_rejection_rules_prio    CHECK (priority BETWEEN 0 AND 10000),
    CONSTRAINT ck_rejection_rules_version CHECK (version BETWEEN 1 AND 1000000)
);

-- Права: явно, независимо от ALTER DEFAULT PRIVILEGES. Сервис правила только читает.
REVOKE ALL ON ktm.rejection_rules FROM PUBLIC;
REVOKE ALL ON ktm.rejection_rules FROM ktm_app;
GRANT SELECT ON ktm.rejection_rules TO ktm_app;

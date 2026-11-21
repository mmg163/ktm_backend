-- V001__event_journal.sql
-- Журнал событий: ktm.events (hypertable) и ktm.event_corrections (ADR-0004, docs/design/Схема_БД_ER.md).
-- Применяется под ролью ktm_migrator ПОСЛЕ db/init/001_roles_and_extension.sql; идемпотентен.
-- Без метакоманд psql и CREATE INDEX CONCURRENTLY: один файл для psql и для Npgsql.
-- Уже применённый файл не правится: изменение схемы - новый файл V00N.
-- Источник истины по DDL - этот файл, а не Схема_БД_ER.md п. 9.

-- Предусловия: роль и расширение.
DO $$
BEGIN
    IF current_user <> 'ktm_migrator' THEN
        RAISE EXCEPTION 'migration must run as ktm_migrator, current role is %', current_user;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE EXCEPTION 'extension timescaledb is missing: run db/init/001_roles_and_extension.sql first';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS ktm.events (
    event_id            uuid        NOT NULL,
    source              text        NOT NULL,
    source_event_id     text        NOT NULL,
    occurred_at         timestamptz NOT NULL,
    received_at         timestamptz NOT NULL,
    event_type          text        NOT NULL,
    device_id           text        NULL,
    external_person_id  text        NOT NULL,
    trust               smallint    NOT NULL,
    payload             jsonb       NOT NULL,
    schema_version      smallint    NOT NULL,

    CONSTRAINT pk_events PRIMARY KEY (event_id, occurred_at),
    CONSTRAINT ux_events_natural_key UNIQUE (source, source_event_id, occurred_at),
    CONSTRAINT ck_events_source          CHECK (length(source) BETWEEN 1 AND 32),
    CONSTRAINT ck_events_source_event_id CHECK (length(source_event_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_events_event_type      CHECK (length(event_type) BETWEEN 1 AND 128),
    CONSTRAINT ck_events_device_id       CHECK (device_id IS NULL OR length(device_id) > 0),
    CONSTRAINT ck_events_person          CHECK (length(external_person_id) > 0),
    CONSTRAINT ck_events_trust           CHECK (trust BETWEEN 0 AND 100),
    CONSTRAINT ck_events_payload_object  CHECK (jsonb_typeof(payload) = 'object'),
    -- 256 КиБ - страховка БД поверх контракта приёма (payload <= 64 КиБ, ER п. 7.5). pg_column_size возвращает размер
    -- хранимого (возможно сжатого) значения, поэтому предел заведомо с запасом.
    CONSTRAINT ck_events_payload_size    CHECK (pg_column_size(payload) <= 262144)
);

SELECT create_hypertable('ktm.events', 'occurred_at',
                         chunk_time_interval => INTERVAL '7 days',
                         if_not_exists       => TRUE);

CREATE INDEX IF NOT EXISTS ix_events_person_time
    ON ktm.events (external_person_id, occurred_at);

-- Сжатие. На TimescaleDB 2.32.1 повторный ALTER ... SET (timescaledb.compress...) на таблице со сжатыми чанками даёт
-- только NOTICE, ошибки нет. DO-блок оставлен ради чистого повтора (без лишних сообщений) и как явная защита:
-- настройка применяется, только если сжатие ещё не включено.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables
                   WHERE hypertable_schema = 'ktm' AND hypertable_name = 'events' AND compression_enabled) THEN
        ALTER TABLE ktm.events SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'external_person_id',
            timescaledb.compress_orderby   = 'occurred_at DESC'
        );
    END IF;
END
$$;

SELECT add_compression_policy('ktm.events', INTERVAL '30 days', if_not_exists => TRUE);
-- Политика retention намеренно НЕ создаётся (Схема_БД_ER.md, вопрос 3).

CREATE TABLE IF NOT EXISTS ktm.event_corrections (
    correction_id               uuid        NOT NULL DEFAULT gen_random_uuid(),
    source                      text        NOT NULL,
    correction_source_event_id  text        NOT NULL,
    correction_occurred_at      timestamptz NOT NULL,
    correction_event_id         uuid        NOT NULL,
    original_source             text        NOT NULL,
    original_source_event_id    text        NOT NULL,
    original_occurred_at        timestamptz NOT NULL,
    original_event_id           uuid        NULL,
    reason                      text        NULL,
    actor                       text        NOT NULL,
    corrected_at                timestamptz NOT NULL,

    CONSTRAINT pk_event_corrections PRIMARY KEY (correction_id),
    CONSTRAINT ux_event_corrections_correction_key
        UNIQUE (source, correction_source_event_id, correction_occurred_at),
    CONSTRAINT ux_event_corrections_event_id UNIQUE (correction_event_id),
    CONSTRAINT ck_event_corrections_source                     CHECK (length(source) BETWEEN 1 AND 32),
    CONSTRAINT ck_event_corrections_correction_source_event_id CHECK (length(correction_source_event_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_event_corrections_original_source            CHECK (length(original_source) BETWEEN 1 AND 32),
    CONSTRAINT ck_event_corrections_original_source_event_id   CHECK (length(original_source_event_id) BETWEEN 1 AND 128),
    CONSTRAINT ck_event_corrections_reason                     CHECK (reason IS NULL OR length(reason) > 0),
    CONSTRAINT ck_event_corrections_actor CHECK (length(actor) > 0)
    -- FK на ktm.events НЕ создаются намеренно (Схема_БД_ER.md п. 4.2)
);

CREATE INDEX IF NOT EXISTS ix_event_corrections_original
    ON ktm.event_corrections (original_source, original_source_event_id, original_occurred_at);

-- Страховка неизменяемости поверх прав.
CREATE OR REPLACE FUNCTION ktm.forbid_modify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is append-only (% is forbidden)', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'insufficient_privilege';
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'ktm.events'::regclass AND tgname = 'trg_events_immutable') THEN
        CREATE TRIGGER trg_events_immutable BEFORE UPDATE OR DELETE ON ktm.events
            FOR EACH ROW EXECUTE FUNCTION ktm.forbid_modify();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'ktm.event_corrections'::regclass AND tgname = 'trg_event_corrections_immutable') THEN
        CREATE TRIGGER trg_event_corrections_immutable BEFORE UPDATE OR DELETE ON ktm.event_corrections
            FOR EACH ROW EXECUTE FUNCTION ktm.forbid_modify();
    END IF;
    -- TRUNCATE: statement-level триггеры (владелец ktm_migrator иначе мог бы очистить журнал). DROP/drop_chunks не закрыты.
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'ktm.events'::regclass AND tgname = 'trg_events_no_truncate') THEN
        CREATE TRIGGER trg_events_no_truncate BEFORE TRUNCATE ON ktm.events
            FOR EACH STATEMENT EXECUTE FUNCTION ktm.forbid_modify();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                   WHERE tgrelid = 'ktm.event_corrections'::regclass AND tgname = 'trg_event_corrections_no_truncate') THEN
        CREATE TRIGGER trg_event_corrections_no_truncate BEFORE TRUNCATE ON ktm.event_corrections
            FOR EACH STATEMENT EXECUTE FUNCTION ktm.forbid_modify();
    END IF;
END
$$;

-- Права: явно, независимо от ALTER DEFAULT PRIVILEGES. UPDATE/DELETE/TRUNCATE сервису не выдаются.
REVOKE ALL ON ktm.events, ktm.event_corrections FROM PUBLIC;
REVOKE ALL ON ktm.events, ktm.event_corrections FROM ktm_app;
GRANT SELECT, INSERT ON ktm.events, ktm.event_corrections TO ktm_app;

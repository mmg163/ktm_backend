-- 001_roles_and_extension.sql
-- Инициализация базы журнала событий (ADR-0004, docs/design/Схема_БД_ER.md п. 4.3, 5).
-- Выполняется ОДИН раз администратором (суперпользователем) в целевой базе (ktm_revisor); идемпотентен.
-- Без метакоманд psql: применим и через psql, и через Npgsql.
-- Пароли здесь не задаются (в репозиторий они не попадают): администратор задаёт их отдельно, способ - db/README.md.

-- Роли: ktm_migrator - владелец схемы ktm и DDL; ktm_app - Data Saver и читающие компоненты.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ktm_migrator') THEN
        CREATE ROLE ktm_migrator LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'ktm_app') THEN
        CREATE ROLE ktm_app LOGIN;
    END IF;
END
$$;

-- Расширение (требует суперпользователя). Остаётся в схеме public: функции create_hypertable и др. вызывает ktm_migrator.
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Подключение к текущей базе (имя базы не зашито: скрипт одинаков для dev, тестов и сред).
DO $$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ktm_migrator, ktm_app', current_database());
END
$$;

-- Схема журнала: владелец ktm_migrator, ktm_app только USAGE.
-- Владелец уже существующей схемы молча не меняется: чужая схема ktm - ошибка, разбирается администратором.
CREATE SCHEMA IF NOT EXISTS ktm AUTHORIZATION ktm_migrator;
DO $$
DECLARE
    v_owner name;
BEGIN
    SELECT pg_get_userbyid(nspowner) INTO v_owner FROM pg_namespace WHERE nspname = 'ktm';
    IF v_owner IS DISTINCT FROM 'ktm_migrator' THEN
        RAISE EXCEPTION 'schema ktm exists and is owned by %, expected ktm_migrator: resolve manually', v_owner;
    END IF;
END
$$;
REVOKE ALL ON SCHEMA ktm FROM PUBLIC;
GRANT USAGE ON SCHEMA ktm TO ktm_app;

-- В public выполняется именно REVOKE CREATE, а не REVOKE ALL (docs/design/Схема_БД_ER.md п. 5): функции TimescaleDB
-- лежат в public, и ktm_migrator нужен USAGE, чтобы вызывать create_hypertable и др. В PG15+ CREATE у PUBLIC уже отозван
-- по умолчанию; здесь фиксируем явно для кластеров, где public создан иначе.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Таблицы, которые создаст ktm_migrator в схеме ktm, автоматически доступны ktm_app только на SELECT, INSERT.
ALTER DEFAULT PRIVILEGES FOR ROLE ktm_migrator IN SCHEMA ktm GRANT SELECT, INSERT ON TABLES TO ktm_app;

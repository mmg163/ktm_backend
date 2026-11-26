-- UTC в timestamp without time zone (ADR-0007). V001..V004 остаются неизменными.
-- Один DO-блок = одна транзакция и для psql, и для Npgsql: при ошибке откатывается весь переход.
-- На непустом журнале требуется окно обслуживания: ACCESS EXCLUSIVE, место для распаковки и переписывания чанков.
DO $$
DECLARE
    events_alter text;
    corrections_alter text;
    compressed_chunks regclass[];
    chunk regclass;
    previous_timezone text;
BEGIN
    IF current_user <> 'ktm_migrator' THEN
        RAISE EXCEPTION 'migration must run as ktm_migrator, current role is %', current_user;
    END IF;

    -- Блокируем также дочерние таблицы: запись и compression job не пересекаются с переходом.
    LOCK TABLE ktm.events, ktm.event_corrections IN ACCESS EXCLUSIVE MODE;

    IF EXISTS (
        SELECT 1
        FROM (VALUES ('ktm.events'::regclass, 'occurred_at'),
                     ('ktm.events'::regclass, 'received_at'),
                     ('ktm.event_corrections'::regclass, 'correction_occurred_at'),
                     ('ktm.event_corrections'::regclass, 'original_occurred_at'),
                     ('ktm.event_corrections'::regclass, 'corrected_at')) AS expected(table_oid, column_name)
        LEFT JOIN pg_attribute a ON a.attrelid = expected.table_oid AND a.attname = expected.column_name
                                AND NOT a.attisdropped
        WHERE a.atttypid IS NULL OR a.atttypid NOT IN ('timestamp'::regtype, 'timestamptz'::regtype)
    ) THEN
        RAISE EXCEPTION 'UTC timestamp migration requires all five temporal columns to be timestamp or timestamptz';
    END IF;

    SELECT string_agg(format('ALTER COLUMN %I TYPE timestamp without time zone USING %I AT TIME ZONE ''UTC''',
                             attname, attname), ', ' ORDER BY attnum)
      INTO events_alter
      FROM pg_attribute
     WHERE attrelid = 'ktm.events'::regclass AND attname IN ('occurred_at', 'received_at')
       AND atttypid = 'timestamptz'::regtype AND NOT attisdropped;

    SELECT string_agg(format('ALTER COLUMN %I TYPE timestamp without time zone USING %I AT TIME ZONE ''UTC''',
                             attname, attname), ', ' ORDER BY attnum)
      INTO corrections_alter
      FROM pg_attribute
     WHERE attrelid = 'ktm.event_corrections'::regclass
       AND attname IN ('correction_occurred_at', 'original_occurred_at', 'corrected_at')
       AND atttypid = 'timestamptz'::regtype AND NOT attisdropped;

    IF events_alter IS NULL AND corrections_alter IS NULL THEN
        RETURN; -- Повторный прогон не переписывает данные и не трогает сжатие.
    END IF;

    -- Во время rewrite PostgreSQL ещё проверяет прежние CHECK чанков с timestamptz-границами.
    -- Их неявное приведение к новому timestamp должно тоже выполняться в UTC; затем TimescaleDB
    -- пересоздаёт ограничения для нового типа dimension. Локальная настройка откатится при ошибке.
    previous_timezone := current_setting('TimeZone');
    PERFORM set_config('TimeZone', 'UTC', true);

    IF events_alter IS NOT NULL THEN
        -- TimescaleDB 2.32.1 допускает ALTER TYPE при включённом сжатии, но без сжатых чанков.
        -- Сохраняем policy/job и настройки: распаковываем и затем сжимаем ровно прежние чанки.
        SELECT coalesce(array_agg(format('%I.%I', chunk_schema, chunk_name)::regclass
                                  ORDER BY chunk_schema, chunk_name), ARRAY[]::regclass[])
          INTO compressed_chunks
          FROM timescaledb_information.chunks
         WHERE hypertable_schema = 'ktm' AND hypertable_name = 'events' AND is_compressed;

        FOREACH chunk IN ARRAY compressed_chunks LOOP
            PERFORM decompress_chunk(chunk, if_compressed => true);
        END LOOP;

        -- Явный AT TIME ZONE сохраняет UTC-значение независимо от TimeZone сеанса мигратора.
        EXECUTE 'ALTER TABLE ktm.events ' || events_alter;

        FOREACH chunk IN ARRAY compressed_chunks LOOP
            PERFORM compress_chunk(chunk, if_not_compressed => true);
        END LOOP;
    END IF;

    IF corrections_alter IS NOT NULL THEN
        EXECUTE 'ALTER TABLE ktm.event_corrections ' || corrections_alter;
    END IF;

    PERFORM set_config('TimeZone', previous_timezone, true);
END
$$;

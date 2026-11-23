# db: схема журнала событий

SQL-скрипты схемы `ktm` (журнал событий Data Saver). Решение по инструменту - `docs/adr/0004-db-migrations.md`,
описание схемы - `docs/design/Схема_БД_ER.md`. Источник истины по DDL - файлы в `db/migrations/`.

СУБД: PostgreSQL 16 + TimescaleDB (образ `timescale/timescaledb:2.32.1-pg16`, тег фиксируется, не `latest`). База: `ktm_revisor`.

## Состав

| Файл | Кто применяет | Что делает |
|---|---|---|
| `init/001_roles_and_extension.sql` | суперпользователь, один раз | роли `ktm_migrator` и `ktm_app` (без паролей), схема `ktm`, `GRANT CONNECT`, `GRANT USAGE ON SCHEMA public TO ktm_migrator`, `ALTER DEFAULT PRIVILEGES`; расширение `timescaledb` не создаёт (уже установлено администратором; для среды без него раскомментировать `CREATE EXTENSION` в скрипте) |
| `migrations/V001__event_journal.sql` | `ktm_migrator` | `ktm.events` (hypertable, чанк 7 суток), `ktm.event_corrections`, индексы, триггеры неизменяемости (UPDATE/DELETE и TRUNCATE), сжатие (30 суток), `REVOKE/GRANT` |
| `migrations/V002__rejection_rules.sql` | `ktm_migrator` | таблица правил отбраковки `ktm.rejection_rules` (ADR-0005): CHECK-и, `ktm_app` - только `SELECT`; триггеров, аудита и `NOTIFY` нет |
| `migrations/V003__rejection_rules_examples.sql` | `ktm_migrator` | шесть **выключенных** примеров правил (`skud`, `cv`, `1c-do`, общее `*`); `INSERT ... ON CONFLICT (rule_id) DO NOTHING` |

## Правила

- Файлы идемпотентны: повторный запуск безопасен, схема не меняется. Таблицы истории применённых версий нет, порядок
  задаёт имя файла (`V001`, `V002`, ...); порядок и полноту наката обеспечивает контейнер `migrate` (ниже). Первый
  неидемпотентный файл - условие пересмотра: таблица истории и готовый инструмент DbUp/Flyway (ADR-0004, раздел 5).
- Только простой SQL: без метакоманд `psql` и `CREATE INDEX CONCURRENTLY`. Тот же файл выполняется через `psql` и через Npgsql
  (интеграционные тесты читают файлы из репозитория, не копии).
- Применённый файл не правится: изменение схемы - новый файл `V00N__смысл.sql`.
- Миграции применяются только под `ktm_migrator`; `V001` под другой ролью завершается ошибкой. Приложение работает под `ktm_app`
  (журнал: `SELECT, INSERT`, `UPDATE`/`DELETE`/`TRUNCATE` нет; правила отбраковки: только `SELECT`).
- Пароли в репозиторий не попадают.

## Применение

Порядок: `init` (администратор, вручную), пароли ролей (администратор), затем миграции.

```bash
# 1. Администратор (суперпользователь), один раз. Автоматизации нет: роли, расширение, схема и пароли - ручной шаг.
psql -v ON_ERROR_STOP=1 -U postgres -d ktm_revisor -f db/init/001_roles_and_extension.sql

# 2. Пароли - см. раздел «Пароли ролей» ниже
```

### Миграции: контейнер `migrate` (основной путь)

В `deploy/data-saver/docker-compose.yml` разовый контейнер `migrate` (образ `postgres:16.16-alpine3.24`, `db/` монтируется только на
чтение) выполняет `db/migrations/V*.sql` под `ktm_migrator` и завершается; `data-saver` стартует только после его успешного
завершения (ADR-0004, раздел 5). Подробно - `docs/manuals/data-saver.md`.

```bash
cd deploy/data-saver
umask 077
read -rs -p 'пароль ktm_migrator: ' P && printf '%s' "$P" > secrets/ktm_migrator_password; unset P; echo   # не передавать пароль аргументом команды
sudo chown 1654:1654 secrets/ktm_migrator_password && chmod 0400 secrets/ktm_migrator_password
docker compose up -d            # migrate -> data-saver
docker compose logs migrate     # ход и ошибки миграции
docker compose run --rm migrate # отдельный (повторный) прогон; безопасен, файлы идемпотентны
```

Что делает скрипт: файлы `V<число>__<смысл>.sql` по возрастанию имени (номера не повторяются, порядок имён должен совпадать с числовым: одиночный `V2` допустим, а `V2` вместе с `V10` отвергаются - нужны ведущие нули; строки с метакомандами psql (начинаются с `\`) отвергаются; пустой каталог - ошибка); один сеанс `psql -v ON_ERROR_STOP=1` на файл; на весь прогон удерживается
`pg_advisory_lock` (параллельный запуск ждёт); при первой ошибке - ненулевой код, сервис не стартует. Пароль читается
только из файла секрета и не попадает в `.env`, argv и лог. Хост, порт, база и `SslMode` берутся из
`DataSaver__Postgres__*` (`.env`), пользователь всегда `ktm_migrator`.

### Миграции вручную (запасной путь, без compose)

```bash
for f in $(ls db/migrations/V*.sql | LC_ALL=C sort); do
  psql -v ON_ERROR_STOP=1 -U ktm_migrator -d ktm_revisor -f "$f" || exit 1
done
```

В PowerShell: после каждого вызова `psql` проверять `$LASTEXITCODE` и прерываться при ненулевом:

```powershell
Get-ChildItem db/migrations/V*.sql | Sort-Object Name | ForEach-Object {
  psql -v ON_ERROR_STOP=1 -U ktm_migrator -d ktm_revisor -f $_.FullName
  if ($LASTEXITCODE -ne 0) { throw "migration failed: $($_.Name)" }
}
```

Вручную защиты от параллельного наката нет: не запускать одновременно с контейнером `migrate`.

### Пароли ролей

Пароль не передаётся в `psql -c "ALTER ROLE ... PASSWORD '...'"`: он попадёт в историю shell и в список процессов.
Пароли не хранятся в репозитории.

Интерактивно (пароль не эхом и не в истории; `psql` сам хеширует его на клиенте):

```text
psql -U postgres -d ktm_revisor
\password ktm_migrator
\password ktm_app
```

Для автоматизации: значения берутся из переменных окружения (секрет-хранилище), `psql` читает их сам метакомандой
`\getenv` (нужен `psql` 15+), так что пароль не попадает в argv (список процессов). Команда читается со stdin (не через `-c`:
в `-c` подстановка `:'var'` не выполняется). Не использовать `-v ddl_pw="$DDL_PW"`: значение окажется в argv.
`:'var'` сам экранирует значение как строковый литерал.

```bash
# DDL_PW и DML_PW экспортированы из секрет-хранилища; в историю, argv и репозиторий не попадают
psql -v ON_ERROR_STOP=1 -U postgres -d ktm_revisor <<'SQL'
\getenv ddl_pw DDL_PW
\getenv dml_pw DML_PW
ALTER ROLE ktm_migrator PASSWORD :'ddl_pw';
ALTER ROLE ktm_app PASSWORD :'dml_pw';
SQL
```

При первом применении `V001` TimescaleDB выдаёт три предупреждения `column "event_id" / "source" / "source_event_id"
should be used for segmenting or ordering` - информационные сообщения о настройке сжатия, ошибкой не являются.

## Правила отбраковки (`ktm.rejection_rules`)

Таблица создаётся `V002`, примеры - `V003` (решения - `docs/adr/0005-data-saver-rules-engine.md`). Условие правила - jsonb в нотации
JsonLogic (`docs/api/rejection-condition.schema.json`); столбцы `source` и `enabled` обязательны (значения по умолчанию нет),
`source = '*'` - правило для всех источников. Сервис (`ktm_app`) правила только читает (`SELECT`); `INSERT`/`UPDATE`/`DELETE` ему
запрещены (`insufficient_privilege`).

Размер условия ограничен CHECK `ck_rejection_rules_cond`: `octet_length(condition::text) <= 16384` (логическая длина нормализованного
текста `jsonb`: после `:` и `,` пробел, порядок ключей приведён базой, поэтому условие без пробелов «весит» больше, чем написано).
`octet_length` считает байты в кодировке сервера, поэтому **база данных должна быть создана в кодировке UTF8** (проверка:
`SHOW server_encoding;`); тогда лимит - 16384 байта UTF-8. Такая же проверка есть в компиляторе сервиса (читает текст из базы).

**Кто может править:** `ktm_migrator` (владелец схемы) либо роль, которую администратор выдаст отдельно. Отдельной роли-редактора пока нет.
**Триггеров и аудита нет:** кто и когда изменил правило, БД не фиксирует (временное отступление, принято лидом; ADR-0005 п. 5);
`version` увеличивает автор правки вручную, а сервис пишет Warning, если содержимое изменилось при прежней `version`.
Изменение вступает в силу не позже интервала опроса сервиса (по умолчанию 60 с, `DataSaver__Rules__RefreshIntervalSeconds`).

Правка SQL-командой (под `ktm_migrator`):

```sql
-- включить пример и поднять версию
UPDATE ktm.rejection_rules SET enabled = true, version = version + 1 WHERE rule_id = 'skud-test-event';

-- изменить условие
UPDATE ktm.rejection_rules
   SET condition = '{"==": [{"var": "eventType"}, "test_event"]}'::jsonb, version = version + 1
 WHERE rule_id = 'skud-test-event';

-- выключить
UPDATE ktm.rejection_rules SET enabled = false, version = version + 1 WHERE rule_id = 'skud-test-event';
```

**Примеры выключают, не удаляют.** `V003` выполняется при каждом прогоне `migrate`: удалённый пример (`DELETE`) вернётся выключенным,
а изменённый вручную (`enabled`, `condition`, `version`) повторный прогон не затирает (`ON CONFLICT DO NOTHING`).

## Проверка

Идемпотентность: выполнить `migrate` (или `V001` под `ktm_migrator`) дважды, оба раза код 0, `pg_dump --schema-only -n ktm` совпадает
(строки `\restrict`/`\unrestrict` с случайным токеном при сравнении отбрасываются).

Права и неизменяемость (под `ktm_app`):

```sql
UPDATE ktm.events SET trust = 1;          -- ERROR: permission denied for table events
DELETE FROM ktm.events;                   -- ERROR: permission denied for table events
UPDATE ktm.event_corrections SET reason = 'x';  -- ERROR: permission denied
```

Идемпотентность приёма: повторный `INSERT` с тем же `(source, source_event_id, occurred_at)` отклоняется уникальным индексом
(или даёт 0 строк с `ON CONFLICT (source, source_event_id, occurred_at) DO NOTHING`).

Локальная проверка в Docker:

```bash
docker run -d --name ktm-db -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=ktm_revisor timescale/timescaledb:2.32.1-pg16
# затем docker cp файлов и psql, как выше; в конце: docker rm -f ktm-db
```

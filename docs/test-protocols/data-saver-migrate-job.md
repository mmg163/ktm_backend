# Протокол: контейнер `migrate` (локальная проверка)

Изменение OpenSpec `add-data-saver-migrate-job`, задачи 3.1, 3.2 и ручная часть 2.x. Dev-стенд не использовался
(задачи 5.2, 5.3 не выполнялись). Дата: 2026-11-21. Docker 29.9.3 (Docker Desktop, Windows), Docker Compose v2.

## Стенд

- БД: `timescale/timescaledb:2.32.1-pg16`, база `ktm_revisor`, `db/init` выполнен суперпользователем, пароль `ktm_migrator`
  со спецсимволами `a:b\c d`.
- Клиент: `postgres:16.16-alpine3.24`, `psql` 16.16, `docker images` - 420 МБ на диске (распакованный размер).
- Запуск как в compose: `user 1654:1654`, `read_only`, `tmpfs /tmp`, `db` и скрипт только на чтение, секрет файлом.

## Автоматические тесты (`MigrateScriptTests`)

- Двукратный прогон реального `db/migrations`: оба раза код 0, `pg_dump --schema-only -n ktm` совпадает.
  Каждый `V*.sql` при втором прогоне выполняется поверх накатанной схемы (метатест состава). Проверено: временный файл
  `V002__nonidempotent.sql` с `CREATE TABLE ktm.t(a int)` краснит тест.
- Неверный SQL во `V002`: код не 0, в выводе `V002__broken.sql` и `syntax error`, `V003` не выполнен, пароль в выводе нет.

## Ручные сценарии

| Сценарий | Результат |
|---|---|
| Порядок: `V010`, `V002`, `V001` в каталоге | в логе `apply V001`, `V002`, `V010`, код 0 |
| Дубль номера (`V001__a`, `V1__b`) | код 1, `duplicate migration number 1`, файлы не выполнены |
| Неверное имя (`Vx__b.sql`) | код 1, `file name does not match` |
| Пустой каталог | код 1, `no files matching V*.sql` |
| `V2__a` и `V10__b` | код 1, `name order differs from numeric order` |
| Два запуска одновременно (файлы с пробой `pg_try_advisory_lock(777)` и `pg_sleep(1)`) | оба код 0; один пишет `lock is held by another session, waiting`, проба взаимного исключения не сработала |
| Сторонний сеанс держит блокировку, `MIGRATE_LOCK_TIMEOUT_SECONDS=2` | код 3, `timed out waiting for migration lock`, файл не выполнен |
| Сторонний сеанс снимает блокировку через 5 с, таймаут 120 | запуск ждёт, затем `apply`, код 0 |
| Сеанс блокировки завершён (`pg_terminate_backend`) между `V002` и `V003` | код 3, `lock session lost before V003`, `V003` не выполнен |
| Пароль во время работы (`pg_sleep(5)`) | в argv всех процессов контейнера, в `environ`, в `docker inspect` и в логе - 0 вхождений |
| `SslMode=Require` и по умолчанию на сервере без SSL | код 1, подключение отклонено (`server does not support SSL`) |
| `SslMode=Prefer` / `Disable` | код 0 |
| `SslMode=VerifyFull` (нет сервера с SSL) | код 1 |
| Неизвестный `SslMode`, пустой `Host`, нет файла пароля | код 2, сообщение с именем переменной / путём файла |
| `DataSaver__Postgres__Username=ktm_app` в окружении | миграции выполнены под `ktm_migrator`, код 0 |

Не проверено: `VerifyCA`/`VerifyFull` на сервере с SSL (на dev принят `Require`; корневой сертификат вне объёма).

## Compose (`deploy/data-saver`)

- `docker compose config -q` валиден; `docker compose run --rm migrate` на БД после `init` - код 0, повтор - код 0.
- `docker compose up -d` на мигрированной БД: `migrate` Exited (0), затем `data-saver` стартует.
- Повторный `up -d` при работающем стеке: `migrate` запускается заново (тот же контейнер, новый `StartedAt`), код 0;
  `data-saver` остаётся `Running`, не пересоздаётся.
- Сломанный `V002__broken.sql` (временный файл, удалён): `up -d` завершается ошибкой
  `service "migrate" didn't complete successfully: exit 1`; `data-saver` в состоянии `Created`, не запущен;
  `docker compose logs migrate` содержит `psql:/db/migrations/V002__broken.sql:2: ERROR: syntax error at or near "SELEC"`.
- `docker inspect migrate` не содержит пароля.

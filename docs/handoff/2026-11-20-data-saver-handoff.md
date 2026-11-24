# Handoff: Data Saver (OpenSpec `add-data-saver-core`)

Дата: 2026-11-21 (переписан; первая версия от 2026-11-20 описывала состояние до группы 6). Файл для переноса контекста в новую сессию. В git не коммитится (каталог `docs/handoff/` неотслеживаемый).

## 1. Задача
Сервис Data Saver (.NET Worker, Confluent.Kafka + Npgsql): читает топик `ktm.events.raw` (группа `ktm-data-saver`), пишет события пачками одним `INSERT … unnest … ON CONFLICT DO NOTHING` в `ktm.events`, коммитит offset только после COMMIT в БД, при недоступной БД паузит чтение и повторяет с нарастающей паузой, корректировки пишет в `event_corrections` (и в `events` по варианту B3), health `/health/live` и `/health/ready`, настройки из переменных окружения.
Критерии приёмки: событие в БД за секунды; 100 событий с 10 дублями -> 90 строк; остановка посреди обработки без потерь/дублей; БД остановлена на 2 мин без потерь/дублей; интеграционные тесты на Testcontainers (Kafka + Postgres) проходят; корректировка попадает в `event_corrections`, `events` исходного не меняется; замер событий/с; работа на dev.
Вне объёма (на будущее, группа 10): движок правил отбраковки, топик отбракованных по правилам; C5 (проверка событий) и C6 (обработка ошибок записи) подключены как порты-заглушки.
Источники: `docs/design/Схема_БД_ER.md`, `docs/api/kafka-event-format.md`, `docs/api/kafka-event.schema.json`, ADR-0001..0004 в `docs/adr/`, `src/README.md`, `docs/test-protocols/` (`data-saver-throughput.md`, `data-saver-scenario-coverage.md`), изменение `openspec/changes/add-data-saver-core/` (proposal, design, specs, tasks).

## 2. Правила проекта и договорённости с лидом
- `CLAUDE.md`: роли `teamlead` (ADR, OpenSpec-артефакты, разбор замечаний), `developer` (код), `reviewer` (ревью). Сессия сама артефакты и код не пишет. Одна feature-ветка на изменение: `feature/data-saver-core`. В `main`/`master` не пушить никогда, даже под нажимом. Пока есть замечания ревью - чекбокс не отмечается, коммита и пуша нет; разбор через `teamlead`.
- Режим ревью (утверждён лидом): повторное ревью проверяет закрытие прошлых замечаний; блокируют только Critical/Major, minor — списком. Перед ревью developer прикладывает реальный прогон проверок.
- Только бесплатные библиотеки (MIT/Apache/BSD, проверять лицензию бинарного релиза на NuGet). TimescaleDB как расширение PostgreSQL — выбор проекта, вопрос закрыт.
- Тестов на конфигурационные файлы и метафайлы (csproj, props, deps.json, .gitignore, .env.example) не пишем; тесты поведения кода и БД — пишем.
- Вариант B3 для корректировок: запись и в `events`, и в `event_corrections`. Единый топик `ktm.events.raw`. Девять партиций (решение teamlead в `infra/kafka`, в ТЗ числа нет).
- Решения лида:
  - `DataSaver__Postgres__SslMode` по умолчанию `Require`;
  - потолок backoff жёсткий 30 с после джиттера;
  - Resume перед commit (ADR-0002 D-7);
  - healthcheck контейнера на `/health/live`;
  - `CommandTimeoutSeconds=10`;
  - принятые пробелы покрытия из сверки 11.1 (`docs/test-protocols/data-saver-scenario-coverage.md`): №2 (три источника в журнале - только `Perf`) и №14 (остановка при недоступной БД - юнит-уровень) приняты как частичные; №18 (секреты в репозитории) и №38 (стадия не знает о цикле) приняты как «закрыто иначе, без автотеста» (ручная проверка + чек-лист ревью 11.4);
  - ранее (2026-11-20): Q1 ограничение Commit/Close отложено (10.9); Q2 `source`/`sourceEventId` = null для неразобранных сообщений; Q3 `stop_grace_period` >= `TimeoutSeconds` + 5 с.
- Память проекта: `C:\Users\tlustyx\.claude\projects\C--Users-tlustyx-RiderProjects-ktm-backend\memory\` (три feedback-файла: режим ревью - блок только Major+; только бесплатные библиотеки; не тестировать конфиги и метафайлы).

## 3. Состояние репозитория
Ветка `feature/data-saver-core`, HEAD `d77e66a` ("tasks: отмечена 4.2 (проверка в 7.7)"). Ветка запушена в `origin/feature/data-saver-core` (совпадает с HEAD), открывается PR в `main` (номер не указан). Рабочее дерево чистое, кроме неотслеживаемого `docs/handoff/`.
Worktree-ветки агентов слиты в `feature/data-saver-core` и удалены.

Проверка на HEAD:
- `dotnet build src/Ktm/Ktm.sln -warnaserror`: 0 предупреждений / 0 ошибок;
- unit: 240 зелёных;
- интеграционные: 61 зелёных с фильтром `Category!=Long&Category!=Perf` (Docker: apache/kafka, timescale/timescaledb:2.32.1-pg16);
- `Long`: 2/2 зелёных.
Протоколы: `docs/test-protocols/data-saver-throughput.md` (замер, группа 8), `data-saver-scenario-coverage.md` (сверка 11.1).

Структура (`src/Ktm/`): `Ktm.DataSaver.Core` (модель, декодер, конвейер, цикл `IngestionLoop`, порты, `Batcher`, `OffsetTracker`, `BackoffPolicy`, `WriteErrorClassifier`), `Ktm.DataSaver.Infrastructure` (`Kafka/*`, `Postgres/*`, `Observability/*`), `Ktm.DataSaver` (host: `Configuration/`, health, composition root), `tests/Ktm.DataSaver.Tests` (unit), `tests/Ktm.DataSaver.IntegrationTests` (Testcontainers). БД: `db/init/001_roles_and_extension.sql`, `db/migrations/V001__event_journal.sql`, `db/README.md`. Деплой: Dockerfile и compose в `deploy/data-saver/`, руководство `docs/manuals/data-saver.md`. Пакеты: Confluent.Kafka 2.16.0, Npgsql 10.0.3, LateApexEarlySpeed.Json.Schema 5.0.0 (BSD-3-Clause), Microsoft.Extensions.Logging.Abstractions 10.0.15, xUnit v2.

### Задачи `tasks.md`
- Выполнены и слиты (`[x]`): группы 1-8, 9 (кроме 9.2), 11 (в т.ч. 4.2 - проверка `TwoInstancesInOneGroup...` в 7.7).
- Открыто: 9.2 (выкладка на dev); группа 10 - вне объёма, все пункты `[ ]` по определению (10.10 закрыто решением по N1 ревью 9.5, см. ADR-0002 D-11).

## 4. Открытые вопросы и хвосты
- **9.2**: развернуть на `dev-backend` (Kafka `10.14.0.6:9092`, БД на `dev-data`, роль `ktm_app`). Проверка: тестовое сообщение (`source = skud`) в `ktm.events.raw` даёт строку в `ktm.events` за секунды, `/health/ready` = 200, `docker compose stop` завершается без потерь. Нужен доступ к стенду.
- **10.27**: полный Perf-замер на dev вместе с 9.2 (9.2 проверяет только сообщение и остановку); дополнить протокол цифрами стенда и выводом о запасе к 500 соб/с. Связанные: 10.16 (замер `CommandTimeout` на команду, синхронная репликация), 10.17-10.28 (хвосты замера).
- Хвосты 10.x кратко (все вне объёма, minor, не блокируют):
  - 10.1-10.5: движок правил, топик `rule-rejected`, C5/C6, доверие источников из хранилища, метрики/алерты;
  - 10.6-10.8: образы по digest, hardening compose (проверять с 9.2), валидация `<source>` в настройках доверия;
  - 10.9, 10.11, 10.13, 10.14, 10.15: Commit/Close по бюджету остановки, `Dispose` с `_adminUse` без таймаута, двойной лог сбоя `Consume`, `Touch()` вокруг `IWriteFailureHandler`, троттлинг логов;
  - 10.12: фильтр по умолчанию для `Long`;
  - 10.17-10.26, 10.28: хвосты ревью группы 8 (EXPLAIN реальным SQL, молчаливые отказы замера, `PerfSupport`, структура `ThroughputPerfTests`, окно монитора задержки, `GETPID`, прогон batch с другими параметрами);
  - 10.29: 24 minor итогового ревью 11.4/11.5 (логирование исключений без `Message`, `IsNetworkType`, `SslMode`, тесты `NpgsqlEventStore`/`IngestionLoop`/`EventDecoder`, health без `WithName`/`Produces`);
  - 10.30: пробелы покрытия №2 и №14 (приняты лидом, закрытие по желанию; №14 дёшево - `StoppableDatabase`).
- Ключ сообщения: в формате `{source}:{externalPersonId}`, в infra `<source>:<sourceEventId>` — вопрос владельцам адаптеров.
- Docker в CI неизвестен: интеграционные тесты и приёмка пока только локально.
- Версия TimescaleDB на стенде предприятия неизвестна (проверено 2.32.1).
- Дубли в `rejected` при частичном сбое публикации — принято (at-least-once).

## 5. Следующие шаги
1. Довести PR `feature/data-saver-core` -> `main` до ревью лидом (лид мержит сам; в `main` не пушим).
2. 9.2: выкладка на `dev-backend` по `docs/manuals/data-saver.md`, проверка тестовым сообщением и остановкой; затем отметить `[x]`.
3. 10.27: замер на dev, дополнение `docs/test-protocols/data-saver-throughput.md`.
4. После приёмки - `openspec archive` изменения (`$openspec-archive-change`, пишет `teamlead`); хвосты группы 10 - отдельными изменениями по приоритету лида.

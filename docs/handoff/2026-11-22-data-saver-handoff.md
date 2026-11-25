# Handoff: Data Saver (КТМ backend), состояние на 2026-11-22

Файл для переноса контекста в новый чат. В git не коммитится (каталог `docs/handoff/` неотслеживаемый). Заменяет `2026-11-20-data-saver-handoff.md`, тот устарел.

## 1. Что сделано

Репозиторий `C:\Users\tlustyx\RiderProjects\ktm_backend`, GitHub `mmg163/ktm_backend`. Рабочая ветка локально `main`, HEAD `f3b4539`, совпадает с `origin/main`. Дерево чистое, кроме `docs/handoff/`.

В `main` слиты два изменения OpenSpec:
- **PR #6, `add-data-saver-core`** — сервис Data Saver: Kafka `ktm.events.raw` -> Postgres/Timescale `ktm.events`, корректировки в `event_corrections` (вариант B3), отбракованное в `ktm.events.rejected`; commit offset только после COMMIT БД и публикации rejected; health `/health/live`, `/health/ready`; настройки `DataSaver__*`; Docker и compose; тесты; протоколы.
- **PR #7, `add-data-saver-migrate-job`** — разовый контейнер `migrate` (`postgres:16.16-alpine3.24`, POSIX sh `deploy/data-saver/migrate/migrate.sh`) накатывает `db/migrations/V*.sql` под ролью `ktm_migrator`; `data-saver` стартует после него (`depends_on: service_completed_successfully`). Пароль `ktm_migrator` — Docker secret `secrets/ktm_migrator_password` (`chown 1654:1654`, `0400`), подключён только к `migrate`. Роли, расширение `timescaledb`, схема `ktm` и пароли ролей остаются ручным шагом администратора БД (`db/init/001_roles_and_extension.sql` под суперпользователем).

Проверки на последнем HEAD ветки перед слиянием: `dotnet build src/Ktm/Ktm.sln -warnaserror` 0/0; unit 240; интеграционные (`--filter "Category!=Long&Category!=Perf"`) 61–64; `Category=Long` 2/2. После слияния в `main` тесты не прогонялись.

Также есть `src/README.md` (назначение, контекст, схемы модулей, таблица настроек `DataSaver__*`, сборка и запуск, ссылки).

## 2. Правила проекта (CLAUDE.md) и договорённости с лидом

- Роли только через субагентов: `teamlead` (ADR, артефакты OpenSpec, документы, разбор замечаний), `developer` (код и тесты), `reviewer` (ревью). Сессия сама код и артефакты не пишет, ведёт `tasks.md` (чекбоксы после ревью без Critical/Major) и мержит worktree-ветки.
- Одно изменение OpenSpec = одна ветка `feature/<имя>`. В `main`/`master` не пушить никогда. Пуш feature-ветки и PR — по команде лида (лид создаёт PR сам: утилиты `gh` на машине нет).
- Режим ревью: повторное ревью = закрытие прошлых замечаний; блокируют только Critical/Major, minor списком.
- Только бесплатные библиотеки (MIT/Apache/BSD). Тесты на конфиги и метафайлы (csproj, props, `.env.example`, compose) не пишем; тестов в migrate-job лид просил немного.
- Память проекта: `C:\Users\tlustyx\.claude\projects\C--Users-tlustyx-RiderProjects-ktm-backend\memory\` (три feedback-файла: режим ревью, бесплатные библиотеки, нет тестов на конфиги).

Решения лида (оглядываться при вопросах): `SslMode` по умолчанию `Require`; потолок backoff 30 с; Resume перед commit (ADR-0002 D-7); healthcheck контейнера на `/health/live`; `CommandTimeoutSeconds=10`; принятые пробелы покрытия сценариев №2, №14, №18, №38 (тест №14 — хвост 10.30); пароль мигратора — Docker secret (не `.env`); суперпользователя на `dev-data` нет, `init` делает администратор БД; Docker Hub с `dev-backend` доступен; `VerifyCA/VerifyFull` вне объёма.

## 3. Что открыто

**Требует команды лида и доступа к стенду:**
- **9.2 / 5.2 / 10.27 — выкладка на `dev-backend`** (Kafka `10.14.0.6:9092`, БД на `dev-data`, роль `ktm_app`). Порядок: (1) администратор БД выполняет `db/init` и задаёт пароли `ktm_app`, `ktm_migrator`; (2) на хосте создаются файлы секретов `ktm_app_password`, `ktm_migrator_password` (`chown 1654:1654`, `chmod 0400`; создание через `read -rs`, не `printf` с паролем в команде); (3) `docker compose up -d` в `deploy/data-saver`: `migrate`, затем сервис; (4) `/health/ready` = 200, тестовое сообщение в Kafka, штатная остановка; (5) замер на стенде (10.27), подтверждение допущений А1 и А4 (5.3). Предложено подготовить протокол выкладки отдельным документом — лид пока не просил.
- Права файла секрета при `chown 1654` / `0400` на Linux на стенде не проверены; версия TimescaleDB на стенде предприятия неизвестна (проверено 2.32.1); Docker в CI неизвестен.

**Архивация OpenSpec:** изменения `add-data-saver-core` и `add-data-saver-migrate-job` не архивированы; рекомендовано архивировать после выкладки на dev.

**Хвосты вне объёма** — `openspec/changes/add-data-saver-core/tasks.md`, блок 10 (10.1–10.30): движок правил, топик отбраковки по правилам, содержимое C5/C6, digest образов и hardening compose, валидация `<source>`, бюджет Commit/Close/Flush, `Dispose` источника с таймаутом, фильтр по умолчанию для `Long/Perf`, замер допущения Npgsql про `CommandTimeout`, EXPLAIN реальным `InsertEventsSql`, minor итогового ревью (10.29: логирование `ex` в `IngestionLoop.cs:222/446`, `DataSaverWorker.cs:42`, `IAdminNotifier` получает полный `ex`, `IsNetworkType` только верхний уровень, тесты сравнивают лишь булевы вердикты декодера и Postgres и т. п.), пробелы покрытия №2 и №14 (10.30).

**Расхождения документации, найденные при README (не исправлены):** руководство `docs/manuals/data-saver.md` п. 7 говорит, что протокол замера «будет создан в задаче 8» (он уже есть); XML-комментарий `KafkaOptions.cs` и пункты 7.1/9.5 в `tasks.md` называют переменные `Kafka__*` вместо `DataSaver__Kafka__*`; п. 8 руководства обещает код 78 на любую неверную настройку (после 11.5.1 это исправлено в коде, формулировку стоит сверить).

**Мелочи:** локальные ветки `feature/data-saver-core` и `feature/data-saver-migrate-job` слиты, можно удалить (`git branch -d`); worktree-ветки агентов уже удалены. Ожидаются замечания лида по MR, если будут: круг `teamlead` -> `developer` -> `reviewer`.

## 4. Справка по сервису

Проекты `src/Ktm/`: `Ktm.DataSaver` (host: Program, StartupErrors, Worker, Health, Options), `Ktm.DataSaver.Core` (IngestionLoop, Batcher, OffsetTracker, Pipeline, декодер/валидатор JSON Schema, BackoffPolicy, WriteErrorClassifier, порты), `Ktm.DataSaver.Infrastructure` (Kafka source/committer/rejection publisher, Npgsql store, метрики, LogThrottle); тесты `tests/Ktm.DataSaver.Tests` (unit), `tests/Ktm.DataSaver.IntegrationTests` (Testcontainers; категории `Long` (2: БД недоступна 2 мин; commit против замороженного брокера) и `Perf` (нагрузка, Skip без `KTM_PERF_*`)). `Ktm.Api` — шаблонная заготовка, к Data Saver не относится.

Ключевое: чтение по одному сообщению, пачки собирает `Batcher` (500 штук или 1 с от первого); одна транзакция на пачку (`INSERT … unnest … ON CONFLICT DO NOTHING`); `received_at` единый на пачку, берётся один раз; при сбое БД consumer на паузе, backoff 1→30 с с джиттером ±20%, опрос продолжается; коды выхода 0 (штатно), 78 (ошибка конфигурации, одна строка в stderr), 70 (фатальная ошибка Kafka), 1 (падение цикла) — проверены на реальном процессе (протокол в `docs/test-protocols/data-saver-scenario-coverage.md`, раздел «Коды выхода»; код 1 воспроизведён только с временным патчем); формула `LiveStaleSeconds > max(ceil(PollIntervalMs/1000), ConnectTimeout+3*CommandTimeout, ceil(ProducerMessageTimeoutMs/1000))`.

Нагрузочный замер (локальный Docker, НФТ не подтверждает): 500 соб/с 15 мин, задержка p50/p95/p99 759/1230/1333 мс, лаг ≈ размер пачки, максимум одного экземпляра ≈ 8000 соб/с (запас ≈ 16–17×). Протокол: `docs/test-protocols/data-saver-throughput.md`.

Документы: `docs/adr/0001..0004` (0004 — миграции БД, раздел 5 про `migrate`), `docs/manuals/data-saver.md`, `docs/api/kafka-event-format.md`, `docs/api/kafka-event.schema.json`, `docs/design/Схема_БД_ER.md`, `db/README.md`, `docs/test-protocols/*`, `openspec/changes/add-data-saver-core|add-data-saver-migrate-job`.

## 5. Как продолжить в новом чате

Прочитай этот файл, `CLAUDE.md` и `tasks.md` обоих изменений. Первый вопрос лиду: выкладываем на `dev-backend` (тогда подготовить протокол выкладки) или сначала разбираем замечания к MR / берём хвосты блока 10.

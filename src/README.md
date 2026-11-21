# КТМ backend: solution `src/Ktm`

## 1. Назначение

КТМ - система мониторинга показателей производственной деятельности: сбор, нормализация и интерпретация данных о работе
персонала предприятия из СКУД, видеоаналитики и 1С:ДО (требования - `docs/`). Система выдаёт сигналы, решения принимает
человек; автоматических санкций нет (`docs/01-цели-и-принципы.md`).

Этот solution - серверная часть уровня 1 (сбор): сервис **Data Saver**. Он закрывает задачу надёжного приёма потока событий
в журнал:

```
адаптеры источников -> Kafka ktm.events.raw -> Data Saver -> PostgreSQL + TimescaleDB, таблица ktm.events
                                                  |-> ktm.events.rejected (Kafka): сообщения, не прошедшие проверку формата
                                                  '-> ktm.event_corrections: ссылки корректировок на исходные события
```

- читает `ktm.events.raw`, проверяет сообщение по `docs/api/kafka-event.schema.json`, пишет пачками;
- offset коммитится только после COMMIT в БД; идемпотентность гарантирует уникальный ключ
  `(source, source_event_id, occurred_at)`, а не код (`ON CONFLICT DO NOTHING`);
- журнал неизменяем: сервис работает под ролью `ktm_app` (`SELECT, INSERT`), корректировки - отдельными записями;
- недоступность БД или Kafka не теряет данные: пачка повторяется с нарастающей паузой, партиции стоят на паузе.

Вне этого solution: REST API для фронтенда (`ktm_revisor`), ролевая интерпретация, движок правил, дашборды. Проект
`Ktm.Api` в solution - шаблонная заготовка (`/weatherforecast`), функционала не содержит.

## 2. Что реализовано и что нет

Изменение OpenSpec `add-data-saver-core` (`openspec/changes/add-data-saver-core/tasks.md`) **не закрыто**; на `dev` сервис не
выложен.

Реализовано (отмечено в `tasks.md`):

- группы 1-8: каркас solution, разбор и проверка сообщения (схема, лимиты, `\u0000`, совместимость с `jsonb`),
  конвейер стадий, `OffsetTracker`, `Batcher`, backoff, классификатор ошибок записи, цикл `IngestionLoop`;
  Kafka-адаптеры (consumer, producer `rejected`, commit), пакетная запись в Npgsql, корректировки, SQL-миграции `db/`,
  health, остановка, интеграционные тесты на Testcontainers, замер пропускной способности (на локальном Docker, не на dev);
- группа 9: Dockerfile, `deploy/data-saver/compose.yml`, руководство `docs/manuals/data-saver.md`, 9.4, 9.5;
- 11.1 (сверка со спеками), 11.3 (`openspec validate`).

Не закрыто (честно):

| Пункт | Состояние |
|---|---|
| 9.2 выкладка на `dev-backend`/`dev-data` | не выполнена |
| 11.2 прогон `dotnet test` и `Long` как приёмка | не отмечен |
| 11.4 итоговое ревью и 11.5.1-11.5.9 | открыты. Среди замечаний: нечисловое значение настройки (`DataSaver__Batch__MaxSize=abc`) пока не даёт код 78 (11.5.1); число с очень длинной дробной частью без экспоненты проходит декодер, а БД его отвергнет (11.5.2); `received_at` пересчитывается при повторе записи (11.5.4) |
| 4.2 | не отмечен, хотя проверка есть (`7.7`); решает лид (10.29) |
| Группа 10 | вне объёма (ниже) |

Вне объёма этой версии:

- **C5** (проверка событий по бизнес-правилам): заглушка `AcceptAllEventValidator`, принимает всё;
- **C6** (политика при ошибках данных `22xxx`/`23xxx`): заглушка `BlockingWriteFailureHandler`, всегда `Retry` - партиция
  блокируется, пока причина не устранена; данные остаются в Kafka (хранение 7 суток);
- движок правил отбраковки и топик отбракованных правилами; dead-letter топик (`ktm.events.dlq`);
- коэффициенты доверия источников берутся из переменных окружения (временно), не из хранилища;
- экспорт метрик (Prometheus/OTel), канал уведомления администратора (сейчас лог и счётчик);
- выполнение `Commit`/`Close`/`Flush` не ограничено бюджетом остановки (10.9).

Замер (`docs/test-protocols/data-saver-throughput.md`): на локальном Docker один экземпляр записал 500 соб/с без роста лага,
минимальная ёмкость 7967 соб/с. Протокол сам оговаривает, что НФТ (`docs/07`) он **не подтверждает**: замер на dev не делался.

## 3. Схема модулей

Зависимости направлены в сторону `Core`; `Core` не ссылается на Confluent.Kafka, Npgsql и ASP.NET (ADR-0001).

```mermaid
flowchart TB
    subgraph EXT[Внешние системы]
        K[(Kafka<br/>ktm.events.raw<br/>ktm.events.rejected)]
        PG[(PostgreSQL + TimescaleDB<br/>ktm.events, ktm.event_corrections)]
    end

    subgraph HOST[Ktm.DataSaver: Host]
        W[DataSaverWorker]
        H[Health: /health/live, /health/ready]
        CFG[DataSaverOptions: DataSaver__*]
    end

    subgraph INFRA[Ktm.DataSaver.Infrastructure: адаптеры]
        KS[KafkaMessageSource]
        KC[KafkaOffsetCommitter]
        KR[KafkaRejectionPublisher]
        NS[NpgsqlEventStore]
        OBS[Observability: метрики, логи]
    end

    subgraph CORE[Ktm.DataSaver.Core: домен и порты]
        LOOP[IngestionLoop]
        BT[Batcher + OffsetTracker]
        PIPE[IngestionPipeline:<br/>FormatStage -> EventValidationStage C5]
        RES[BackoffPolicy, WriteErrorClassifier]
        PORTS{{Порты: IMessageSource, IEventStore,<br/>IRejectionPublisher, IOffsetCommitter,<br/>IEventValidator, IWriteFailureHandler}}
    end

    W --> LOOP
    H --> LOOP
    CFG --> W
    HOST --> INFRA
    INFRA --> CORE
    LOOP --> BT
    LOOP --> PIPE
    LOOP --> RES
    LOOP --> PORTS
    KS -. реализует .-> PORTS
    KC -. реализует .-> PORTS
    KR -. реализует .-> PORTS
    NS -. реализует .-> PORTS
    KS <--> K
    KC --> K
    KR --> K
    NS --> PG
```

Путь одной пачки (`IngestionLoop`):

```mermaid
flowchart LR
    A[poll: Consume<br/>PollIntervalMs] --> B[Batcher<br/>500 сообщений или 1 с<br/>от первого]
    B --> C[Pipeline:<br/>формат, C5]
    C -->|принято| D[запись в БД<br/>одна транзакция,<br/>ON CONFLICT DO NOTHING]
    C -->|отбраковано| E[публикация в<br/>ktm.events.rejected]
    D --> F[commit offset+1<br/>по партициям]
    E --> F
    D -. сбой: Pause, backoff,<br/>повтор той же пачки .-> D
    E -. сбой: повтор .-> E
```

Порядок строгий: запись в БД, публикация отбракованных, затем commit. Commit offset учитывает все сообщения пачки,
включая отбракованные.

| Проект | Модуль | Роль |
|---|---|---|
| `Ktm.DataSaver.Core` | `Model`, `Decoding` | `ParsedEvent`, `ConsumedMessage`; `EventDecoder`: JSON -> событие, проверка схемы, UTC-микросекунды, причины отбраковки (`malformed_json`, `unknown_schema_version`, `invalid_schema`) |
| | `Pipeline` | стадии `Accept`/`Reject`, `FormatStage`, порты `IEventValidator` (C5) и `IWriteFailureHandler` (C6) с заглушками |
| | `Batching` | `Batcher` (размер или время), `OffsetTracker` |
| | `Resilience` | `BackoffPolicy`, `WriteErrorClassifier` (временная / конфигурационная / данные) |
| | `Ingestion` | `IngestionLoop`, порты источника, хранилища, публикации, commit, `ISourceTrustProvider`, `IAdminNotifier` |
| `Ktm.DataSaver.Infrastructure` | `Kafka` | consumer, producer `rejected` (идемпотентный, `acks=all`), commit, обработка фатальных ошибок, троттлинг логов librdkafka |
| | `Postgres` | `NpgsqlEventStore` (пакетная запись, корректировки), `PostgresOptions`, `EnvironmentSourceTrustProvider` |
| | `Observability` | метрики `System.Diagnostics.Metrics` (meter `Ktm.DataSaver`), декораторы портов, `LoggingAdminNotifier` |
| `Ktm.DataSaver` | Host | composition root, `DataSaverWorker`, опции `DataSaver__*` с проверкой при старте, health (Kestrel), коды выхода |
| `Ktm.DataSaver.Tests` | unit | Core и host на fake-портах |
| `Ktm.DataSaver.IntegrationTests` | интеграционные | Testcontainers: `apache/kafka` и `timescale/timescaledb` pg16; приёмочные, `Long`, `Perf` |
| `Ktm.Api` | - | шаблонная заготовка, не относится к Data Saver |

Вне solution, но нужное для запуска: `db/` (SQL-миграции, ADR-0004), `deploy/data-saver/` (compose), `Dockerfile` в корне.

## 4. Настройки

Все настройки - переменные окружения `DataSaver__<Секция>__<Имя>`; значения по умолчанию заданы в классах options и
проверяются при старте. Ошибка - отказ запуска с сообщением, где названа переменная (значения секретов в него не входят).
Источник: `Ktm.DataSaver/Configuration/DataSaverOptions.cs`, `Ktm.DataSaver.Infrastructure/Kafka/KafkaOptions.cs`,
`Ktm.DataSaver.Infrastructure/Postgres/PostgresOptions.cs`; шаблон - `deploy/data-saver/.env.example`.

Секреты (пароли БД и SASL) задаются **файлом** (`*PasswordFile`, в compose - Docker secret); параметры `Password` и
`PasswordFile` взаимоисключающие. Завершающий перевод строки в файле отбрасывается.

**Обязательные**

| Переменная | Ограничения |
|---|---|
| `DataSaver__Kafka__BootstrapServers` | не пусто |
| `DataSaver__Postgres__Host` | не пусто |
| `DataSaver__Postgres__Database` | не пусто |
| `DataSaver__Postgres__Username` | не пусто (роль `ktm_app`) |
| `DataSaver__Postgres__PasswordFile` или `DataSaver__Postgres__Password` | ровно один из двух; файл должен читаться и быть непустым |

**Kafka**

| Переменная | По умолчанию | Назначение | Ограничения |
|---|---|---|---|
| `DataSaver__Kafka__Topic` | `ktm.events.raw` | входной топик | не пусто |
| `DataSaver__Kafka__RejectedTopic` | `ktm.events.rejected` | топик отбракованных | не пусто |
| `DataSaver__Kafka__GroupId` | `ktm-data-saver` | группа потребителей | не пусто |
| `DataSaver__Kafka__ClientId` | не задан | `client.id` | |
| `DataSaver__Kafka__GroupInstanceId` | не задан | `group.instance.id` (static membership) | уникален на реплику |
| `DataSaver__Kafka__SessionTimeoutMs` | `45000` | session timeout группы | >= 1 |
| `DataSaver__Kafka__ProducerMessageTimeoutMs` | `30000` | предел подтверждения публикации в `rejected` | >= 1; входит в правило `LiveStaleSeconds` |
| `DataSaver__Kafka__SecurityProtocol` | не задан (по умолчанию клиента) | протокол | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
| `DataSaver__Kafka__SaslMechanism` | не задан | механизм SASL | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, `OAUTHBEARER` |
| `DataSaver__Kafka__SaslUsername` | не задан | пользователь SASL | |
| `DataSaver__Kafka__SaslPasswordFile` / `SaslPassword` | не задан | пароль SASL | взаимоисключающие; предпочтителен файл |

Зафиксировано в коде и не настраивается: `earliest` при отсутствии offset группы, автокоммит выключен, `cooperative-sticky`,
producer `rejected` идемпотентный с `acks=all`.

**PostgreSQL**

| Переменная | По умолчанию | Назначение | Ограничения |
|---|---|---|---|
| `DataSaver__Postgres__Port` | `5432` | порт | 1..65535 |
| `DataSaver__Postgres__SslMode` | `Require` | шифрование канала | `Disable`, `Prefer`, `Require`, `VerifyCA`, `VerifyFull`; `Disable` - только для разработки |
| `DataSaver__Postgres__CommandTimeoutSeconds` | `10` | предел одной команды (запись пачки - три обращения) | >= 1 |
| `DataSaver__Postgres__ConnectTimeoutSeconds` | `15` | предел подключения | >= 1 |
| `DataSaver__Postgres__MinPoolSize` | `1` | минимум пула | `0 <= Min <= Max` |
| `DataSaver__Postgres__MaxPoolSize` | `10` | максимум пула | >= 1 |
| `DataSaver__Postgres__KeepAliveSeconds` | `30` | keepalive | >= 0 |

**Приём, повторы, остановка, health**

| Переменная | По умолчанию | Назначение | Ограничения |
|---|---|---|---|
| `DataSaver__PollIntervalMs` | `500` | предел ожидания одного Consume | >= 1 |
| `DataSaver__Batch__MaxSize` | `500` | пачка закрывается по размеру... | >= 1 |
| `DataSaver__Batch__MaxWaitMs` | `1000` | ...или по времени от первого сообщения | >= 1 |
| `DataSaver__Backoff__InitialMs` | `1000` | первая пауза повтора при сбое записи | >= 1 |
| `DataSaver__Backoff__MaxMs` | `30000` | потолок паузы | >= `InitialMs` |
| `DataSaver__Backoff__Multiplier` | `2.0` | множитель | >= 1 |
| `DataSaver__Backoff__JitterFraction` | `0.2` | доля джиттера | 0..1 |
| `DataSaver__Shutdown__TimeoutSeconds` | `30` | предел дописывания пачки от сигнала остановки | >= 1; `stop_grace_period` в compose >= значение + 5 с (сейчас 40 с) |
| `DataSaver__Health__Port` | `8080` | порт health-эндпоинтов | 1..65535 |
| `DataSaver__Health__LiveStaleSeconds` | `60` | через сколько без отметки цикла liveness = 503 | >= 1 и **строго больше** `max(ceil(PollIntervalMs / 1000), Postgres__ConnectTimeoutSeconds + 3 * Postgres__CommandTimeoutSeconds, ceil(Kafka__ProducerMessageTimeoutMs / 1000))`, иначе отказ запуска |

С умолчаниями правило выполняется: `max(1, 15 + 3 * 10, 30) = 45 < 60`.

**Коэффициенты доверия источников** (временно, до хранилища настроек; применяются после перезапуска)

| Переменная | По умолчанию | Назначение | Ограничения |
|---|---|---|---|
| `DataSaver__Trust__Default` | `100` | коэффициент по умолчанию | целое 0..100 |
| `DataSaver__Trust__Source__<source>` | - | переопределение для источника, например `...__skud=80` | целое 0..100; имя `<source>` не проверяется, опечатка молча игнорируется |

**Коды выхода процесса**

| Код | Когда |
|---|---|
| `0` | штатная остановка по SIGTERM |
| `70` | фатальная ошибка клиента Kafka |
| `1` | ошибка цикла приёма (лог `Critical`) |
| `78` | ошибка конфигурации: одна строка в stderr с именем настройки; рестарт её не лечит |

Коды 70 и 1 в автотестах на уровне процесса не проверены (11.5.8, ручной прогон открыт).

## 5. Сборка, тесты, запуск

```bash
dotnet build src/Ktm/Ktm.sln
```

Unit и быстрые интеграционные тесты (интеграционным нужен запущенный Docker):

```bash
dotnet test src/Ktm/Ktm.sln --filter "Category!=Long&Category!=Perf"
```

- `Long` - длительные сценарии (остановка БД на 2 минуты, остановка брокера): `--filter "Category=Long"`.
- `Perf` - замер пропускной способности; без параметров `KTM_PERF_*` пропускается. Команды и параметры - в протоколе
  `docs/test-protocols/data-saver-throughput.md`, п. 7.
- Фильтр по умолчанию для `Long` не задан (10.12): без `--filter` он запускается вместе с остальными.

Запуск (каталог `deploy/data-saver`; подробности, права файла секрета и остановка - в руководстве):

```bash
cp .env.example .env            # подставить Kafka, хост и базу БД
mkdir -p secrets && printf '%s' '<пароль ktm_app>' > secrets/ktm_app_password
sudo chown 1654:1654 secrets/ktm_app_password && chmod 0400 secrets/ktm_app_password
docker compose up -d --build    # образ собирается из корня репозитория: нужен docs/api
```

Предусловия: БД создана по `db/README.md` (роли, схема `ktm`, миграции `db/migrations/V*.sql`; сервис схему не создаёт),
топики Kafka созданы заранее.

Health (порт `DataSaver__Health__Port`, внутренняя сеть, без аутентификации):

- `GET /health/live` - 200, пока цикл приёма отмечается; 503 `loop_stalled` или `consumer_fatal_error`. Недоступность БД и
  Kafka liveness не роняет. На него направлен healthcheck контейнера.
- `GET /health/ready` - 200, если consumer получил партиции, цикл не в `Degraded`, брокер и БД отвечают; иначе 503 с одним
  из кодов `no_partitions_assigned`, `ingestion_degraded`, `broker_unavailable`, `database_unavailable`.

Статус `healthy` в `docker compose ps` готовность не подтверждает: смотрите `/health/ready`.

## 6. Документы

- `docs/adr/0001-backend-stack.md` - стек, структура solution, конфигурация, тесты;
- `docs/adr/0002-data-saver-ingestion.md` - пачки, идемпотентность, offset, сбои, настройки (D-11);
- `docs/adr/0003-data-saver-validation-pipeline.md` - конвейер стадий и точка расширения под правила;
- `docs/adr/0004-db-migrations.md` - миграции SQL-скриптами;
- `docs/api/kafka-event-format.md`, `docs/api/kafka-event.schema.json` - формат сообщения;
- `docs/design/Схема_БД_ER.md`, `db/README.md` - схема журнала и порядок применения;
- `docs/manuals/data-saver.md` - руководство по эксплуатации;
- `docs/test-protocols/data-saver-throughput.md` - протокол замера пропускной способности;
- `docs/test-protocols/data-saver-scenario-coverage.md` - сверка сценариев спеков с тестами;
- `openspec/changes/add-data-saver-core/` - proposal, design, specs, tasks.

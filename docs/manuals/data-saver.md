# Data Saver: руководство по эксплуатации

Сервис читает топик Kafka `ktm.events.raw` (группа `ktm-data-saver`), проверяет сообщения по схеме
`docs/api/kafka-event.schema.json` и пишет события пачками в таблицу `ktm.events` (и ссылки корректировок в
`ktm.event_corrections`). Offset коммитится только после COMMIT в БД. Сообщения, не прошедшие проверку формата, уходят в
`ktm.events.rejected`. Решения и обоснования - `docs/adr/0002-data-saver-ingestion.md` (далее ADR-0002), формат
сообщений - `docs/api/kafka-event-format.md`.

Пометка «уточнить» - то, что в коде или ADR однозначно не определено; до выяснения считать недоказанным.

## 1. Что нужно до запуска

1. **БД.** PostgreSQL с TimescaleDB, схема и роли созданы по `db/README.md` (`init/001_roles_and_extension.sql`, затем
   миграции `db/migrations/V*.sql` под `ktm_migrator`, пароль роли `ktm_app` задан). Сервис работает под ролью `ktm_app`
   (`SELECT, INSERT`). Сервис схему сам не создаёт и не мигрирует.
2. **Kafka.** Топики созданы заранее (автосоздание выключено): `ktm.events.raw` (9 партиций, хранение 7 суток) и
   `ktm.events.rejected` (3 партиции, 30 суток) - `infra/kafka/README.md`. Если `ktm.events.raw` нет, сервис стартует, но
   `/health/ready` = 503 `no_partitions_assigned` (п. 5).
3. **Docker.** Образ собирается из корня репозитория (нужен `docs/api`, который встраивается в сборку):

   ```bash
   docker build -t ktm-data-saver .
   ```

## 2. Переменные окружения

Все настройки - переменные `DataSaver__<Секция>__<Имя>` (двойное подчёркивание - разделитель уровней). Источник истины -
`src/Ktm/Ktm.DataSaver/Configuration/DataSaverOptions.cs`, `src/Ktm/Ktm.DataSaver.Infrastructure/Kafka/KafkaOptions.cs`,
`src/Ktm/Ktm.DataSaver.Infrastructure/Postgres/PostgresOptions.cs`. Значения по умолчанию в `appsettings.json` не заданы
(там только уровни логирования); умолчания ниже - из кода. Ошибка в настройках - отказ запуска с сообщением, где названа
переменная (значения секретов в сообщения не попадают).

### Обязательные

| Переменная | Назначение |
|---|---|
| `DataSaver__Kafka__BootstrapServers` | адреса брокеров, `host:port[,host:port]` |
| `DataSaver__Postgres__Host` | хост БД |
| `DataSaver__Postgres__Database` | имя базы |
| `DataSaver__Postgres__Username` | роль (`ktm_app`) |
| `DataSaver__Postgres__PasswordFile` **или** `DataSaver__Postgres__Password` | пароль: ровно один из двух (оба или ни одного - отказ запуска); в проде и compose - только файл (Docker secret), завершающий перевод строки в файле отбрасывается |

### Kafka

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DataSaver__Kafka__Topic` | `ktm.events.raw` | входной топик |
| `DataSaver__Kafka__RejectedTopic` | `ktm.events.rejected` | топик отбракованных |
| `DataSaver__Kafka__GroupId` | `ktm-data-saver` | группа потребителей |
| `DataSaver__Kafka__ClientId` | не задан | `client.id` |
| `DataSaver__Kafka__GroupInstanceId` | не задан | `group.instance.id` (static membership), уникален на реплику, п. 4 |
| `DataSaver__Kafka__SessionTimeoutMs` | `45000` | session timeout группы |
| `DataSaver__Kafka__ProducerMessageTimeoutMs` | `30000` | предел подтверждения публикации в `rejected` |
| `DataSaver__Kafka__SecurityProtocol` | не задан (библиотека) | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`; на dev - PLAINTEXT (ADR-0002, R-3) |
| `DataSaver__Kafka__SaslMechanism` | не задан | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `GSSAPI`, `OAUTHBEARER` |
| `DataSaver__Kafka__SaslUsername` | не задан | пользователь SASL |
| `DataSaver__Kafka__SaslPasswordFile` / `DataSaver__Kafka__SaslPassword` | не задан | пароль SASL, взаимоисключающие; предпочтителен файл |

Без вашего участия фиксированы: чтение с `earliest` при отсутствии offset группы, автокоммит выключен, стратегия
распределения `cooperative-sticky`, автосоздание топиков выключено, producer `rejected` идемпотентный с `acks=all`.

### PostgreSQL

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DataSaver__Postgres__Port` | `5432` | порт |
| `DataSaver__Postgres__SslMode` | `Require` | `Disable`, `Prefer`, `Require`, `VerifyCA`, `VerifyFull`; `Disable` - только для локальной разработки |
| `DataSaver__Postgres__CommandTimeoutSeconds` | `30` | предел команды |
| `DataSaver__Postgres__ConnectTimeoutSeconds` | `15` | предел подключения |
| `DataSaver__Postgres__MinPoolSize` / `MaxPoolSize` | `1` / `10` | размер пула |
| `DataSaver__Postgres__KeepAliveSeconds` | `30` | keepalive |

### Приём, повторы, остановка, health

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DataSaver__Batch__MaxSize` | `500` | пачка закрывается по размеру... |
| `DataSaver__Batch__MaxWaitMs` | `1000` | ...или по времени от первого сообщения |
| `DataSaver__PollIntervalMs` | `500` | предел ожидания одного Consume (он же период опроса группы в паузе) |
| `DataSaver__Backoff__InitialMs` | `1000` | первая пауза повтора при сбое записи |
| `DataSaver__Backoff__MaxMs` | `30000` | потолок паузы (жёсткий, после джиттера) |
| `DataSaver__Backoff__Multiplier` | `2.0` | множитель (не меньше 1) |
| `DataSaver__Backoff__JitterFraction` | `0.2` | доля джиттера, 0..1 |
| `DataSaver__Shutdown__TimeoutSeconds` | `30` | предел дописывания пачки от сигнала остановки |
| `DataSaver__Health__Port` | `8080` | порт health-endpoint |
| `DataSaver__Health__LiveStaleSeconds` | `60` | через сколько без отметки цикла liveness = 503; должен быть строго больше `Postgres__ConnectTimeoutSeconds + Postgres__CommandTimeoutSeconds` (иначе отказ запуска) |

### Коэффициенты доверия источников (временно, до появления хранилища настроек)

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `DataSaver__Trust__Default` | `100` | коэффициент доверия по умолчанию, целое 0..100 |
| `DataSaver__Trust__Source__<source>` | - | переопределение для источника, например `DataSaver__Trust__Source__skud=80` |

Допустимые значения `<source>` - из enum `source` в `docs/api/kafka-event.schema.json`. Значение `<source>` в имени
переменной **не проверяется**: опечатка молча игнорируется, и для реального источника останется `Trust__Default`
(`EnvironmentSourceTrustProvider.cs:40-41`). Имена сверяйте вручную.

Шаблон: `deploy/data-saver/.env.example`. Реальный `.env` в git не попадает.

## 3. Запуск и остановка

Каталог `deploy/data-saver`:

```bash
cp .env.example .env        # подставить Kafka, хост и базу БД
mkdir -p secrets
printf '%s' '<пароль ktm_app>' > secrets/ktm_app_password
sudo chown 1654:1654 secrets/ktm_app_password && chmod 0400 secrets/ktm_app_password   # владелец - uid пользователя контейнера
docker compose up -d --build
docker compose ps                        # STATUS должен стать "healthy" (это только liveness, см. ниже)
docker compose logs -f data-saver
```

`secrets/` и `.env` в git и в контекст сборки образа не попадают (`.gitignore`, `.dockerignore`). Файл секрета должен быть
читаем из контейнера: процесс работает не от root (пользователь `app`, uid 1654), при недоступном файле сервис не стартует
(сообщение называет путь, не содержимое). Поэтому владелец файла - uid 1654 и права `0400`. Если `chown` невозможен,
допустима альтернатива `chmod 0444`, но **тогда пароль читает любой локальный пользователь хоста** - использовать только на
изолированном dev-хосте.

Признак успешного запуска: `GET /health/ready` отвечает 200 (`{"status":"ok","reason":null}`) - то есть consumer получил
назначение партиций, брокер и БД отвечают. Статус `healthy` в `docker compose ps` для этого **недостаточен**: healthcheck
контейнера смотрит `/health/live` (п. 5), а он остаётся 200 и при недоступных БД и Kafka.

Проверка ready вручную (в образе нет curl); для liveness замените `ready` на `live`:

```bash
docker compose exec data-saver bash -c 'exec 3<>/dev/tcp/127.0.0.1/8080; printf "GET /health/ready HTTP/1.0\r\nHost: x\r\n\r\n" >&3; cat <&3'
```

**Остановка** - `docker compose stop` (SIGTERM). Сервис прекращает чтение, дописывает текущую пачку (запись в БД,
публикация отбракованных, commit offset), выходит из группы и завершается; код выхода 0. Предел - 
`DataSaver__Shutdown__TimeoutSeconds` (30 с) **от сигнала**; если БД недоступна и предел вышел, процесс выходит без
коммита, пачка придёт повторно (дубликатов в журнале не будет - их гасит уникальный ключ). Остановка посреди обработки и
`kill -9` данные не теряют и дубликатов не создают (ADR-0002, D-8).

`stop_grace_period` в `compose.yml` задан 40 с = `Shutdown__TimeoutSeconds` (30) + запас host 5 с (решение лида Q3) +
ещё 5 с запаса. Значения связаны.
**При увеличении `Shutdown__TimeoutSeconds` увеличьте и `stop_grace_period`**, иначе Docker пошлёт SIGKILL раньше
предела (данные не теряются, но остановка перестаёт быть штатной).

Обновление версии: `docker compose up -d --build` (контейнер пересоздаётся с остановкой по правилам выше).

## 4. Масштабирование копиями

Несколько копий с одним `DataSaver__Kafka__GroupId`; партиции между ними делит Kafka, общего состояния и координации нет
(ADR-0002, D-9). Предел параллелизма - число партиций `ktm.events.raw` (9): копий больше 9 смысла не имеют.

- **Static membership** (`DataSaver__Kafka__GroupInstanceId`) необязателен. Если используется, значение должно быть
  **уникальным на каждую реплику** и стабильным между перезапусками реплики (например `data-saver-1`, `data-saver-2`):
  тогда перезапуск реплики в пределах `DataSaver__Kafka__SessionTimeoutMs` (45 с) не вызывает ребаланс. Одинаковый
  `GroupInstanceId` у двух живых реплик недопустим: одну из них брокер вытеснит. Как именно поведёт себя сервис при
  вытеснении (ошибка клиента, фатальная или нет) - **уточнить** по результатам интеграционного теста 7.7.
- Каждой реплике нужен свой `.env` (или переменная `GroupInstanceId`, переопределённая на реплику) и свой порт health,
  если реплики делят сетевой стек хоста.
- Как масштабировать через compose (`docker compose up --scale` не подходит при уникальном `GroupInstanceId` и общем
  `env_file`) - **уточнить**; штатный путь на сегодня - отдельные сервисы/проекты compose со своими `.env`.
- Реплика, которой не досталось партиций (копий больше партиций), по определению проверки не получает назначения и
  отвечает на ready 503 `no_partitions_assigned` - **уточнить** на стенде.

## 5. Health и что делать при `ready = 503`

Два маршрута на порту `DataSaver__Health__Port` (внутренняя сеть, без аутентификации; тело содержит только статус и
фиксированный код причины, без адресов и текстов ошибок):

- `GET /health/live` - 200, если цикл приёма отмечался не позже `Health__LiveStaleSeconds` назад. 503 при
  `consumer_fatal_error` (фатальная ошибка клиента Kafka, процесс при этом завершается) или `loop_stalled` (цикл завис).
  **Недоступность БД и Kafka liveness не роняет** - перезапуск их не лечит.
- `GET /health/ready` - 200 только если consumer получил назначение партиций, состояние не `Degraded`, брокер Kafka
  отвечает на запрос метаданных, БД отвечает на `SELECT 1`. Проверки брокера и БД ограничены 2 с; результат пробы брокера
  кэшируется на 5 с.

**Что видно в статусе контейнера.** Healthcheck в `compose.yml` направлен на `/health/live` (дешёвая проверка без
обращения к брокеру и БД). Следствия:

- `unhealthy` в `docker compose ps` означает `loop_stalled` (цикл приёма завис) или `consumer_fatal_error`;
- недоступность БД и Kafka (`ready = 503`) в статусе контейнера **не видна**: контейнер остаётся `healthy`. Состояние ready
  смотреть напрямую (команда в п. 3, `... /health/ready`) или настроить внешний мониторинг на этот endpoint;
- Docker `unhealthy`-контейнер **сам не перезапускает**, а `restart: unless-stopped` реагирует только на выход процесса.
  При `loop_stalled` перезапускайте вручную: `docker compose restart data-saver` (данные не теряются, пачка придёт повторно).

Коды причин `ready = 503` (порядок проверки сверху вниз, возвращается первая сработавшая):

| `reason` | Что значит | Что делать |
|---|---|---|
| `no_partitions_assigned` | consumer не получил партиций: идёт старт или ребаланс, **топика `ktm.events.raw` нет в кластере**, нет связи с брокером на старте | подождать ребаланс (до `SessionTimeoutMs`); проверить, что топик создан (`infra/kafka/README.md`), `BootstrapServers` верен, брокер доступен из контейнера; смотреть лог `Kafka client error` |
| `ingestion_degraded` | запись пачки или публикация в `rejected` не удалась: партиции на паузе, цикл повторяет **ту же** пачку с нарастающей паузой (1 с x 2^n, потолок 30 с). Данные не теряются | в логе искать `Error`/`Warning` цикла (классы ошибок: п. ниже); устранить причину - сервис восстановится сам, перезапуск не нужен |
| `broker_unavailable` | брокер не ответил на запрос метаданных за 2 с | проверить доступность `BootstrapServers`, сеть/UFW (на dev 9092 открыт для WireGuard и docker-сети, `infra/kafka/README.md`), состояние брокера |
| `database_unavailable` | БД не ответила на `SELECT 1` за 2 с | проверить доступность БД, пароль, `SslMode`, pg_hba; пока БД недоступна, цикл тоже в `Degraded` |

Классы ошибок записи (ADR-0002, D-7), по которым цикл уходит в `Degraded`:

- **временные** (разрыв соединения, таймаут команды, `57P01-57P03`, `53xxx`, `40001`, `40P01`, сбой публикации в Kafka) -
  пауза и повтор без ограничения числа попыток; восстановление автоматическое;
- **конфигурационные** (`42501` нет прав, `42P01` нет таблицы, ошибка аутентификации) - то же поведение, но на каждой
  попытке лог `Error` и сообщение `Administrator notification: fatal configuration/permission write error ...` (пока
  канала уведомлений нет, это только лог и счётчик); требуют вмешательства: права роли `ktm_app`, применённые миграции,
  пароль;
- **данные** (`22xxx`, `23xxx`) - решение за портом C6 (`IWriteFailureHandler`), который в этой версии не реализован;
  заглушка всегда повторяет пачку, то есть партиция блокируется, пока причина не устранена; данные остаются в Kafka
  (хранение 7 суток). Лог `Error` на каждой попытке.

Недоступность БД - технический сбой сервиса, а не ухудшение оценки сотрудника: по духу `docs/07`, п. 7.2 (при сбое
источника - технический инцидент, а не негативная оценка); прямого требования именно про недоступность БД в `docs/07`
нет - **уточнить**.

`ready = 503` (БД, Kafka, `ingestion_degraded`) в `docker compose ps` не отражается, см. выше; Docker перезапуск для него не
делает, и перезапуск не нужен (сервис восстанавливается сам).

## 6. Топик `ktm.events.rejected`

Сюда попадают сообщения, не прошедшие проверку формата (они не попадают в журнал и не влияют на оценки; это технический
инцидент интеграции, а не ухудшение оценки сотрудника): offset таких сообщений коммитится, партицию они не блокируют.
Тело - оригинальное, ключ сохраняется.

Заголовки (значения - UTF-8):

| Заголовок | Содержимое |
|---|---|
| `ktm-reject-reason` | `malformed_json`, `unknown_schema_version`, `invalid_schema` |
| `ktm-reject-detail` | подробности причины |
| `ktm-source-topic`, `ktm-source-partition`, `ktm-source-offset` | координаты исходного сообщения |

Чтение (пример для брокера из контейнера `apache/kafka`; путь к утилитам и способ доступа - **уточнить** на стенде):

```bash
kafka-console-consumer.sh --bootstrap-server <kafka-host>:9092 --topic ktm.events.rejected \
  --from-beginning --property print.headers=true --property print.key=true
```

Хранение - 30 суток. Что делать:

- `unknown_schema_version`: адаптер перешёл на версию формата, которую сервис ещё не поддерживает, - нужен релиз Data
  Saver (то же при новом значении `source`, `docs/api/kafka-event-format.md`, п. 3);
- `invalid_schema` / `malformed_json`: ошибка адаптера-источника, передать его владельцам вместе с `ktm-source-*`;
- повторная обработка после исправления - ручная операция администратора (автоматического повтора нет); потребитель
  `rejected` должен быть идемпотентен по `topic/partition/offset` исходного сообщения: при частичном сбое публикации
  пачки возможны дубли в `rejected` (принято, at-least-once).

В логе отбраковка пишется с уровнем `Error` и полями `Topic`, `Partition`, `Offset`, `Source`, `SourceEventId` (для
неразобранного сообщения `Source` и `SourceEventId` = `null`, решение лида Q2); тело сообщения на уровне Information в лог
не попадает.

## 7. Лаг группы и алерты

Сервис метрик наружу пока не отдаёт: метрики `System.Diagnostics.Metrics` (meter `Ktm.DataSaver`, инструменты
`ktm.datasaver.*`) создаются, но экспорт (Prometheus/OTel) и канал уведомления администратора - вне объёма этого этапа
(`tasks.md`, 10.5). Поэтому алерт на лаг настраивается **снаружи**, по состоянию группы в Kafka:

```bash
kafka-consumer-groups.sh --bootstrap-server <kafka-host>:9092 --describe --group ktm-data-saver
```

Столбец `LAG` по партициям `ktm.events.raw`. Алертить нужно:

- рост лага при `ready = 200` (сервис не успевает; решение - масштабирование копиями, п. 4, и замер из
  `docs/test-protocols/data-saver-throughput.md` - будет создан в задаче 8);
- ненулевой и растущий лаг при `ready = 503` / `ingestion_degraded` (сервис не пишет);
- жёсткий предел: сообщения в `ktm.events.raw` хранятся 7 суток, лаг в часах не должен приближаться к этому сроку (иначе
  непрочитанные сообщения будут удалены брокером).

Пороговые значения лага (число сообщений / время) в ТЗ и ADR не заданы - **уточнить** у лида.

## 8. Коды выхода процесса

| Код | Когда |
|---|---|
| `0` | штатная остановка по SIGTERM |
| `70` | фатальная ошибка клиента Kafka (`Error.IsFatal`): ретраи не помогают, процесс завершается после штатной остановки цикла; перед этим `/health/live` отвечает 503 `consumer_fatal_error`. Выход процесса перезапускает `restart: unless-stopped` |
| `1` | ошибка цикла приёма, перехваченная и залогированная (`DataSaverWorker.cs:39-44`): лог `Critical` `Ingestion loop failed; the process is terminating`, затем процесс завершается этим кодом |

Отказ запуска из-за ошибки настроек - необработанное исключение host с текстом вида
`OptionsValidationException: DataSaver__Kafka__BootstrapServers is required.` в логе. Код выхода в этом случае при
проверке образа на Docker 29 / Linux контейнере получился `139` (причина не подтверждена - **уточнить**; код сервисом
не задаётся); контракта на этот код нет, ориентируйтесь на текст в логе.

При любом ненулевом выходе сначала смотрите лог последних строк `Critical` перед завершением.

## 9. Принятые решения лида (на которые опирается эксплуатация)

- **Q1.** Ограничение `Commit`/`Close`/`Flush` общим бюджетом остановки отложено: теоретически блокирующий `Commit` при
  зависшем брокере может удлинить остановку сверх `Shutdown__TimeoutSeconds`; тогда Docker остановит процесс по
  `stop_grace_period` (SIGKILL), данные не теряются, пачка придёт повторно.
- **Q2.** Для неразобранного сообщения `Source` и `SourceEventId` в логе отбраковки = `null`.
- **Q3.** `stop_grace_period` >= `Shutdown__TimeoutSeconds` + 5 с (запас host); в `compose.yml` принято 40 с (30 + 5 + 5 запас).
- Прочие решения, на которые стоит оглянуться: `DataSaver__Postgres__SslMode` по умолчанию `Require`; потолок backoff
  жёсткий (30 с после джиттера); `Resume` партиций выполняется перед commit offset (ADR-0002, D-7).

## 10. Известные ограничения

- Движок правил отбраковки, топик отбракованных правилами, содержимое проверок C5 и обработка ошибок данных C6 (в т.ч.
  `ktm.events.dlq`) - вне объёма этой версии (`tasks.md`, группа 10).
- Коэффициент доверия источника задаётся переменными окружения (п. 2) и применяется после перезапуска.
- При недоступном Kafka шум в логе большой: порядка 1700 строк в минуту через `ILogger` (`Kafka client error`) плюс
  нативные строки librdkafka в stderr (в проверке со старта контейнера с несуществующим хостом). Лог контейнера в
  `compose.yml` ограничен `json-file`, `max-size: 10m`, `max-file: 3` (до 30 МБ): при флуде старые строки вытесняются,
  то есть более ранние `Critical`/`Error` могут пропасть из `docker compose logs` - при разборе инцидента снимайте лог
  сразу. Если логи собирает внешняя система, закладывайте этот объём.

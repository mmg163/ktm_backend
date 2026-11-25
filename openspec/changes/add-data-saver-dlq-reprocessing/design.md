# Design

## Context

Мотивация - `proposal.md`, требования - `specs/data-saver/{dead-letter,reprocessing}/spec.md` и дельты `event-ingestion`, `ingestion-pipeline`,
решения и обоснование - `docs/adr/0006-data-saver-dlq-and-reprocessing.md` (ссылки Р-n ниже - на реализацию, п. X - на ADR). Здесь - как решение встраивается в код,
порядок работ и риски.

Исходное состояние (проверено по коду `main` 2274664):

- `IngestionLoop.ProcessBatchAsync`: `ClassifyStageAsync` (исключение стадии конвейера - Error-лог, Degraded, повтор **всей пачки без предела**) -> `WriteStageAsync(results, deadLettered, receivedAt)` -> `PublishStageAsync` -> commit. В `WriteStageAsync`
  исключение классифицируется `WriteErrorClassifier` (`Transient` / `FatalConfig` / `Data`); для `Data` вызывается `IWriteFailureHandler`; `DeadLetter` принимается
  `TryAcceptDeadLetter` (сообщения из пачки, набор вырос) и сообщения исключаются из повторной записи множеством `deadLettered`; иначе - `EnterDegraded` + `BackoffWaitAsync`.
  `_retryCount` общий для стадий и сбрасывается после успешной пачки; `receivedAt` один на пачку.
- `BlockingWriteFailureHandler` всегда `Retry`: партиция блокируется. Тесты поведения порта: `IngestionLoopTests` (`DataError_DeadLettered...`, `DeadLetterWithoutProgress...`,
  `DeadLetterOfMessagesOutsideTheBatch...`, `FailureHandlerCancelledByDeadline...`).
- `KafkaRejectionPublisher`: idempotent producer, `acks=all`, ждёт подтверждения всех `ProduceAsync`, результат `RejectionPublishResult` (отказ - значение).
- `ConsumedMessage(Topic, Partition, Offset, Key, Body, Timestamp)` - заголовков нет; `Timestamp` Kafka доступен.
- `AddDataSaver` - единый composition root под web-хост (`Sdk.Web`), `Program.cs` сразу строит `WebApplication`.
- `LiveStale`-валидатор учитывает три блокирующих ожидания; `failureHandler` и Commit - исключения (комментарий в `IngestionLoop`).

## Goals / Non-Goals

**Goals:** бюджет попыток для любой ошибки, привязанной к сообщению (разбор, C5, правила, запись), и изоляция ядовитого сообщения без блокировки партиции; `ktm.events.dlq` с подтверждением до commit и описанным форматом; различие `rejected`/`dlq`;
повторная обработка `dlq`, `rejected`, `raw` за период через HTTP admin-API (асинхронные задачи) и админку `/admin/ui/`, расширяемая область `/admin/v1/<раздел>/`, идемпотентная, безопасная для рабочего потока; метрики, логи, документация для оператора; тесты на каждую
ветвь ядра и приёмка по критериям лида (admin API, Swagger/OpenAPI и UI исключены из тестового объёма решением пользователя от 2026-11-25); сохранение NFR (500 соб/с, 5 минут).

**Non-Goals:** `dlq` для ошибок инфраструктуры (недоступность БД, сеть, таймауты, права; ADR-0006 п. 2.1, В-1); режим командной строки `reprocess`; аутентификация/роли и отдельный порт admin (решение лида: сетевая защита, ADR-0006 п. 2.6); расписание; аудит-таблица запусков;
хранение задач между перезапусками (состояние в памяти), чекпоинт прерванной задачи (идемпотентность вместо него); другие разделы админки (кроме `reprocessing`); публикация результатов повторной обработки обратно в `rejected`/`dlq`; «сухой» запуск (dry-run); окно по `occurredAt`;
автоматическая повторная обработка `dlq`; экспорт метрик (Prometheus/OTel); запасное хранилище при недоступном `dlq`; изменения схемы БД, прав, compose, `Dockerfile`; изменения основного REST API и контракта с фронтендом.

## Decisions

### Р-1. Структура в коде

```
Ktm.DataSaver.Core/
    Resilience/WriteErrorClassifier      +класс Unexpected; порядок: инфраструктура -> Data (22/23) -> Unexpected (п. 2.2 ADR)
    Resilience/DlqPolicy                 record DlqPolicy(Enabled, MaxAttempts, BreakerThreshold)
    Resilience/PoisonIsolator            деление пополам над IEventStore: Isolate(items, receivedAt, onWritten, touch, ct) -> IsolationResult
    Resilience/DlqBreaker                счётчик подряд ушедших без успешной записи; Open/Close
    Pipeline/PipelineStageException      (Stage: parse|validation|rules, inner; стадия `write` - из цикла записи; отдельной стадии mapping нет: JSON->событие = parse, событие->SQL = write); IngestionPipeline оборачивает неожиданное исключение стадии
    Ingestion/IDeadLetterPublisher       порт: PublishAsync(IReadOnlyList<DeadLetteredMessage>, ct) -> DeadLetterPublishResult (отказ - значение)
    Ingestion/IngestionLoop              +бюджет, изоляция, повтор по сообщению для стадий конвейера, стадия публикации в dlq, предохранитель
    Reprocessing/ReprocessRequest|Report|Runner|IReplaySource|ReprocessJobManager   независимы от Kestrel и от IngestionLoop
Ktm.DataSaver.Infrastructure/
    Kafka/KafkaDeadLetterPublisher       заголовки ktm-dlq-* (+stage, exception-type), отдельный idempotent producer
    Kafka/KafkaReplaySource              assign + OffsetsForTimes/seek, без группы и commit; чтение до снятых конечных offset
    Observability/ObservedDeadLetterPublisher   метрика dead_lettered{reason,stage}, лог Error (по образцу ObservedRejectionPublisher)
Ktm.DataSaver/
    Configuration/DlqOptions, ReprocessOptions, AdminOptions  (+ DataSaverChecks); KafkaOptions.DlqTopic
    Admin/IAdminSection                  MapEndpoints(RouteGroupBuilder); реестр разделов; GET /admin/v1/sections
    Admin/ReprocessingSection            тонкий слой: DTO, валидация -> ProblemDetails, вызовы ReprocessJobManager
    Admin/AdminAudit                     структурный лог вызовов (JobId, RemoteIp, X-Requested-By, параметры)
    Admin/Ui/                            встроенные ресурсы: index.html, app.js, sections/index.js, sections/reprocessing.js, app.css; раздача /admin/ui/ с CSP
```

Граница уровней: Core не знает о Kafka/Npgsql/ASP.NET (порты), `Reprocessing` в Core не ссылается на `IngestionLoop`; уровень 1 по-прежнему не оценивает данные.
Admin-эндпоинты - тонкий слой над сервисом Core; бизнес-логики в обработчиках маршрутов нет.

### Р-2. Классификация (п. 2.1, 2.2 ADR)

`WriteErrorClass` получает член `Unexpected` (обрабатывается как `Data`: бюджет -> изоляция -> `dlq`, но порт `IWriteFailureHandler` не вызывается и причина другая). Порядок `Classify`: `AggregateException` разворачивается;
сначала признаки инфраструктуры (сеть/IO/таймаут/`NpgsqlException` без `PostgresException`/исчерпание пула/любой SQLSTATE кроме `22/23`) -> `Transient` или `FatalConfig`; затем SQLSTATE `22/23` -> `Data`; **затем всё остальное -> `Unexpected`**
(в записи с типом из набора `InvalidCastException`/`FormatException`/`OverflowException`/`ArgumentOutOfRangeException` - причина `write_value_error`, иначе `unexpected_error`). Прежнее правило «неизвестное -> Transient» **удалено**.
`PipelineStageException` классифицируется по `inner` тем же порядком; `Stage` берётся из обёртки (`write` - для стадии записи). Тест: таблица классификатора (в т.ч. неожиданное исключение валидатора/правил -> `Unexpected`, `TimeoutException`/`SocketException`/`NpgsqlException` -> `Transient`, пул -> `Transient`) и
реальные исключения Npgsql на интеграционной БД (tasks 1.1).

### Р-3. Изменения `WriteStageAsync` (п. 2.3)

Локальное состояние стадии: `dataFailures` (int), `written` (HashSet<ConsumedMessage> по ссылке, как `deadLettered`), `poison` (список с причиной) - живут на время стадии, не между пачками.

```
catch (Exception ex):
    class = Classify(ex)
    if class != Data:  (Transient | FatalConfig) -> существующая ветка без изменений
    else:
        dataFailures++
        handler.HandleAsync(...)                      // как сейчас; принятый DeadLetter -> deadLettered, continue (приоритет над бюджетом)
        if !policy.Enabled or dataFailures < policy.MaxAttempts: EnterDegraded; Backoff; continue
        // бюджет исчерпан
        iso = await isolator.IsolateAsync(items, receivedAt, ...)    // дописывает written; poison - в список
        if iso.Interrupted (ошибка инфраструктуры / отмена): обычный путь Degraded+Backoff без нового расхода dataFailures; continue
        breaker.Register(poison.Count)  // Open -> NotifyFatalConfig-подобный путь, poison не публикуются, повтор без предела
        publish poison -> IDeadLetterPublisher (стадия внутри WriteStage: цикл «опубликовать/backoff» как PublishStageAsync, отдельный Retry("dlq_publish"))
        deadLettered.UnionWith(poison); continue         // следующий проход: items = results \ deadLettered \ written; пусто -> return true
```

- Строки, записанные изоляцией, исключаются из повторной записи (`written`), поэтому `BatchWritten` метрики не раздувает `duplicates` (изолятор вызывает уже оборачивающий `MeteredEventStore`; итоговая
  повторная запись остатка идёт только если что-то осталось).
- Состояние `poison` переживает backoff публикации (ничего не пересчитывается). Отозванные партиции: перед публикацией poison фильтруется по актуальному `results` (`DrainAssignmentChanges`).
- `Touch()` - после каждой записи изолятора и после подтверждения публикации в `dlq`; в `LiveStale`-проверку добавляется `ProducerMessageTimeoutMs` (тот же параметр), новых настроек нет.
- Остановка (`IoToken`): отмена во время изоляции/публикации - `return false` (без commit), как для записи сегодня.
- **Стадии конвейера (`ClassifyStageAsync`).** Вместо «повтор всей пачки без предела» обработка идёт поштучно: `try { pipeline.Process(message) } catch (Exception ex)`; `Classify(ex)`: инфраструктура - прежний путь (Error-лог, Degraded, backoff без предела,
  перезапуск стадии пачки); `Unexpected` - счётчик попыток **этого сообщения** (`stageFailures[message]`), при `< MaxAttempts` - Degraded, `BackoffWaitAsync`, повтор `Process` для этого сообщения; при `== MaxAttempts` сообщение получает результат
  `PipelineResult.Poison(stage, exceptionType)` (не `Rejected` и не `Accepted`), исключается из записи и складывается в общий список `poison` пачки вместе с ядовитыми стадии записи. Публикация `poison` - один шаг пачки (Р-3 выше) **до commit**;
  предохранитель считает и их (до записи пачки: `BreakerThreshold` и более стадийных ядовитых в одной пачке откроют предохранитель, даже если рядом есть валидные - ограничение m6, ADR п. 2.3 п. 6). При открытом предохранителе стадийный сбой после `MaxAttempts` попыток **не** уходит в `dlq`: повтор без предела, Degraded,
  `NotifyDeadLetterBreakerOpen` на каждой попытке (решение лида Q-4, вариант A); перезапуск сбрасывает предохранитель (состояние в памяти). Повтор по сообщению не пересчитывает уже обработанные сообщения пачки. `Enabled=false` - прежнее поведение (повтор пачки без предела). Метрика повторов - `retries{stage="pipeline"}`.
  Лог ошибки - как сейчас: `ErrorType` без текста исключения.
- Лог: `LogWriteFailure` без изменений; новые записи Error на сообщение в `dlq` из `ObservedDeadLetterPublisher` (после подтверждения).

### Р-4. `PoisonIsolator`

Чистый компонент над портом `IEventStore` и `WriteErrorClassifier` (ядовитое - класс `Data` или `Unexpected`, инфраструктура - прерывание); рекурсия через явный стек (без рекурсии вызовов на 500 элементов - допустимо рекурсивно, глубина log2 N):

```
Isolate(list):
   if list.Count == 1: try write -> ok => written; Data => poison(one); not-Data => Interrupted
   else: split into A, B; for each part: try write part -> ok => written; Data => Isolate(part); not-Data => Interrupted(сохранить остаток)
```

Результат: `Written`, `Poison` (сообщение + исключение для заголовков: SQLSTATE, имя ограничения/таблицы через `IDbErrorLocation`, тип исключения, стадия `write`), `Remaining` (при прерывании). Не пишет в лог тел; число записей
считается и проверяется тестом (`<= 2*ceil(log2 N)+1` при одном ядовитом). Сообщения `received_at` - переданное значение.

### Р-5. Публикация в DLQ

`KafkaDeadLetterPublisher` повторяет структуру `KafkaRejectionPublisher` (подтверждение каждого `ProduceAsync`, результат-значение, `NonTransient`-набор с Error-логом); общая часть
(сборка `ProducerConfig`, разбор отказа) выносится в внутренний помощник без изменения поведения `KafkaRejectionPublisher` (его тесты зелёные до и после). Заголовки и усечения - п. 2.4 (в т.ч. `ktm-dlq-stage` и `ktm-dlq-exception-type` из `DeadLetterInfo(Stage, ExceptionType, ...)`, тип - полное имя без текста и стека);
`detail` строится из `IDbErrorLocation`/SQLSTATE/типа, **не из `Exception.Message`**. Producer создаётся при старте цикла (не лениво в горячем пути), `Dispose` вместе с остальным в `DataSaverWorker`
(порядок: consumer, затем оба producer-а). Заголовок `ktm-source-timestamp` добавляется и в `KafkaRejectionPublisher.ToMessage` (аддитивно; обновляются тесты заголовков).

### Р-6. `KafkaReplaySource`

Один consumer `Assign` (не `Subscribe`): `group.id = <GroupId>-reprocess` (требуется librdkafka, но offset никогда не коммитится: `EnableAutoCommit=false`, `Commit` не вызывается), `AutoOffsetReset=Earliest`.
Старт: метаданные топика -> список партиций; `QueryWatermarkOffsets` для конечных offset (снимок); при заданном `from` - `OffsetsForTimes(from)` (проверка retention - чистая функция по low/high watermark и времени первого сохранённого сообщения партиции, правило в ADR п. 2.7; отсутствующий топик - `ReplaySourceException`, не ошибка аргумента; открытие ограничено общим временем и токеном); `to` - `OffsetsForTimes(to)` как исключающая граница (partition читается до `min(end, offsetFor(to)) - 1`). Чтение партиций последовательно или конкурентно без
гарантии порядка между партициями (порядок не важен: D-4). Конец - когда все партиции дочитаны до границы. Выдаёт `ReplayMessage(ConsumedMessage Original, string? Reason, ...)`: заголовки читаются здесь
(`ktm-source-*`, `ktm-dlq-reason`/`ktm-reject-reason`), `ConsumedMessage` строится с исходными координатами и Kafka-`Timestamp`. Порт `IReplaySource` в Core позволяет тестировать раннер без Kafka.

### Р-7. `ReprocessRunner` и `ReprocessJobManager`

`ReprocessJobManager` (Core, singleton): `Start(ReprocessRequest) -> StartResult(Started(job) | Conflict(activeJobId))` с атомарной проверкой «одна задача» (`Interlocked`/lock), `Get(id)`, `List()` (10 последних, кольцевой буфер в памяти),
`Cancel(id)`; задача исполняется `Task` под токеном, связанным с токеном остановки хоста (остановка сервиса отменяет задачу; коммитов нет). Состояние задачи - потокобезопасный снимок (`ReprocessJobSnapshot`): `state`, `outcome`, `error{code}`,
параметры, счётчики, прогресс по партициям, времена UTC; отчёт - список координат с потолком (`MaxReportEntries`, 100000; превышение - `truncated = true`).

`ReprocessRunner` (Core): источник -> фильтр `reason` -> пачка (`Batcher`/цикл по `MaxSize`) -> `IngestionPipeline.Process` (неожиданное исключение стадии - `Unexpected`: бюджет `MaxAttempts` попыток, затем в отчёт со стадией, без публикации) -> принятые
пишутся `IEventStore` (общая обёртка устойчивости: классификатор, `BackoffPolicy`, ожидание не дольше `MaxUnavailableSeconds`; Data/Unexpected - `PoisonIsolator`, ядовитые идут в отчёт, а не в `dlq`), отбракованные - в отчёт.
Ограничение скорости - токен-бакет по `TimeProvider` (тестируется `FakeTimeProvider`); `maxRate` проверяется на `1..MaxRateLimit`. Счётчики: `read`, `skippedByFilter`, `written`, `duplicates` (число вставленных строк берётся из
`IBatchWriter.WriteBatchAsync -> EventWriteResult`; для задачи - свой декоратор `CountingEventStore` в Infrastructure, порт `IEventStore` не меняется), `rejectedAgain{reason}`, `failedAgain`. Итог - `outcome` по таблице ADR п. 2.6. Тела в отчёт не входят.
Набор правил - текущий снимок работающего сервиса, берётся на каждое сообщение (`RuleSnapshotProvider`), на старте задачи не фиксируется (ADR R-10), отдельной загрузки нет.

Общий с циклом код устойчивости (классификация + backoff + изоляция) не копируется: используются `WriteErrorClassifier`, `BackoffPolicy`, `PoisonIsolator`; два потребителя, цикл остаётся владельцем паузы партиций.

### Р-8. Admin-API и админка в существующем web-хосте

Отдельного слушателя нет: маршруты `/admin/...` регистрируются в том же `WebApplication` и на том же Kestrel-порту, что `/health/*` (`DataSaver__Health__Port`); аутентификации и ключа нет (решение лида; сетевая защита - требование к деплою).
`MapAdmin(app)` создаёт `RouteGroupBuilder` `/admin/v1`, регистрирует разделы из реестра `IAdminSection` (первый - `ReprocessingSection`), `GET /admin/v1/sections` и общие фильтры: единый обработчик ошибок -> `application/problem+json`
(`AddProblemDetails` + `IExceptionHandler` для группы; 5xx без текста исключения), `X-Requested-By` -> аудит, метаданные OpenAPI (тег = раздел, `ProducesProblem`, `WithSummary`). Валидация запроса - в DTO/валидаторе раздела, ответ `400` через `Results.ValidationProblem`.
OpenAPI: встроенный `Microsoft.AspNetCore.OpenApi` (MIT) для документа `/admin/openapi/v1.json` (только группа `/admin`; health в документ не входит), Swagger UI - пакет с MIT/Apache-лицензией на `/admin/swagger`, флаг `DataSaver__Admin__SwaggerEnabled` (true);
лицензии бинарных релизов проверяются (tasks 5.4). Файл `docs/api/data-saver-admin.openapi.json` - снимок документа (сверяется при ревью; не вручную).

Админка: `Admin/Ui` - встроенные ресурсы (`EmbeddedResource`) и маршрут `/admin/ui/{**path}` с `ContentTypeProvider`, заголовки CSP/nosniff/no-store/no-referrer, без сборки и без внешних зависимостей. Каркас `app.js` читает `/admin/v1/sections`, подключает модули
из реестра `sections/index.js` (`{ id, title, render(container, api) }`; unknown section id из API без модуля - пункт скрыт); `api.js` - обёртка `fetch` с разбором ProblemDetails; вывод - только `textContent`; данные в браузере не сохраняются; подтверждение запуска и отмены - модальный диалог
(`<dialog>`) с итоговыми параметрами. Опрос прогресса - раз в 2 с, пока `state` активный, остановка при уходе со страницы.

Правки composition root минимальны: `AddDataSaver` регистрирует `ReprocessJobManager`, `KafkaReplaySource`, `CountingEventStore`, разделы admin; рефакторинг на `AddDataSaverCore` **не нужен** (сервис не разделяется на два режима).
Так как задача идёт в процессе сервиса, `ReprocessJobManager` получает собственный `IEventStore` и `IngestionPipeline` из того же контейнера, но не `IngestionLoop`.

### Р-9. Тесты

- Unit (`Ktm.DataSaver.Tests`, `FakeTimeProvider`, fake-порты из `IngestionHarness`): классификатор (таблица, включая `Unexpected` и инфраструктурные исключения), `PoisonIsolator` (число записей, один/несколько/все ядовитые, прерывание), бюджет в цикле для записи **и для стадий конвейера**
  (неожиданное исключение в валидаторе, в правилах, при разборе, ошибка значения при записи (`write_value_error`): повторы `MaxAttempts`, затем `dlq` со стадией; временное исключение - обработано без `dlq`; инфраструктурное - повтор без предела), публикация в dlq и commit-порядок, недоступный dlq,
  предохранитель (в т.ч. на исключениях стадий), `Enabled=false`, ребаланс/остановка при изоляции, `ToMessage` заголовков dlq (в т.ч. `ktm-dlq-stage`, `ktm-dlq-exception-type`), `ReprocessJobManager` (одна задача, 409, отмена, история, потокобезопасность), раннер (счётчики, дубли, фильтр, скорость, недоступная БД, `outcome`), валидация запросов, опции.
- Интеграционные (`Ktm.DataSaver.IntegrationTests`, Testcontainers Kafka + Timescale, уникальные топики на тест; повторная обработка запускается через Core `ReprocessJobManager`/`ReprocessRunner`, без HTTP-слоя): ядовитое сообщение для записи задаётся
  **тестовым CHECK-ограничением** (`ALTER TABLE ktm.events ADD CONSTRAINT ... CHECK (source_event_id <> 'poison')`; под ролью-владельцем, как `DatabaseProvisioner`) - детерминированное `23514`; «исправление причины» - `DROP CONSTRAINT`. Ядовитое для стадии конвейера задаётся
  **тестовым правилом/валидатором, бросающим исключение на маркере** (подмена зарегистрированной стадии в тестовом хосте) - «починка» - отключение подмены. Поведение при недоступном `dlq`: публикация в несуществующий топик (создание запрещено `AllowAutoCreateTopics=false`), затем создание топика.
- Решение пользователя от 2026-11-25: admin-область (HTTP-эндпоинты, Swagger/OpenAPI и UI) не тестируется; новые тесты и ручные прогоны исключены. Существующие тесты сохраняются, их запуск не блокирует группу 5. Функциональность сверяет reviewer по коду и контрактным артефактам. Проверки ядра повторной обработки, лицензий зависимостей и сетевой изоляции развёртывания сохраняются.
- `Perf` (вручную): 500 соб/с с долей 0,1 % ядовитых - пропускная способность и задержка; параллельная задача с `maxRate 200` (tasks 6.7).

## Risks / Trade-offs

- **R-1. Ложная классификация** (ADR R-1): инфраструктурная ошибка, не распознанная классификатором, или баг кода с исключением на каждом сообщении уведёт сообщения в `dlq`; смягчение - порядок «сначала инфраструктура», тест на реальных исключениях, предохранитель, повторная обработка `dlq` после починки (заголовки стадии и типа).
- **R-2. Стоимость ошибки Data**: при доле ядовитых > 0,1 % паузы `MaxAttempts - 1` (по умолчанию 3 с) на каждого заметно режут пропускную способность; смягчение - `MaxAttempts` настраивается, замер 6.7. Отдельная проблема - `Degraded`/`ready=503` на каждое ядовитое сообщение (вспышки на мониторинге): принято, решение отделять «короткое Degraded» от аварийного - вне объёма.
- **R-3. Рефакторинг composition root** затрагивает общий код сервиса: защита - существующие тесты хоста и отдельная группа задач 5.1 до остальных.
- **R-4. `IEventStore.WriteAsync` не возвращает число вставленных**: раннеру нужны `written`/`duplicates`; порт не расширяется, число берёт декоратор над `IBatchWriter` (4.2). Побочно: `retries{stage=write}` считает и неудачные записи изоляции (это отказы записи, допустимо).
- **R-5. Окно по Kafka-timestamp** (ADR R-4): документировано, в отчёте печатаются фактические границы offset/время по партициям.
- **R-6. Повторная обработка `raw` с изменённым набором правил** отбракует то, что раньше писалось, но не удалит записанное (журнал неизменяем); в отчёте - `rejectedAgain`; описано в руководстве.
- **R-7. Долгая задача**: задача может идти часами при `maxRate 200` на большом `raw` (7 суток = десятки миллионов). Оценка времени входит в статус задачи (по снятым offset и скорости); отмена - `POST .../cancel`. Исполнение в процессе сервиса делит пул соединений и память с рабочим потоком: ограничено `MaxRateLimit` и одной задачей, замер - 6.7.
- **R-8. Утечка ПДн**: `dlq`/`rejected` содержат исходное тело; отчёт, статус, логи - без тел (тест на отсутствие маркера из тела).
- **R-9. `/admin` без аутентификации (принято лидом)**: любой, кто достучался до порта сервиса, может запускать повторную обработку; защита сетевая, ingress не публикует `/admin` - требование к деплою и пункт протокола приёмки (ADR-0006 R-8).
- **R-10. Потеря состояния задач при перезапуске**: допустимо (идемпотентность), документируется.
- **R-11. Админка**: XSS через отчёт - CSP, `textContent`, отчёт без тел.

## Migration Plan

1. Выкладка кода без включённых изменений схемы; `migrate` не нужен. Топик `ktm.events.dlq` на dev уже есть; проверить `kafka-topics --describe` (3 партиции, 30 суток).
2. Выставить `DataSaver__Kafka__DlqTopic` (если не умолчание) и при желании `DataSaver__Dlq__*` в `.env`; перезапуск сервиса.
3. Проверка на dev-стенде: подать ядовитое сообщение (тестовый CHECK не нужен: например `source_event_id` длиннее лимита колонки, если допустим схемой; уточнить) - убедиться, что оно в `dlq` с `ktm-dlq-stage`, остальные записаны; проверить повторную обработку `dlq` через Core manager/runner в диагностическом запуске с реальными Kafka/Postgres (без добавления нового CLI-режима). Проверить, что `/admin` не опубликован наружу (пункт протокола).
4. Откат: `DataSaver__Dlq__Enabled=false` (перезапуск) - прежнее поведение; admin-API остаётся безвредным (задачи не запускаются сами); отключить Swagger - `DataSaver__Admin__SwaggerEnabled=false`.

## Open Questions

Решения лида (ADR-0006, разделы 8-9): В-1...В-11 приняты, вопросов по защите и размещению админки нет. Дополнительно:

- Способ создать ядовитое сообщение на dev-стенде без изменения схемы (для протокола приёмки): нужен вариант, при котором БД отвергает значение, а декодер пропускает (проверить по `Схема_БД_ER.md`).
- Подтвердить владельцу контура, что ingress/периметр не публикует `/admin` (требование к деплою, R-9).
- Доступ на запись в `ktm.events.dlq` для сервисной учётной записи в проде (ACL): вопрос владельцу контура (`add-infrastructure-topology`).
- Регламент разбора `dlq` (кто, как часто, срок 30 суток) и адресат алерта на `dead_lettered` - решает эксплуатация/лид.
- Нужна ли таблица аудита запусков повторной обработки до появления ролевой модели (В-6); сейчас - структурный лог без персональной идентификации.

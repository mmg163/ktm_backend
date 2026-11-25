# Задача для developer: исправление замечаний ревью группы 4 (review-fixes-2)

Автор: teamlead. Основание: ревью группы 4 (`Core/Reprocessing`, `Infrastructure/Kafka/KafkaReplaySource.cs`), ADR-0006 пп. 2.6-2.8 (редакция от review-fixes-2), `specs/data-saver/reprocessing/spec.md`.
Ветка: `feature/add-data-saver-dlq-reprocessing`. Не коммитить и не пушить до повторного ревью. Чекбоксы 4.2-4.4 в `tasks.md` не отмечать, пока `reviewer` не снимет Major.
Правила: тесты - только на поведение кода (не на умолчания конфигурации, csproj, документы); только бесплатные библиотеки, новых пакетов нет; запуск тестов - `$run-tests`.
Docker на машине разработки недоступен: интеграционные тесты пишутся, помечаются теми же трейтами, что соседние в `KafkaReplaySourceTests`, и перечисляются в отчёте как «не выполнены, нужен Docker».
Спеки, ADR, design, tasks правит только teamlead; developer их не трогает. **Документы по этой задаче уже поправлены teamlead** (ADR-0006 пп. 2.6, 2.7, 2.8, R-10, R-11, раздел 9; spec `reprocessing`; design Р-6, Р-7) - код должен соответствовать им.

## Параллельная работа: группа 5 (другой developer)

Группа 5 делает `Ktm.DataSaver/Admin/**`, DI (`DataSaverServiceCollectionExtensions.cs`), опции, `Program.cs`, `Directory.Packages.props`, `docs/api/*`, тесты `Ktm.DataSaver.Tests/Admin` и `Host`. **Эти файлы и каталоги не трогать.** Чтобы группа 5 не сломалась, сохраняются без изменения:

- `ReprocessJobManager.StartAsync(ReprocessRequest, CancellationToken) -> ValueTask<StartResult>`; `StartResult`, `StartStatus` (`Started/Conflict/Invalid/SourceUnavailable`), фабрики и свойства (`Job`, `ActiveJobId`, `Errors`); `Get`, `List`, `Cancel`, `GetReport`, `WhenFinishedAsync`, `StopAsync`, `HistorySize`;
- конструкторы `ReprocessJobManager`, `ReprocessRunner`, `KafkaReplaySource`, `ReprocessSettings`, `ReprocessRequestValidator.Validate(request, maxRateLimit)` и `TryParseUtc`;
- `ReprocessErrorCodes` (новых кодов нет: FatalConfig -> `internal`), `ReprocessJobSnapshot`, `ReprocessCounters`, `ReprocessReportEntry`, `IReplaySource`/`IReplayReader`/`ReplayOpenResult`.

Разрешено и совместимо: сделать `ReprocessRunner` не `sealed` и `RunAsync` `virtual` (п. m14-б); новые `internal` типы; новые поля у внутренних классов. После правок проект `Ktm.DataSaver` должен собираться без изменений в нём; если сборка группы 5 в рабочей копии временно красная из-за их незавершённой работы - это не ваш дефект, проверять сборкой `Ktm.DataSaver.Core`, `Ktm.DataSaver.Infrastructure`, `Ktm.DataSaver.Tests` (без каталога `Admin`).

Что нужно от группы 5 по итогам решений (сообщает лид, developer группы 4 это не делает): маппинг `StartStatus.SourceUnavailable -> 503 ProblemDetails` (без текста исключения); `AdminAudit` пишет запись об отмене (`JobId`, `CancelStatus`, `RemoteIp`, `X-Requested-By`); в OpenAPI у `POST /jobs` добавить `503`, у `error.code` перечислены те же три значения.

## Что блокирует, что нет

| № | Серьёзность | Блокирует | Суть |
|---|---|---|---|
| M1 | Major | да | несуществующий топик источника возвращается как `Invalid` (400), должен быть `SourceUnavailable` (503) |
| M2 | Major | да | проверка retention пропускает партицию, целиком очищенную retention (и первое сообщение без timestamp, и недочитанное первое сообщение) |
| m1 | Minor | нет, делается | FatalConfig в раннере ждёт `MaxUnavailable`; класс Data в стадии конвейера уходит в ветку инфраструктуры |
| m3 | Minor | нет, делается | открытие источника не ограничено общим временем и токеном |
| m4 | Minor | нет, делается | `job.Finish` и освобождение слота не атомарны |
| m5 | Minor | нет, делается | `StopAsync` не ждёт открывающуюся задачу, `_activeTask` присваивается поздно |
| m6 | Minor | нет, делается | исключение после занятия слота оставляет слот занятым |
| m7 | Minor | нет, делается | `reason` без ограничений попадает в лог |
| m8 | Minor | нет, **кода нет** | отмена не логируется в Core - решено: остаётся за `AdminAudit` группы 5 (ADR 2.8, правки teamlead внесены) |
| m9 | Minor | нет, **кода нет** | набор правил не фиксируется на старте - решено: задокументировано (ADR 2.6, R-10, design Р-7, spec) |
| m10 | Minor | нет, делается | `to` раньше retention и формулировка «максимум» в комментарии |
| m11 | Minor | нет, делается | `TimestampType.NotAvailable` в окне |
| m12 | Minor | нет, делается | единый источник имён заголовков Kafka |
| m14 | Minor | нет, делается | недостающие тесты (список в конце) |

Порядок работы: M1 -> M2 (вместе с m10, m11) -> m3 -> m12 -> m4+m5+m6 (одна правка менеджера) -> m7 -> m1 -> m14. M1, M2, m3, m12 правят один файл `KafkaReplaySource.cs` - делать подряд, не параллельно. Все изменения менеджера (m4-m6) - в `ReprocessJobManager.cs` одним проходом.

---

## M1. Несуществующий топик - отказ источника, а не ошибка аргумента

**Решение.** По ADR-0006 п. 2.6 (таблица `POST /jobs`) и п. 2.7: `400` только для ошибок аргументов запроса. Отсутствие топика `DataSaver__Kafka__Topic/DlqTopic/RejectedTopic` - неверная конфигурация или окружение (запрос корректен), поэтому исход - `StartStatus.SourceUnavailable` (admin: `503`). Это же верно, если метаданные топика пришли без партиций или с иной ошибкой.

**Точное исправление.**
1. `KafkaReplaySource.Plan` (строки 94-107): ветку «топика нет» (`topicMetadata is null` либо `Error.Code == UnknownTopicOrPart`) заменить на `throw new ReplaySourceException($"Kafka: topic '{topic}' not found")`. Имя топика - из конфигурации, не данные сообщения, в текст исключения его включать можно. `ReadPlan.Errors` после этого содержит только ошибки retention (M2).
2. Разбор метаданных вынести в `internal static IReadOnlyList<int> ResolvePartitions(string topic, TopicMetadata? metadata)` (чистая функция, без Kafka): `null`, `UnknownTopicOrPart`, любая иная `Error.IsError`, пустой список партиций -> `ReplaySourceException`; иначе номера партиций по возрастанию. `Plan` вызывает её.
3. `ReprocessJobManager.StartAsync` (строки 114-119): в Error-лог добавить причину: `logger.LogError("Reprocess job {JobId} was not started: the source is unavailable: {Reason}", job.Id, ex.Message)`. Контракт `ReplaySourceException` - текст без данных сообщений (оно уже так документировано в `ReplayPorts.cs`), поэтому `ex.Message` в лог допустим; тип результата не меняется.
4. Комментарий класса `KafkaReplaySource` (строка 15) и `<summary>` `StartStatus.Invalid` в менеджере: «ошибки аргументов запроса и период, начало которого нельзя подтвердить (retention)»; убрать формулировку про несуществующий топик, если она есть.

**Тесты.**
- Unit (`Ktm.DataSaver.Tests`, рядом с тестами Kafka): таблица `ResolvePartitions`: `null`; `new TopicMetadata(topic, [], new Error(ErrorCode.UnknownTopicOrPart))`; `LeaderNotAvailable`; пустой список без ошибки; успешный ответ с партициями `[2,0,1]` -> `[0,1,2]`. Все ошибочные -> `ReplaySourceException`, а не `ReplayOpenResult.Invalid`.
- Unit менеджера: `IReplaySource` бросает `ReplaySourceException("Kafka: topic 'x' not found")` -> `StartStatus.SourceUnavailable`, `Errors` пуст, слот свободен (следующий `StartAsync` с рабочим источником -> `Started`), в логе Error есть причина `topic 'x' not found` и нет маркера из тела сообщения.
- Интеграционный (Docker, написать, не прогонять): `KafkaReplaySourceTests.MissingSourceTopic_IsSourceUnavailable_AndTopicIsNotCreated` - `OpenAsync` для `raw` и `dlq` на несуществующем топике бросает `ReplaySourceException`; после вызова топика по-прежнему нет (`AllowAutoCreateTopics = false` действует и для metadata-запроса зависимого admin-клиента).

**Критерий.** Несуществующий топик нигде не превращается в `ReplayOpenResult.Invalid`; тесты выше зелёные; поиск `"does not exist"` в `src` не находит прежний текст `400`.

---

## M2. Retention: партиция, целиком очищенная retention

**Решение teamlead (зафиксировано в ADR-0006 п. 2.7, spec «Источники и период», R-11).** Начало периода должно быть доказуемо покрыто сохранёнными данными, иначе `400` (поле `from`), а не молчаливый пропуск. Для каждой партиции:

| Состояние партиции | Вердикт |
|---|---|
| `low = 0` (в том числе `low = high = 0`) | покрыта: retention ничего не удалял |
| `low > 0`, `low < high`, timestamp первого сохранённого сообщения `Tp` известен | покрыта, только если `from >= Tp` |
| `low > 0`, `low == high` (всё очищено) | **нельзя подтвердить ни для какого `from`** -> `400` |
| `low > 0`, `low < high`, первое сообщение имеет `TimestampType.NotAvailable`, либо чтение дошло до EOF и сообщения нет | нельзя подтвердить -> `400` |
| `low > 0`, `low < high`, за отведённое время сообщение не получено, EOF не было | отказ источника: `ReplaySourceException` (503), не `400` |

Допустимое `from` (когда все партиции доказуемы) - не раньше **самого позднего** из `Tp` по партициям. Причина консервативности: удалённые сообщения лежат раньше первого сохранённого по offset, но их время неизвестно; ложный отказ безопаснее потерянных событий. Цена (R-11): редкая партиция блокирует `raw` до появления трафика - принято, триггер пересмотра в ADR.

**Точное исправление (`KafkaReplaySource.cs`).**
1. Чистая функция `internal static IReadOnlyList<ReprocessValidationError> CheckRetention(string topic, DateTimeOffset from, IReadOnlyList<PartitionRetention> partitions)` и `internal readonly record struct PartitionRetention(int Partition, long Low, long High, DateTimeOffset? FirstMessageTime)`. `FirstMessageTime = null` при `Low > 0` означает «нельзя подтвердить». Правила - таблица выше. Результат: пусто; либо одна ошибка `from` для неподтверждаемых партиций (перечислить их номера, без времени): `The start of the period cannot be verified: partition(s) 2, 5 of topic '{topic}' have no retained message with a known time after retention cleanup.`; либо, если неподтверждаемых нет, одна ошибка `from` прежнего вида с самым ранним допустимым временем (`yyyy-MM-dd'T'HH:mm:ss.fff'Z'`). Неподтверждаемые партиции имеют приоритет над «слишком рано».
2. `EarliestRetained` заменить на `ReadFirstMessageTimes(...)`: возвращает `PartitionRetention` по каждой партиции. Первое сообщение читать только для `Low > 0 && Low < High` и только когда задан `from`. Исходы чтения: сообщение с `Timestamp.Type != NotAvailable` -> `FirstMessageTime = UTC`; `NotAvailable` или `IsPartitionEOF` без сообщения -> `null`; срок (общий бюджет открытия, m3) истёк без сообщения и без EOF -> `throw new ReplaySourceException("Kafka: no data from the broker within the open limit")`. Текущая ошибка «цикл по таймауту молча пропускает партицию» исчезает. `Unassign()` в `finally` сохранить.
3. Partitions с `Low > 0 && Low == High` в чтение не идут вообще (читать нечего): `FirstMessageTime = null` сразу.
4. `Plan` вызывает `CheckRetention` только при `request.From is not null`; без `from` (`dlq`/`rejected` «от начала до снимка») проверки нет - это документировано.
5. (m10) Комментарий к методу: убрать формулировку «максимум из первых сообщений», написать «самое позднее из времён первых сохранённых сообщений партиций с очищенным началом». `to` отдельно не проверяется (период, целиком раньше сохранённого, отклоняется по `from`; документировано в ADR) - это нужно закрепить тестом, не кодом.
6. (m11) Фильтр окна вынести: `internal static bool InWindow(Timestamp timestamp, DateTimeOffset? from, DateTimeOffset? to)` (чистая функция, использует читатель). `TimestampType.NotAvailable` при заданном периоде -> `true` (включается; ADR 2.7), полуинтервал `[from, to)` без изменений. Комментарий в коде объясняет, почему включаем.

**Тесты.**
- Unit `CheckRetention` (таблица, имена по поведению):
  - все `Low = 0` (в том числе пустая `0/0`), любое `from` -> ошибок нет;
  - одна партиция `Low = 5, High = 9, FirstMessageTime = T`: `from = T - 1 мс` -> ошибка поля `from`, в тексте время `T` в формате `...fff'Z'`; `from = T` и `T + 1 мс` -> ошибок нет;
  - две партиции с `T1 < T2`: `from` между ними -> ошибка называет `T2`; `from >= T2` -> нет;
  - **пустая очищенная** (`Low = 5, High = 5`): `from` далеко в прошлом, равное «сейчас», будущее -> ошибка поля `from`, в тексте номер партиции, времени в тексте нет; вместе с нормальной партицией - всё равно ошибка;
  - `Low = 5, High = 9, FirstMessageTime = null` (NotAvailable/EOF) -> та же ошибка;
  - неподтверждаемая и «слишком ранняя» одновременно -> одна ошибка про неподтверждаемую (приоритет);
  - период целиком раньше сохранённого (`from < to < Tp`) -> ошибка (закрепляет m10: `to` отдельно не нужен).
- Unit `InWindow`: границы `t-1мс` вне, `t` внутри, `t2-1мс` внутри, `t2` вне; `NotAvailable` с периодом -> внутри; без `from`/`to` -> внутри.
- Интеграционные (Docker, написать, не прогонять; очистку делать `IAdminClient.DeleteRecordsAsync`, а не ожиданием retention): (1) 3 сообщения, `DeleteRecords` до `High` -> `low = high = 3`, `raw` с любым `from` -> `Invalid` с ошибкой `from`; (2) после этого опубликовать сообщение с timestamp `T`: `from < T` -> `Invalid`, `from = T` -> успех и ровно это сообщение; (3) топик без удалений и пустая партиция `0/0` при `from` в прошлом -> успех (регрессия существующего «период раньше retention» теста обновить под новое сообщение); (4) `dlq` без `from` на топике с очищенной партицией -> успех (проверки нет).

**Критерий.** Новые unit-тесты падают на текущей реализации (пустая очищенная партиция проходила) и проходят после правки; нет пути, на котором `low > 0` партиция без доказанного времени пропускает `from`; нет молчаливого `continue` по таймауту первого сообщения.

---

## m3. Открытие источника: общий срок и токен

**Решение.** Открытие (метаданные, `QueryWatermarkOffsets` по партициям, `OffsetsForTimes`, чтение первых сообщений) не должно занимать `N * 10 с`. Общий срок - 30 с, токен проверяется перед каждым блокирующим вызовом; отмена видна не позднее одного вызова (до 10 с) - блокирующий вызов librdkafka прервать нельзя, ADR 2.7 это фиксирует.

**Точное исправление.**
1. `internal sealed class OpenBudget(TimeProvider time, TimeSpan total, TimeSpan perCall, CancellationToken token)`: `TimeSpan NextTimeout()` - проверяет `token.ThrowIfCancellationRequested()`, возвращает `min(perCall, остаток)`, при остатке <= 0 бросает `ReplaySourceException("Kafka: open limit exceeded")`. Константы `OpenTimeout = 30 с`, `MetadataTimeout = 10 с` остаются `private` в `KafkaReplaySource` (опций не добавлять: `KafkaOptions` и конфигурация - зона группы 5).
2. Все вызовы `GetMetadata`, `QueryWatermarkOffsets`, `OffsetsForTimes`, а также цикл чтения первого сообщения (M2 п. 2) берут таймаут из `budget.NextTimeout()`. Цикл `Consume` проверяет токен на каждой итерации.
3. `Task.Run(() => Open(...), cancellationToken)` оставить; `OperationCanceledException` из открытия - `consumer.Dispose()` и проброс (существующий `catch`).
4. `KafkaException` при открытии остаётся `ReplaySourceException` (как сейчас).

**Тесты.** Unit `OpenBudget` на `FakeTimeProvider`: `NextTimeout` возвращает `perCall`, пока остаток больше; возвращает остаток, когда он меньше `perCall`; после расходования 30 с - `ReplaySourceException`; отменённый токен -> `OperationCanceledException` до расчёта. Интеграционный (Docker, не прогонять): токен, отменённый до `OpenAsync`, -> `OperationCanceledException`, consumer закрыт.

**Критерий.** Суммарное ожидание открытия ограничено 30 с независимо от числа партиций; нет блокирующего вызова без таймаута из бюджета.

---

## m12. Единый источник имён заголовков

**Решение.** Имена `ktm-source-*`, `ktm-dlq-*`, `ktm-reject-*` лежат в трёх классах литералами и константами. Дрейф имени между публикатором и читателем повторной обработки молча разорвёт координаты и причину.

**Точное исправление.**
1. Новый `internal static class KafkaHeaders` в `Infrastructure/Kafka` с константами: `SourceTopic`, `SourcePartition`, `SourceOffset`, `SourceTimestamp`, `RejectReason`, `RejectDetail`, `RejectRuleId`, `RejectRuleName`, `RejectRuleVersion`, `DlqReason`, `DlqStage`, `DlqExceptionType`, `DlqDetail`, `DlqAttempts`, `DlqFailedAt`, `DlqSqlState`. Значения те же, что сейчас (заголовки на проводе не меняются).
2. `KafkaDeadLetterPublisher`, `KafkaRejectionPublisher`, `KafkaReplaySource` используют только эти константы; прежние `internal const` в публикаторах и `RejectReasonHeader` в источнике удалить, ссылки в тестах заменить (имена в тестах, объявленные литералом на проводе, оставить - они проверяют формат).
3. Чтение сообщения вынести из вложенного `KafkaReplayReader` в `internal static ReplayMessage MapMessage(ConsumeResult<byte[], byte[]> result, ReprocessSource source)` (поведение прежнее).

**Тесты.** Unit-«круг» без брокера: сообщение, собранное `KafkaDeadLetterPublisher.ToMessage` / `KafkaRejectionPublisher.ToMessage`, оборачивается в `ConsumeResult` (topic/partition/offset другие, чем исходные) и разбирается `MapMessage`: исходные координаты (topic, partition, offset, timestamp), причина, стадия (для `dlq`) совпадают с исходными, `CoordinatesFromHeaders = true`, `ActualTopic/Partition/Offset` - координаты `dlq`/`rejected`. Отдельно: заголовков нет -> фактические координаты, `false`; `ktm-source-partition = "abc"` или отрицательное -> `false`; нет `ktm-source-timestamp` -> фактическое время сообщения; `raw` -> координаты фактические, `true`, причины и стадии нет.

**Критерий.** Строка `"ktm-` встречается в `src` только в `KafkaHeaders.cs`; круговые тесты зелёные.

---

## m4, m5, m6. `ReprocessJobManager`: слот, завершение, остановка

Один проход по файлу. Публичная сигнатура не меняется (раздел «Параллельная работа»).

**Решение.**
- **m4.** `job.Finish(...)` и освобождение слота - под одной блокировкой `_gate`, чтобы наблюдатель (`Get`, который берёт ту же блокировку) никогда не видел «задача завершена, слот занят» и не получал `409` на уже завершённую задачу. Порядок замков `_gate` -> замок задачи; код задачи/раннера `_gate` не берёт.
- **m5, m6.** Слот имеет «сигнал освобождения»: при занятии слота (под `_gate`, там же, где `_active = job`) создаётся `TaskCompletionSource` (`RunContinuationsAsynchronously`), хранится как `_activeDone`. Завершается он ровно один раз: в конце `ExecuteAsync` (после финального лога) либо при отказе до запуска фонового исполнения. `StopAsync` и `WhenFinishedAsync` ждут `_activeDone.Task`, а не `_activeTask` (поле `_activeTask` можно убрать). Тем самым остановка хоста ждёт и открывающуюся задачу.
- Токен открытия источника: `CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token)` - остановка хоста прерывает `OpenAsync`.
- После успешного `OpenAsync`, **под `_gate`**: если `_stopping.IsCancellationRequested` - закрыть читатель (`DisposeAsync`, исключения глотать с Warning-логом типа), освободить слот, `throw new OperationCanceledException(_stopping.Token)`; иначе `MarkRunning`, вставка в историю и `Task.Run(ExecuteAsync)`.
- Всё, что идёт после занятия слота и до передачи владения `ExecuteAsync` (открытие, `reader.Partitions`, `MarkRunning`, история, `Task.Run`), - в `try`; в `catch (Exception)` при ещё не переданном владении: закрыть читатель (если он есть), освободить слот и завершить `_activeDone`, пробросить исключение. Задача, не дошедшая до запуска, в историю не попадает. Исключения `ReplaySourceException` и `Invalid` - прежние исходы.

**Точное исправление (ориентиры по текущему коду).** `StartAsync` 99-142, `ExecuteAsync` 263-270 (`Finish` + `Release`), `StopAsync` 212-225, `WhenFinishedAsync` 203-209, `Release` 229-238.

**Тесты** (`ReprocessJobManagerTests`, fake `IReplaySource`/`IReplayReader`, `FakeTimeProvider` где нужно):
- m4: быстрый fake-раннер, цикл 300 итераций: после запуска ждать, пока `Get(id).State` не станет завершённым (крутить `Task.Yield`), затем сразу `StartAsync` -> всегда `Started`, ни одного `Conflict`. Без правки тест нестабильно падает (допустима стохастичность: тест не должен быть флейки на исправленном коде - после правки гонки нет по построению).
- m6: читатель, у которого `Partitions` бросает `InvalidOperationException` -> `StartAsync` бросает это исключение; читатель закрыт (`DisposeAsync` вызван один раз); `List()` пуст; следующий `StartAsync` -> `Started`; `StopAsync` не зависает.
- m5-а: `OpenAsync` ждёт токен (`Task.Delay(Timeout.Infinite, token)`); параллельно `StopAsync` -> `OpenAsync` получает отмену, `StartAsync` бросает `OperationCanceledException`, слот свободен, `StopAsync` завершается; ни одна фоновая задача не запущена.
- m5-б: `OpenAsync` игнорирует токен и возвращает читатель уже после вызова `StopAsync` -> `StartAsync` бросает `OperationCanceledException`, читатель закрыт, фоновое исполнение не запускалось, `StopAsync` завершается (не висит).
- Регрессии (без изменения утверждений): гонка двух `StartAsync` -> ровно один `Started`; остановка хоста отменяет активную задачу; история 10; `WhenFinishedAsync` ждёт итога.

**Критерий.** Нет пути, на котором слот остаётся занятым; `StopAsync` ждёт и открытие, и исполнение; тесты выше зелёные.

---

## m7. `reason`: длина и символы

**Решение.** Значение уходит в лог запуска как есть; коды причин проекта - короткие латинские (`ReasonCodeFormat` правил: до 64 символов, системные - `write_data_error` и т.п.). Жёсткий шаблон не вводить (список кодов расширяется правилами), ограничить длину и запретить символы, ломающие строку лога.

**Точное исправление (`ReprocessRequestValidator.Validate`).** Для `reason`, не равного `null` и не пустого/пробельного (оно уже даёт ошибку «blank»): ошибка поля `reason`, если длина > 64 (`reason must not be longer than 64 characters.`) либо есть символ категории `Control`, `LineSeparator` или `ParagraphSeparator` (`reason must not contain control characters.`). Значение в тексте ошибки не повторять. Порядок проверок: для `raw` прежняя ошибка «not allowed», остальные для `reason` не добавляются.

**Тесты** (`ReprocessRequestValidatorTests`): `malformed_json`, `write_value_error`, 64 символа -> ошибок нет; 65 символов -> ошибка `reason`; `"a\nb"`, `"a\rb"`, `"a\tb"`, `"a\0b"`, `"a\u001bb"`, `"a b"`, `"a b"` -> ошибка `reason`, сообщение не содержит исходного значения; `source = raw` и `reason` с переводом строки -> одна ошибка (not allowed); другие поля запроса не затрагиваются. Менеджер: `Invalid` приходит до занятия слота (слот свободен, источник не открывался) - если такой тест уже есть, дополнить.

**Критерий.** В лог запуска невозможно записать многострочное или произвольно длинное `reason`.

---

## m1. `ReprocessRunner`: FatalConfig и класс Data в стадии

**Решение.** Раннер - фоновая задача с единственным слотом. Ждать 10 минут (`MaxUnavailable`) ошибку, которую исправит только администратор (нет прав/таблицы, SQLSTATE `28/42/3D/3F/0A`), нельзя: слот занят, код `db_unavailable` вводит в заблуждение. Рекомендация принята: **быстрый отказ `Failed` с кодом `internal`** (новый код `config_error` не вводим: контракт, который параллельно делает группа 5, не меняем); причина - в Error-логе (тип исключения, SQLSTATE, без текста). Класс Data в стадии конвейера - свойство сообщения, а не инфраструктуры.

**Точное исправление (`ReprocessRunner.cs`).**
1. `ClassifyAsync` (187-191): `switch` по `WriteErrorClassifier.Classify(ex)`: `Transient` -> `WaitForRecoveryAsync(Internal)` и повтор; `FatalConfig` -> быстрый отказ (п. 3); `Data` и `Unexpected` -> бюджет `MaxAttempts`, затем `FailedAgain` (как сейчас для `Unexpected`).
2. `WriteAsync` (247-251): `Transient` -> `WaitForRecoveryAsync(DbUnavailable)` как сейчас; `FatalConfig` -> быстрый отказ. Ветка прерванной изоляции (`IsolationOutcome.Interrupted`, 274-276): если `Classify(isolation.InterruptedBy!) == FatalConfig` -> быстрый отказ, иначе `WaitForRecoveryAsync(DbUnavailable)`.
3. Быстрый отказ: Error-лог `Reprocess job {JobId} stopped: fatal configuration error while writing: {ErrorType} SqlState={SqlState}` (тип через `GetType().Name`, SQLSTATE через `WriteErrorClassifier.FindSqlState`, текст исключения не писать), затем `throw new ReprocessFailedException(ReprocessErrorCodes.Internal)`; `RunAsync` уже превращает его в `Failed(code)`. Достигнутый прогресс сохраняется (счётчики и `Advance` уже учтены по завершённым пачкам).
4. Комментарии `ReprocessErrorCodes`/`<summary>` исхода `Failed` и XML-док в `ReprocessJob.cs`: добавить «отказ записи класса FatalConfig - `internal`».

**Тесты** (`ReprocessRunnerTests`, на `FakeTimeProvider`; исключения - тестовые типы с `ISqlStateError`/`DbException`-подобные, как в существующих тестах классификатора):
- запись бросает ошибку FatalConfig (`42501`) -> `Failed(internal)`, писатель вызван один раз, `FakeTimeProvider` не продвигался (нет ожидания), счётчики прежних пачек сохранены, в логе Error с типом и SQLSTATE и без маркера из текста исключения;
- то же, но FatalConfig приходит как `InterruptedBy` во время изоляции (первая запись пачки - Data, затем FatalConfig на половине) -> `Failed(internal)` сразу;
- исключение стадии класса Data (`23514`) -> бюджет `MaxAttempts` с паузами, затем `FailedAgain` в отчёте (стадия и код причины), `outcome = CompletedWithFailures`, задача не ждёт `MaxUnavailable`;
- исключение стадии класса Transient (`SocketException`) -> ожидание и успех после восстановления; недоступно дольше `MaxUnavailable` -> `Failed(internal)` (код стадии - `internal`, `db_unavailable` для стадии не используется);
- регрессия: `DatabaseUnavailableLongerThanLimit_...` без изменения утверждений.

**Критерий.** Ни одна ошибка FatalConfig не приводит к ожиданию `MaxUnavailable`; класс Data стадии конвейера не уходит в ветку инфраструктуры.

---

## m14. Остальные тесты (по списку ревью)

а) Уже перечислены выше: несуществующий топик (M1), пустая очищенная партиция (M2), отмена во время открытия (m3, m5-а), инфраструктурный путь стадии (m1).

б) **Исключение из раннера в `ExecuteAsync` менеджера.** Сейчас `ReprocessRunner` не позволяет подменить `RunAsync`. Сделать класс `public class ReprocessRunner` (не `sealed`), `RunAsync` - `public virtual`; поведение и конструктор не менять. Тест: наследник бросает `InvalidOperationException` с маркером в тексте -> задача `Failed`, `error.code = internal`, `outcome = Failed`, слот свободен (`StartAsync` снова `Started`), читатель закрыт, в логе только тип исключения, маркера нет. Тест: `reader.DisposeAsync` бросает исключение -> задача всё равно завершена штатно (Warning с типом), слот свободен.

в) **`write_value_error`.** Раннер, писатель бросает `OverflowException` на одном сообщении -> после `MaxAttempts` и изоляции в отчёте запись `FailedAgain`: стадия `write`, код `write_value_error`, `ExceptionType = System.OverflowException`; соседи записаны; `outcome = CompletedWithFailures`; маркера из текста исключения нет в отчёте/статусе/логе. (Классификацию причины делает `DeadLetterInfo`, тест проверяет, что раннер её не теряет.)

г) **`CountingStore`.** Через раннер: писатель вернул «вставлено 1 из 3» -> `written = 1`, `duplicates = 2`; пачка с ядовитым и изоляцией -> `written` = число записанных соседей, `duplicates = 0`, неудавшиеся записи не попадают в счётчики; повторный проход после прерванной инфраструктурой изоляции не удваивает `duplicates` (счётчики сбрасываются `Flush` на каждый успех).

д) **`EstimatedRemaining`** (`ReprocessJob`, unit): `Queued` и завершённая -> `null`; `Running` с `total = 0` -> `null`; `total = 1000`, обработано 400 (через `Advance`), `MaxRate = 100` -> 6 с; `MaxRate = 300` -> 2 с; после `MarkPartitionsDone` -> 0 с; `Advance` не выходит за `EndOffset` и не идёт назад. Комментарий метода уточнить: «оценка по смещениям, пропуски offset в партиции делают её завышенной».

**Критерий.** Все перечисленные тесты существуют и зелёные, существующие тесты группы 4 проходят без изменения утверждений.

---

## Что НЕ делается

- m8 (логирование отмены в Core): не делается, решено - запись об отмене пишет `AdminAudit` группы 5 (ADR 2.8). В Core при завершении по отмене остаётся существующий итоговый лог с `Outcome = Cancelled`.
- m9 (фиксация снимка правил на старте): не делается, решено - документ (ADR 2.6, R-10, design Р-7, spec); `RuleRejectionStage`, `IngestionPipeline` и регистрацию не менять.
- Нет новых кодов ошибок (`config_error`), опций, пакетов, изменений схемы БД и заголовков Kafka на проводе. Нет изменений в `Ktm.DataSaver/**`, `Directory.Packages.props`, `docs/**`, `openspec/**` (кроме отметок teamlead).
- Не расширять объём: группы 5-8 не трогать.

## Определение готовности

1. M1, M2 выполнены; m1, m3-m7, m10-m12, m14 выполнены; тесты зелёные, существующие проходят без ослабления утверждений.
2. `$run-tests`: unit-проект `Ktm.DataSaver.Tests` целиком (без каталога `Admin`, если он красный из-за группы 5); интеграционные - только без Docker. В отчёте developer: что прогнано, что нет (с причиной), какие интеграционные тесты написаны, но не выполнены (список: M1 интеграционный, M2 (1)-(4), m3 токен).
3. Повторное ревью (`reviewer`, режим «блок только Major+») закрывает M1 и M2; minor отмечает, но не блокирует.

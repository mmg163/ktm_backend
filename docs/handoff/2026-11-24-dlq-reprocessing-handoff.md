# Handoff: DLQ и повторная обработка Data Saver, состояние на конец сессии 2026-11-24

В git не коммитится. Ветка `feature/add-data-saver-dlq-reprocessing` (от main). **Ничего не закоммичено и не запушено** (ни планирование, ни код). Пуш и PR - только по команде лида, в `main` не пушить.

## Задача
Обработка событий, которые не удалось записать (retry с backoff -> `ktm.events.dlq`), различие `rejected` (плохие данные) и `dlq` (хорошие данные, не смогли сохранить), ядовитое сообщение не блокирует партицию, повторная обработка `dlq`/`rejected`/`raw` за период, README с командами. Зависит от C4, C5 (готовы).

## Артефакты планирования (готовы, согласованы)
- ADR: `docs/adr/0006-data-saver-dlq-and-reprocessing.md` (статус «принят», раздел 9 - история решений).
- OpenSpec: `openspec/changes/add-data-saver-dlq-reprocessing/` (proposal, design, specs `dead-letter`, `reprocessing`, MODIFIED `event-ingestion`/`ingestion-pipeline`, `tasks.md`, `review-fixes-1.md` (закрыт), `review-fixes-2.md` (задача для developer по группе 4, в работе)).

## Решения лида (все приняты)
1. dlq получает любую ошибку, привязанную к сообщению, на стадиях parse/validation/rules/write после `MaxAttempts`=3 с backoff; **стадии `mapping` нет** (JSON->событие = parse, событие->SQL = write). Без предела повторяется только инфраструктура (БД недоступна, таймауты, права, сеть). `rejected` не меняется (не JSON, схема, правила).
2. Предохранитель `BreakerThreshold`=20: подряд 20 ушедших в dlq без успешной записи -> dlq отключается, Degraded, оповещение, повтор без предела; закрывается первой успешной записью. Вариант A: стадийный сбой при открытом предохранителе блокирует consumer, оповещение на каждой попытке, перезапуск сбрасывает; ограничение m6 (20+ стадийных ядовитых в одной пачке откроют его) принято.
3. Недоступный dlq: Degraded, offset не коммитится, запасного хранилища нет.
4. Reprocess - **HTTP admin-API** + **админка внутри Data Saver** (`/admin/ui/`, статика из встроенных ресурсов), Swagger/OpenAPI. **Без аутентификации, на том же порту, на отдельных роутах `/admin`** (принятый риск R-8: защита только сетевая, ingress не должен публиковать `/admin`; пункт в протокол приёмки). Окно `raw` - по Kafka-timestamp, `[from,to)` UTC с `Z`.
5. **На админку тесты писать не надо** (последнее указание лида). Уже написанные `tests/Ktm.DataSaver.Tests/Admin/AdminUiEndpointsTests.cs` - решить с лидом: удалить или оставить (написаны до указания); новые тесты на UI/клиент не добавлять. Скорее всего относится только к UI-статике, не к серверным эндпоинтам - уточнить у лида при необходимости.
6. Только бесплатные библиотеки (MIT/Apache/BSD, проверять лицензию бинарного релиза); тесты только на поведение кода, не на конфиги.

## Статус по tasks.md
- **0.1** [x]. **0.2** (Docker, базовая линия OutageAcceptanceTests) и **0.3** (хелпер `PoisonConstraint`, написан, на БД не проверен) - не отмечены: на машине не работает Docker.
- **Группы 1-3** [x]: реализованы, ревью пройдено (нет Major+). Unit-тесты зелёные (на тот момент 722-773). Не покрыто: тест на сброс `isolationStarted` (R3, minor).
- **Группа 4** (4.1-4.4): реализована (`Core/Reprocessing/*`, `Infrastructure/Kafka/KafkaReplaySource.cs`, `Postgres/CountingEventStore.cs`), ревью **не принято: 2 Major** (M1 несуществующий топик -> должен быть SourceUnavailable/503, не 400; M2 retention пропускает полностью очищенную партицию). Teamlead написал `review-fixes-2.md`; developer начал править, **оборван лимитом, состояние дерева неизвестно**.
- **Группа 5**: клиент админки (5.5-5.7: `Ktm.DataSaver/Admin/Ui/*`, `AdminUiEndpoints.cs`, `Program.cs`) готов; `.env.example` (5.8) и документация (7.1: `src/README.md` п.8, `docs/manuals/data-saver.md` п.12) написаны, **curl-примеры не выполнялись и не сверены с кодом**. Серверная часть (`IAdminSection`, эндпоинты `/admin/v1/...`, OpenAPI/Swagger, DI, опции `Reprocess__*`, `ReprocessJobHostedService`, аудит) - **developer оборван лимитом**, в дереве есть частичная работа: `Ktm.DataSaver/Admin/`, `Hosting/ReprocessJobHostedService.cs`, `docs/api/data-saver-admin.openapi.json`, правки `DataSaverServiceCollectionExtensions.cs`, `Directory.Packages.props`, `Ktm.DataSaver.csproj`. Что именно доделано - проверить.
- **Группа 6** (интеграционные/приёмочные/перф), **7.2-7.4** (kafka-event-format, ADR-0002/0003 правки, сценарии покрытия, протокол `docs/test-protocols/data-saver-dlq-reprocessing.md`), **8** (ревью всего, dev-стенд, sync specs, archive) - не начаты.

## Что надо знать из ревью (для группы 5)
- `StartStatus.SourceUnavailable` -> 503 ProblemDetails без текста исключения; в OpenAPI у `POST /jobs` добавить 503.
- `AdminAudit` обязан писать запись об отмене (`JobId`, `CancelStatus`, `RemoteIp`, `X-Requested-By`); Core пишет только итоговый лог.
- `ReprocessSettings.MaxAttempts` брать из `DataSaver__Dlq__MaxAttempts`, `BatchSize` из `DataSaver__Batch__MaxSize` (не дублировать значения).
- Клиент админки (`reprocessing.js`, `app.js`) читает DTO «с запасом» - сверить имена полей с реальными DTO.
- Значения опций `Reprocess__*` (`MaxRatePerSecond`=200, `MaxRateLimit`=400, `MaxUnavailableSeconds`=600) и `Admin__SwaggerEnabled` в документации взяты из ADR - сверить с кодом.
- Порядок работы review-fixes-2: сигнатуры `StartAsync`, `StartResult/StartStatus`, конструкторы менеджера/раннера/источника, `ReprocessErrorCodes`, `Validate(request, maxRateLimit)` менять нельзя (допустимо: `ReprocessRunner` не sealed, `RunAsync` virtual).

## Как продолжить
1. `git status`, `dotnet build src/Ktm/Ktm.sln -warnaserror`, `dotnet test src/Ktm/tests/Ktm.DataSaver.Tests` - понять, собирается ли дерево после оборванных агентов. Интеграционные тесты нужны на машине с Docker (`Category!=Long&Category!=Perf`); там же закрыть 0.2/0.3 и прогнать новые тесты групп 1-4 (реальные исключения Npgsql, `KafkaDeadLetterPublisherTests`, `KafkaReplaySourceTests`).
2. `developer`: довести `review-fixes-2.md` (M1, M2, minor), затем серверную часть группы 5 (параллельно можно, файлы разные: Core/Infrastructure vs `Ktm.DataSaver/Admin`).
3. `reviewer`: группа 4 заново (закрытие M1/M2, minor не блокируют), затем группа 5. Пока есть Major - чекбоксы не отмечать, не коммитить; разбор через `teamlead`.
4. Документы: сверить README/мануал/`.env.example` с кодом, выполнить curl-примеры, `.http`-файл.
5. Группа 6 (интеграционные, без тестов на админку), 7.2-7.4, группа 8. Коммит и PR - только по команде лида.
6. Роли - только через субагентов; `teamlead` не запускает `developer` сам.

## Открытые вопросы к лиду / эксплуатации
- Как получить ядовитое сообщение на dev-стенде без изменения схемы (для протокола 8.2).
- Права на запись в `ktm.events.dlq` для сервисной учётки в проде (ACL); адресат алерта на `dead_lettered`; регламент разбора dlq (хранение 30 суток, потом потеря); потребитель топика dlq не определён.
- R-11: консервативная проверка retention может блокировать `raw` на разреженном dev-стенде (точное решение - `DescribeConfigs`, нужны права).
- Аудит запусков reprocess - только структурный лог, без таблицы (отступление от docs/04 п. 4.2/4.4, как у правил).
- Лимит 5 часов API исчерпан (сброс в 18:40 Самара): оборванные агенты - группа 5 (`a393dbc4f16e0c144`) и исправления группы 4 (`a8471123a071638c8`); их можно продолжить SendMessage только в этой сессии, в новой - начинать новых по этому документу.

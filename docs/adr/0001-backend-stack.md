# ADR-0001. Стек backend, структура solution, конфигурация, тестовая инфраструктура

- Статус: принят (утверждено лидом проекта в чате сессии, 2026-11-20: «Утверждаю, старт» после просмотра каркаса и ADR-0001..0004)
- Дата: 2026-11-20
- Область: весь backend; первое применение - Data Saver (`openspec/changes/add-data-saver-core`)

## 1. Контекст

`CLAUDE.md` требует зафиксировать стек в ADR до первой задачи `apply`. Фактически уже есть: `src/Ktm/Ktm.sln`,
проект `Ktm.Api` на `net10.0`; Kafka (KRaft, один брокер, входящий топик `ktm.events.raw.<source>` для каждого источника) развёрнута ansible-ролью
`infra/kafka`; схема журнала событий - PostgreSQL 16 + TimescaleDB 2.32.x (проверено на 2.32.1, см. ADR-0004), схема `ktm`, роли `ktm_migrator` / `ktm_app`
(`docs/design/Схема_БД_ER.md`). Ограничения ТЗ: пик 500 событий/с, 15 млн событий в месяц (`docs/07`, п. 7.1); журнал
неизменяем, идемпотентность гарантирует схема БД (`docs/03`, п. 3.2; `docs/05`, п. 5.1); хранение секретов вне
репозитория (`docs/07`, п. 7.7).

## 2. Решение

| Область | Решение |
|---|---|
| Платформа | .NET 10 (LTS), C# 14, `Nullable=enable`, `TreatWarningsAsErrors=true` в новых проектах. Версии пакетов - централизованно (`Directory.Packages.props`), без плавающих версий. |
| СУБД | PostgreSQL 16 + TimescaleDB (как в схеме B3). Драйвер - Npgsql 10.x. |
| Шина | Apache Kafka; клиент `Confluent.Kafka` (актуальная стабильная 2.x, версия фиксируется в `Directory.Packages.props`). |
| Доступ к данным | **Разделение по задаче, а не один инструмент на всё.** Горячий путь записи событий (Data Saver) - Npgsql напрямую, пакетный SQL (см. ADR-0002, D-2). EF Core - для CRUD-конфигурации и чтения в REST API (правила, роли, инциденты, настройки), где нужны трекинг изменений, миграции модели и LINQ; запросы пишутся по `$optimizing-ef-core-queries`. |
| Схема БД | Версионируемые идемпотентные SQL-скрипты в `db/migrations/` под `ktm_migrator`, создаются в рамках `add-data-saver-core` на базе DDL из `docs/design/Схема_БД_ER.md` п. 9; выбор инструмента и обоснование - ADR-0004. Приложения (Data Saver, API) **не применяют миграции при старте** и работают под `ktm_app` (`SELECT, INSERT` на журнал). |
| Тесты | **xUnit v2 (`xunit` 2.9.3)**, не v3 - см. «Выбор xUnit» ниже; `FakeTimeProvider` (`Microsoft.Extensions.TimeProvider.Testing`); FluentAssertions не обязателен (`Assert` достаточно); интеграционные - Testcontainers (Kafka + TimescaleDB), см. ADR-0002, D-12; запускаются локально (Docker на машине разработчика есть). |
| Время | UTC везде, `DateTimeOffset`; в Data Saver `TimeProvider` внедряется, `DateTime.UtcNow` запрещён. |
| Логи | `Microsoft.Extensions.Logging`, структурные поля (`topic`, `partition`, `offset`, `source`, `sourceEventId`), JSON-вывод в консоль в контейнере; без тел сообщений на уровне Information. Сроки хранения - по `docs/07`, п. 7.4 (забота инфраструктуры). |
| Метрики | `System.Diagnostics.Metrics` (`Meter`) в коде; экспортёр (Prometheus/OTel) - отдельное решение вместе с мониторингом `dev-infra`, в этот ADR не входит. |
| Конфигурация | Только переменные окружения (`__` как разделитель секций ASP.NET Core) + `appsettings.json` с **несекретными** значениями по умолчанию. Секреты - переменная или файл (`*_FILE`, Docker secrets); в репозиторий не попадают. Опции валидируются при старте (`ValidateOnStart`), сообщение об ошибке не содержит значений секретов. |
| Контейнеры | Dockerfile на каждый исполняемый проект (multi-stage, non-root пользователь); оркестрации нет (`openspec/changes/add-infrastructure-topology`, Р-8), dev - `docker compose` на узле `dev-backend`. |

### Структура solution (`src/Ktm/`)

Правило границ: `Core` не ссылается на Confluent.Kafka, Npgsql, EF Core, ASP.NET (домен/конвейер не зависит от
инфраструктуры, `docs/03`, п. 3.1). Зависимости направлены внутрь: Host -> Infrastructure -> Core. Граница обеспечивается
структурой ссылок проектов и ревью (`reviewer` сверяет ссылки с этим ADR); автоматического теста нет (см. «Правила тестов»).

```
Ktm.Api                          существующий REST API (код не меняется; изменения в `Ktm.Api.csproj`:
                                 (1) `<TargetFramework>net10.0</TargetFramework>` удалён - значение теперь задаёт
                                 `Directory.Build.props`; (2) версия `Microsoft.AspNetCore.OpenApi` убрана из
                                 `PackageReference` и перенесена в `Directory.Packages.props` (CPM);
                                 (3) версия обновлена 10.0.13 -> 10.0.15; (4) `Nullable`/`ImplicitUsings` из
                                 проекта убираются как дубль `Directory.Build.props` (задача developer, группа 1))
Ktm.DataSaver.Core               модель события, стадии конвейера, батчер, политика backoff, трекер offset,
                                 порты (IEventValidator, IWriteFailureHandler, IEventStore, IMessageSource, ...)
Ktm.DataSaver.Infrastructure     Confluent.Kafka (consumer/producer), Npgsql (EventStore), опции, health-проверки
Ktm.DataSaver                    host: Worker + минимальный Kestrel (/health/*), composition root, Dockerfile
tests/Ktm.DataSaver.Tests               unit
tests/Ktm.DataSaver.IntegrationTests    Testcontainers
```

Тесты лежат в `src/Ktm/tests/`. Общий код Level 2/3 в эти сборки не добавляется. Выделять общую сборку контрактов
(`Ktm.Contracts`) сейчас не нужно: Data Saver - единственный потребитель формата Kafka; выделим, когда появится второй.

### Правила тестов

Тесты на конфигурационные файлы и метафайлы не пишем: `csproj`, `props`/`targets`, `deps.json`, `.gitignore`,
`.editorconfig`, `.env.example`, разбор файлов проекта и сверка файлов с документами как тест. Тестируется поведение кода
(в том числе опции `DataSaver__*` с `ValidateOnStart`) и поведение БД (применение SQL-миграций на реальной БД в
интеграционных тестах). Соответствие метафайлов решениям ADR проверяет `reviewer`.

### Выбор xUnit v2 и версий тестовых пакетов

Решение: остаёмся на xUnit v2 (`xunit` 2.9.3, последняя 2.x). xUnit v3 (`xunit.v3` 4.0.x на 2026-11-20) вышел, но:
проекты v3 - самостоятельные исполняемые файлы с другой схемой пакетов и запуска, скиллы проекта (`$code-testing`,
`$run-tests`) и привычные инструменты рассчитаны на v2, а выигрыша для задач Data Saver (юнит-тесты конвейера,
Testcontainers) v3 не даёт. Миграция на v3 - отдельное решение вне этого change, когда появится потребность
(параллелизм/новые возможности) либо v2 перестанет получать исправления; её стоимость невысока (смена пакетов,
`OutputType`, `IAsyncLifetime`).

Версии (проверены по nuget.org 2026-11-20):

| Пакет | Версия | Примечание |
|---|---|---|
| `xunit` | 2.9.3 | последняя v2 |
| `xunit.runner.visualstudio` | 3.1.5 (4.0.1 допустим) | оба умеют запускать v1/v2/v3; берём 3.1.5 - ветка 3.x, без смены мажорной у раннера в этом change |
| `Microsoft.NET.Test.Sdk` | 18.12.0 | net8.0+; совместим с xunit 2.9.3 |
| `coverlet.collector` | 10.1.1 | актуальная; ветка 6.x устарела |
| `Microsoft.AspNetCore.OpenApi` | 10.0.15 | patch-обновление 10.0.x (в NuGet есть и 11.0.0 - не берём, платформа net10.0) |
| `Confluent.Kafka` | 2.16.0 | клиент Kafka, актуальная стабильная 2.x |
| `Npgsql` | 10.0.3 | драйвер PostgreSQL, ветка 10.x по платформе |
| `Testcontainers.Kafka` | 4.17.0 | интеграционные тесты, ADR-0002 D-12 |
| `Testcontainers.PostgreSql` | 4.17.0 | то же; образ TimescaleDB задаётся в фикстуре |
| `Microsoft.Extensions.TimeProvider.Testing` | 10.11.1 | `FakeTimeProvider` |
| `LateApexEarlySpeed.Json.Schema` | 5.0.0 | лицензия BSD-3-Clause (nuspec, без платных условий; https://licenses.nuget.org/BSD-3-Clause); проверка тела сообщения по JSON Schema 2020-12 (`docs/api/kafka-event.schema.json`), `format: date-time` проверяется; используется за `ISchemaValidator`, только в `Core/Decoding`. Встроенный `date-time` не принимает дробную часть секунд длиннее 7 цифр, поэтому подменён собственным форматом RFC 3339 (декодер сам усекает время до микросекунд) |

Выбор пакета для JSON Schema: `JsonSchema.Net` 9.7.0 отклонён по решению лида проекта - его бинарный релиз
распространяется под Open Source Maintenance Fee (плата при годовой выручке от 10 000 USD), что противоречит правилу
«только бесплатные библиотеки» (ниже).

Наблюдение: зависимости `LateApexEarlySpeed.Json.Schema` 5.0.0 - `LateApexEarlySpeed.Nullability.Generic` 1.0.4,
`Microsoft.Extensions.Http` 3.1.0, `System.Text.Json` 9.0.14. Это лёгкая транзитивность, попадающая в `Core`
(`Microsoft.Extensions.Http` не используется кодом Core, `HttpClient` не создаётся); граница Core (нет Kafka, Npgsql,
EF Core, ASP.NET) не нарушается, но состав транзитивных пакетов проверяется при обновлении версии.

### Правила пакетов

- Используются только бесплатные библиотеки. Лицензия бинарного релиза (а не только исходников репозитория)
  проверяется на NuGet (nuspec / страница пакета) до добавления пакета и при смене мажорной версии.
- Новый пакет или версия меняются в `src/Ktm/Directory.Packages.props`, пакет вне перечня - отдельным решением.

Единственный источник версий - `src/Ktm/Directory.Packages.props`; таблица выше - сводка на 2026-11-20 и при
расхождении уступает файлу. Новая версия или новый пакет меняются в файле, а не в `csproj`.

Совместимость подтверждается не чтением примечаний, а прогоном: `dotnet restore` без предупреждений NU1xxx,
`dotnet test` зелёный (обновление пакетов оформляется задачей developer, группа 1).

## 3. Альтернативы

- **Только EF Core везде.** EF Core не умеет `INSERT ... ON CONFLICT DO NOTHING` пакетом: `AddRange`/`SaveChanges` при
  конфликте падает целиком; обходы (сторонние bulk-расширения, перехват исключений с повторной вставкой по одной строке)
  либо тянут зависимость, либо ломают пропускную способность. Для горячего пути отклонено.
- **Только Npgsql везде.** Для CRUD API это ручной SQL и ручной маппинг без выигрыша. Отклонено.
- **Dapper.** Не даёт ничего сверх Npgsql для одного пакетного SQL; лишняя зависимость.
- **RabbitMQ вместо Kafka.** Kafka уже развёрнута и описана в инфраструктурном change; `docs/05` допускает оба варианта.
- **Один проект Data Saver без разделения Core/Infrastructure.** Быстрее на старте, но конвейер (стадии валидации,
  отбраковки, учёт offset) нельзя тестировать без Kafka и БД, а расширение под движок правил привело бы к ссылкам домена
  на инфраструктуру. Отклонено.

## 4. Последствия

- Любой пакет вне перечня выше добавляется отдельным решением (ADR или запись в `design.md` изменения).
- Скрипты миграции создаются в репозитории этим change (ADR-0004); отдельной поставки от B3 не ждём.
- Интеграционные тесты (Testcontainers) запускаются локально: Docker на машине разработчика есть. Открытым остаётся
  только вопрос CI: если в CI нет Docker, интеграционные тесты там не выполняются, и автоматическая приёмка по
  критериям Data Saver в CI невозможна (до решения по CI приёмочный прогон - локальный, протокол прикладывается).
- Уровни: код Level 2/3 (интерпретация, дашборды) не вызывает Data Saver напрямую; обмен - через БД журнала и Kafka.

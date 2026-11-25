# Handoff 2: движок отбраковки Data Saver, состояние на конец сессии 2026-11-23

Дополняет `2026-11-23-rules-engine-handoff.md` (решения, правила проекта - там). В git не коммитится.

## Состояние
Ветка `feature/add-data-saver-rules-engine`. **Ничего не закоммичено** (ни планирование, ни код). Docker на машине запущен, образ `timescale/timescaledb:2.32.1-pg16` есть.

Отмечено `[x]` в `openspec/changes/add-data-saver-rules-engine/tasks.md` (после ревью без Critical/Major): 0.1, 0.2, 1.1-1.5, 2.1-2.9, 3.1-3.4, 5.1, 5.2.

## Реализовано, но чекбокс НЕ отмечен
- **Группа 4 (4.1-4.4)** - код и тесты готовы (build `-warnaserror` чистый, unit 532/532, интеграционные 76/76 без Long/Perf), **ревью не завершено**: агент-reviewer упал по лимиту. Нужно запустить `reviewer` заново (файлы: `Infrastructure/Postgres/NpgsqlRuleSource.cs`, `Ktm.DataSaver/Hosting/{RuleRefreshService,DataSaverWorker}.cs`, `Configuration/DataSaverOptions.cs`, `DataSaverServiceCollectionExtensions.cs`, `Health/HealthEvaluator.cs`, тесты `Host/RuleRefreshServiceTests`, `RulesHostTests`, `Postgres/NpgsqlRuleSource*Tests`, `Acceptance/RulesLoadingAcceptanceTests`).
- **5.3** - метрики в `DataSaverMetrics` есть и приняты; чекбокс отметить после ревью группы 4 (метрики `rules.reloads/active/invalid/snapshot_age` теперь вызываются из `RuleRefreshService`).

## Прервано по лимиту (работа частично сохранена в рабочем дереве, надо проверить и добить)
1. **Группа 6 (6.1-6.5)** - developer писал интеграционные/перф-тесты (начал с support-хелперов). Состояние неизвестно: `git status`, build, затем продолжить или переписать. 6.6 - ручная на стенде, не делается.
2. **Сверка документов с реализацией** (teamlead): `docs/rules-engine.md`, `docs/manuals/data-saver.md`, `src/README.md` (7.1/7.3 написаны ранее, но не сверены с кодом), 7.2 (запись в `design.md` Open Questions про уведомление потребителя `rejected`). Что сверить: границы `MaxRules` (>=1, верхней нет); причина `rules_not_loaded` в `/health/ready` (добавить в таблицу кодов `data-saver.md` п.5); `Enabled=false` не регистрирует стадию/сервис; формат `ktm-reject-detail` по реальной трассе; тексты логов и частоты `LogThrottle`; `trigger=signal`; хэш правил включает `enabled`; лимиты компилятора (строка 1024, name 1..200, форматы `reason_code`/`rule_id`, `source='*'` отвергается движком - адаптер мапит в null); число вне `decimal` в литерале = ошибка загрузки (подтвердить у лида); расхождение `kafka-event-format.md:79` и `:89` про дублирование rule-id в detail; висящая ссылка «п. ниже» в ADR-0005:381. Не проверялись условия-примеры из документа (рекомендовано прогнать через компилятор, добавить в тест 2.9).

## Не начато
- 8.1-8.4 (общий прогон, сверка критериев, хвост 10.1/10.2 архива без правки архивных файлов, синхронизация спек и правка `Purpose` в `openspec/specs/data-saver/ingestion-pipeline/spec.md`).
- 6.6 и выкладка на dev.

## Открытые вопросы к лиду
- Коммитить ли артефакты планирования и по какому разбиению коммитов (всё ещё не коммитили).
- Kafka-консьюмер создаётся при построении DI (до первой загрузки правил); `Consume`/commit до загрузки не вызываются (`messages.read` = 0), но создание не ленивое. Нужна ли переделка `KafkaMessageSource`?
- `RuleRefreshService` лежит в `Ktm.DataSaver/Hosting`, а не в `Infrastructure/Postgres` (как в ADR п. 2.1): нужны `StartupErrors` (internal) и `DataSaverWorker`.
- Число вне `decimal` в литерале правила - ошибка загрузки (ADR п. 2.6 говорит о поле; решение реализации).
- Лимит условия 16 КиБ: в `V002` стоит `pg_column_size` (размер хранения, не логический), компилятор размер не проверяет; ADR требует логические 16 КиБ - нужен `octet_length(condition::text)` и проверка в компиляторе.
- Из прежнего списка: роль-редактор правил вместо `ktm_migrator`; режим старта при недоступной БД правил («ждать с backoff»).

## Принятые, но не закрытые minor (не блокируют, при желании - отдельной задачей)
Группа 1-2 (17 minor): валидация только БД-CHECK-ами для части полей, пробелы в покрытии `Rfc3339`/`FieldPath`, исключение на смещениях +15..+23:59 в `Rfc3339` (горячий путь), аллокация `GetString` при сравнении дат, схема не ограничивает длину строк и зону дат, хрупкий тест данных `V003`, неточные имена нескольких тестов.
Группа 3: длина `name` в UTF-16 вместо code points (расхождение с `length(btrim(name))` в БД), сообщение об ошибке `source` упоминает `*`, `HolderWith` лежит в тестах Rules, `ArgumentNullException` при `Id == null` из порта, `await pump` может зависнуть при сигнале, игнорирующем токен; сбой сигнала прерывает и таймер; `MaxRules` считает только компилируемые правила; `Replace` снимка и времени - две отдельные записи.
Группа 5: Warning на каждую отбраковку без троттлинга; теги `result/trigger` - строки без констант; пустой `RuleId` попадёт в метрику.

## Как продолжить
1. `git status`, `dotnet build src/Ktm/Ktm.sln -warnaserror`, unit и интеграционные (`Category!=Long&Category!=Perf`) - убедиться, что дерево собирается после оборванных агентов.
2. `reviewer` по группе 4 -> при отсутствии Critical/Major отметить 4.1-4.4 и 5.3.
3. Закончить группу 6 (`developer`), ревью, отметить.
4. Закончить сверку документов (`teamlead`), ревью документов, отметить 7.1-7.3.
5. Группа 8. Пуш и PR - только по команде лида, в `main` не пушить.
Роли - только через субагентов (`teamlead` не может запускать `developer` сам: поручение передаёт основная сессия).

# Handoff: движок отбраковки Data Saver, состояние на 2026-11-23

Файл для переноса контекста в новую сессию. В git не коммитится (`docs/handoff/` неотслеживаемый). Дополняет `2026-11-22-data-saver-handoff.md`; из того файла устарело: изменения `add-data-saver-core` и `add-data-saver-migrate-job` уже архивированы (PR #10-#12 в `main`, `openspec/changes/archive/2026-11-22-*`, основные спеки в `openspec/specs/data-saver/*`), протокол выкладки на dev есть (`docs/test-protocols/data-saver-deployment-dev.md`).

## 1. Задача

Движок правил отбраковки в Data Saver: правило описывает условие на поля события; сработало - событие не пишется в БД, уходит в `ktm.events.rejected` с `id` и названием правила и причиной, плюс warning в лог. Правила без пересборки (кеш, перечитывается раз в минуту), метрики отбракованного по источникам и правилам, `docs/rules-engine.md` для человека, не читавшего код. Критерии готовности и исходный текст задачи - в `openspec/changes/add-data-saver-rules-engine/proposal.md` и `tasks.md` (таблица «критерий -> задача»).

## 2. Где мы

Ветка `feature/add-data-saver-rules-engine` (от `main` 94089bc). **Ничего не закоммичено**, код не писался. В рабочем дереве (неотслеживаемое/изменённое):
- `docs/adr/0005-data-saver-rules-engine.md` (ред. 3) - **ADR принят лидом 2026-11-23**;
- `docs/api/rejection-condition.schema.json` - JSON Schema поля `condition`;
- `docs/api/kafka-event-format.md` п. 3 (блок про отбраковку правилом), `docs/adr/0003-...` (одна строка);
- `openspec/changes/add-data-saver-rules-engine/` - proposal, design, tasks (8 групп), specs (`rules-engine`, MODIFIED-дельты `event-ingestion`, `ingestion-pipeline`). `openspec validate` проходит.

Следующий шаг: отметить 0.2 (Docker/образ TimescaleDB `2.32.1-pg16` для тестов), закоммитить артефакты планирования на ветке (с лидом согласовать), затем `$openspec-apply-change`: `developer` по группам tasks.md 1 -> 8, после каждой группы `reviewer` (блок только Critical/Major).

## 3. Принятые решения (кратко; полностью - ADR-0005)

- **Источник правил - таблица `ktm.rejection_rules` (урезанная БД).** Прошли путь: БД (полная, ред. 1) -> JSON-файл (ред. 2) -> урезанная БД (ред. 3, решение лида). Колонки: `rule_id`, `name`, `description`, `source` (`'*'` = все), `condition` jsonb, `reason_code`, `enabled`, `priority`, `version` (ведёт автор вручную). `source` и `enabled` без умолчаний. Миграции: `V002__rejection_rules.sql` (схема, CHECK, `ktm_app` только `SELECT`) и отдельная `V003__rejection_rules_examples.sql` (выключенные примеры skud/cv/1c-do; `ON CONFLICT DO NOTHING`, примеры выключают, не удаляют).
- **Без**: триггера версии, аудита, `LISTEN/NOTIFY`, JSON-файла, перечитывания «по команде». Обновление - только кеш внутри сервиса раз в минуту (`DataSaver__Rules__*`, интервал 5..3600 с). Лид подтвердил: `NOTIFY` не нужен вообще. Шов `IRuleChangeSignal` оставлен пустой точкой расширения. Аудит отсутствует - временное отступление от `docs/04` п. 4.2/4.4; триггер возврата (приложение A ADR): первое реальное правило или потребность в аудите.
- **Условие - нотация JsonLogic, вычислитель собственный, библиотека не подключается.** Операции первой итерации: `==`, `!=`, `<`, `<=`, `>`, `>=`, `between` (трёхаргументный `<=`, границы включительно - решение лида), `and`, `or`, `!`, `var` с точечным путём (`payload.confidence`). Строгие типы без приведения, числа `decimal`, даты RFC 3339 с зоной; отсутствующее поле на сравнениях даёт ложь; `!=` - строгое отрицание `==` (на отсутствующем поле истинно, ловушка описывается в `docs/rules-engine.md`). Отложены (реестр операторов, архитектура не закрыта): `in`, `contains`, `matches` (регэксп; политика NonBacktracking + таймаут 50 мс сохранена в ADR), `is_empty`/`is_not_empty`, `ignoreCase`. Пример из исходной задачи (пустой `externalPersonId` ИЛИ `deviceId ~ ^TEST-.*`) пока не выражается - нужны `is_empty` и регэксп; если нужен в первом реальном правиле, добавить первыми.
- **Готовые библиотеки отклонены** (ADR п. 3.1): Microsoft RulesEngine, DynamicExpresso, NCalc (компиляция/строки), NRules (правила - код), Corvus JsonLogic (чужой JSON-стек, семантика null/типов не наша), Goplasmatic.Datalogic, GoRules.Zen (нативные бинарники), Cel.NET (тяжёлые зависимости), json-everything `JsonLogic`/`JsonPath.Net` (лицензия OSMFEULA - платная для бинарников на NuGet), `JsonLogic.Net` (Newtonsoft 9.0.1 с уязвимостью). Условие пересмотра - потребность в арифметике/функциях -> CEL или Corvus отдельным ADR. Правило проекта: только MIT/Apache/BSD, проверка лицензии бинарного релиза.
- **Порядок стадий:** формат (уже в `add-data-saver-core`: `malformed_json`, `unknown_schema_version`, `invalid_schema`) -> C5 -> правила. Срабатывает первое правило по `priority`, затем `rule_id`; в `rejected` уходит оно одно. Тот же топик `ktm.events.rejected`; заголовки `ktm-reject-reason` (= `reason_code` правила), `ktm-reject-rule-id`, новые `ktm-reject-rule-name`, `ktm-reject-rule-version`, `ktm-reject-detail`. В логе - Warning для правила (Error остаётся для формата и C5). Правила применяются и к корректирующим сообщениям.
- **Сбои:** ошибочное правило выпадает одно (Error в лог), остальные работают; сбой чтения БД после первой загрузки - держим последний валидный набор; пустая таблица - штатно; БД недоступна на первой загрузке - повтор с backoff, Kafka не читается, `/health/ready` = 503; код 78 - неверные настройки `DataSaver__Rules__*` и `42P01`/`42501` при первой загрузке.
- **Метрики:** `rules.rejected{source,rule_id}` (rule_id только из загруженного набора), `messages.rejected{reason="rule_matched"}`, `rules.active`, `rules.invalid`, `rules.reloads{result,trigger=timer|startup}`, `rules.snapshot_age`.
- **Принято по умолчанию (лид не оспаривал):** правки правил SQL под `ktm_migrator` до появления отдельной роли-редактора; `version` вручную (забытый инкремент ловит только Warning).

## 4. Правила проекта (CLAUDE.md и память)

Роли только через субагентов: `teamlead` (ADR, артефакты OpenSpec, документы, разбор замечаний), `developer` (код, тесты), `reviewer` (ревью). Сессия сама их не пишет, ведёт `tasks.md` (чекбокс после ревью без Critical/Major). Ветка `feature/add-data-saver-rules-engine`, пуш и PR только по команде лида (лид создаёт PR сам, `gh` на машине нет), в `main` не пушить никогда. Только бесплатные библиотеки; тесты на поведение кода, не на конфиги/метафайлы. Память: `C:\Users\tlustyx\.claude\projects\C--Users-tlustyx-RiderProjects-ktm-backend\memory\`.

## 5. Открытые вопросы

К лиду (не блокируют начало): отдельная роль-редактор правил вместо `ktm_migrator` - когда; режим старта при недоступной БД правил оставлен «ждать с backoff».
К предприятию: реальные критерии брака по источникам и список полей `payload` (опечатки в путях внутри `payload` при загрузке не ловятся); срок хранения `ktm.events.rejected` и потребитель новых заголовков (согласовать контракт, задача 7.2).
Из прежнего handoff остаются: выкладка на `dev-backend` (протокол есть), хвосты блока 10 архивного `tasks.md` (кроме 10.1/10.2, закрываемых этим изменением; при архивации - отметка в отчёте, архивный файл не правим; задача 8.4 - вручную убрать из `Purpose` спеки `ingestion-pipeline` фразу «движок вне объёма»).

## 6. Как продолжить

Прочитай этот файл, `CLAUDE.md`, ADR-0005, `openspec/changes/add-data-saver-rules-engine/tasks.md`. `git status` на ветке `feature/add-data-saver-rules-engine` должен показать артефакты планирования неотслеживаемыми. Первый вопрос лиду: коммитим артефакты планирования и начинаем `apply` с группы 1 (миграции V002/V003 и права)?

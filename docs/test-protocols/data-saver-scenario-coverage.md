# Data Saver: сверка сценариев спеков с тестами (задача 11.1)

Изменение OpenSpec `add-data-saver-core`, ветка `feature/data-saver-core`. Источник сценариев:
`openspec/changes/add-data-saver-core/specs/data-saver/event-ingestion/spec.md` (27 сценариев) и
`.../ingestion-pipeline/spec.md` (11 сценариев). Всего 38.

Обозначения тестовых проектов: **U** - `src/Ktm/tests/Ktm.DataSaver.Tests` (юнит), **I** - `src/Ktm/tests/Ktm.DataSaver.IntegrationTests`
(Kafka и Postgres в контейнерах). Категории: без метки - обычный прогон, `Long` - 2 минуты простоя БД, `Perf` - нагрузка (запускается вручную).
Столбец «Статус»: **Покрыт** - есть тест, проверяющий утверждение THEN; **Частично** - тест есть, но часть
утверждения проверяется косвенно или другим уровнем; **Закрыт иначе** - автотеста нет, утверждение подтверждается другим способом;
**ПРОБЕЛ** - явного теста на утверждение нет.

Прогон тестов в этой задаче не выполнялся (это 11.2); таблица составлена чтением кода тестов и продуктового кода.

## event-ingestion

| # | Requirement / Scenario | Тест (проект: файл, класс.метод) | Статус |
|---|---|---|---|
| 1 | Чтение топика / Событие доходит до журнала | I: Acceptance/IngestionAcceptanceTests.cs, `IngestionAcceptanceTests.SingleEvent_AppearsInJournalWithinFiveSeconds_WithDefaultSettings_UnderAppRole` | Покрыт |
| 2 | Чтение топика / Несколько источников в одном топике | I: Kafka/KafkaMessageSourceTests.cs, `KafkaMessageSourceTests.Reads_events_of_all_sources_from_raw_topic_and_ignores_rejected` (чтение `skud`/`cv`/`1c-do`); попадание всех трёх источников в журнал - сквозной `Perf`-прогон `Acceptance/ThroughputPerfTests.cs` (источники `skud`, `cv`, `1c-do`) | Частично: в обычном прогоне сквозного теста «три источника -> журнал» нет, только чтение и `Perf` |
| 3 | Пакетная запись / Дубли внутри потока | I: `IngestionAcceptanceTests.HundredMessagesWithTenDuplicates_Give90Rows_DuplicatesMetric10_NoErrorsLogged`; на уровне БД `Postgres/NpgsqlEventStoreTests.Duplicates_inside_batch_and_repeated_batch_do_not_create_rows`; метрики `Host/DataSaverHostTests.HealthyService_IsReady_AndMetricsMatchTheRun_90Written10Duplicates1Rejected`; U: `Host/HostBehaviorTests.StoreMetrics_CountWrittenAndDuplicatesAndCorrections_AndRetryOnFailure` | Покрыт |
| 4 | Пакетная запись / Закрытие пачки по размеру и по времени | U: Batching/BatcherTests.cs, `BatcherTests.Closes_by_size_and_starts_new_batch`, `Closes_by_time_counted_from_first_message_not_from_last`, `Wait_is_restarted_by_first_message_of_next_batch`; I: `NpgsqlEventStoreTests.Batch_of_500_is_written_by_one_call_and_rows_match_messages`; пачки по 500 на `Perf` (`Sustained_load_at_target_rate_has_no_lag_growth_and_reports_latency`, протокол `data-saver-throughput.md`, строка «Пачки»); одиночное событие в простое попадает в журнал за 5 с - тест 1 | Покрыт (закрытие по времени и по размеру проверено юнитом на логике батчера; отдельного интеграционного теста «поток, затем одиночное событие» нет) |
| 5 | Пакетная запись / Повторная доставка после сбоя | I: `RestartAndScalingAcceptanceTests.CrashWithoutCommit_ThenRestart_Exactly5000Keys_RedeliveredAreDeduplicated`; `NpgsqlEventStoreTests.Store_does_not_require_update_privilege`; `Postgres/DatabaseSetupTests.App_role_can_insert_but_cannot_update_or_delete`, `Owner_cannot_modify_journal_either_because_of_trigger` | Покрыт |
| 6 | Фиксация offset / Сбой записи | U: Ingestion/IngestionLoopTests.cs, `TransientWriteError_PausesRetriesSameBatchKeepsPolling_ThenResumesAndCommits` (нет commit до успеха), `FatalConfigError_IsRetriedInDegradedState_AndNotifiesAdminOnEveryAttempt`; I: `NpgsqlEventStoreTests.Constraint_violation_in_batch_surfaces_as_data_error_and_writes_nothing` (откат пачки целиком), `OutageAcceptanceTests.DatabaseDown_CommittedOffsetStaysPut_ThenGrowsOnlyBehindRowsInDatabase` | Покрыт |
| 7 | Фиксация offset / Падение между записью и фиксацией offset | I: `RestartAndScalingAcceptanceTests.CrashWithoutCommit_ThenRestart_Exactly5000Keys_RedeliveredAreDeduplicated`; `BrokerOutageAcceptanceTests.CommitAgainstFrozenBroker_HangIsVisibleAsLiveness503_HostStopIsBounded_NoLossOnRestart` (`Long`); U: `IngestionLoopTests.CommitFailure_IsLoggedAndLoopContinuesWithNextBatch` | Покрыт |
| 8 | Корректирующие / Корректировка существующего события | I: Postgres/NpgsqlEventStoreTests.cs, `Correction_of_existing_event_links_original_and_leaves_it_unchanged`; `Acceptance/IngestionAcceptanceTests.Corrections_LinkToExistingOriginal_OrStayUnlinked_RepeatsAreIdempotent_OriginalRowUntouched` | Покрыт |
| 9 | Корректирующие / Корректировка раньше исходного | I: `NpgsqlEventStoreTests.Correction_before_original_has_null_link_and_repeat_adds_nothing`; `IngestionAcceptanceTests.Corrections_LinkToExistingOriginal_OrStayUnlinked_...` | Покрыт |
| 10 | Корректирующие / Повтор корректировки | I: `NpgsqlEventStoreTests.Correction_before_original_has_null_link_and_repeat_adds_nothing` (повтор: `EventWriteResult(0, 0)`), `IngestionAcceptanceTests.Corrections_LinkToExistingOriginal_OrStayUnlinked_RepeatsAreIdempotent_...` | Покрыт |
| 11 | Устойчивость к БД / БД остановлена на 2 минуты | I: Acceptance/OutageAcceptanceTests.cs, `DatabaseDownFor2Minutes_NoLossNoDuplicates_NoRestart_NoRebalance_BackoffCapped` (`Long`); обычный прогон - `DatabaseDownFor15Seconds_NoLossNoDuplicates_NoRestart_NoRebalance_BackoffGrows`; U: `IngestionLoopTests.TransientWriteError_...`, `PartitionAssignedWhileDegraded_IsPausedImmediately` | Покрыт (результат `Long` приложит 11.2) |
| 12 | Устойчивость к БД / Нарастающая пауза | U: Resilience/BackoffPolicyTests.cs, `Delays_double_up_to_ceiling_and_stay_there`, `Jitter_stays_within_twenty_percent_at_both_extremes`, `Ceiling_is_hard_after_jitter`, `Reset_restarts_from_initial_delay`; I: `OutageAcceptanceTests.DatabaseDownFor15Seconds_...BackoffGrows`, `DatabaseDownFor2Minutes_...BackoffCapped` | Покрыт |
| 13 | Корректная остановка / Остановка посреди обработки | I: `RestartAndScalingAcceptanceTests.GracefulStopAtRandomMoment_ThenRestart_Exactly5000Keys_NothingReread`; U: `IngestionLoopTests.Stop_FinishesAccumulatedBatchWithWriteAndCommit`, `Stop_DuringBackoffWait_TriesOnceMoreImmediatelyAndCommitsOnSuccess` | Покрыт |
| 14 | Корректная остановка / Остановка при недоступной БД | U: `IngestionLoopTests.Stop_WhileDatabaseIsDown_ExitsAtDeadlineWithoutCommit`, `StopAlreadySignalled_RetriesImmediatelyOnce_ThenWaitsWithinDeadline`, `HungWriteInFlight_IsCancelledByDeadlineCountedFromStopSignal_WithoutCommit`; I: `BrokerOutageAcceptanceTests.CommitAgainstFrozenBroker_...HostStopIsBounded...` (ограниченная остановка при зависшем брокере) | Частично: «остановка при недоступной БД» проверена на юнит-уровне с поддельным хранилищем; интеграционного теста с остановленной реальной БД и замером предельного времени нет |
| 15 | Масштабирование / Два экземпляра | I: `RestartAndScalingAcceptanceTests.TwoInstancesInOneGroup_SplitPartitions_ThenOneStops_EveryEventOnce`; `Kafka/KafkaMessageSourceTests.Two_instances_of_same_group_split_partitions_and_report_changes` | Покрыт |
| 16 | Масштабирование / Остановка одного экземпляра | I: `RestartAndScalingAcceptanceTests.TwoInstancesInOneGroup_SplitPartitions_ThenOneStops_EveryEventOnce`; U: `IngestionLoopTests.RevokedPartition_MessagesAccumulatedForIt_AreDroppedWithoutWriteOrOffset`, `AllMessagesRevoked_NothingIsCommitted` | Покрыт |
| 17 | Конфигурация / Отсутствует обязательная настройка | U: Host/DataSaverOptionsTests.cs, `MissingRequiredSettings_FailStartupNamingEachSettingWithoutSecrets`; I: Host/ConfigExitCodeTests.cs, `InvalidConfiguration_ExitsWith78_AndOneLineWithSettingName_NoStackTraceNoSecrets` | Покрыт |
| 18 | Конфигурация / Проверка репозитория на секреты | Автотеста нет (по правилам проекта тесты на конфиги и метафайлы не пишутся). Разовая проверка при сверке: `git grep` по `deploy/`, `db/`, `*.yml`, `*.sql`, `*.json` и коду не находит значений паролей; из env-файлов в `git ls-files` только `deploy/data-saver/.env.example` (пароль - файлом `...PasswordFile`, остальное - заглушки `<...>`); в `db/README.md` пароли вводятся интерактивно. Поведенческая часть требования (секреты не попадают в лог и сообщения): `DataSaverOptionsTests.InvalidValues_FailWithSettingNames_AndNeverEchoSecrets`, `PostgresOptionsTests.Password_from_file_is_applied_with_pool_timeout_and_keepalive_and_never_printed` | Закрыт иначе (ручная проверка + пункт чек-листа 11.4); автотеста нет |
| 19 | Проверки состояния / БД недоступна | I: Host/DataSaverHostTests.cs, `DatabaseStopped_ReadinessIs503_ButLivenessStays200`; U: Host/HostBehaviorTests.cs, `Readiness_RequiresAssignmentRunningStateBrokerAndDatabase`, `Liveness_IgnoresDatabaseBrokerAndDegraded_ButFailsOnStalledLoopOrFatalConsumer`; U: Host/HealthProbeTests.cs, `DatabaseProbe_DisposedDataSourceIsUnavailable_...` | Покрыт |
| 20 | Проверки состояния / Брокер недоступен | I: `DataSaverHostTests.BrokerStopped_ReadinessIs503BrokerUnavailableWithinProbeWindow_LivenessStays200`; `BrokerOutageAcceptanceTests.BrokerPausedThenResumed_ReadinessGoesBrokerUnavailable_ThenBack200_AndIngestionContinues`; U: `HostBehaviorTests.Readiness_BrokerUnavailableWhileAssignmentStillHeld_Is503BrokerUnavailable_LivenessStays200_AndRecovers`, `HealthProbeTests.BrokerProbe_HungCheckIsUnavailableWithinLimit_...`, Kafka/KafkaMessageSourceUnitTests.cs `HasAssignment_FollowsRebalanceHandlers_WithoutAskingTheConsumer` | Покрыт |
| 21 | Проверки состояния / Топик отсутствует | I: `DataSaverHostTests.MissingTopic_ReadinessIs503NoPartitionsAssigned_NotBrokerUnavailable`; `KafkaMessageSourceTests.ProbeBroker_for_missing_topic_does_not_fail_because_broker_is_available` | Покрыт |
| 22 | Проверки состояния / Брокер восстановлен | I: `BrokerOutageAcceptanceTests.BrokerPausedThenResumed_...Back200_...`; U: `HostBehaviorTests.Readiness_BrokerUnavailableWhileAssignmentStillHeld_..._AndRecovers`, `HealthProbeTests.BrokerProbe_FailureAndDisposedClientAreUnavailable_AndRecoveryIsSeenAfterTtl` | Покрыт |
| 23 | Проверки состояния / Штатная работа | I: `DataSaverHostTests.HealthyService_IsReady_AndMetricsMatchTheRun_90Written10Duplicates1Rejected`; U: `HostBehaviorTests.Readiness_RequiresAssignmentRunningStateBrokerAndDatabase` | Покрыт |
| 24 | Наблюдаемость / Отбраковка разобранного события | U: `HostBehaviorTests.RejectionPublisher_ParsedEventRejection_LogsSourceAndSourceEventIdFromEnvelope`; `IngestionLoopTests.StageRejection_PassesOriginToPublisher_ButUnparsedMessageHasNone` | Покрыт |
| 25 | Наблюдаемость / Отбраковка неразобранного сообщения | U: `HostBehaviorTests.RejectionPublisher_CountsByReason_LogsErrorWithCoordinatesAndNoBody`; `IngestionLoopTests.StageRejection_PassesOriginToPublisher_ButUnparsedMessageHasNone` | Покрыт |
| 26 | Наблюдаемость / Сбой записи пачки | U: `IngestionLoopTests.WriteFailure_LogsRecordPerPartitionGroupOfAcceptedEvents_AndHeartbeatAdvances`, `DataWriteFailure_NeverLogsExceptionText_OnlyErrorTypeAndSqlState` | Покрыт |
| 27 | Наблюдаемость / Измерение пропускной способности | I (`Perf`): Acceptance/ThroughputPerfTests.cs, `Sustained_load_at_target_rate_has_no_lag_growth_and_reports_latency`, `Backlog_drain_gives_maximum_single_instance_rate_and_insert_plan_on_filled_table`; протокол `docs/test-protocols/data-saver-throughput.md` (соб/с на устойчивом интервале, задержка p50/p99) | Покрыт |

## ingestion-pipeline

| # | Requirement / Scenario | Тест (проект: файл, класс.метод) | Статус |
|---|---|---|---|
| 28 | Формат и отбраковка / Неизвестная версия | I: Acceptance/IngestionAcceptanceTests.cs, `RejectedMessages_GoToRejectedTopicWithHeaders_NothingInJournal_OffsetCommitted_PartitionNotBlocked` (`schemaVersion=2`: тело и ключ, заголовки, в `events` пусто, offset, следующее событие записано); U: Decoding/EventDecoderTests.cs (теория отбраковки: `schemaVersion` 2, строка, отсутствует); Kafka/KafkaRejectionMessageTests.cs | Покрыт |
| 29 | Формат и отбраковка / Не JSON | I: тот же `RejectedMessages_GoToRejectedTopicWithHeaders_...` (`malformed_json`, чтение не блокируется); U: `EventDecoderJsonbCompatibilityTests.Decode_DuplicatePropertyNames_AreRejectedAsMalformedJson`, `Decode_Utf8Bom_IsRejectedAsMalformedJson` | Покрыт |
| 30 | Формат и отбраковка / Пачка из одних отбракованных | U: `IngestionLoopTests.BatchOfOnlyRejected_IsNotWritten_ButIsCommitted`, Batching/OffsetTrackerTests.cs `Batch_of_only_rejected_messages_still_advances_offset`; I: `RejectedMessages_GoToRejectedTopicWithHeaders_...` (пачка из пяти отбракованных, `RowsAsync() == 0`, committed == 5) | Покрыт |
| 31 | Формат и отбраковка / Ошибка публикации в rejected | U: `IngestionLoopTests.RejectedPublishFailure_NeverCommitsBeforeSuccess_AndDoesNotRewriteBatch` (теория: результат-ошибка и исключение), `RejectedPublishNeverSucceeds_StopEndsLoopWithoutCommit`; I: Kafka/KafkaRejectionPublisherTests.cs `Unreachable_broker_yields_failure_result_instead_of_exception` | Покрыт |
| 32 | Порт C5 / Заглушка C5 | U: Pipeline/IngestionPipelineTests.cs, `Process_AcceptAllValidator_AcceptsAnyEventType` | Покрыт |
| 33 | Порт C5 / Порт отбраковал событие | U: `IngestionPipelineTests.Process_ValidatorRejection_BecomesValidationKindRejection`, `Process_PortOrStageException_IsNotARejection` (исключение порта - сбой пачки, не отбраковка); `IngestionLoopTests.PipelineStageException_IsBatchFailure_RetriedWithoutLossOrRejection`; I: `IngestionAcceptanceTests.ExtensionStage_RejectsEveryThirdToItsTopicWithRuleId_...` (отбракованное не в журнал, в топик отбраковки, offset вперёд) | Покрыт (отбраковка портом проверена юнитом конвейера, сквозной эффект - тестом стадии; интеграционного теста именно с реализацией `IEventValidator` нет) |
| 34 | Порт C6 / Временная ошибка | U: `IngestionLoopTests.TransientWriteError_PausesRetriesSameBatchKeepsPolling_ThenResumesAndCommits` (счётчик `FakeFailureHandler.Calls == 0`). Поведение обеспечено кодом: `Ktm.DataSaver.Core/Ingestion/IngestionLoop.cs` (около стр. 296) вызывает `failureHandler.HandleAsync` только при `errorClass == WriteErrorClass.Data`; классификация проверена в Resilience/WriteErrorClassifierTests.cs (`Timeout_and_socket_break_are_transient`, `Unknown_exception_is_transient_so_nothing_is_lost`) | Покрыт |
| 35 | Порт C6 / Ошибка данных | U: `IngestionLoopTests.DataError_DeadLetteredMessageIsExcludedFromRewrite_ButItsOffsetIsCommitted` (порт получает `WriteFailureContext` с пачкой, ошибкой и попыткой); `IngestionPipelineTests.BlockingWriteFailureHandler_AlwaysRetries_AndLosesNothing` (заглушка всегда `Retry`, ничего не отбраковывает); `IngestionLoopTests.DataError_WithBlockingStub_RetriesSameBatch_StaysDegraded_AndCommitsNothing` (цикл с настоящим `BlockingWriteFailureHandler`: повтор той же пачки, `Degraded`, commit не было); I: `NpgsqlEventStoreTests.Constraint_violation_in_batch_surfaces_as_data_error_and_writes_nothing` (реальная ошибка 23514 -> класс `Data`, пачка откатывается) | Покрыт |
| 36 | Точка расширения / Тестовая стадия отбраковывает часть сообщений | I: `IngestionAcceptanceTests.ExtensionStage_RejectsEveryThirdToItsTopicWithRuleId_OnlyAcceptedInJournal_OffsetIsLastPlusOne`; U: `IngestionPipelineTests.Process_FirstRejectionStopsChain_AndKeepsRuleInfo`, `Process_RunsStagesInRegistrationOrder_AndAcceptsWithParsedEvent`; Kafka/KafkaRejectionMessageTests.cs `RuleId_goes_to_header_as_utf8_for_any_kind_and_topic_and_is_not_duplicated_in_detail`, `Long_RuleId_is_truncated_to_128_chars_without_splitting_a_surrogate_pair`; I: `KafkaRejectionPublisherTests.Message_is_routed_to_target_topic_when_specified_else_default` | Покрыт |
| 37 | Точка расширения / Отбраковка без правила не несёт идентификатор | U: `KafkaRejectionMessageTests.Missing_or_blank_RuleId_gives_no_header`; I: `KafkaRejectionPublisherTests.Published_message_keeps_key_body_and_carries_all_reject_headers`, `IngestionAcceptanceTests.RejectedMessages_GoToRejectedTopicWithHeaders_...` (для всех отбракованных форматом, включая `malformed_json`, проверяет `DoesNotContain("ktm-reject-rule-id", record.Headers.Keys)`) | Покрыт |
| 38 | Точка расширения / Стадия не знает о цикле | Поведенческого теста на такое утверждение нет (это свойство структуры кода). Косвенное подтверждение: стадия `EveryThirdMessageRule` регистрируется через DI (`services: s => s.AddSingleton<IIngestionStage>(...)`) в `ExtensionStage_RejectsEveryThirdToItsTopicWithRuleId_...` и работает без правок цикла | Закрыт иначе (структурно: тест 36 + ревью 11.4); автотеста нет |

## Итог

- Сценариев: 38. **Покрыт**: 34. **Частично**: 2 (№ 2, 14). **Закрыт иначе, без автотеста**: 2 (№ 18, 38). **ПРОБЕЛ**: 0.
- Условие 11.1 («непокрытых сценариев нет») не выполнено, пока не решена судьба сценариев «частично» и «закрыт иначе»; чекбокс 11.1 не отмечен.

### Список пробелов для разбора

1. ~~**№ 34 ПРОБЕЛ (C6, временная ошибка).** Добавить в `IngestionLoopTests` проверку, что при временной ошибке записи (`IOException`) `FakeFailureHandler` не вызывался (счётчик вызовов в `Ingestion/IngestionHarness.cs`), а при ошибке данных вызывался.~~ Закрыт.
2. ~~**№ 35 частично.** Тест цикла с реальным `BlockingWriteFailureHandler` на ошибке данных (`FakeDbException("23505")`): запись повторяется, состояние `Degraded`, offset не зафиксирован, пачка не потеряна.~~ Закрыт.
3. ~~**№ 37 частично.** В `RejectedMessages_GoToRejectedTopicWithHeaders_...` добавить утверждение, что заголовка `ktm-reject-rule-id` у сообщений `malformed_json` нет.~~ Закрыт.
4. **№ 14 частично.** Интеграционный тест остановки при остановленной БД (`StoppableDatabase`): процесс завершается в пределах `Shutdown:TimeoutSeconds`, committed offset не вырос, после восстановления БД и перезапуска нет потерь. Либо принять юнит-покрытие решением в ЧТЗ.
5. **№ 2 частично.** Обычный (не `Perf`) тест «три источника -> три строки в журнале» либо принять покрытие `Perf`-прогоном.
6. **№ 18 и № 38.** Решения без автотеста (ручная проверка репозитория на секреты; свойство структуры). Принять как «закрыто иначе» в приёмке или оформить пунктами чек-листа ревью 11.4.

## Коды выхода

Ручной прогон 11.5.8: реальный процесс `Ktm.DataSaver` (`dotnet publish -c Release`, запуск `dotnet Ktm.DataSaver.dll` как
дочернего процесса, не `WebApplicationFactory`), код выхода читался из `Process.ExitCode` (PowerShell) или `$?` (bash).
Дата: 2026-11-21. Версия коммита: `26a69cc` (`feature/data-saver-core`). Инфраструктура в Docker: `apache/kafka:3.9.1`
(KRaft, один брокер, порт 19092, топики `ktm.events.raw` и `ktm.events.rejected` по одной партиции) и
`timescale/timescaledb:2.32.1-pg16` (порт 15432, база `ktm`, роль `postgres`, `SslMode=Disable`; схема журнала не
создавалась, событий в топик не писалось: для проверяемых путей БД не нужна). Общие переменные запуска:
`DataSaver__Kafka__BootstrapServers=localhost:19092`, `DataSaver__Postgres__{Host,Port,Database,Username,Password,SslMode}`,
`DataSaver__Health__Port=<свободный порт>`.

| Код | Случай | Способ воспроизведения | Команда | Наблюдаемый код | Патч |
|---|---|---|---|---|---|
| 0 | Штатная остановка | Процесс проработал 20 с и получил `CTRL_BREAK_EVENT` в своей консоли (аналог Ctrl+C: `ConsoleLifetime` -> `StopApplication`) | `dotnet Ktm.DataSaver.dll`; сигнал из отдельного процесса PowerShell через `AttachConsole` + `GenerateConsoleCtrlEvent(1, 0)` | **0**; в логе «Application is shutting down», «Data Saver ingestion loop finished» | нет |
| 78 | Ошибка конфигурации | Запуск без обязательных настроек | `dotnet Ktm.DataSaver.dll` (переменные `DataSaver__*` не заданы) | **78**; в stderr одна строка «Invalid configuration, the service is not started: DataSaver__Kafka__BootstrapServers is required. DataSaver__Postgres__Host is required.», без стек-трейса | нет |
| 70 | Фатальная ошибка клиента Kafka | **Честный способ без патча**: static membership. Реплика A запущена с `DataSaver__Kafka__GroupInstanceId=same-id`, через 20 с запущена реплика B с тем же `group.instance.id` (другой `Health__Port`). Брокер выгнал A, librdkafka выставил фатальную ошибку `FencedInstanceId` («Static consumer fenced by other consumer with same group.instance.id», `Error.IsFatal`) | две копии `dotnet Ktm.DataSaver.dll` с одинаковым `GroupInstanceId` | **70** у реплики A; в логе `KafkaFatalErrorHandler` -> `HostProcessTerminator`: «Process termination requested with exit code 70: Kafka fatal error FencedInstanceId...». Реплика B осталась работать (остановлена вручную) | нет |
| 1 | Падение цикла приёма | Временный патч (ниже): `KafkaMessageSource.ConsumeAsync` бросает `InvalidOperationException`. Штатных путей, где из цикла честно вылетает нефатальное исключение, найти не удалось: сбои записи, стадий и commit цикл обрабатывает повтором или предупреждением, нефатальные ошибки Consume проглатываются | `dotnet Ktm.DataSaver.dll` из публикации с патчем; процесс завершился сам | **1**; в логе «Ingestion loop failed; the process is terminating» и «Process termination requested with exit code 1: Ingestion loop failed: InvalidOperationException» | да |

Временный патч для кода 1 (в репозиторий не попал, перед коммитом `git status` чист по `src/`): в начале
`KafkaMessageSource.ConsumeAsync` на время сборки публикации была добавлена строка с условным `throw new
InvalidOperationException(...)` (условие `cancellationToken.CanBeCanceled`, чтобы компилятор не ругался на недостижимый
код: предупреждения собираются как ошибки). Публикация собрана в отдельный каталог, патч сразу откачен
`git checkout -- src`. Для кодов 0, 70 и 78 использовалась непатченная публикация.

Наблюдения:

- Для кода 70 без патча сработала вся цепочка: `ConsumeException` с `IsFatal` -> `KafkaFatalErrorHandler` (70) ->
  `HostProcessTerminator` -> `Environment.ExitCode`. Затем `DataSaverWorker` поймал то же исключение и вызвал
  `Terminate(1, ...)`, но терминатор идемпотентен, поэтому итог остался 70, а не 1. Закрытие consumer при остановке
  дало предупреждение «Kafka consumer close failed» (на фатально сброшенном клиенте ожидаемо), на код оно не повлияло.
- `return Environment.ExitCode` в `Program.cs` работает: коды 70 и 1 доходят до процесса, не превращаясь в 0.
- Не проверялось: код 70 от фатальной ошибки producer (идемпотентный producer не включён) и код 1 без патча.

## Итоговый прогон 11.2 (2026-11-21, HEAD d9f280e)

- `dotnet build src/Ktm/Ktm.sln -warnaserror`: 0 предупреждений, 0 ошибок.
- `dotnet test --filter "Category!=Long&Category!=Perf"`: unit 240/240, интеграционные 61/61 (2 мин 36 с).
- `Category=Long`: 2/2, 2,99 мин. `DatabaseDownFor2Minutes_NoLossNoDuplicates_NoRestart_NoRebalance_BackoffCapped` (2 мин 15 с) и `CommitAgainstFrozenBroker_HangIsVisibleAsLiveness503_HostStopIsBounded_NoLossOnRestart` (27 с) пройдены; записей уровня `fail:` и `EventLogInternal` в выводе нет.

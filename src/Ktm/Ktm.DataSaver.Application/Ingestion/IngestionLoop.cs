using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Core.Ingestion;

/// <summary>Состояние цикла приёма для readiness-проверки.</summary>
public enum IngestionState
{
    /// <summary>Штатная работа: партиции читаются, записи проходят.</summary>
    Running,

    /// <summary>Сбой записи или публикации: партиции на паузе, идёт повтор той же пачки (readiness = 503).</summary>
    Degraded,
}

/// <summary>Параметры цикла. <c>PollInterval</c> - предел таймаута Consume; <c>ShutdownTimeout</c> - D-8.</summary>
public sealed record IngestionLoopOptions(TimeSpan PollInterval, TimeSpan ShutdownTimeout);

/// <summary>
/// Цикл приёма (ADR-0002, D-5...D-8): накопление пачки, запись, публикация в dlq и в rejected, commit offset+1 по всем
/// сообщениям пачки. Commit только после успеха всех шагов. Весь цикл в одном потоке; остановку только
/// сигнализируют токеном, пачку дописывает сам цикл. Ошибки данных решает порт C6, затем бюджет попыток (ADR-0006):
/// после <see cref="DlqPolicy.MaxAttempts"/> неудач ядовитое сообщение изолируется и уходит в dlq с подтверждением до commit;
/// без предела повторяется только инфраструктура.
/// Экземпляр одноразовый: <see cref="RunAsync"/> можно вызвать один раз.
/// </summary>
public sealed class IngestionLoop(
    IMessageSource source,
    IngestionPipeline pipeline,
    IEventStore store,
    IRejectionPublisher rejectionPublisher,
    IDeadLetterPublisher deadLetterPublisher,
    IOffsetCommitter committer,
    IWriteFailureHandler failureHandler,
    IAdminNotifier adminNotifier,
    Batcher<ConsumedMessage> batcher,
    BackoffPolicy backoff,
    IngestionLoopOptions options,
    DlqPolicy policy,
    DlqBreaker breaker,
    TimeProvider timeProvider,
    ILogger<IngestionLoop> logger,
    IIngestionObserver? observer = null)
{
    private readonly OffsetTracker _tracker = new();
    private readonly PoisonIsolator _isolator = new(store);

    private sealed record WriteItem(ConsumedMessage Message, ParsedEvent Event);

    // Предел остановки: отсчёт стартует в момент сигнала stop (см. RunAsync); до него токен не отменяется.
    private readonly CancellationTokenSource _deadline = new();
    private CancellationToken _stop;
    private Func<HashSet<TopicPartitionKey>, int>? _pruneCurrent;
    private volatile IngestionState _state = IngestionState.Running;
    private int _retryCount;
    private int _started;
    private bool _immediateRetryGranted;
    private long _heartbeatTicks = timeProvider.GetUtcNow().UtcTicks;

    /// <summary>Состояние для readiness: <c>Degraded</c> = 503.</summary>
    public IngestionState State => _state;

    /// <summary>Число неудачных попыток текущей пачки (0 при нормальной работе).</summary>
    public int RetryCount => Volatile.Read(ref _retryCount);

    /// <summary>
    /// Момент последней отметки цикла (D-10, liveness): начало итерации, окончание ожидания Consume (с IdleGuard) до
    /// записи, успешная запись (в том числе каждая запись изоляции) и успешная публикация в dlq и rejected, каждая итерация
    /// ожидания backoff (в том числе в <c>Degraded</c>). Commit и failureHandler - исключения: отметки вокруг них нет.
    /// Блокирующие ожидания (Consume, запись, публикация) не суммируются: между ними есть отметка. Читается из потока health-проверки.
    /// </summary>
    public DateTimeOffset LastHeartbeat => new(Interlocked.Read(ref _heartbeatTicks), TimeSpan.Zero);

    private void Touch() => Interlocked.Exchange(ref _heartbeatTicks, timeProvider.GetUtcNow().UtcTicks);

    private CancellationToken IoToken => _deadline.Token;

    /// <summary>
    /// Работает до сигнала остановки, затем дописывает накопленное в пределах <c>ShutdownTimeout</c>, который
    /// отсчитывается от сигнала. Возврат без исключения - и штатный выход, и выход без commit при недоступной БД
    /// (см. логи). Повторный вызов бросает <see cref="InvalidOperationException"/>.
    /// </summary>
    public async Task RunAsync(CancellationToken stop)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
        {
            throw new InvalidOperationException("IngestionLoop.RunAsync may be called only once.");
        }

        _stop = stop;
        ITimer? deadlineTimer = null;
        var registration = stop.Register(() =>
            Volatile.Write(ref deadlineTimer, timeProvider.CreateTimer(_ => _deadline.Cancel(), null, options.ShutdownTimeout, Timeout.InfiniteTimeSpan)));
        try
        {
            while (!stop.IsCancellationRequested)
            {
                Touch();
                DrainAssignmentChanges();

                var wait = batcher.TimeUntilDeadline() is { } left && left < options.PollInterval ? left : options.PollInterval;
                var started = timeProvider.GetTimestamp();
                ConsumedMessage? message;
                try
                {
                    message = await source.ConsumeAsync(wait, stop);
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    break;
                }

                // Изменения назначения применяются ДО добавления сообщения: при Revoke(P)+Assign(P) в одном Consume
                // сообщение P уже принадлежит новому назначению и не должно быть выброшено.
                DrainAssignmentChanges();

                if (message is not null)
                {
                    batcher.Add(message);
                }
                else
                {
                    await IdleGuardAsync(started, wait);
                }

                // Ожидание Consume (до PollInterval) закончилось: отмечаемся до записи, чтобы оно не суммировалось с ней.
                Touch();

                if (batcher.TryTakeReady() is { } batch && !await ProcessBatchAsync(batch))
                {
                    return;
                }
            }

            // Остановка: прекращаем Consume, дописываем накопленное (включая пришедшее во время Degraded).
            DrainAssignmentChanges();
            if (batcher.TakeAll() is { } rest)
            {
                await ProcessBatchAsync(rest);
            }
        }
        finally
        {
            await registration.DisposeAsync();
            Volatile.Read(ref deadlineTimer)?.Dispose();
            _deadline.Dispose();
        }
    }

    /// <summary>
    /// Защита от busy-loop: адаптер мог вернуть null раньше таймаута (контракт требует блокировки до таймаута).
    /// Недостающую часть ожидания добираем сами.
    /// </summary>
    private async Task IdleGuardAsync(long started, TimeSpan wait)
    {
        var rest = wait - timeProvider.GetElapsedTime(started);
        if (rest <= TimeSpan.Zero)
        {
            return;
        }

        try
        {
            await Task.Delay(rest, timeProvider, _stop);
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
            // Сигнал остановки: вызывающий проверит токен.
        }
    }

    /// <summary>false - предел остановки вышел до успеха, commit не выполнен.</summary>
    private async Task<bool> ProcessBatchAsync(IReadOnlyList<ConsumedMessage> batch)
    {
        var raw = batch.ToList();
        var results = new List<PipelineResult>();
        _pruneCurrent = revoked =>
        {
            bool IsRevoked(ConsumedMessage m) => revoked.Contains(new TopicPartitionKey(m.Topic, m.Partition));
            return raw.RemoveAll(IsRevoked) + results.RemoveAll(r => IsRevoked(r.Message));
        };

        try
        {
            var deadLettered = new HashSet<ConsumedMessage>(ReferenceEqualityComparer.Instance);
            // received_at один на пачку: повторы записи, изоляция и продолжение после DeadLetter используют то же значение (ADR-0002, D-3).
            var receivedAt = timeProvider.GetUtcNow();
            if (!await ClassifyStageAsync(raw, results)
                || !await WriteStageAsync(results, deadLettered, receivedAt)
                || !await DeadLetterStageAsync(results)
                || !await PublishStageAsync(results))
            {
                logger.LogWarning("Shutdown deadline reached before batch of {Size} was finished; offsets are not committed", batch.Count);
                return false;
            }

            DrainAssignmentChanges();
            _tracker.Reset();
            foreach (var result in results)
            {
                _tracker.Track(new PartitionOffset(result.Message.Topic, result.Message.Partition, result.Message.Offset));
            }

            var offsets = _tracker.GetOffsetsToCommit();
            _tracker.Reset();

            backoff.Reset();
            Volatile.Write(ref _retryCount, 0);
            LeaveDegraded();

            if (offsets.Count == 0)
            {
                return true;
            }

            try
            {
                await committer.CommitAsync(offsets, IoToken);
            }
            catch (OperationCanceledException) when (IoToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                // D-6: ошибка commit (потеря партиции) не роняет сервис; пачка придёт повторно и будет погашена ключом.
                logger.LogWarning(ex, "Offset commit failed for batch of {Size}; the batch will be redelivered", batch.Count);
            }

            return true;
        }
        finally
        {
            _pruneCurrent = null;
        }
    }

    /// <summary>
    /// Классификация пачки конвейером, поштучно. Неожиданное исключение стадии - сбой обработки сообщения, а не отбраковка:
    /// повтор этого сообщения с backoff до <see cref="DlqPolicy.MaxAttempts"/> попыток, затем сообщение признаётся ядовитым
    /// (результат с причиной dlq). Исключение инфраструктуры повторяется без предела. Штатные результаты бюджета не расходуют.
    /// </summary>
    private async Task<bool> ClassifyStageAsync(List<ConsumedMessage> raw, List<PipelineResult> results)
    {
        DrainAssignmentChanges();
        results.Clear();
        // Принадлежность текущей пачке: после revoke во время backoff сообщения отозванных партиций обрабатывать нельзя
        // (иначе их offset попал бы в commit). Множество пересобирается только после backoff (редкий путь).
        var live = new HashSet<ConsumedMessage>(raw, ReferenceEqualityComparer.Instance);
        foreach (var message in raw.ToArray())
        {
            if (!live.Contains(message))
            {
                continue;
            }

            var attempts = 0;
            while (true)
            {
                try
                {
                    results.Add(pipeline.Process(message));
                    break;
                }
                catch (Exception ex)
                {
                    var errorClass = WriteErrorClassifier.Classify(ex);
                    // Исключение целиком не логируем: его текст может нести фрагмент тела сообщения (ПДн).
                    var errorType = DeadLetterInfo.ShortTypeName(ex);
                    var stage = ex is PipelineStageException stageError ? stageError.Stage : PipelineStages.Parse;
                    var blockedByBreaker = false;
                    if (policy.Enabled && errorClass == WriteErrorClass.Unexpected)
                    {
                        attempts++;
                        if (attempts >= policy.MaxAttempts && !breaker.IsOpen)
                        {
                            var info = DeadLetterInfo.FromException(ex, PipelineStages.Parse, attempts, timeProvider.GetUtcNow());
                            RegisterPoisonCandidate();
                            results.Add(PipelineResult.Poison(message, info, (ex as PipelineStageException)?.Origin));
                            logger.LogError(
                                "Message {Topic} {Partition} {Offset} failed {Attempts} times at stage {Stage}: {ErrorType}; it is set aside for the dead letter topic",
                                message.Topic, message.Partition, message.Offset, attempts, info.Stage, errorType);
                            break;
                        }

                        // Оповещение на каждой попытке при открытом предохранителе, независимо от расхода бюджета сообщения.
                        blockedByBreaker = breaker.IsOpen;
                    }

                    var attempt = Interlocked.Increment(ref _retryCount);
                    logger.LogError(
                        "Pipeline stage failed for message {Topic} {Partition} {Offset}: {ErrorType} at stage {Stage}, attempt {Attempt}; message is retried, not rejected",
                        message.Topic, message.Partition, message.Offset, errorType, stage, attempt);
                    if (blockedByBreaker)
                    {
                        adminNotifier.NotifyDeadLetterBreakerOpen(ex, attempt);
                    }

                    observer?.Retry("pipeline");
                    EnterDegraded();
                    if (!await BackoffWaitAsync())
                    {
                        return false;
                    }

                    DrainAssignmentChanges();
                    live = new HashSet<ConsumedMessage>(raw, ReferenceEqualityComparer.Instance);
                    if (!live.Contains(message))
                    {
                        // Партиция отозвана во время паузы: сообщение выброшено, результат для него не нужен.
                        break;
                    }
                }
            }
        }

        return true;
    }

    private async Task<bool> WriteStageAsync(List<PipelineResult> results, HashSet<ConsumedMessage> deadLettered, DateTimeOffset receivedAt)
    {
        // Записанные изоляцией сообщения повторно не пишутся (иначе раздувается duplicates и растёт нагрузка).
        var written = new HashSet<ConsumedMessage>(ReferenceEqualityComparer.Instance);
        var dataFailures = 0;
        // Неудавшиеся записи изоляции по событиям (для ktm-dlq-attempts); живёт всё время стадии, как written.
        var failedWrites = new Dictionary<ParsedEvent, int>(ReferenceEqualityComparer.Instance);
        var isolating = false;
        var isolationStarted = false;
        while (true)
        {
            DrainAssignmentChanges();
            var items = results
                .Where(r => r.Event is not null && !deadLettered.Contains(r.Message) && !written.Contains(r.Message))
                .Select(r => new WriteItem(r.Message, r.Event!))
                .ToList();
            if (items.Count == 0)
            {
                return true;
            }

            if (isolating)
            {
                if (!isolationStarted)
                {
                    isolationStarted = true;
                    observer?.WriteIsolation();
                    logger.LogError(
                        "Write failed {Attempts} times with a message-bound error; isolating the poison message among {Count} messages",
                        dataFailures, items.Count);
                }

                var isolation = await _isolator.IsolateAsync(
                    items,
                    i => i.Event,
                    receivedAt,
                    part =>
                    {
                        foreach (var item in part)
                        {
                            written.Add(item.Message);
                        }

                        OnEventsWritten(part.Count);
                    },
                    Touch,
                    IoToken,
                    // Ядовитое регистрируется в момент обнаружения: успешные записи изоляции между ядовитыми закрывают предохранитель.
                    poison => RegisterPoison(results, poison, dataFailures + failedWrites.GetValueOrDefault(poison.Item.Event)),
                    failedWrites);

                if (isolation.Outcome == IsolationOutcome.Cancelled)
                {
                    return false;
                }

                if (isolation.Outcome == IsolationOutcome.Interrupted)
                {
                    // Инфраструктура: обычный путь без нового расхода бюджета, изоляция продолжится по остатку.
                    var interrupted = isolation.InterruptedBy!;
                    var attempt = Interlocked.Increment(ref _retryCount);
                    var interruptedClass = WriteErrorClassifier.Classify(interrupted);
                    LogWriteFailure(interrupted, interruptedClass, attempt, isolation.Remaining);
                    if (interruptedClass == WriteErrorClass.FatalConfig)
                    {
                        adminNotifier.NotifyFatalConfig(interrupted, attempt);
                    }

                    EnterDegraded();
                    if (!await BackoffWaitAsync())
                    {
                        return false;
                    }

                    continue;
                }

                // Изоляция завершена. Не отправленные в dlq из-за открытого предохранителя остаются в items:
                // следующий проход пишет их обычным путём и повторяет без предела.
                isolating = false;
                isolationStarted = false;
                continue;
            }

            try
            {
                await store.WriteAsync(items.Select(i => i.Event).ToList(), receivedAt, IoToken);
                // Во время самой записи отметки нет: от ложного loop_stalled защищает валидатор
                // (LiveStaleSeconds > max(Poll, Connect + 3 * Command, ProducerMessageTimeout);
                // запись = BeginTransaction + ExecuteNonQuery + Commit, CommandTimeout на каждое обращение).
                // Touch после записи нужен, чтобы ожидания записи и публикации не суммировались в одно окно без отметки.
                Touch();
                OnEventsWritten(items.Count);
                return true;
            }
            catch (OperationCanceledException) when (IoToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                var attempt = Interlocked.Increment(ref _retryCount);
                var errorClass = WriteErrorClassifier.Classify(ex);
                LogWriteFailure(ex, errorClass, attempt, items);

                if (errorClass == WriteErrorClass.Data)
                {
                    var context = new WriteFailureContext(results.Select(r => r.Message).ToList(), ex, attempt);
                    WriteFailureDecision decision;
                    try
                    {
                        decision = await failureHandler.HandleAsync(context, IoToken);
                    }
                    catch (OperationCanceledException) when (IoToken.IsCancellationRequested)
                    {
                        return false;
                    }
                    catch (OperationCanceledException)
                    {
                        logger.LogWarning("Write failure handler was cancelled by its own timeout, attempt {Attempt}; treated as Retry", attempt);
                        decision = WriteFailureDecision.Retry;
                    }

                    // Решение порта C6 имеет приоритет над бюджетом и его не расходует.
                    if (decision.Kind == WriteFailureKind.DeadLetter && TryAcceptDeadLetter(context, decision, deadLettered))
                    {
                        logger.LogError("Write data error: {Count} messages handed over to the failure handler", decision.DeadLetterItems.Count);
                        continue;
                    }

                    if (!(policy.Enabled && breaker.IsOpen))
                    {
                        // При открытом предохранителе блокировку уже несут общая запись сбоя и оповещение.
                        logger.LogWarning("Write failure handler made no progress, attempt {Attempt}; partition stays blocked", attempt);
                    }
                }
                else if (errorClass == WriteErrorClass.FatalConfig)
                {
                    adminNotifier.NotifyFatalConfig(ex, attempt);
                }

                if (errorClass is WriteErrorClass.Data or WriteErrorClass.Unexpected && policy.Enabled)
                {
                    dataFailures++;
                    if (breaker.IsOpen)
                    {
                        // Системная проблема, замаскированная под данные: dlq не используется, повтор без предела.
                        adminNotifier.NotifyDeadLetterBreakerOpen(ex, attempt);
                    }
                    else if (dataFailures >= policy.MaxAttempts)
                    {
                        isolating = true;
                        continue;
                    }
                }

                EnterDegraded();
                if (!await BackoffWaitAsync())
                {
                    return false;
                }
            }
        }
    }

    /// <summary>Успешная запись событий: закрывает предохранитель (запись пустой пачки его не закрывает).</summary>
    private void OnEventsWritten(int count)
    {
        var wasOpen = breaker.IsOpen;
        breaker.OnWriteSucceeded(count);
        if (wasOpen && !breaker.IsOpen)
        {
            logger.LogWarning("Dead letter breaker is closed: a write succeeded");
        }
    }

    /// <summary>
    /// Единая точка учёта ядовитого сообщения предохранителем; переход «закрыт -> открыт» логируется один раз.
    /// </summary>
    private void RegisterPoisonCandidate()
    {
        var wasOpen = breaker.IsOpen;
        breaker.Register(1);
        if (!wasOpen && breaker.IsOpen)
        {
            logger.LogError(
                "Dead letter breaker is open: {Count} messages in a row went to the dead letter topic without a successful write (threshold {Threshold}); further poison messages are retried without limit",
                breaker.Count, policy.BreakerThreshold);
        }
    }

    /// <summary>
    /// Ядовитое сообщение изоляции заменяет результат (оно больше не пишется и уходит в dlq). Открытый предохранитель
    /// оставляет сообщение в пачке: повтор без предела. Вызывается изолятором в момент обнаружения, в порядке пачки.
    /// </summary>
    private void RegisterPoison(List<PipelineResult> results, IsolatedPoison<WriteItem> poison, int attempts)
    {
        if (breaker.IsOpen)
        {
            return;
        }

        RegisterPoisonCandidate();
        var info = DeadLetterInfo.FromException(poison.Error, PipelineStages.Write, attempts, timeProvider.GetUtcNow());
        var message = poison.Item.Message;
        var index = results.FindIndex(r => ReferenceEquals(r.Message, message));
        if (index >= 0)
        {
            results[index] = PipelineResult.Poison(message, info, new EventOrigin(poison.Item.Event.Source, poison.Item.Event.SourceEventId));
        }
    }

    /// <summary>
    /// Публикация ядовитых сообщений (любой стадии) в dlq одним шагом пачки: подтверждение брокера до commit offset.
    /// Отказ - Degraded, пауза и повтор без предела, commit не выполняется; результат изоляции не пересчитывается.
    /// </summary>
    private async Task<bool> DeadLetterStageAsync(List<PipelineResult> results)
    {
        while (true)
        {
            DrainAssignmentChanges();
            var poison = results
                .Where(r => r.DeadLetter is not null)
                .Select(r => new DeadLetteredMessage(r.Message, r.Origin, r.DeadLetter!))
                .ToList();
            if (poison.Count == 0)
            {
                return true;
            }

            DeadLetterPublishResult outcome;
            try
            {
                outcome = await deadLetterPublisher.PublishAsync(poison, IoToken);
            }
            catch (OperationCanceledException) when (IoToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                // Исключение порта - отказ публикации. Текст исключения не логируем: возможен фрагмент тела.
                logger.LogError("Dead letter publisher threw {ErrorType} for {Count} messages", ex.GetType().Name, poison.Count);
                outcome = DeadLetterPublishResult.Failure(ex.GetType().Name);
            }

            if (outcome.IsSuccess)
            {
                Touch();
                return true;
            }

            var attempt = Interlocked.Increment(ref _retryCount);
            logger.LogWarning("Publishing {Count} messages to the dead letter topic failed, attempt {Attempt}: {Error}", poison.Count, attempt, outcome.Error);
            EnterDegraded();
            if (!await BackoffWaitAsync())
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Лог сбоя записи (6.5): общая запись (класс, попытка, размер пачки), затем запись на каждую группу (Topic, Partition)
    /// принятых событий с первым offset, последним offset, числом и происхождением первого события. Тел сообщений нет.
    /// Исключение в лог не передаётся (текст может содержать значения из строки). Ошибка БД: SQLSTATE, ограничение, таблица;
    /// сетевые типы (DbException, SocketException, IOException, TimeoutException): цепочка типов и код сокета; прочие: только тип.
    /// </summary>
    private void LogWriteFailure(Exception ex, WriteErrorClass errorClass, int attempt, IReadOnlyList<WriteItem> items)
    {
        var level = errorClass == WriteErrorClass.Transient ? LogLevel.Warning : LogLevel.Error;
        // Исключение целиком не логируется ни для какого класса: Message драйвера может содержать значения из строки.
        var sqlState = WriteErrorClassifier.FindSqlState(ex);
        var location = WriteErrorClassifier.FindLocation(ex);
        string? chain = null;
        string? code = null;
        if (string.IsNullOrEmpty(sqlState) && IsNetworkType(ex))
        {
            chain = string.Join(" > ", EnumerateChain(ex).Select(e => e.GetType().Name));
            code = FindNetworkCode(ex);
        }

        logger.Log(level,
            "Batch write failed: {ErrorClass}, {ErrorType}, {SqlState}, {ConstraintName}, {TableName}, {ErrorChain}, {ErrorCode}, attempt {Attempt}, batch size {BatchSize}",
            errorClass, ex.GetType().Name, sqlState, location?.ConstraintName, location?.TableName, chain, code, attempt, items.Count);

        foreach (var group in items.GroupBy(i => (i.Message.Topic, i.Message.Partition)))
        {
            var ordered = group.OrderBy(i => i.Message.Offset).ToList();
            var first = ordered[0];
            logger.Log(level,
                "Batch write failed for partition: {Topic} {Partition} {Offset} {LastOffset} {Count} {Source} {SourceEventId}",
                group.Key.Topic, group.Key.Partition, first.Message.Offset, ordered[^1].Message.Offset, ordered.Count,
                first.Event.Source, first.Event.SourceEventId);
        }
    }

    private static IEnumerable<Exception> EnumerateChain(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            yield return current;
        }
    }

    /// <summary>Типы, текст которых (хост, код) нужен для диагностики; остальные логируются только по типу. DbException покрывает NpgsqlException.</summary>
    private static bool IsNetworkType(Exception ex) =>
        ex is System.Data.Common.DbException or System.Net.Sockets.SocketException or IOException or TimeoutException or ISqlStateError;

    private static string? FindNetworkCode(Exception ex)
    {
        foreach (var current in EnumerateChain(ex))
        {
            if (current is System.Net.Sockets.SocketException socket)
            {
                return $"{socket.SocketErrorCode} ({socket.ErrorCode})";
            }
        }

        return null;
    }

    /// <summary>
    /// Решение DeadLetter принимается, только если все сообщения принадлежат текущей пачке и набор реально вырос;
    /// иначе (нет прогресса, чужие сообщения) это Retry с backoff - иначе цикл крутился бы без пауз.
    /// </summary>
    private static bool TryAcceptDeadLetter(WriteFailureContext context, WriteFailureDecision decision, HashSet<ConsumedMessage> deadLettered)
    {
        var items = decision.DeadLetterItems;
        var batch = new HashSet<ConsumedMessage>(context.Batch, ReferenceEqualityComparer.Instance);
        if (items.Count == 0 || !items.All(batch.Contains))
        {
            return false;
        }

        var before = deadLettered.Count;
        deadLettered.UnionWith(items);
        return deadLettered.Count > before;
    }

    private async Task<bool> PublishStageAsync(List<PipelineResult> results)
    {
        while (true)
        {
            DrainAssignmentChanges();
            var rejected = results
                .Where(r => !r.Verdict.IsAccepted)
                .Select(r => new RejectedMessage(r.Message, r.Verdict.Rejection!, r.Origin))
                .ToList();
            if (rejected.Count == 0)
            {
                return true;
            }

            RejectionPublishResult outcome;
            try
            {
                outcome = await rejectionPublisher.PublishAsync(rejected, IoToken);
            }
            catch (OperationCanceledException) when (IoToken.IsCancellationRequested)
            {
                return false;
            }
            catch (Exception ex)
            {
                // Исключение порта - сбой публикации, а не отбраковка сообщения: commit не выполняется, пачка повторяется.
                logger.LogError("Rejection publisher threw {ErrorType} for {Count} messages", ex.GetType().Name, rejected.Count);
                outcome = RejectionPublishResult.Failure(ex.GetType().Name);
            }

            if (outcome.IsSuccess)
            {
                Touch();
                return true;
            }

            var attempt = Interlocked.Increment(ref _retryCount);
            logger.LogWarning("Publishing {Count} rejected messages failed, attempt {Attempt}: {Error}", rejected.Count, attempt, outcome.Error);
            EnterDegraded();
            if (!await BackoffWaitAsync())
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Пауза перед повтором. В штатном режиме опрашиваем Consume (на паузе он сообщений не отдаёт), чтобы не вылететь
    /// по max.poll.interval. Когда приходит сигнал остановки, ожидание прерывается и следующая попытка выполняется
    /// немедленно (один раз); дальнейшие паузы идут уже в пределах дедлайна остановки. false - дедлайн вышел.
    /// </summary>
    private async Task<bool> BackoffWaitAsync()
    {
        var delay = backoff.NextDelay();

        if (_stop.IsCancellationRequested)
        {
            if (!_immediateRetryGranted)
            {
                _immediateRetryGranted = true;
                return true;
            }

            try
            {
                await Task.Delay(delay, timeProvider, IoToken);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        var until = timeProvider.GetUtcNow() + delay;
        while (true)
        {
            Touch();
            var remaining = until - timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return true;
            }

            var wait = remaining < options.PollInterval ? remaining : options.PollInterval;
            var started = timeProvider.GetTimestamp();
            try
            {
                var message = await source.ConsumeAsync(wait, _stop);
                DrainAssignmentChanges();
                if (message is not null)
                {
                    batcher.Add(message);
                }
                else
                {
                    await IdleGuardAsync(started, wait);
                }

                Touch();
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                _immediateRetryGranted = true;
                return true;
            }

            if (_stop.IsCancellationRequested)
            {
                _immediateRetryGranted = true;
                return true;
            }
        }
    }

    /// <summary>
    /// Применяет ребаланс: сообщения отозванных партиций выбрасываются из накопителя и текущей пачки (Track для них
    /// не вызывается - контракт OffsetTracker), новые партиции в Degraded сразу ставятся на паузу.
    /// </summary>
    private void DrainAssignmentChanges()
    {
        var changes = source.TakeAssignmentChanges();
        if (changes.IsEmpty)
        {
            return;
        }

        if (changes.Revoked.Count > 0)
        {
            var revoked = changes.Revoked.ToHashSet();
            _tracker.Revoke(revoked);
            var removed = batcher.RemoveWhere(m => revoked.Contains(new TopicPartitionKey(m.Topic, m.Partition)));
            removed += _pruneCurrent?.Invoke(revoked) ?? 0;
            logger.LogInformation("Partitions revoked: {Partitions}; {Removed} messages dropped without write", revoked.Count, removed);
        }

        if (changes.Assigned.Count > 0 && _state == IngestionState.Degraded)
        {
            source.Pause(changes.Assigned);
        }
    }

    private void EnterDegraded()
    {
        if (_state == IngestionState.Degraded)
        {
            return;
        }

        _state = IngestionState.Degraded;
        source.Pause(source.Assignment);
        logger.LogWarning("Ingestion is Degraded: partitions paused, polling continues");
    }

    private void LeaveDegraded()
    {
        if (_state == IngestionState.Running)
        {
            return;
        }

        source.Resume(source.Assignment);
        _state = IngestionState.Running;
        logger.LogInformation("Ingestion recovered: partitions resumed");
    }
}

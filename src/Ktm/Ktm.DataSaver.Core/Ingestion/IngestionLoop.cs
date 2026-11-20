using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Core.Ingestion;

public enum IngestionState
{
    Running,
    Degraded,
}

/// <summary>Параметры цикла. <c>PollInterval</c> - предел таймаута Consume; <c>ShutdownTimeout</c> - D-8.</summary>
public sealed record IngestionLoopOptions(TimeSpan PollInterval, TimeSpan ShutdownTimeout);

/// <summary>
/// Цикл приёма (ADR-0002, D-5...D-8): накопление пачки, запись, публикация отбракованных, commit offset+1 по всем
/// сообщениям пачки. Commit только после успеха обоих шагов. Весь цикл в одном потоке; остановку только
/// сигнализируют токеном, пачку дописывает сам цикл. Ошибки данных решает порт C6.
/// </summary>
public sealed class IngestionLoop(
    IMessageSource source,
    IngestionPipeline pipeline,
    IEventStore store,
    IRejectionPublisher rejectionPublisher,
    IOffsetCommitter committer,
    IWriteFailureHandler failureHandler,
    IAdminNotifier adminNotifier,
    Batcher<ConsumedMessage> batcher,
    BackoffPolicy backoff,
    IngestionLoopOptions options,
    TimeProvider timeProvider,
    ILogger<IngestionLoop> logger)
{
    private readonly OffsetTracker _tracker = new();
    private CancellationTokenSource? _shutdownDeadline;
    private volatile IngestionState _state = IngestionState.Running;
    private int _retryCount;

    /// <summary>Состояние для readiness: <c>Degraded</c> = 503.</summary>
    public IngestionState State => _state;

    /// <summary>Число неудачных попыток текущей пачки (0 при нормальной работе).</summary>
    public int RetryCount => Volatile.Read(ref _retryCount);

    private CancellationToken IoToken => _shutdownDeadline?.Token ?? CancellationToken.None;

    private bool ShuttingDown => _shutdownDeadline is not null;

    /// <summary>
    /// Работает до сигнала остановки, затем дописывает накопленное в пределах <c>ShutdownTimeout</c>.
    /// Возврат без исключения - и штатный выход, и выход без commit при недоступной БД (см. логи).
    /// </summary>
    public async Task RunAsync(CancellationToken stop)
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                DrainAssignmentChanges(null);

                var wait = batcher.TimeUntilDeadline() is { } left && left < options.PollInterval ? left : options.PollInterval;
                try
                {
                    if (await source.ConsumeAsync(wait, stop) is { } message)
                    {
                        batcher.Add(message);
                    }
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    break;
                }

                DrainAssignmentChanges(null);

                if (batcher.TryTakeReady() is { } batch && !await ProcessBatchAsync(batch, stop))
                {
                    return;
                }
            }

            // Остановка: прекращаем Consume, дописываем накопленное (включая пришедшее во время Degraded).
            StartShutdown();
            DrainAssignmentChanges(null);
            if (batcher.TakeAll() is { } rest)
            {
                await ProcessBatchAsync(rest, stop);
            }
        }
        finally
        {
            _shutdownDeadline?.Dispose();
        }
    }

    /// <summary>false - предел остановки вышел до успеха, commit не выполнен.</summary>
    private async Task<bool> ProcessBatchAsync(IReadOnlyList<ConsumedMessage> batch, CancellationToken stop)
    {
        var results = batch.Select(pipeline.Process).ToList();
        var deadLettered = new HashSet<ConsumedMessage>(ReferenceEqualityComparer.Instance);

        if (!await WriteStageAsync(results, deadLettered, stop) || !await PublishStageAsync(results, stop))
        {
            logger.LogWarning("Shutdown deadline reached before batch of {Size} was finished; offsets are not committed", batch.Count);
            return false;
        }

        DrainAssignmentChanges(results);
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

    private async Task<bool> WriteStageAsync(List<PipelineResult> results, HashSet<ConsumedMessage> deadLettered, CancellationToken stop)
    {
        while (true)
        {
            DrainAssignmentChanges(results);
            var events = results
                .Where(r => r.Event is not null && !deadLettered.Contains(r.Message))
                .Select(r => r.Event!)
                .ToList();
            if (events.Count == 0)
            {
                return true;
            }

            try
            {
                await store.WriteAsync(events, IoToken);
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

                if (errorClass == WriteErrorClass.Data)
                {
                    var context = new WriteFailureContext(results.Select(r => r.Message).ToList(), ex, attempt);
                    var decision = await failureHandler.HandleAsync(context, IoToken);
                    if (decision.Kind == WriteFailureKind.DeadLetter && decision.DeadLetterItems.Count > 0)
                    {
                        deadLettered.UnionWith(decision.DeadLetterItems);
                        logger.LogError(ex, "Write failed with data error; {Count} messages handed over to the failure handler", decision.DeadLetterItems.Count);
                        continue;
                    }

                    logger.LogError(ex, "Write failed with data error, attempt {Attempt}; partition stays blocked (no failure handler decision)", attempt);
                }
                else if (errorClass == WriteErrorClass.FatalConfig)
                {
                    logger.LogError(ex, "Write failed with configuration/permission error, attempt {Attempt}", attempt);
                    adminNotifier.NotifyFatalConfig(ex, attempt);
                }
                else
                {
                    logger.LogWarning(ex, "Write failed (transient), attempt {Attempt}", attempt);
                }

                EnterDegraded();
                if (!await BackoffWaitAsync(results, stop))
                {
                    return false;
                }
            }
        }
    }

    private async Task<bool> PublishStageAsync(List<PipelineResult> results, CancellationToken stop)
    {
        while (true)
        {
            DrainAssignmentChanges(results);
            var rejected = results
                .Where(r => !r.Verdict.IsAccepted)
                .Select(r => new RejectedMessage(r.Message, r.Verdict.Rejection!))
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
                logger.LogError(ex, "Rejection publisher threw for {Count} messages", rejected.Count);
                outcome = RejectionPublishResult.Failure(ex.GetType().Name);
            }

            if (outcome.IsSuccess)
            {
                return true;
            }

            var attempt = Interlocked.Increment(ref _retryCount);
            logger.LogWarning("Publishing {Count} rejected messages failed, attempt {Attempt}: {Error}", rejected.Count, attempt, outcome.Error);
            EnterDegraded();
            if (!await BackoffWaitAsync(results, stop))
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Пауза перед повтором. В штатном режиме опрашиваем Consume (на паузе он сообщений не отдаёт), чтобы не вылететь
    /// по max.poll.interval. Сигнал остановки прерывает ожидание: следующая попытка без паузы, дальше паузы идут в
    /// пределах ShutdownTimeout. false - предел остановки вышел.
    /// </summary>
    private async Task<bool> BackoffWaitAsync(List<PipelineResult> current, CancellationToken stop)
    {
        var delay = backoff.NextDelay();

        if (!ShuttingDown && stop.IsCancellationRequested)
        {
            StartShutdown();
        }

        if (ShuttingDown)
        {
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
            var remaining = until - timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                return true;
            }

            try
            {
                if (await source.ConsumeAsync(remaining < options.PollInterval ? remaining : options.PollInterval, stop) is { } message)
                {
                    batcher.Add(message);
                }
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                StartShutdown();
                return true;
            }

            DrainAssignmentChanges(current);

            if (stop.IsCancellationRequested)
            {
                StartShutdown();
                return true;
            }
        }
    }

    /// <summary>
    /// Применяет ребаланс: сообщения отозванных партиций выбрасываются из накопителя и текущей пачки (Track для них
    /// не вызывается - контракт OffsetTracker), новые партиции в Degraded сразу ставятся на паузу.
    /// </summary>
    private void DrainAssignmentChanges(List<PipelineResult>? current)
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
            removed += current?.RemoveAll(r => revoked.Contains(new TopicPartitionKey(r.Message.Topic, r.Message.Partition))) ?? 0;
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

    private void StartShutdown() =>
        _shutdownDeadline ??= new CancellationTokenSource(options.ShutdownTimeout, timeProvider);
}

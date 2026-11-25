using System.Diagnostics.CodeAnalysis;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Core.Reprocessing;

/// <summary>Итог исполнения задачи раннером: <c>ErrorCode</c> заполнен только для <see cref="ReprocessOutcome.Failed"/>.</summary>
public sealed record ReprocessRunResult(ReprocessOutcome Outcome, string? ErrorCode);

/// <summary>
/// Исполнение одной задачи повторной обработки (ADR-0006 п. 2.6-2.8, design Р-7): сообщения источника проходят тот же
/// <see cref="IngestionPipeline"/> и ту же идемпотентную запись, что и рабочий поток, но ничего не публикуют и ничего не
/// коммитят. Повторно отбракованное и не записанное сообщение уходит в отчёт задачи, а не в <c>rejected</c>/<c>dlq</c>.
/// Скорость ограничивает токен-бакет по <see cref="TimeProvider"/>. Не знает ни Kafka, ни Npgsql, ни ASP.NET, ни <c>IngestionLoop</c>.
/// </summary>
/// <remarks>Политика паузы создаётся фабрикой на каждый запуск: <see cref="BackoffPolicy"/> не потокобезопасна и не должна делиться с рабочим циклом.</remarks>
public class ReprocessRunner
{
    private readonly IngestionPipeline _pipeline;
    private readonly IReplayEventWriter _writer;
    private readonly ReprocessSettings _settings;
    private readonly Func<BackoffPolicy> _backoffFactory;
    private readonly TimeProvider _time;
    private readonly ILogger<ReprocessRunner> _logger;

    public ReprocessRunner(
        IngestionPipeline pipeline,
        IReplayEventWriter writer,
        ReprocessSettings settings,
        Func<BackoffPolicy> backoffFactory,
        TimeProvider time,
        ILogger<ReprocessRunner> logger)
    {
        _pipeline = pipeline;
        _writer = writer;
        _settings = settings;
        _backoffFactory = backoffFactory;
        _time = time;
        _logger = logger;
    }

    /// <summary>
    /// Выполняет задачу до конца источника. Отмена задачи (<see cref="ReprocessJob.RequestCancel"/>) действует между пачками и в
    /// ожиданиях; остановка хоста (<paramref name="stopping"/>) - в любом месте. Исключения наружу не выходят: итог - в результате.
    /// </summary>
    public virtual async Task<ReprocessRunResult> RunAsync(ReprocessJob job, IReplayReader reader, CancellationToken stopping)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(reader);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(stopping, job.CancelToken);
        var run = new Execution(this, job, reader, stopping, linked.Token);
        try
        {
            await run.ExecuteAsync();
            var outcome = job.Snapshot().Counters.FailedAgain > 0 ? ReprocessOutcome.CompletedWithFailures : ReprocessOutcome.Succeeded;
            return new ReprocessRunResult(outcome, null);
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested || stopping.IsCancellationRequested)
        {
            return new ReprocessRunResult(ReprocessOutcome.Cancelled, null);
        }
        catch (ReprocessFailedException ex)
        {
            return new ReprocessRunResult(ReprocessOutcome.Failed, ex.Code);
        }
        catch (ReplaySourceException)
        {
            return new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.SourceUnavailable);
        }
        catch (Exception ex)
        {
            // Только тип: текст исключения может нести фрагмент тела сообщения.
            _logger.LogError("Reprocess job {JobId} failed with an unexpected error: {ErrorType}", job.Id, ex.GetType().Name);
            return new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.Internal);
        }
    }

    /// <summary>Задача завершается ошибкой: инфраструктура не восстановилась за предел ожидания либо отказ записи не лечится повтором.</summary>
    private sealed class ReprocessFailedException(string code) : Exception(code)
    {
        public string Code { get; } = code;
    }

    private sealed record Item(ReplayMessage Source, ParsedEvent Event);

    private sealed class CountingStore(IReplayEventWriter inner) : IEventStore
    {
        public long Attempted { get; private set; }

        public long Inserted { get; private set; }

        public async ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken)
        {
            var inserted = await inner.WriteAsync(events, receivedAt, cancellationToken);
            Attempted += events.Count;
            Inserted += inserted;
        }

        public void Reset()
        {
            Attempted = 0;
            Inserted = 0;
        }
    }

    /// <summary>Токен-бакет: ёмкость - секунда работы; нехватка токенов - долг, который гасится ожиданием.</summary>
    private sealed class RateLimiter(int ratePerSecond, TimeProvider time)
    {
        private double _tokens = ratePerSecond;
        private long _last = time.GetTimestamp();

        public async ValueTask AcquireAsync(int count, CancellationToken cancellationToken)
        {
            var elapsed = time.GetElapsedTime(_last);
            _last = time.GetTimestamp();
            _tokens = Math.Min(ratePerSecond, _tokens + elapsed.TotalSeconds * ratePerSecond) - count;
            if (_tokens >= 0)
            {
                return;
            }

            await Task.Delay(TimeSpan.FromSeconds(-_tokens / ratePerSecond), time, cancellationToken);
        }
    }

    /// <summary>Состояние одного запуска: политика паузы, предел недоступности, лимитер.</summary>
    private sealed class Execution(ReprocessRunner owner, ReprocessJob job, IReplayReader reader, CancellationToken stopping, CancellationToken soft)
    {
        private readonly BackoffPolicy _backoff = owner._backoffFactory();
        private readonly RateLimiter _limiter = new(job.Request.MaxRate, owner._time);
        private DateTimeOffset? _unavailableSince;

        public async Task ExecuteAsync()
        {
            while (true)
            {
                // Отмена - между пачками: текущая пачка дописывается. Остановка хоста видна сразу, не дожидаясь связанного токена.
                stopping.ThrowIfCancellationRequested();
                soft.ThrowIfCancellationRequested();
                var batch = await reader.ReadBatchAsync(owner._settings.BatchSize, stopping);
                if (batch.Count == 0)
                {
                    job.MarkPartitionsDone();
                    return;
                }

                var selected = batch.Where(m => job.Request.Reason is null || string.Equals(m.Reason, job.Request.Reason, StringComparison.Ordinal)).ToList();
                job.AddRead(batch.Count, batch.Count - selected.Count);

                if (selected.Count > 0)
                {
                    await _limiter.AcquireAsync(selected.Count, soft);
                    var accepted = new List<Item>(selected.Count);
                    foreach (var message in selected)
                    {
                        if (await ClassifyAsync(message) is { } item)
                        {
                            accepted.Add(item);
                        }
                    }

                    await WriteAsync(accepted, owner._time.GetUtcNow());
                }

                foreach (var message in batch)
                {
                    job.Advance(message.ActualTopic, message.ActualPartition, message.ActualOffset);
                }
            }
        }

        /// <summary>Сообщение, принятое конвейером, либо null (повторно отбраковано или не обработано - занесено в отчёт).</summary>
        private async ValueTask<Item?> ClassifyAsync(ReplayMessage message)
        {
            var attempts = 0;
            while (true)
            {
                PipelineResult result;
                try
                {
                    result = owner._pipeline.Process(message.Message);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    switch (WriteErrorClassifier.Classify(ex))
                    {
                        case WriteErrorClass.Transient:
                            await WaitForRecoveryAsync(ReprocessErrorCodes.Internal);
                            continue;
                        case WriteErrorClass.FatalConfig:
                            FailFast(ex);
                            break;
                    }

                    // Data и Unexpected - свойство сообщения: бюджет попыток, затем в отчёт.
                    attempts++;
                    if (attempts < owner._settings.MaxAttempts)
                    {
                        await BackoffAsync();
                        continue;
                    }

                    var info = DeadLetterInfo.FromException(ex, PipelineStages.Parse, attempts, owner._time.GetUtcNow());
                    job.AddFailedAgain(Entry(ReprocessReportKind.FailedAgain, message, info.Stage, info.Reason, info.ExceptionType, null));
                    return null;
                }

                Recovered();
                if (result.Verdict.Rejection is { } rejection)
                {
                    job.AddRejectedAgain(Entry(ReprocessReportKind.RejectedAgain, message, StageOf(rejection.Kind), rejection.Reason, null, null));
                    return null;
                }

                return result.Event is { } parsed ? new Item(message, parsed) : null;
            }
        }

        private async ValueTask WriteAsync(List<Item> items, DateTimeOffset receivedAt)
        {
            if (items.Count == 0)
            {
                return;
            }

            var store = new CountingStore(owner._writer);
            var isolator = new PoisonIsolator(store);
            IReadOnlyList<Item> remaining = items;
            var dataFailures = 0;

            while (remaining.Count > 0)
            {
                Exception error;
                try
                {
                    await store.WriteAsync(remaining.Select(i => i.Event).ToList(), receivedAt, stopping);
                    Flush(store);
                    Recovered();
                    return;
                }
                catch (OperationCanceledException) when (stopping.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    error = ex;
                }

                var errorClass = WriteErrorClassifier.Classify(error);
                if (errorClass == WriteErrorClass.FatalConfig)
                {
                    FailFast(error);
                }

                if (errorClass == WriteErrorClass.Transient)
                {
                    await WaitForRecoveryAsync(ReprocessErrorCodes.DbUnavailable);
                    continue;
                }

                // Ошибка, привязанная к сообщению: бюджет попыток, затем изоляция (деление пополам). Ядовитые идут в отчёт.
                dataFailures++;
                if (dataFailures < owner._settings.MaxAttempts)
                {
                    await BackoffAsync();
                    continue;
                }

                var isolation = await isolator.IsolateAsync(remaining, i => i.Event, receivedAt, null, null, stopping);
                Flush(store);
                foreach (var poison in isolation.Poison)
                {
                    var info = DeadLetterInfo.FromException(poison.Error, PipelineStages.Write, dataFailures + 1, owner._time.GetUtcNow());
                    job.AddFailedAgain(Entry(ReprocessReportKind.FailedAgain, poison.Item.Source, info.Stage, info.Reason, info.ExceptionType, info.SqlState));
                }

                remaining = isolation.Remaining.ToList();
                switch (isolation.Outcome)
                {
                    case IsolationOutcome.Cancelled:
                        throw new OperationCanceledException(stopping);
                    case IsolationOutcome.Interrupted:
                        // Прервавшая изоляцию ошибка - не свойство сообщения; если её не лечит повтор, ждать нельзя.
                        if (isolation.InterruptedBy is { } interruptedBy && WriteErrorClassifier.Classify(interruptedBy) == WriteErrorClass.FatalConfig)
                        {
                            FailFast(interruptedBy);
                        }

                        await WaitForRecoveryAsync(ReprocessErrorCodes.DbUnavailable);
                        break;
                    default:
                        Recovered();
                        break;
                }
            }
        }

        /// <summary>
        /// Быстрый отказ при ошибке конфигурации/прав БД (класс FatalConfig): её исправит только администратор, ждать
        /// <c>MaxUnavailable</c> и занимать слот нельзя. Достигнутый прогресс сохранён. В лог - только тип и SQLSTATE.
        /// </summary>
        [DoesNotReturn]
        private void FailFast(Exception error)
        {
            owner._logger.LogError(
                "Reprocess job {JobId} stopped: fatal configuration error while writing: {ErrorType} SqlState={SqlState}",
                job.Id, error.GetType().Name, WriteErrorClassifier.FindSqlState(error));
            throw new ReprocessFailedException(ReprocessErrorCodes.Internal);
        }

        private void Flush(CountingStore store)
        {
            job.AddWritten(store.Inserted, store.Attempted - store.Inserted);
            store.Reset();
        }

        private void Recovered()
        {
            _unavailableSince = null;
            _backoff.Reset();
        }

        private async ValueTask BackoffAsync() => await Task.Delay(_backoff.NextDelay(), owner._time, soft);

        /// <summary>Пауза при недоступной инфраструктуре; дольше <c>MaxUnavailable</c> - задача завершается ошибкой.</summary>
        private async ValueTask WaitForRecoveryAsync(string errorCode)
        {
            var now = owner._time.GetUtcNow();
            _unavailableSince ??= now;
            var left = owner._settings.MaxUnavailable - (now - _unavailableSince.Value);
            if (left <= TimeSpan.Zero)
            {
                throw new ReprocessFailedException(errorCode);
            }

            var delay = _backoff.NextDelay();
            await Task.Delay(delay < left ? delay : left, owner._time, soft);
        }

        private static ReprocessReportEntry Entry(
            ReprocessReportKind kind, ReplayMessage message, string stage, string code, string? exceptionType, string? sqlState) =>
            new(kind, message.Message.Topic, message.Message.Partition, message.Message.Offset, message.CoordinatesFromHeaders, stage, code, exceptionType, sqlState);

        private static string StageOf(RejectionKind kind) => kind switch
        {
            RejectionKind.Format => PipelineStages.Parse,
            RejectionKind.Validation => PipelineStages.Validation,
            _ => PipelineStages.Rules,
        };
    }
}

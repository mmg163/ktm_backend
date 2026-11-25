using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Core.Reprocessing;

public enum StartStatus
{
    Started,

    /// <summary>Уже есть активная задача (<c>409</c>, <c>activeJobId</c>).</summary>
    Conflict,

    /// <summary>Ошибки аргументов запроса и период, начало которого нельзя подтвердить (retention): <c>400</c>, ошибки по полям.</summary>
    Invalid,

    /// <summary>Источник не ответил при открытии или его нет (брокер недоступен, топика нет); запрос корректен, задача не создана.</summary>
    SourceUnavailable,
}

/// <summary>Итог запуска: заполнено то поле, которое соответствует <see cref="Status"/>.</summary>
public sealed record StartResult
{
    private StartResult(StartStatus status) => Status = status;

    public StartStatus Status { get; }

    public ReprocessJobSnapshot? Job { get; private init; }

    public Guid? ActiveJobId { get; private init; }

    public IReadOnlyList<ReprocessValidationError> Errors { get; private init; } = [];

    internal static StartResult Started(ReprocessJobSnapshot job) => new(StartStatus.Started) { Job = job };

    internal static StartResult Conflict(Guid activeJobId) => new(StartStatus.Conflict) { ActiveJobId = activeJobId };

    internal static StartResult Invalid(IReadOnlyList<ReprocessValidationError> errors) => new(StartStatus.Invalid) { Errors = errors };

    internal static StartResult SourceUnavailable() => new(StartStatus.SourceUnavailable);
}

public enum CancelStatus
{
    Accepted,
    NotFound,

    /// <summary>Задача уже завершена (<c>409</c>).</summary>
    AlreadyFinished,
}

public enum ReportStatus
{
    Ok,
    NotFound,

    /// <summary>Задача ещё активна (<c>409</c>).</summary>
    NotFinished,
}

public sealed record ReprocessReportResult(ReportStatus Status, ReprocessReportPage? Page);

/// <summary>
/// Задачи повторной обработки (ADR-0006 п. 2.6, design Р-7): одна активная задача (атомарная проверка при запуске),
/// история последних <see cref="HistorySize"/> в памяти, кооперативная отмена, отмена задачи при остановке хоста.
/// Состояние теряется при перезапуске сервиса: повторный запуск безопасен благодаря идемпотентности записи.
/// Не знает ни HTTP, ни Kafka, ни Npgsql, ни <c>IngestionLoop</c>; ничего не публикует и не коммитит.
/// </summary>
public sealed class ReprocessJobManager(
    IReplaySource source,
    ReprocessRunner runner,
    ReprocessSettings settings,
    TimeProvider time,
    ILogger<ReprocessJobManager> logger)
{
    public const int HistorySize = 10;

    private readonly Lock _gate = new();
    private readonly List<ReprocessJob> _history = [];
    private readonly CancellationTokenSource _stopping = new();
    private ReprocessJob? _active;

    // Сигнал освобождения слота: создаётся вместе с занятием слота и завершается ровно один раз - когда задача закончила
    // исполнение либо не дошла до запуска. Остановка хоста ждёт его, поэтому ждёт и открывающуюся задачу.
    private TaskCompletionSource? _activeDone;

    /// <summary>
    /// Запускает задачу. Слот «одна задача» занимается атомарно до открытия источника, поэтому два одновременных запуска
    /// дают ровно одну задачу. Открытие источника (снимок границ, проверка retention) идёт синхронно с запросом, чтобы ошибку
    /// периода вернуть как <see cref="StartStatus.Invalid"/>; само чтение - в фоне. Слот освобождается на любом пути, кроме
    /// передачи задачи фоновому исполнению; остановка хоста прерывает открытие источника.
    /// </summary>
    public async ValueTask<StartResult> StartAsync(ReprocessRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        _stopping.Token.ThrowIfCancellationRequested();

        var errors = ReprocessRequestValidator.Validate(request, settings.MaxRateLimit);
        if (errors.Count > 0)
        {
            return StartResult.Invalid(errors);
        }

        var job = new ReprocessJob(Guid.NewGuid(), request, settings.MaxReportEntries, time.GetUtcNow());
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_gate)
        {
            // Слот, занятый после начала остановки, StopAsync не увидел бы: отказываем под тем же замком.
            _stopping.Token.ThrowIfCancellationRequested();
            if (_active is { } running)
            {
                return StartResult.Conflict(running.Id);
            }

            _active = job;
            _activeDone = done;
        }

        IReplayReader? reader = null;
        var handedOver = false;
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopping.Token);
            ReplayOpenResult opened;
            try
            {
                opened = await source.OpenAsync(request, linked.Token);
            }
            catch (ReplaySourceException ex)
            {
                logger.LogError("Reprocess job {JobId} was not started: {ErrorCode}, {ErrorType}", job.Id, ReprocessErrorCodes.SourceUnavailable, ex.GetType().Name);
                return StartResult.SourceUnavailable();
            }

            if (opened.Reader is not { } opening)
            {
                return StartResult.Invalid(opened.Errors);
            }

            reader = opening;
            var partitions = reader.Partitions;
            lock (_gate)
            {
                if (_stopping.IsCancellationRequested)
                {
                    throw new OperationCanceledException(_stopping.Token);
                }

                job.MarkRunning(time.GetUtcNow(), partitions);
                _history.Insert(0, job);
                if (_history.Count > HistorySize)
                {
                    _history.RemoveRange(HistorySize, _history.Count - HistorySize);
                }

                _ = Task.Run(() => ExecuteAsync(job, opening, done), CancellationToken.None);
                handedOver = true;
            }
        }
        finally
        {
            if (!handedOver)
            {
                // Задача, не дошедшая до запуска, в историю не попадает; читатель закрывается, слот освобождается.
                await CloseReaderAsync(job, reader);
                Release(job, done);
            }
        }

        logger.LogInformation(
            "Reprocess job {JobId} started: Source={Source} From={From} To={To} Reason={Reason} MaxRate={MaxRate}",
            job.Id, request.Source, request.From, request.To, request.Reason, request.MaxRate);
        return StartResult.Started(job.Snapshot());
    }

    public ReprocessJobSnapshot? Get(Guid id)
    {
        lock (_gate)
        {
            return Find(id)?.Snapshot();
        }
    }

    /// <summary>Последние задачи, новые первыми.</summary>
    public IReadOnlyList<ReprocessJobSnapshot> List()
    {
        lock (_gate)
        {
            return _history.Select(j => j.Snapshot()).ToArray();
        }
    }

    /// <summary>Просит отменить задачу: она остановится после текущей пачки, достигнутый прогресс сохраняется.</summary>
    public CancelStatus Cancel(Guid id)
    {
        lock (_gate)
        {
            if (Find(id) is not { } job)
            {
                return CancelStatus.NotFound;
            }

            if (job.IsFinished)
            {
                return CancelStatus.AlreadyFinished;
            }

            job.RequestCancel();
            return CancelStatus.Accepted;
        }
    }

    public ReprocessReportResult GetReport(Guid id, int limit, int offset)
    {
        lock (_gate)
        {
            if (Find(id) is not { } job)
            {
                return new ReprocessReportResult(ReportStatus.NotFound, null);
            }

            return job.IsFinished
                ? new ReprocessReportResult(ReportStatus.Ok, job.GetReportPage(limit, offset))
                : new ReprocessReportResult(ReportStatus.NotFinished, null);
        }
    }

    /// <summary>Завершение задачи (в том числе после остановки хоста); для неактивной или неизвестной - уже выполненная задача.</summary>
    public Task WhenFinishedAsync(Guid id)
    {
        lock (_gate)
        {
            return _active?.Id == id && _activeDone is { } done ? done.Task : Task.CompletedTask;
        }
    }

    /// <summary>
    /// Остановка хоста: прерывает открытие источника, отменяет активную задачу (offset никогда не коммитился) и ждёт, пока слот
    /// освободится: и открывающаяся, и исполняемая задача.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Синхронная отмена: к моменту возврата токены раннера и открытия уже отменены (CancelAsync распространяет отмену позже).
        _stopping.Cancel();
        Task? pending;
        lock (_gate)
        {
            pending = _activeDone?.Task;
        }

        if (pending is not null)
        {
            await pending.WaitAsync(cancellationToken);
        }
    }

    private ReprocessJob? Find(Guid id) => _history.FirstOrDefault(j => j.Id == id);

    /// <summary>Освобождает слот и сообщает об этом ожидающим; повторный вызов безвреден.</summary>
    private void Release(ReprocessJob job, TaskCompletionSource done)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_active, job))
            {
                _active = null;
            }
        }

        done.TrySetResult();
    }

    private async ValueTask CloseReaderAsync(ReprocessJob job, IReplayReader? reader)
    {
        if (reader is null)
        {
            return;
        }

        try
        {
            await reader.DisposeAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning("Reprocess job {JobId}: closing the source failed: {ErrorType}", job.Id, ex.GetType().Name);
        }
    }

    private async Task ExecuteAsync(ReprocessJob job, IReplayReader reader, TaskCompletionSource done)
    {
        try
        {
            ReprocessRunResult result;
            try
            {
                result = await runner.RunAsync(job, reader, _stopping.Token);
            }
            catch (Exception ex)
            {
                // Раннер исключений не выпускает; это страховка, чтобы слот не остался занятым навсегда.
                logger.LogError("Reprocess job {JobId} failed in the runner: {ErrorType}", job.Id, ex.GetType().Name);
                result = new ReprocessRunResult(ReprocessOutcome.Failed, ReprocessErrorCodes.Internal);
            }

            await CloseReaderAsync(job, reader);

            var state = result.Outcome switch
            {
                ReprocessOutcome.Failed => ReprocessJobState.Failed,
                ReprocessOutcome.Cancelled => ReprocessJobState.Cancelled,
                _ => ReprocessJobState.Completed,
            };

            // Завершение и освобождение слота - под одним замком: наблюдатель не увидит «завершена, но слот занят».
            // Порядок замков: _gate -> замок задачи; код задачи и раннера _gate не берёт.
            lock (_gate)
            {
                job.Finish(state, result.Outcome, result.ErrorCode, time.GetUtcNow());
                if (ReferenceEquals(_active, job))
                {
                    _active = null;
                }
            }

            var counters = job.Snapshot().Counters;
            logger.LogInformation(
                "Reprocess job {JobId} finished: Outcome={Outcome} ErrorCode={ErrorCode} Read={Read} SkippedByFilter={SkippedByFilter} Written={Written} Duplicates={Duplicates} RejectedAgain={RejectedAgain} FailedAgain={FailedAgain}",
                job.Id, result.Outcome, result.ErrorCode, counters.Read, counters.SkippedByFilter, counters.Written, counters.Duplicates,
                counters.RejectedAgain.Values.Sum(), counters.FailedAgain);
        }
        finally
        {
            // Слот мог остаться занятым только при исключении выше (например, в логгере): освобождаем всегда.
            Release(job, done);
        }
    }
}

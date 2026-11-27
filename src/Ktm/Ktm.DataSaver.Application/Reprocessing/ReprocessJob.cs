namespace Ktm.DataSaver.Application.Reprocessing;

public enum ReprocessJobState
{
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// <summary>Итог завершённой задачи (ADR-0006 п. 2.6): заменяет коды выхода CLI.</summary>
public enum ReprocessOutcome
{
    /// <summary>Нет сообщений, снова не записанных из-за ошибки, привязанной к сообщению (повторно отбракованные в отчёте).</summary>
    Succeeded,

    /// <summary>Часть сообщений снова не записана.</summary>
    CompletedWithFailures,

    /// <summary>Инфраструктурный сбой либо отказ записи класса FatalConfig (код <c>internal</c>); задачу можно запустить заново после устранения причины.</summary>
    Failed,

    Cancelled,
}

/// <summary>
/// Значения <c>error.code</c> задачи в состоянии <see cref="ReprocessJobState.Failed"/>. Отказ записи класса FatalConfig
/// (нет прав или таблицы, ошибка конфигурации БД) - <see cref="Internal"/>: причина в логе, ждать её устранения нельзя.
/// </summary>
public static class ReprocessErrorCodes
{
    public const string DbUnavailable = "db_unavailable";
    public const string SourceUnavailable = "source_unavailable";
    public const string Internal = "internal";
}

public enum ReprocessReportKind
{
    /// <summary>Конвейер снова отбраковал сообщение (формат, C5, правило).</summary>
    RejectedAgain,

    /// <summary>Сообщение снова не удалось обработать (ошибка, привязанная к сообщению).</summary>
    FailedAgain,
}

/// <summary>
/// Запись отчёта: только координаты, стадия и коды (ADR-0006 п. 2.8). Тел, значений полей и текстов исключений нет;
/// <c>ExceptionType</c> - имя типа. <c>CoordinatesFromHeaders = false</c> - заголовков <c>ktm-source-*</c> не было, координаты фактические.
/// </summary>
public sealed record ReprocessReportEntry(
    ReprocessReportKind Kind,
    string Topic,
    int Partition,
    long Offset,
    bool CoordinatesFromHeaders,
    string Stage,
    string Code,
    string? ExceptionType = null,
    string? SqlState = null);

/// <summary>Страница отчёта: <c>Total</c> - число записей отчёта; <c>Truncated</c> - потолок записей достигнут, часть не сохранена (счётчики полные).</summary>
public sealed record ReprocessReportPage(IReadOnlyList<ReprocessReportEntry> Entries, int Total, int Limit, int Offset, bool Truncated);

public sealed record ReprocessPartitionProgress(string Topic, int Partition, long StartOffset, long EndOffset, long NextOffset);

public sealed record ReprocessCounters(
    long Read,
    long SkippedByFilter,
    long Written,
    long Duplicates,
    IReadOnlyDictionary<string, long> RejectedAgain,
    long FailedAgain);

/// <summary>Неизменяемый снимок состояния задачи: потокобезопасен, тел сообщений не содержит.</summary>
public sealed record ReprocessJobSnapshot(
    Guid Id,
    ReprocessRequest Request,
    ReprocessJobState State,
    ReprocessOutcome? Outcome,
    string? ErrorCode,
    ReprocessCounters Counters,
    IReadOnlyList<ReprocessPartitionProgress> Partitions,
    long TotalMessages,
    TimeSpan? EstimatedRemaining,
    int ReportEntries,
    bool ReportTruncated,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? FinishedAt)
{
    public bool IsActive => State is ReprocessJobState.Queued or ReprocessJobState.Running;
}

/// <summary>
/// Состояние одной задачи в памяти. Изменяется раннером и менеджером (internal), читается через <see cref="Snapshot"/>.
/// Все обращения под одним замком: потоков два (исполнитель задачи и читатели статуса).
/// </summary>
public sealed class ReprocessJob
{
    private readonly Lock _gate = new();
    private readonly CancellationTokenSource _cancel = new();
    private readonly Dictionary<string, long> _rejectedAgain = new(StringComparer.Ordinal);
    private readonly List<ReprocessReportEntry> _report = [];
    private readonly int _maxReportEntries;
    private ReprocessPartitionProgress[] _partitions = [];
    private ReprocessJobState _state = ReprocessJobState.Queued;
    private ReprocessOutcome? _outcome;
    private string? _errorCode;
    private long _read;
    private long _skipped;
    private long _written;
    private long _duplicates;
    private long _failedAgain;
    private long _total;
    private bool _truncated;
    private DateTimeOffset? _startedAt;
    private DateTimeOffset? _finishedAt;

    internal ReprocessJob(Guid id, ReprocessRequest request, int maxReportEntries, DateTimeOffset createdAt)
    {
        Id = id;
        Request = request;
        CreatedAt = createdAt;
        _maxReportEntries = maxReportEntries;
    }

    public Guid Id { get; }

    public ReprocessRequest Request { get; }

    public DateTimeOffset CreatedAt { get; }

    /// <summary>Запрошена отмена: раннер проверяет её между пачками и в ожиданиях.</summary>
    internal CancellationToken CancelToken => _cancel.Token;

    internal void RequestCancel() => _cancel.Cancel();

    internal void MarkRunning(DateTimeOffset now, IReadOnlyList<ReplayPartition> partitions)
    {
        lock (_gate)
        {
            _state = ReprocessJobState.Running;
            _startedAt = now;
            _partitions = partitions.Select(p => new ReprocessPartitionProgress(p.Topic, p.Partition, p.StartOffset, p.EndOffset, p.StartOffset)).ToArray();
            _total = partitions.Sum(p => p.Count);
        }
    }

    internal void AddRead(int count, int skippedByFilter)
    {
        lock (_gate)
        {
            _read += count;
            _skipped += skippedByFilter;
        }
    }

    internal void AddWritten(long inserted, long duplicates)
    {
        lock (_gate)
        {
            _written += inserted;
            _duplicates += duplicates;
        }
    }

    internal void AddRejectedAgain(ReprocessReportEntry entry)
    {
        lock (_gate)
        {
            _rejectedAgain[entry.Code] = _rejectedAgain.GetValueOrDefault(entry.Code) + 1;
            AddEntry(entry);
        }
    }

    internal void AddFailedAgain(ReprocessReportEntry entry)
    {
        lock (_gate)
        {
            _failedAgain++;
            AddEntry(entry);
        }
    }

    /// <summary>Сдвигает позицию партиции читаемого топика после прочитанного сообщения.</summary>
    internal void Advance(string topic, int partition, long offset)
    {
        lock (_gate)
        {
            for (var i = 0; i < _partitions.Length; i++)
            {
                var current = _partitions[i];
                if (current.Topic == topic && current.Partition == partition && offset + 1 > current.NextOffset)
                {
                    _partitions[i] = current with { NextOffset = Math.Min(offset + 1, current.EndOffset) };
                    return;
                }
            }
        }
    }

    /// <summary>Источник исчерпан: все партиции дочитаны до границ.</summary>
    internal void MarkPartitionsDone()
    {
        lock (_gate)
        {
            for (var i = 0; i < _partitions.Length; i++)
            {
                _partitions[i] = _partitions[i] with { NextOffset = _partitions[i].EndOffset };
            }
        }
    }

    internal void Finish(ReprocessJobState state, ReprocessOutcome outcome, string? errorCode, DateTimeOffset now)
    {
        lock (_gate)
        {
            _state = state;
            _outcome = outcome;
            _errorCode = errorCode;
            _finishedAt = now;
        }
    }

    /// <summary>Задача завершена (отчёт доступен).</summary>
    public bool IsFinished
    {
        get
        {
            lock (_gate)
            {
                return _state is ReprocessJobState.Completed or ReprocessJobState.Failed or ReprocessJobState.Cancelled;
            }
        }
    }

    public ReprocessReportPage GetReportPage(int limit, int offset)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);

        lock (_gate)
        {
            var page = _report.Skip(offset).Take(limit).ToArray();
            return new ReprocessReportPage(page, _report.Count, limit, offset, _truncated);
        }
    }

    public ReprocessJobSnapshot Snapshot()
    {
        lock (_gate)
        {
            return new ReprocessJobSnapshot(
                Id,
                Request,
                _state,
                _outcome,
                _errorCode,
                new ReprocessCounters(_read, _skipped, _written, _duplicates, new Dictionary<string, long>(_rejectedAgain, StringComparer.Ordinal), _failedAgain),
                _partitions.ToArray(),
                _total,
                EstimateRemaining(),
                _report.Count,
                _truncated,
                CreatedAt,
                _startedAt,
                _finishedAt);
        }
    }

    // Оценка по скорости, заданной задачей: сколько осталось по снятым границам. Нет оценки для не запущенной и завершённой задачи.
    // Оценка по смещениям: пропуски offset в партиции (удалённые retention, служебные записи) делают её завышенной.
    private TimeSpan? EstimateRemaining()
    {
        if (_state != ReprocessJobState.Running || _total == 0)
        {
            return null;
        }

        var processed = _partitions.Sum(p => Math.Max(0, p.NextOffset - p.StartOffset));
        var remaining = Math.Max(0, _total - processed);
        return TimeSpan.FromSeconds((double)remaining / Math.Max(1, Request.MaxRate));
    }

    private void AddEntry(ReprocessReportEntry entry)
    {
        if (_report.Count >= _maxReportEntries)
        {
            _truncated = true;
            return;
        }

        _report.Add(entry);
    }
}

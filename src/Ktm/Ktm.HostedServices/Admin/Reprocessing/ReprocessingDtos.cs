using Ktm.DataSaver.Application.Reprocessing;

namespace Ktm.HostedServices.Admin.Reprocessing;

/// <summary>Запрос запуска повторной обработки. Время - RFC 3339, только UTC с суффиксом <c>Z</c>.</summary>
public sealed record StartJobRequest
{
    /// <summary>Источник: <c>dlq</c>, <c>rejected</c> или <c>raw</c>.</summary>
    public string? Source { get; init; }

    /// <summary>Для raw: ktm.events.raw.skud, ktm.events.raw.cv или ktm.events.raw.1c-do; обязательно при regex-подписке. Для других источников запрещено.</summary>
    public string? Topic { get; init; }

    /// <summary>Начало периода по Kafka-timestamp сообщения (включительно). Для <c>raw</c> обязательно.</summary>
    public string? From { get; init; }

    /// <summary>Конец периода (не включая). Для <c>raw</c> обязательно.</summary>
    public string? To { get; init; }

    /// <summary>Фильтр по коду причины (<c>ktm-dlq-reason</c>/<c>ktm-reject-reason</c>); не для <c>raw</c>; до 200 символов.</summary>
    public string? Reason { get; init; }

    /// <summary>Сообщений в секунду; по умолчанию <c>DataSaver__Reprocess__MaxRatePerSecond</c>, не больше <c>MaxRateLimit</c>.</summary>
    public int? MaxRate { get; init; }
}

/// <summary>Параметры задачи, как они были приняты.</summary>
/// <param name="Source">Источник: <c>dlq</c>, <c>rejected</c>, <c>raw</c>.</param>
/// <param name="From">Начало периода (UTC) либо <c>null</c>.</param>
/// <param name="To">Конец периода (UTC, не включая) либо <c>null</c>.</param>
/// <param name="Reason">Фильтр по коду причины либо <c>null</c>.</param>
/// <param name="MaxRate">Скорость, сообщений в секунду.</param>
/// <param name="Topic">Выбранный входной топик либо <c>null</c>.</param>
public sealed record JobParameters(string Source, DateTimeOffset? From, DateTimeOffset? To, string? Reason, int MaxRate, string? Topic);

/// <summary>Ошибка завершения задачи в состоянии <c>Failed</c>.</summary>
/// <param name="Code"><c>db_unavailable</c>, <c>source_unavailable</c> или <c>internal</c>.</param>
public sealed record JobError(string Code);

/// <summary>Прогресс по партиции читаемого топика (границы сняты при запуске).</summary>
public sealed record JobPartition(string Topic, int Partition, long StartOffset, long EndOffset, long NextOffset);

/// <summary>Состояние и прогресс задачи. Тел сообщений и значений полей нет.</summary>
/// <param name="Id">Идентификатор задачи.</param>
/// <param name="State"><c>Queued</c>, <c>Running</c>, <c>Completed</c>, <c>Failed</c>, <c>Cancelled</c>.</param>
/// <param name="Outcome">Итог завершённой задачи: <c>Succeeded</c>, <c>CompletedWithFailures</c>, <c>Failed</c>, <c>Cancelled</c>.</param>
/// <param name="Error">Код ошибки для <c>Failed</c>.</param>
/// <param name="Parameters">Принятые параметры.</param>
/// <param name="Read">Прочитано сообщений.</param>
/// <param name="SkippedByFilter">Пропущено фильтром <c>reason</c>.</param>
/// <param name="Written">Записано новых событий.</param>
/// <param name="Duplicates">Отсечено как дубли (ключ уже есть в журнале).</param>
/// <param name="RejectedAgain">Снова отбраковано конвейером, по коду причины.</param>
/// <param name="FailedAgain">Снова не записано из-за ошибки, привязанной к сообщению.</param>
/// <param name="Partitions">Прогресс по партициям.</param>
/// <param name="TotalMessages">Сообщений в снятых границах.</param>
/// <param name="EstimatedRemainingSeconds">Оценка оставшегося времени для выполняющейся задачи.</param>
/// <param name="ReportEntries">Записей в отчёте.</param>
/// <param name="ReportTruncated">Потолок записей отчёта достигнут, часть записей не сохранена (счётчики полные).</param>
/// <param name="CreatedAt">Время создания (UTC).</param>
/// <param name="StartedAt">Время начала чтения (UTC).</param>
/// <param name="FinishedAt">Время завершения (UTC).</param>
public sealed record JobResponse(
    Guid Id,
    ReprocessJobState State,
    ReprocessOutcome? Outcome,
    JobError? Error,
    JobParameters Parameters,
    long Read,
    long SkippedByFilter,
    long Written,
    long Duplicates,
    IReadOnlyDictionary<string, long> RejectedAgain,
    long FailedAgain,
    IReadOnlyList<JobPartition> Partitions,
    long TotalMessages,
    double? EstimatedRemainingSeconds,
    int ReportEntries,
    bool ReportTruncated,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? FinishedAt)
{
    public static JobResponse From(ReprocessJobSnapshot job) => new(
        job.Id,
        job.State,
        job.Outcome,
        job.ErrorCode is null ? null : new JobError(job.ErrorCode),
        new JobParameters(SourceName(job.Request.Source), job.Request.From, job.Request.To, job.Request.Reason, job.Request.MaxRate, job.Request.Topic),
        job.Counters.Read,
        job.Counters.SkippedByFilter,
        job.Counters.Written,
        job.Counters.Duplicates,
        job.Counters.RejectedAgain,
        job.Counters.FailedAgain,
        job.Partitions.Select(p => new JobPartition(p.Topic, p.Partition, p.StartOffset, p.EndOffset, p.NextOffset)).ToArray(),
        job.TotalMessages,
        job.EstimatedRemaining?.TotalSeconds,
        job.ReportEntries,
        job.ReportTruncated,
        job.CreatedAt,
        job.StartedAt,
        job.FinishedAt);

    public static string SourceName(ReprocessSource source) => source switch
    {
        ReprocessSource.Dlq => "dlq",
        ReprocessSource.Rejected => "rejected",
        _ => "raw",
    };
}

/// <summary>Список последних задач (новые первыми, не больше 10; состояние в памяти).</summary>
/// <param name="Items">Задачи.</param>
public sealed record JobListResponse(IReadOnlyList<JobResponse> Items);

/// <summary>Запись отчёта: координаты и причина, без тел сообщений и текстов исключений.</summary>
/// <param name="Kind"><c>RejectedAgain</c> (снова отбраковано) или <c>FailedAgain</c> (снова не записано).</param>
/// <param name="Topic">Исходный топик сообщения.</param>
/// <param name="Partition">Партиция.</param>
/// <param name="Offset">Offset.</param>
/// <param name="CoordinatesFromHeaders"><c>true</c> - координаты из <c>ktm-source-*</c>; <c>false</c> - фактические (заголовков не было).</param>
/// <param name="Stage">Стадия: <c>parse</c>, <c>validation</c>, <c>rules</c>, <c>write</c>.</param>
/// <param name="Code">Код причины.</param>
/// <param name="ExceptionType">Имя типа исключения (для <c>FailedAgain</c>), без текста.</param>
/// <param name="SqlState">SQLSTATE ошибки записи, если есть.</param>
public sealed record ReportEntryResponse(
    ReprocessReportKind Kind,
    string Topic,
    int Partition,
    long Offset,
    bool CoordinatesFromHeaders,
    string Stage,
    string Code,
    string? ExceptionType,
    string? SqlState)
{
    public static ReportEntryResponse From(ReprocessReportEntry e) =>
        new(e.Kind, e.Topic, e.Partition, e.Offset, e.CoordinatesFromHeaders, e.Stage, e.Code, e.ExceptionType, e.SqlState);
}

/// <summary>Страница отчёта задачи.</summary>
/// <param name="Items">Записи страницы.</param>
/// <param name="Total">Всего записей в отчёте.</param>
/// <param name="Limit">Размер страницы.</param>
/// <param name="Offset">Смещение страницы.</param>
/// <param name="Truncated">Потолок числа записей достигнут: часть записей не сохранена.</param>
public sealed record ReportPageResponse(IReadOnlyList<ReportEntryResponse> Items, int Total, int Limit, int Offset, bool Truncated);

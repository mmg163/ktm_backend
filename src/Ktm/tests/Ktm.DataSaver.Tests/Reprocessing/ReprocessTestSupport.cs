using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Reprocessing;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Tests.Decoding;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Reprocessing;

/// <summary>Читатель источника из заранее заданных сообщений: отдаёт пачки по порядку, как Kafka-источник.</summary>
internal sealed class FakeReader(IReadOnlyList<ReplayMessage> messages) : IReplayReader
{
    private int _next;

    public List<int> Requested { get; } = [];

    /// <summary>Выполняется перед выдачей каждой пачки (номер вызова с 1): блокировка, сбой источника.</summary>
    public Func<int, ValueTask>? BeforeRead { get; set; }

    public bool Disposed { get; private set; }

    public IReadOnlyList<ReplayPartition> Partitions { get; } =
        [new("ktm.events.dlq", 0, 0, messages.Count)];

    public async ValueTask<IReadOnlyList<ReplayMessage>> ReadBatchAsync(int maxCount, CancellationToken cancellationToken)
    {
        Requested.Add(maxCount);
        if (BeforeRead is { } before)
        {
            await before(Requested.Count);
        }

        var batch = messages.Skip(_next).Take(maxCount).ToArray();
        _next += batch.Length;
        return batch;
    }

    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}

/// <summary>Запись с уникальным ключом, как в БД: уже существующий ключ - дубль (не считается добавленным).</summary>
internal sealed class FakeWriter : IReplayEventWriter
{
    public HashSet<string> Rows { get; } = [];

    public List<string[]> Calls { get; } = [];

    /// <summary>Исключение вместо записи по номеру вызова (с 1) и составу пачки.</summary>
    public Func<int, string[], Exception?> Fail { get; set; } = (_, _) => null;

    public ValueTask<int> WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken)
    {
        var ids = events.Select(e => e.SourceEventId).ToArray();
        Calls.Add(ids);
        if (Fail(Calls.Count, ids) is { } error)
        {
            throw error;
        }

        return ValueTask.FromResult(ids.Count(Rows.Add));
    }
}

/// <summary>Стадия по идентификатору события: отбраковка, исключение либо приём.</summary>
internal sealed class ScriptedStage(Func<string, int, IngestionVerdict?> script, string stage = PipelineStages.Rules) : IIngestionStage
{
    private readonly Dictionary<string, int> _calls = [];

    public string Stage => stage;

    public int CallsFor(string sourceEventId) => _calls.GetValueOrDefault(sourceEventId);

    public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
    {
        _calls[parsed.SourceEventId] = _calls.GetValueOrDefault(parsed.SourceEventId) + 1;
        return script(parsed.SourceEventId, _calls[parsed.SourceEventId]) ?? IngestionVerdict.Accept;
    }

    public static IngestionVerdict Reject(string reason) => IngestionVerdict.Reject(new RejectionInfo(RejectionKind.Rule, reason, "detail"));
}

internal sealed class ReprocessFixture
{
    public readonly FakeTimeProvider Time = new();
    public readonly FakeWriter Writer = new();
    public readonly FakeLogger<ReprocessRunner> Logger = new();
    public readonly ReprocessRunner Runner;
    public readonly ReprocessSettings Settings;

    public ReprocessFixture(ScriptedStage? stage = null, int batchSize = 3, TimeSpan? maxUnavailable = null, int maxReportEntries = 1000, int maxAttempts = 3)
    {
        Settings = new ReprocessSettings(400, maxUnavailable ?? TimeSpan.FromMinutes(10), maxReportEntries, batchSize, maxAttempts);
        IIngestionStage[] stages = stage is null ? [] : [stage];
        Runner = new ReprocessRunner(
            new IngestionPipeline(new FormatStage(new EventDecoder()), stages),
            Writer,
            Settings,
            () => new BackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 2, 0, () => 0.5),
            Time,
            Logger);
    }

    public static ReplayMessage Valid(string id, long offset, string? reason = null, string? personId = null) =>
        Wrap(EventSamples.Message(
            EventSamples.SkudWith(n =>
            {
                n["sourceEventId"] = id;
                if (personId is not null)
                {
                    n["externalPersonId"] = personId;
                }
            }), 0, offset), reason);

    public static ReplayMessage Garbage(long offset) => Wrap(EventSamples.Message(System.Text.Encoding.UTF8.GetBytes("not json"), 0, offset), null);

    private static ReplayMessage Wrap(ConsumedMessage message, string? reason) =>
        new(message with { Topic = "ktm.events.raw" }, reason, null, "ktm.events.dlq", message.Partition, message.Offset, true);

    public ReprocessJob NewJob(ReprocessRequest? request = null, int maxReportEntries = 1000) =>
        new(Guid.NewGuid(), request ?? new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 400), maxReportEntries, Time.GetUtcNow());

    /// <summary>
    /// Запуск с «насосом» часов: ожидания раннера (скорость, backoff) ждут таймера, а время в тесте не идёт само.
    /// </summary>
    public async Task<(ReprocessRunResult Result, ReprocessJobSnapshot Snapshot)> RunAsync(ReprocessJob job, IReplayReader reader, CancellationToken stopping = default)
    {
        job.MarkRunning(Time.GetUtcNow(), reader.Partitions);
        using var done = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            while (!done.IsCancellationRequested)
            {
                Time.Advance(TimeSpan.FromMilliseconds(500));
                await Task.Delay(1);
            }
        });

        try
        {
            var result = await Runner.RunAsync(job, reader, stopping).WaitAsync(TimeSpan.FromSeconds(20));
            return (result, job.Snapshot());
        }
        finally
        {
            await done.CancelAsync();
            await pump;
        }
    }
}

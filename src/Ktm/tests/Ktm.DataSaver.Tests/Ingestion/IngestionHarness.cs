using System.Text;
using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Tests.Decoding;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Ingestion;

internal sealed class FakeDbException(string sqlState) : Exception("db"), ISqlStateError
{
    public string? SqlState { get; } = sqlState;
}

internal sealed class FakeSource(FakeTimeProvider time, List<string> log) : IMessageSource
{
    private readonly List<ConsumedMessage> _queue = [];
    private readonly List<TopicPartitionKey> _assigned = [];
    private readonly List<TopicPartitionKey> _revoked = [];
    private readonly HashSet<TopicPartitionKey> _paused = [];
    private int _delivered;

    public List<TopicPartitionKey> CurrentAssignment { get; } = [Harness.P(0), Harness.P(1)];

    public List<IReadOnlyCollection<TopicPartitionKey>> PauseCalls { get; } = [];

    public List<IReadOnlyCollection<TopicPartitionKey>> ResumeCalls { get; } = [];

    /// <summary>Адаптер, нарушающий контракт: пустой опрос возвращается сразу, без ожидания таймаута.</summary>
    public bool ReturnNullImmediately { get; set; }

    /// <summary>Consume не возвращается до отмены токена (замена зависшего клиента брокера).</summary>
    public bool Hang { get; set; }

    public int ConsumeCalls { get; private set; }

    public int IdleCalls { get; private set; }

    /// <summary>Вызывается на каждом пустом опросе (время при этом сдвигается на таймаут, как при блокирующем Consume).</summary>
    public Action<int>? OnIdle { get; set; }

    /// <summary>Вызывается после выдачи n-го сообщения.</summary>
    public Action<int>? OnDelivered { get; set; }

    public IReadOnlyCollection<TopicPartitionKey> Assignment => CurrentAssignment;

    public void Enqueue(params ConsumedMessage[] messages) => _queue.AddRange(messages);

    public void ScheduleRevoke(TopicPartitionKey partition)
    {
        _revoked.Add(partition);
        CurrentAssignment.Remove(partition);
    }

    public void ScheduleAssign(TopicPartitionKey partition)
    {
        _assigned.Add(partition);
        if (!CurrentAssignment.Contains(partition))
        {
            CurrentAssignment.Add(partition);
        }
    }

    public async ValueTask<ConsumedMessage?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        ConsumeCalls++;
        if (Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        return ConsumeCore(timeout);
    }

    private ConsumedMessage? ConsumeCore(TimeSpan timeout)
    {
        var index = _queue.FindIndex(m => !_paused.Contains(new TopicPartitionKey(m.Topic, m.Partition)));
        if (index >= 0)
        {
            var message = _queue[index];
            _queue.RemoveAt(index);
            _delivered++;
            OnDelivered?.Invoke(_delivered);
            return message;
        }

        IdleCalls++;
        if (!ReturnNullImmediately)
        {
            time.Advance(timeout);
        }

        OnIdle?.Invoke(IdleCalls);
        return null;
    }

    public AssignmentChanges TakeAssignmentChanges()
    {
        if (_assigned.Count == 0 && _revoked.Count == 0)
        {
            return AssignmentChanges.None;
        }

        var changes = new AssignmentChanges(_assigned.ToArray(), _revoked.ToArray());
        _assigned.Clear();
        _revoked.Clear();
        return changes;
    }

    public void Pause(IReadOnlyCollection<TopicPartitionKey> partitions)
    {
        log.Add("pause");
        PauseCalls.Add(partitions.ToArray());
        _paused.UnionWith(partitions);
    }

    public void Resume(IReadOnlyCollection<TopicPartitionKey> partitions)
    {
        log.Add("resume");
        ResumeCalls.Add(partitions.ToArray());
        _paused.ExceptWith(partitions);
    }
}

internal sealed class FakeStore(List<string> log) : IEventStore
{
    public List<string[]> Writes { get; } = [];

    /// <summary>received_at, с которым вызван порт, по номеру вызова.</summary>
    public List<DateTimeOffset> ReceivedAts { get; } = [];

    /// <summary>Номер вызова (с 1) -> исключение либо null.</summary>
    public Func<int, Exception?> Fail { get; set; } = _ => null;

    public Action<int>? OnCall { get; set; }

    /// <summary>Исключение по содержимому записываемой пачки (идентификаторы событий) либо null; вызывается после <see cref="Fail"/>.</summary>
    public Func<string[], Exception?> FailOn { get; set; } = _ => null;

    /// <summary>Запись «зависает» до отмены токена (замена зависшего соединения с БД).</summary>
    public bool Hang { get; set; }

    public async ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken)
    {
        log.Add("write");
        ReceivedAts.Add(receivedAt);
        Writes.Add(events.Select(e => e.SourceEventId).ToArray());
        OnCall?.Invoke(Writes.Count);
        cancellationToken.ThrowIfCancellationRequested();
        if (Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        if (Fail(Writes.Count) is { } error)
        {
            throw error;
        }

        if (FailOn(Writes[^1]) is { } contentError)
        {
            throw contentError;
        }
    }
}

internal sealed class FakePublisher(List<string> log) : IRejectionPublisher
{
    public List<string[]> Published { get; } = [];

    /// <summary>Происхождение каждого переданного отбракованного сообщения (null - формат не разобран).</summary>
    public List<EventOrigin?> Origins { get; } = [];

    public Func<int, RejectionPublishResult> Result { get; set; } = _ => RejectionPublishResult.Success;

    public Func<int, Exception?> Throw { get; set; } = _ => null;

    public ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken)
    {
        log.Add("publish");
        Published.Add(rejected.Select(r => $"{r.Message.Partition}@{r.Message.Offset}").ToArray());
        Origins.AddRange(rejected.Select(r => r.Origin));
        if (Throw(Published.Count) is { } error)
        {
            throw error;
        }

        return ValueTask.FromResult(Result(Published.Count));
    }
}

internal sealed class FakeDeadLetterPublisher(List<string> log) : IDeadLetterPublisher
{
    /// <summary>Координаты <c>partition@offset</c> по вызовам.</summary>
    public List<string[]> Published { get; } = [];

    public List<DeadLetteredMessage> Messages { get; } = [];

    public Func<int, DeadLetterPublishResult> Result { get; set; } = _ => DeadLetterPublishResult.Success;

    public Func<int, Exception?> Throw { get; set; } = _ => null;

    public Action<int>? OnCall { get; set; }

    public ValueTask<DeadLetterPublishResult> PublishAsync(IReadOnlyList<DeadLetteredMessage> messages, CancellationToken cancellationToken)
    {
        log.Add("dlq");
        Published.Add(messages.Select(m => $"{m.Message.Partition}@{m.Message.Offset}").ToArray());
        Messages.AddRange(messages);
        OnCall?.Invoke(Published.Count);
        if (Throw(Published.Count) is { } error)
        {
            throw error;
        }

        return ValueTask.FromResult(Result(Published.Count));
    }
}

internal sealed class RecordingObserver : IIngestionObserver
{
    public List<string> Retries { get; } = [];

    public int Isolations { get; private set; }

    public void Retry(string stage) => Retries.Add(stage);

    public void WriteIsolation() => Isolations++;
}

internal sealed class FakeCommitter(List<string> log) : IOffsetCommitter
{
    public List<IReadOnlyList<PartitionOffset>> Commits { get; } = [];

    public Func<int, Exception?> Fail { get; set; } = _ => null;

    public ValueTask CommitAsync(IReadOnlyList<PartitionOffset> offsets, CancellationToken cancellationToken)
    {
        log.Add("commit");
        Commits.Add(offsets);
        return Fail(Commits.Count) is { } error ? throw error : ValueTask.CompletedTask;
    }
}

internal sealed class FakeFailureHandler : IWriteFailureHandler
{
    public Func<WriteFailureContext, WriteFailureDecision> Decide { get; set; } = _ => WriteFailureDecision.Retry;

    public bool Hang { get; set; }

    /// <summary>Сколько раз цикл обратился к порту C6.</summary>
    public int Calls { get; private set; }

    public async ValueTask<WriteFailureDecision> HandleAsync(WriteFailureContext context, CancellationToken cancellationToken)
    {
        Calls++;
        if (Hang)
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        return Decide(context);
    }
}

/// <summary>Цикл на fake-портах: размер пачки 3 по умолчанию, ожидание закрытия по времени - час (пачку закрывает размер или остановка).</summary>
internal sealed class Harness
{
    public readonly List<string> Log = [];
    public readonly FakeTimeProvider Time = new();
    public readonly CancellationTokenSource Stop = new();
    public readonly FakeSource Source;
    public readonly FakeStore Store;
    public readonly FakePublisher Publisher;
    public readonly FakeDeadLetterPublisher DeadLetters;
    public readonly DlqBreaker Breaker;
    public readonly RecordingObserver Observer = new();
    public readonly FakeCommitter Committer;
    public readonly FakeFailureHandler Handler = new();
    public readonly CountingAdminNotifier Notifier = new();
    public readonly IngestionLoop Loop;

    public readonly Microsoft.Extensions.Logging.Testing.FakeLogger<IngestionLoop> Logger = new();

    public Harness(int batchSize = 3, params IIngestionStage[] stages)
        : this(batchSize, TimeSpan.FromMilliseconds(100), TimeSpan.FromHours(1), stages, DlqPolicy.Default)
    {
    }

    /// <summary>Цикл с заданным декодером формата (стадия разбора).</summary>
    public Harness(EventDecoder decoder, int batchSize = 3)
        : this(batchSize, TimeSpan.FromMilliseconds(100), TimeSpan.FromHours(1), [], DlqPolicy.Default, decoder)
    {
    }

    /// <summary>Цикл с заданной политикой dlq.</summary>
    public Harness(DlqPolicy policy, int batchSize = 3, params IIngestionStage[] stages)
        : this(batchSize, TimeSpan.FromMilliseconds(100), TimeSpan.FromHours(1), stages, policy)
    {
    }

    /// <summary>Цикл с заданными интервалом опроса и предельным ожиданием закрытия пачки по времени.</summary>
    public Harness(TimeSpan pollInterval, TimeSpan batchMaxWait, int batchSize)
        : this(batchSize, pollInterval, batchMaxWait, [], DlqPolicy.Default)
    {
    }

    private Harness(int batchSize, TimeSpan pollInterval, TimeSpan batchMaxWait, IIngestionStage[] stages, DlqPolicy policy, EventDecoder? decoder = null)
    {
        DeadLetters = new FakeDeadLetterPublisher(Log);
        Breaker = new DlqBreaker(policy.BreakerThreshold);
        Source = new FakeSource(Time, Log);
        Store = new FakeStore(Log);
        Publisher = new FakePublisher(Log);
        Committer = new FakeCommitter(Log);
        Loop = new IngestionLoop(
            Source,
            new IngestionPipeline(new FormatStage(decoder ?? new EventDecoder()), stages),
            Store,
            Publisher,
            DeadLetters,
            Committer,
            Handler,
            Notifier,
            new Batcher<ConsumedMessage>(batchSize, batchMaxWait, Time),
            new BackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 2, 0, () => 0.5),
            new IngestionLoopOptions(pollInterval, TimeSpan.FromSeconds(30)),
            policy,
            Breaker,
            Time,
            Logger,
            Observer);
    }

    public static TopicPartitionKey P(int partition) => new("ktm.events.raw", partition);

    public static ConsumedMessage Valid(int partition, long offset) =>
        EventSamples.Message(EventSamples.SkudWith(n => n["sourceEventId"] = $"e-{partition}-{offset}"), partition, offset);

    public static ConsumedMessage Garbage(int partition, long offset) =>
        EventSamples.Message(Encoding.UTF8.GetBytes("not json"), partition, offset);

    public static PartitionOffset Next(int partition, long offset) => new("ktm.events.raw", partition, offset);

    /// <summary>Остановка на первом пустом опросе в штатном режиме: к этому моменту очередь прочитана, а пачка (если была) обработана.</summary>
    public void StopWhenQueueDrained() => Source.OnIdle = n => { if (n >= 1 && Loop.State == IngestionState.Running) Stop.Cancel(); };

    public async Task RunAsync() => await Loop.RunAsync(Stop.Token).WaitAsync(TimeSpan.FromSeconds(10));

    /// <summary>Запуск с «насосом» часов: паузы в режиме остановки ждут таймера, а время в тесте не идёт само.</summary>
    public async Task RunWithClockPumpAsync()
    {
        using var done = new CancellationTokenSource();
        var pump = Task.Run(async () =>
        {
            while (!done.IsCancellationRequested)
            {
                Time.Advance(TimeSpan.FromSeconds(1));
                await Task.Delay(1);
            }
        });

        try
        {
            await RunAsync();
        }
        finally
        {
            await done.CancelAsync();
            await pump;
        }
    }
}

internal sealed class RejectingStage : IIngestionStage
{
    public string Stage => PipelineStages.Validation;

    public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed) =>
        IngestionVerdict.Reject(new RejectionInfo(RejectionKind.Validation, "test_rule", "rejected by test"));
}

/// <summary>Ошибка БД с «секретным» текстом: значения из строки не должны попадать в лог.</summary>
internal sealed class LeakyDbException(string message, string sqlState) : Exception(message), ISqlStateError
{
    public string? SqlState { get; } = sqlState;
}

/// <summary>Аналог PostgresException: SQLSTATE и объект БД, плюс текст с «секретом» значения строки.</summary>
internal sealed class LeakyDbLocationException(string message, string sqlState, string constraint, string table)
    : Exception(message), ISqlStateError, IDbErrorLocation
{
    public string? SqlState { get; } = sqlState;

    public string? ConstraintName { get; } = constraint;

    public string? TableName { get; } = table;
}

/// <summary>Тип вне списка допустимых для логирования: «тело» в Message.</summary>
internal sealed class ForeignWriterException(string message) : Exception(message);

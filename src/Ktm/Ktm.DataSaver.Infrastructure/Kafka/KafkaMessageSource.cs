using Confluent.Kafka;
using Ktm.DataSaver.Application.Batching;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Model;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Consumer по ADR-0002 D-1. Обработчики ребаланса лишь копят изменения; пауза и очистка сообщений отозванных
/// партиций - дело цикла (<see cref="TakeAssignmentChanges"/>). Чтение и commit - из потока цикла.
/// Consumer создаётся и подписывается лениво, в <see cref="Start"/>: пока не загружены правила, источник не входит в группу.
/// Dispose вызывать только после выхода <c>IngestionLoop.RunAsync</c>: Close() делает последний rebalance и не должен
/// гоняться с Consume/Commit цикла.
/// </summary>
public sealed class KafkaMessageSource : IMessageSource, IConsumerLifecycle
{
    private readonly KafkaOptions _options;
    private readonly Func<ConsumerBuilder<byte[], byte[]>, IConsumer<byte[], byte[]>> _build;
    private readonly Lock _startGate = new();
    private volatile IConsumer<byte[], byte[]>? _consumer;
    private readonly KafkaFatalErrorHandler _fatalHandler;
    private readonly ILogger<KafkaMessageSource> _logger;
    private readonly Lock _gate = new();
    private readonly Lock _adminUse = new();
    private readonly List<TopicPartitionKey> _assigned = [];
    private readonly List<TopicPartitionKey> _revoked = [];
    // Кэш текущего назначения для health-потока: обновляется обработчиками ребаланса, consumer.Assignment оттуда не читается.
    private readonly HashSet<TopicPartitionKey> _current = [];
    private readonly string _topic;
    private IAdminClient? _admin;
    private int _disposed;
    private readonly LogThrottle _throttle;

    public KafkaMessageSource(KafkaOptions options, KafkaFatalErrorHandler fatalHandler, ILogger<KafkaMessageSource> logger, LogThrottle throttle)
        : this(options, fatalHandler, logger, throttle, builder => builder.Build())
    {
    }

    internal KafkaMessageSource(
        KafkaOptions options,
        KafkaFatalErrorHandler fatalHandler,
        ILogger<KafkaMessageSource> logger,
        LogThrottle throttle,
        Func<ConsumerBuilder<byte[], byte[]>, IConsumer<byte[], byte[]>> build)
    {
        _options = options;
        _fatalHandler = fatalHandler;
        _logger = logger;
        _throttle = throttle;
        _topic = options.Topic;
        _build = build;
    }

    /// <summary>Создаёт consumer и подписывается на топик (вход в группу). Повторный вызов ничего не делает; после Dispose бросает <see cref="ObjectDisposedException"/>.</summary>
    public void Start()
    {
        lock (_startGate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_consumer is not null)
            {
                return;
            }

            var nativeLog = new KafkaNativeLogBridge(_logger, _throttle);
            var builder = new ConsumerBuilder<byte[], byte[]>(BuildConfig(_options))
                .SetErrorHandler((_, error) => _fatalHandler.Handle(error))
                .SetLogHandler((_, message) => nativeLog.Handle(message))
                .SetPartitionsAssignedHandler((_, partitions) => OnAssigned(partitions))
                .SetPartitionsRevokedHandler((_, partitions) => OnRevoked(partitions.Select(p => p.TopicPartition)))
                .SetPartitionsLostHandler((_, partitions) => OnRevoked(partitions.Select(p => p.TopicPartition)));
            var consumer = _build(builder);
            consumer.Subscribe(_topic);
            _consumer = consumer;
        }
    }

    private IConsumer<byte[], byte[]> Started =>
        _consumer ?? throw new InvalidOperationException("Kafka consumer is not started: call Start() first.");

    internal static ConsumerConfig BuildConfig(KafkaOptions options)
    {
        var config = new ConsumerConfig
        {
            GroupId = options.GroupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = options.SessionTimeoutMs,
        }.ApplyConnection(options);
        if (!string.IsNullOrWhiteSpace(options.GroupInstanceId))
        {
            config.GroupInstanceId = options.GroupInstanceId;
        }

        return config;
    }

    /// <summary>Consumer подписан и получил назначение (по кэшу обработчиков ребаланса; безопасно из любого потока).</summary>
    public bool HasAssignment
    {
        get
        {
            lock (_gate)
            {
                return _current.Count > 0;
            }
        }
    }

    /// <summary>
    /// Запрос метаданных топика через клиент, зависимый от handle consumer: проверка связи с брокером для readiness.
    /// Вызывать вне потока цикла. После остановки бросает <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <remarks>
    /// Топик, которого нет в кластере, - не сбой пробы: брокер ответил, значит доступен (ответ метаданных с ошибкой топика
    /// не бросает исключение). Отсутствие топика проявится как <c>no_partitions_assigned</c> (D-10 ADR-0002).
    /// Вызов идёт под <see cref="_adminUse"/>: <see cref="Dispose"/> не освободит admin посреди GetMetadata
    /// (ждёт не дольше <paramref name="timeout"/>). Безопасность SafeHandle у confluent подтвердить не удалось, поэтому так.
    /// </remarks>
    public void ProbeBroker(TimeSpan timeout)
    {
        lock (_adminUse)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            var admin = _admin ??= new DependentAdminClientBuilder(Started.Handle).Build();
            admin.GetMetadata(_topic, timeout);
        }
    }

    public IReadOnlyCollection<TopicPartitionKey> Assignment =>
        (_consumer?.Assignment ?? []).Select(p => new TopicPartitionKey(p.Topic, p.Partition.Value)).ToArray();

    public ValueTask<ConsumedMessage?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            // Блокирующий poll ограничен таймаутом цикла; librdkafka обслуживает группу внутри Consume.
            var result = Started.Consume(timeout);
            if (result is null)
            {
                return ValueTask.FromResult<ConsumedMessage?>(null);
            }

            return ValueTask.FromResult<ConsumedMessage?>(new ConsumedMessage(
                result.Topic,
                result.Partition.Value,
                result.Offset.Value,
                result.Message.Key ?? [],
                result.Message.Value ?? [],
                result.Message.Timestamp.UtcDateTime));
        }
        catch (ConsumeException ex)
        {
            if (ex.Error.IsFatal)
            {
                _fatalHandler.Handle(ex.Error);
                throw;
            }

            // Нефатальная ошибка чтения: клиент восстановится сам, цикл продолжает опрос; лог - не чаще раза в 30 с.
            if (_throttle.TryPass("consume:" + ex.Error.Code, out var suppressed))
            {
                _logger.LogError("Kafka consume error {Code}: {Reason} (suppressed since last: {Suppressed}; repeats are not logged for 30 s)",
                    ex.Error.Code, ex.Error.Reason, suppressed);
            }

            return ValueTask.FromResult<ConsumedMessage?>(null);
        }
    }

    public AssignmentChanges TakeAssignmentChanges()
    {
        lock (_gate)
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
    }

    public void Pause(IReadOnlyCollection<TopicPartitionKey> partitions) =>
        Apply(partitions, p => Started.Pause(p), "pause");

    public void Resume(IReadOnlyCollection<TopicPartitionKey> partitions) =>
        Apply(partitions, p => Started.Resume(p), "resume");

    /// <summary>
    /// Pause/Resume толерантны к партициям, которых уже нет в назначении (назначение и отзыв в одном Consume, D-8а):
    /// сначала пачкой, при отказе - по одной; отказ по отдельной партиции - Warning, не исключение.
    /// </summary>
    private void Apply(IReadOnlyCollection<TopicPartitionKey> partitions, Action<IEnumerable<TopicPartition>> action, string operation)
    {
        if (partitions.Count == 0) return;
        try
        {
            action(ToPartitions(partitions));
        }
        catch (Exception ex) when (ex is KafkaException or InvalidOperationException)
        {
            foreach (var partition in ToPartitions(partitions))
            {
                try
                {
                    action([partition]);
                }
                catch (Exception inner) when (inner is KafkaException or InvalidOperationException)
                {
                    _logger.LogWarning("Kafka {Operation} skipped for {Topic}[{Partition}]: {Reason}", operation, partition.Topic, partition.Partition.Value, inner.Message);
                }
            }
        }
    }

    /// <summary>
    /// Синхронный commit (для <see cref="KafkaOffsetCommitter"/>); offsets уже со смещением +1. Таймаута нет: предел
    /// остановки (ShutdownTimeout, D-8) этот вызов не прервёт; поведение при недоступном брокере проверят 7.5/7.6.
    /// </summary>
    internal void Commit(IReadOnlyList<PartitionOffset> offsets) =>
        Started.Commit(offsets.Select(o => new TopicPartitionOffset(o.Topic, o.Partition, o.Offset)));

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        // Ждём завершения идущей пробы (она ограничена своим таймаутом), затем освобождаем admin.
        lock (_adminUse)
        {
            _admin?.Dispose();
            _admin = null;
        }

        IConsumer<byte[], byte[]>? consumer;
        lock (_startGate)
        {
            consumer = _consumer;
        }

        // Остановка до первой загрузки правил: consumer не создавался, закрывать нечего.
        if (consumer is null)
        {
            lock (_gate)
            {
                _current.Clear();
            }

            return;
        }

        try
        {
            consumer.Close();
        }
        catch (KafkaException ex)
        {
            _logger.LogWarning(ex, "Kafka consumer close failed");
        }
        finally
        {
            consumer.Dispose();
            // Если Close упал и revoke-обработчик не сработал, остановленный источник всё равно не должен считаться назначенным.
            lock (_gate)
            {
                _current.Clear();
            }
        }
    }

    private static TopicPartition[] ToPartitions(IReadOnlyCollection<TopicPartitionKey> keys) =>
        keys.Select(k => new TopicPartition(k.Topic, k.Partition)).ToArray();

    internal void OnAssigned(IEnumerable<TopicPartition> partitions)
    {
        var keys = partitions.Select(p => new TopicPartitionKey(p.Topic, p.Partition.Value)).ToArray();
        lock (_gate)
        {
            _assigned.AddRange(keys);
            _current.UnionWith(keys);
        }
    }

    /// <summary>Отзыв и потеря партиций: для цикла и для кэша назначения это одно и то же.</summary>
    internal void OnRevoked(IEnumerable<TopicPartition> partitions)
    {
        var keys = partitions.Select(p => new TopicPartitionKey(p.Topic, p.Partition.Value)).ToArray();
        lock (_gate)
        {
            _revoked.AddRange(keys);
            _current.ExceptWith(keys);
        }
    }
}

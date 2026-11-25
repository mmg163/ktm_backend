using System.Diagnostics;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Reprocessing;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>Состояние партиции для проверки retention: границы и время первого сохранённого сообщения.</summary>
/// <param name="FirstMessageTime">Время первого сохранённого сообщения; null при <c>Low &gt; 0</c> означает «подтвердить нельзя».</param>
internal readonly record struct PartitionRetention(int Partition, long Low, long High, DateTimeOffset? FirstMessageTime);

/// <summary>
/// Источник повторной обработки (ADR-0006 п. 2.6-2.7, design Р-6): отдельный consumer с <c>Assign</c> (без подписки на группу),
/// без commit и без хранения offset: рабочая группа <c>GroupId</c> не затрагивается. Границы читаемого диапазона снимаются при
/// открытии (конечные offset - «снимок», новые сообщения не догоняются); период - по Kafka-timestamp, полуинтервал
/// <c>[from, to)</c>, через <c>OffsetsForTimes</c> без чтения всего топика. Период, начало которого нельзя подтвердить
/// (retention), отклоняется ошибкой аргумента; отсутствие топика и любой отказ брокера - <see cref="ReplaySourceException"/>.
/// Исходные координаты и причина берутся из заголовков <c>ktm-source-*</c>, <c>ktm-dlq-*</c>, <c>ktm-reject-reason</c>
/// (имена - <see cref="KafkaHeaders"/>).
/// </summary>
public sealed class KafkaReplaySource(KafkaOptions options, ILogger<KafkaReplaySource> logger, LogThrottle throttle) : IReplaySource
{
    private static readonly TimeSpan OpenTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MetadataTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ConsumePollTimeout = TimeSpan.FromMilliseconds(500);

    public ValueTask<ReplayOpenResult> OpenAsync(ReprocessRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        // Вызовы метаданных librdkafka блокирующие: не держим поток запроса.
        return new ValueTask<ReplayOpenResult>(Task.Run(() => Open(request, cancellationToken), cancellationToken));
    }

    internal static ConsumerConfig BuildConfig(KafkaOptions options) => new ConsumerConfig
    {
        // group.id нужен librdkafka; consumer не подписывается на группу (Assign) и никогда не коммитит.
        GroupId = options.GroupId + "-reprocess",
        EnableAutoCommit = false,
        EnableAutoOffsetStore = false,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        AllowAutoCreateTopics = false,
        EnablePartitionEof = true,
    }.ApplyConnection(options);

    private ReplayOpenResult Open(ReprocessRequest request, CancellationToken cancellationToken)
    {
        var topic = request.Source switch
        {
            ReprocessSource.Dlq => options.DlqTopic,
            ReprocessSource.Rejected => options.RejectedTopic,
            _ => request.Topic ?? options.Topic,
        };

        var nativeLog = new KafkaNativeLogBridge(logger, throttle);
        var consumer = new ConsumerBuilder<byte[], byte[]>(BuildConfig(options))
            .SetErrorHandler((_, error) =>
            {
                // Сбой этого consumer не должен останавливать процесс (в отличие от рабочего): задача завершится ошибкой источника.
                if (throttle.TryPass("replay:" + error.Code, out var suppressed))
                {
                    logger.LogWarning("Replay consumer error {Code}: {Reason} (suppressed since last: {Suppressed})", error.Code, error.Reason, suppressed);
                }
            })
            .SetLogHandler((_, message) => nativeLog.Handle(message))
            .Build();

        try
        {
            var budget = new OpenBudget(TimeProvider.System, OpenTimeout, MetadataTimeout, cancellationToken);
            var plan = Plan(consumer, topic, request, budget);
            if (plan.Errors.Count > 0)
            {
                consumer.Dispose();
                return ReplayOpenResult.Invalid(plan.Errors);
            }

            var assignments = plan.Partitions.Where(p => p.Count > 0).Select(p => new TopicPartitionOffset(topic, p.Partition, p.StartOffset)).ToList();
            if (assignments.Count > 0)
            {
                consumer.Assign(assignments);
            }

            return ReplayOpenResult.Opened(new KafkaReplayReader(consumer, request.Source, plan.Partitions, request.From, request.To, logger, throttle));
        }
        catch (KafkaException ex)
        {
            consumer.Dispose();
            throw new ReplaySourceException($"Kafka: {ex.Error.Code}");
        }
        catch
        {
            consumer.Dispose();
            throw;
        }
    }

    private sealed record ReadPlan(IReadOnlyList<ReplayPartition> Partitions, IReadOnlyList<ReprocessValidationError> Errors);

    private static ReadPlan Plan(IConsumer<byte[], byte[]> consumer, string topic, ReprocessRequest request, OpenBudget budget)
    {
        using var admin = new DependentAdminClientBuilder(consumer.Handle).Build();
        var metadata = admin.GetMetadata(topic, budget.NextTimeout());
        var partitionIds = ResolvePartitions(topic, metadata.Topics.FirstOrDefault(t => t.Topic == topic));

        var partitions = partitionIds.Select(id => new TopicPartition(topic, id)).ToList();
        var marks = partitions.ToDictionary(p => p, p => consumer.QueryWatermarkOffsets(p, budget.NextTimeout()));

        // Без from (dlq/rejected «от начала до снимка») проверки нет: читается всё, что сохранено.
        if (request.From is { } from)
        {
            var retention = ReadFirstMessageTimes(consumer, marks, budget);
            var errors = CheckRetention(topic, from, retention);
            if (errors.Count > 0)
            {
                return new ReadPlan([], errors);
            }
        }

        var starts = request.From is { } fromTime ? OffsetsByTime(consumer, partitions, fromTime, budget) : null;
        var ends = request.To is { } toTime ? OffsetsByTime(consumer, partitions, toTime, budget) : null;

        var ranges = new List<ReplayPartition>();
        foreach (var partition in partitions)
        {
            var low = marks[partition].Low.Value;
            var high = marks[partition].High.Value;
            // Нет сообщения с timestamp >= границы: для from - читать нечего, для to - читаем до конца снимка.
            var start = starts is null ? low : starts[partition] < 0 ? high : Math.Max(low, starts[partition]);
            var end = ends is null || ends[partition] < 0 ? high : Math.Min(high, ends[partition]);
            ranges.Add(new ReplayPartition(topic, partition.Partition.Value, start, Math.Max(start, end)));
        }

        return new ReadPlan(ranges, []);
    }

    /// <summary>
    /// Номера партиций топика по возрастанию. Отсутствие топика, любая ошибка метаданных и топик без партиций - отказ источника
    /// (<see cref="ReplaySourceException"/>), а не ошибка аргумента: запрос корректен, неверны конфигурация или окружение.
    /// Имя топика берётся из конфигурации, данных сообщений в тексте нет.
    /// </summary>
    internal static IReadOnlyList<int> ResolvePartitions(string topic, TopicMetadata? metadata)
    {
        if (metadata is null || metadata.Error.Code == ErrorCode.UnknownTopicOrPart)
        {
            throw new ReplaySourceException($"Kafka: topic '{topic}' not found");
        }

        if (metadata.Error.IsError)
        {
            throw new ReplaySourceException($"Kafka: {metadata.Error.Code}");
        }

        if (metadata.Partitions.Count == 0)
        {
            throw new ReplaySourceException($"Kafka: topic '{topic}' has no partitions");
        }

        return metadata.Partitions.Select(p => p.PartitionId).Order().ToArray();
    }

    /// <summary>
    /// Проверка начала периода относительно retention (ADR-0006 п. 2.7, R-11): начало должно быть доказуемо покрыто сохранёнными
    /// данными. Партиция с <c>Low = 0</c> покрыта (ничего не удалялось). Партиция с очищенным началом покрыта, только если известно
    /// время первого сохранённого сообщения и <c>from</c> не раньше него; если сохранённых сообщений нет или время неизвестно,
    /// начало подтвердить нельзя ни для какого <c>from</c>. Допустимое <c>from</c> - не раньше самого позднего из времён первых
    /// сохранённых сообщений партиций с очищенным началом: удалённые сообщения лежат раньше по offset, но их время неизвестно,
    /// ложный отказ безопаснее потерянных событий. Конец периода (<c>to</c>) отдельно не проверяется: период, целиком раньше
    /// сохранённого, отклоняется по <c>from</c>. Неподтверждаемые партиции приоритетнее ошибки «слишком рано».
    /// </summary>
    internal static IReadOnlyList<ReprocessValidationError> CheckRetention(string topic, DateTimeOffset from, IReadOnlyList<PartitionRetention> partitions)
    {
        var cleaned = partitions.Where(p => p.Low > 0).ToList();

        var unverifiable = cleaned.Where(p => p.FirstMessageTime is null).Select(p => p.Partition).Order().ToList();
        if (unverifiable.Count > 0)
        {
            var list = string.Join(", ", unverifiable.Select(p => p.ToString(CultureInfo.InvariantCulture)));
            return
            [
                new ReprocessValidationError(
                    "from",
                    $"The start of the period cannot be verified: partition(s) {list} of topic '{topic}' have no retained message with a known time after retention cleanup."),
            ];
        }

        if (cleaned.Count == 0)
        {
            return [];
        }

        var earliest = cleaned.Max(p => p.FirstMessageTime!.Value);
        if (from >= earliest)
        {
            return [];
        }

        return
        [
            new ReprocessValidationError(
                "from",
                $"from is earlier than the oldest retained message of topic '{topic}'; the earliest available time is {earliest.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)}."),
        ];
    }

    /// <summary>
    /// Состояние retention по каждой партиции. Первое сообщение читается только для партиций с очищенным началом, в которых
    /// ещё что-то есть (<c>Low &gt; 0</c>, <c>Low &lt; High</c>). Без времени (<c>null</c>): партиция очищена целиком, у первого
    /// сообщения нет timestamp либо чтение дошло до конца партиции без сообщения. Если брокер за общий срок открытия не
    /// отдал ни сообщения, ни конца партиции - <see cref="ReplaySourceException"/> (отказ источника, не ошибка аргумента).
    /// </summary>
    private static List<PartitionRetention> ReadFirstMessageTimes(
        IConsumer<byte[], byte[]> consumer, Dictionary<TopicPartition, WatermarkOffsets> marks, OpenBudget budget)
    {
        var result = new List<PartitionRetention>(marks.Count);
        foreach (var (partition, mark) in marks.OrderBy(m => m.Key.Partition.Value))
        {
            var low = mark.Low.Value;
            var high = mark.High.Value;
            DateTimeOffset? first = null;

            if (low > 0 && low < high)
            {
                consumer.Assign(new TopicPartitionOffset(partition, mark.Low));
                try
                {
                    first = ReadFirstMessageTime(consumer, budget);
                }
                finally
                {
                    consumer.Unassign();
                }
            }

            result.Add(new PartitionRetention(partition.Partition.Value, low, high, first));
        }

        return result;
    }

    private static DateTimeOffset? ReadFirstMessageTime(IConsumer<byte[], byte[]> consumer, OpenBudget budget)
    {
        while (true)
        {
            // NextTimeout проверяет токен и общий срок на каждой итерации; по истечении срока без сообщения и без EOF - отказ источника.
            var timeout = budget.NextTimeout();
            var result = consumer.Consume(timeout < ConsumePollTimeout ? timeout : ConsumePollTimeout);
            if (result is null)
            {
                continue;
            }

            if (result.IsPartitionEOF)
            {
                return null;
            }

            return result.Message.Timestamp.Type == TimestampType.NotAvailable
                ? null
                : new DateTimeOffset(result.Message.Timestamp.UtcDateTime, TimeSpan.Zero);
        }
    }

    private static Dictionary<TopicPartition, long> OffsetsByTime(
        IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions, DateTimeOffset time, OpenBudget budget)
    {
        var timestamp = new Timestamp(time);
        var result = consumer.OffsetsForTimes(partitions.Select(p => new TopicPartitionTimestamp(p, timestamp)), budget.NextTimeout());
        return result.ToDictionary(r => r.TopicPartition, r => r.Offset.Value);
    }

    /// <summary>
    /// Попадание сообщения в период, полуинтервал <c>[from, to)</c>. Сообщение без timestamp (<c>NotAvailable</c>) при заданном
    /// периоде включается: время неизвестно, отфильтровать его по периоду нельзя, а потеря события хуже повторной обработки
    /// (запись идемпотентна). ADR-0006 п. 2.7.
    /// </summary>
    internal static bool InWindow(Timestamp timestamp, DateTimeOffset? from, DateTimeOffset? to)
    {
        if (timestamp.Type == TimestampType.NotAvailable || (from is null && to is null))
        {
            return true;
        }

        var value = new DateTimeOffset(timestamp.UtcDateTime, TimeSpan.Zero);
        return (from is null || value >= from) && (to is null || value < to);
    }

    /// <summary>
    /// Сообщение читаемого топика в виде задачи повторной обработки: для <c>raw</c> координаты фактические; для
    /// <c>dlq</c>/<c>rejected</c> исходные координаты и время берутся из <c>ktm-source-*</c> (если их нет или они неверны -
    /// фактические, <c>CoordinatesFromHeaders = false</c>), причина и стадия - из заголовков причины.
    /// </summary>
    internal static ReplayMessage MapMessage(ConsumeResult<byte[], byte[]> result, ReprocessSource source)
    {
        var message = result.Message;
        var actualTimestamp = new DateTimeOffset(message.Timestamp.UtcDateTime, TimeSpan.Zero);
        var body = message.Value ?? [];
        var key = message.Key ?? [];

        if (source == ReprocessSource.Raw)
        {
            return new ReplayMessage(
                new ConsumedMessage(result.Topic, result.Partition.Value, result.Offset.Value, key, body, actualTimestamp),
                null, null, result.Topic, result.Partition.Value, result.Offset.Value, true);
        }

        string? Header(string name) =>
            message.Headers is { } headers && headers.TryGetLastBytes(name, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

        var reason = Header(source == ReprocessSource.Dlq ? KafkaHeaders.DlqReason : KafkaHeaders.RejectReason);
        var stage = source == ReprocessSource.Dlq ? Header(KafkaHeaders.DlqStage) : null;

        var topic = Header(KafkaHeaders.SourceTopic);
        var hasCoordinates =
            !string.IsNullOrEmpty(topic)
            & int.TryParse(Header(KafkaHeaders.SourcePartition), NumberStyles.None, CultureInfo.InvariantCulture, out var partition)
            & long.TryParse(Header(KafkaHeaders.SourceOffset), NumberStyles.None, CultureInfo.InvariantCulture, out var offset);
        if (!hasCoordinates)
        {
            return new ReplayMessage(
                new ConsumedMessage(result.Topic, result.Partition.Value, result.Offset.Value, key, body, actualTimestamp),
                reason, stage, result.Topic, result.Partition.Value, result.Offset.Value, false);
        }

        var sourceTimestamp = DateTimeOffset.TryParse(Header(KafkaHeaders.SourceTimestamp), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
            ? parsed
            : actualTimestamp;
        return new ReplayMessage(
            new ConsumedMessage(topic!, partition, offset, key, body, sourceTimestamp),
            reason, stage, result.Topic, result.Partition.Value, result.Offset.Value, true);
    }

    private sealed class KafkaReplayReader : IReplayReader
    {
        private static readonly TimeSpan PollTimeout = TimeSpan.FromMilliseconds(500);
        private static readonly TimeSpan IdleLimit = TimeSpan.FromSeconds(60);

        private readonly IConsumer<byte[], byte[]> _consumer;
        private readonly ReprocessSource _source;
        private readonly Dictionary<int, long> _end;
        private readonly HashSet<int> _done = [];
        private readonly string _topic;
        private readonly DateTimeOffset? _from;
        private readonly DateTimeOffset? _to;
        private readonly ILogger _logger;
        private readonly LogThrottle _throttle;
        private int _disposed;

        public KafkaReplayReader(
            IConsumer<byte[], byte[]> consumer,
            ReprocessSource source,
            IReadOnlyList<ReplayPartition> partitions,
            DateTimeOffset? from,
            DateTimeOffset? to,
            ILogger logger,
            LogThrottle throttle)
        {
            _consumer = consumer;
            _source = source;
            _from = from;
            _to = to;
            _logger = logger;
            _throttle = throttle;
            Partitions = partitions;
            _topic = partitions.Count > 0 ? partitions[0].Topic : string.Empty;
            _end = partitions.ToDictionary(p => p.Partition, p => p.EndOffset);
            foreach (var empty in partitions.Where(p => p.Count == 0))
            {
                _done.Add(empty.Partition);
            }
        }

        public IReadOnlyList<ReplayPartition> Partitions { get; }

        public ValueTask<IReadOnlyList<ReplayMessage>> ReadBatchAsync(int maxCount, CancellationToken cancellationToken)
        {
            var batch = new List<ReplayMessage>();
            var idleSince = Stopwatch.GetTimestamp();

            while (batch.Count < maxCount && _done.Count < Partitions.Count)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ConsumeResult<byte[], byte[]>? result;
                try
                {
                    result = _consumer.Consume(PollTimeout);
                }
                catch (ConsumeException ex)
                {
                    if (ex.Error.IsFatal)
                    {
                        throw new ReplaySourceException($"Kafka: {ex.Error.Code}");
                    }

                    if (_throttle.TryPass("replay-consume:" + ex.Error.Code, out var suppressed))
                    {
                        _logger.LogWarning("Replay consume error {Code} (suppressed since last: {Suppressed})", ex.Error.Code, suppressed);
                    }

                    result = null;
                }

                if (result is null)
                {
                    if (batch.Count > 0)
                    {
                        break;
                    }

                    if (Stopwatch.GetElapsedTime(idleSince) > IdleLimit)
                    {
                        throw new ReplaySourceException("Kafka: no data from the broker within the idle limit");
                    }

                    continue;
                }

                idleSince = Stopwatch.GetTimestamp();
                var partition = result.Partition.Value;
                if (result.IsPartitionEOF || _done.Contains(partition) || !_end.TryGetValue(partition, out var end) || result.Offset.Value >= end)
                {
                    Finish(partition);
                    continue;
                }

                if (result.Offset.Value >= end - 1)
                {
                    Finish(partition);
                }

                if (InWindow(result.Message.Timestamp, _from, _to))
                {
                    batch.Add(MapMessage(result, _source));
                }
            }

            return ValueTask.FromResult<IReadOnlyList<ReplayMessage>>(batch);
        }

        private void Finish(int partition)
        {
            if (!_done.Add(partition))
            {
                return;
            }

            try
            {
                _consumer.Pause([new TopicPartition(_topic, partition)]);
            }
            catch (Exception ex) when (ex is KafkaException or InvalidOperationException)
            {
                // Пауза лишь экономит выборку: дочитанная партиция и так игнорируется.
            }
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            try
            {
                // Группы нет и commit не выполняется: Close только освобождает соединения.
                _consumer.Close();
            }
            catch (KafkaException ex)
            {
                _logger.LogWarning("Replay consumer close failed: {Code}", ex.Error.Code);
            }
            finally
            {
                _consumer.Dispose();
            }

            return ValueTask.CompletedTask;
        }
    }
}

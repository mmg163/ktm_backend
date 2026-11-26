using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Resilience;
using Npgsql;
using NpgsqlTypes;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>Запись пачки с итогом по числу добавленных строк (для метрик); порт Core <see cref="IEventStore"/> остаётся без числа.</summary>
public interface IBatchWriter
{
    Task<EventWriteResult> WriteBatchAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken);
}

/// <summary>Итог записи пачки: сколько строк реально добавлено (дубли не считаются).</summary>
public readonly record struct EventWriteResult(int EventsInserted, int CorrectionsInserted);

/// <summary>
/// Запись пачки в <c>ktm.events</c> и <c>ktm.event_corrections</c> (ADR-0002 D-2, D-4, вариант B3): одна транзакция,
/// один NpgsqlBatch из двух команд. Идемпотентность - уникальные индексы БД и <c>ON CONFLICT DO NOTHING</c>.
/// Нужны только права SELECT и INSERT; UPDATE/DELETE не используются.
/// </summary>
public sealed class NpgsqlEventStore : IEventStore, IBatchWriter
{
    private const string InsertEventsSql = """
        INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, device_id,
                                external_person_id, trust, payload, schema_version)
        SELECT t.event_id, t.source, t.source_event_id, t.occurred_at, @received_at, t.event_type, t.device_id,
               t.external_person_id, t.trust, t.payload, t.schema_version
        FROM unnest(@event_id, @source, @source_event_id, @occurred_at, @event_type, @device_id,
                    @external_person_id, @trust, @payload, @schema_version)
             AS t(event_id, source, source_event_id, occurred_at, event_type, device_id,
                  external_person_id, trust, payload, schema_version)
        ORDER BY t.source, t.source_event_id, t.occurred_at
        ON CONFLICT (source, source_event_id, occurred_at) DO NOTHING
        """;

    // Корректирующее событие уже записано первой командой той же транзакции (или раньше, при повторе), поэтому
    // correction_event_id берётся по его натуральному ключу. Исходное событие только читается; нет его - NULL.
    private const string InsertCorrectionsSql = """
        INSERT INTO ktm.event_corrections (source, correction_source_event_id, correction_occurred_at, correction_event_id,
                                           original_source, original_source_event_id, original_occurred_at,
                                           original_event_id, reason, actor, corrected_at)
        SELECT c.source, c.correction_source_event_id, c.correction_occurred_at, e.event_id,
               c.source, c.original_source_event_id, c.original_occurred_at,
               o.event_id, c.reason, c.source, @received_at
        FROM unnest(@c_source, @c_source_event_id, @c_occurred_at, @c_original_source_event_id, @c_original_occurred_at, @c_reason)
             AS c(source, correction_source_event_id, correction_occurred_at, original_source_event_id, original_occurred_at, reason)
        JOIN ktm.events e ON e.source = c.source AND e.source_event_id = c.correction_source_event_id
                         AND e.occurred_at = c.correction_occurred_at
        LEFT JOIN ktm.events o ON o.source = c.source AND o.source_event_id = c.original_source_event_id
                              AND o.occurred_at = c.original_occurred_at
        ORDER BY c.source, c.correction_source_event_id, c.correction_occurred_at
        ON CONFLICT (source, correction_source_event_id, correction_occurred_at) DO NOTHING
        """;

    private readonly NpgsqlDataSource _dataSource;
    private readonly ISourceTrustProvider _trust;

    public NpgsqlEventStore(NpgsqlDataSource dataSource, ISourceTrustProvider trust)
    {
        _dataSource = dataSource;
        _trust = trust;
    }

    public async ValueTask WriteAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken) =>
        await WriteBatchAsync(events, receivedAt, cancellationToken);

    public async Task<EventWriteResult> WriteBatchAsync(IReadOnlyList<ParsedEvent> events, DateTimeOffset receivedAt, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0) return default;

        // Единый порядок захвата ключей уникального индекса: параллельные писатели не взаимоблокируются.
        var ordered = events
            .OrderBy(e => e.Source, StringComparer.Ordinal)
            .ThenBy(e => e.SourceEventId, StringComparer.Ordinal)
            .ThenBy(e => e.OccurredAt)
            .ToArray();
        var corrections = ordered.Where(e => e.IsCorrection).ToArray();

        // Команды строятся до открытия соединения: ошибки отображения значений (переполнение, формат, доверие источника)
        // остаются свойством сообщения (-> Unexpected/write_value_error) и не удерживают соединение пула.
        var insertEvents = BuildEventsCommand(ordered, receivedAt);
        var insertCorrections = corrections.Length > 0 ? BuildCorrectionsCommand(corrections, receivedAt) : null;

        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            await using var batch = new NpgsqlBatch(connection, transaction);

            batch.BatchCommands.Add(insertEvents);
            if (insertCorrections is not null)
            {
                batch.BatchCommands.Add(insertCorrections);
            }

            await batch.ExecuteNonQueryAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return new EventWriteResult(insertEvents.RecordsAffected, insertCorrections?.RecordsAffected ?? 0);
        }
        catch (NpgsqlException ex) when (!cancellationToken.IsCancellationRequested)
        {
            throw PostgresStoreException.Adapt(ex);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested && !IsMessageBound(ex))
        {
            // Любое иное исключение при работе с соединением (разрыв, остановка источника данных, занятое соединение,
            // истёкшая транзакция) - отказ инфраструктуры, а не свойство сообщения: повтор без предела, не dlq.
            throw PostgresStoreException.ConnectionUnavailable(ex);
        }
    }

    /// <summary>Ошибка значения, привязанная к сообщению: остаётся как есть и не оборачивается в отказ инфраструктуры.</summary>
    internal static bool IsMessageBound(Exception exception) => WriteErrorClassifier.IsMessageBoundValueError(exception);

    private NpgsqlBatchCommand BuildEventsCommand(ParsedEvent[] events, DateTimeOffset receivedAt)
    {
        var count = events.Length;
        var eventIds = new Guid[count];
        var sources = new string[count];
        var sourceEventIds = new string[count];
        var occurred = new DateTime[count];
        var types = new string[count];
        var devices = new string?[count];
        var persons = new string[count];
        var trusts = new short[count];
        var payloads = new string[count];
        var versions = new short[count];
        var trustCache = new Dictionary<string, short>(StringComparer.Ordinal);

        for (var i = 0; i < count; i++)
        {
            var e = events[i];
            eventIds[i] = Guid.CreateVersion7(receivedAt);
            sources[i] = e.Source;
            sourceEventIds[i] = e.SourceEventId;
            occurred[i] = ToUtcTimestamp(e.OccurredAt);
            types[i] = e.EventType;
            devices[i] = e.DeviceId;
            persons[i] = e.ExternalPersonId;
            if (!trustCache.TryGetValue(e.Source, out var trust))
            {
                trust = _trust.GetTrust(e.Source);
                trustCache[e.Source] = trust;
            }

            trusts[i] = trust;
            payloads[i] = e.Body.GetRawText();
            versions[i] = checked((short)e.SchemaVersion);
        }

        var command = new NpgsqlBatchCommand(InsertEventsSql);
        var p = command.Parameters;
        p.Add(new NpgsqlParameter("received_at", NpgsqlDbType.Timestamp) { Value = ToUtcTimestamp(receivedAt) });
        p.Add(new NpgsqlParameter("event_id", NpgsqlDbType.Array | NpgsqlDbType.Uuid) { Value = eventIds });
        p.Add(new NpgsqlParameter("source", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = sources });
        p.Add(new NpgsqlParameter("source_event_id", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = sourceEventIds });
        p.Add(new NpgsqlParameter("occurred_at", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = occurred });
        p.Add(new NpgsqlParameter("event_type", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = types });
        p.Add(new NpgsqlParameter("device_id", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = devices });
        p.Add(new NpgsqlParameter("external_person_id", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = persons });
        p.Add(new NpgsqlParameter("trust", NpgsqlDbType.Array | NpgsqlDbType.Smallint) { Value = trusts });
        p.Add(new NpgsqlParameter("payload", NpgsqlDbType.Array | NpgsqlDbType.Jsonb) { Value = payloads });
        p.Add(new NpgsqlParameter("schema_version", NpgsqlDbType.Array | NpgsqlDbType.Smallint) { Value = versions });
        return command;
    }

    private static NpgsqlBatchCommand BuildCorrectionsCommand(ParsedEvent[] corrections, DateTimeOffset receivedAt)
    {
        var count = corrections.Length;
        var sources = new string[count];
        var ids = new string[count];
        var occurred = new DateTime[count];
        var originalIds = new string[count];
        var originalOccurred = new DateTime[count];
        var reasons = new string?[count];

        for (var i = 0; i < count; i++)
        {
            var e = corrections[i];
            var correction = e.Correction!;
            sources[i] = e.Source;
            ids[i] = e.SourceEventId;
            occurred[i] = ToUtcTimestamp(e.OccurredAt);
            originalIds[i] = correction.CorrectsSourceEventId;
            originalOccurred[i] = ToUtcTimestamp(correction.CorrectsOccurredAt);
            reasons[i] = correction.Reason;
        }

        var command = new NpgsqlBatchCommand(InsertCorrectionsSql);
        var p = command.Parameters;
        p.Add(new NpgsqlParameter("received_at", NpgsqlDbType.Timestamp) { Value = ToUtcTimestamp(receivedAt) });
        p.Add(new NpgsqlParameter("c_source", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = sources });
        p.Add(new NpgsqlParameter("c_source_event_id", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = ids });
        p.Add(new NpgsqlParameter("c_occurred_at", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = occurred });
        p.Add(new NpgsqlParameter("c_original_source_event_id", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = originalIds });
        p.Add(new NpgsqlParameter("c_original_occurred_at", NpgsqlDbType.Array | NpgsqlDbType.Timestamp) { Value = originalOccurred });
        p.Add(new NpgsqlParameter("c_reason", NpgsqlDbType.Array | NpgsqlDbType.Text) { Value = reasons });
        return command;
    }

    // В БД хранится UTC без метки зоны. Kind.Unspecified нужен Npgsql для timestamp without time zone;
    // убираем только метку Kind после нормализации самого момента в UTC.
    private static DateTime ToUtcTimestamp(DateTimeOffset value) =>
        DateTime.SpecifyKind(value.UtcDateTime, DateTimeKind.Unspecified);
}

using Xunit.Abstractions;
using System.Diagnostics;
using System.Text.Json;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Infrastructure.Postgres;
using Npgsql;
using static Ktm.DataSaver.IntegrationTests.Postgres.TestEvents;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задачи 5.1-5.5. Хранилище работает под ролью ktm_app (SELECT, INSERT), поэтому все тесты проверяют и права.</summary>
[Collection(PostgresCollection.Name)]
public sealed class NpgsqlEventStoreTests(PostgresFixture db, ITestOutputHelper output)
{
    private NpgsqlEventStore Store(short trust = 100, params (string, short)[] overrides) =>
        new(db.AppDataSource, new FixedTrustProvider(trust, overrides));

    private async Task<List<EventRow>> ReadEventsAsync(string source)
    {
        await using var connection = await db.AppDataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT event_id, source_event_id, occurred_at, received_at, event_type, device_id, external_person_id, trust,
                   payload::text, schema_version, row_to_json(e)::text
            FROM ktm.events e WHERE source = @s ORDER BY source_event_id
            """, connection);
        command.Parameters.AddWithValue("s", source);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<EventRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new EventRow(reader.GetGuid(0), reader.GetString(1), reader.GetFieldValue<DateTimeOffset>(2),
                reader.GetFieldValue<DateTimeOffset>(3), reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetString(6), reader.GetInt16(7), reader.GetString(8), reader.GetInt16(9), reader.GetString(10)));
        }

        return rows;
    }

    private async Task<List<CorrectionRow>> ReadCorrectionsAsync(string source)
    {
        await using var connection = await db.AppDataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT correction_source_event_id, correction_event_id, original_source, original_source_event_id,
                   original_occurred_at, original_event_id, reason, actor
            FROM ktm.event_corrections WHERE source = @s ORDER BY correction_source_event_id
            """, connection);
        command.Parameters.AddWithValue("s", source);
        await using var reader = await command.ExecuteReaderAsync();
        var rows = new List<CorrectionRow>();
        while (await reader.ReadAsync())
        {
            rows.Add(new CorrectionRow(reader.GetString(0), reader.GetGuid(1), reader.GetString(2), reader.GetString(3),
                reader.GetFieldValue<DateTimeOffset>(4), reader.IsDBNull(5) ? null : reader.GetGuid(5),
                reader.IsDBNull(6) ? null : reader.GetString(6), reader.GetString(7)));
        }

        return rows;
    }

    [Fact]
    public async Task Batch_of_500_is_written_by_one_call_and_rows_match_messages()
    {
        var oneC = NewSource();
        var scud = NewSource();
        var events = Enumerable.Range(0, 500)
            .Select(i => i % 2 == 0
                ? Event(scud, $"e{i:D4}", At(i), "dev-" + i, i)
                : Event(oneC, $"e{i:D4}", At(i), deviceId: null, i))
            .ToList();

        var stopwatch = Stopwatch.StartNew();
        var result = await Store(trust: 90, (oneC, 60)).WriteBatchAsync(events, CancellationToken.None);
        stopwatch.Stop();
        output.WriteLine($"500 events written in {stopwatch.ElapsedMilliseconds} ms (cold pool, single batch)");

        Assert.Equal(new EventWriteResult(500, 0), result);
        var scudRows = await ReadEventsAsync(scud);
        var oneCRows = await ReadEventsAsync(oneC);
        Assert.Equal(250, scudRows.Count);
        Assert.Equal(250, oneCRows.Count);
        Assert.Single(scudRows.Select(r => r.ReceivedAt).Concat(oneCRows.Select(r => r.ReceivedAt)).Distinct());
        Assert.All(scudRows.Concat(oneCRows), r =>
        {
            Assert.Equal(7, r.EventId.Version);
            Assert.Equal("access.pass", r.EventType);
            Assert.Equal((short)1, r.SchemaVersion);
        });

        foreach (var expected in events)
        {
            var row = (expected.Source == scud ? scudRows : oneCRows).Single(r => r.SourceEventId == expected.SourceEventId);
            Assert.Equal(expected.OccurredAt, row.OccurredAt);
            Assert.Equal(expected.DeviceId, row.DeviceId);
            Assert.Equal(expected.ExternalPersonId, row.ExternalPersonId);
            Assert.Equal(expected.Source == scud ? (short)90 : (short)60, row.Trust);
            using var stored = JsonDocument.Parse(row.Payload);
            Assert.Equal(expected.Payload.GetProperty("n").GetInt32(), stored.RootElement.GetProperty("n").GetInt32());
            Assert.Equal("тест", stored.RootElement.GetProperty("text").GetString());
        }

        Assert.All(oneCRows, r => Assert.Null(r.DeviceId));
    }

    [Fact]
    public async Task Duplicates_inside_batch_and_repeated_batch_do_not_create_rows()
    {
        var source = NewSource();
        var events = Enumerable.Range(0, 90).Select(i => Event(source, $"e{i:D3}", At(i), n: i)).ToList();
        events.AddRange(events.Take(10)); // 100 сообщений, 10 дублей
        var store = Store();

        var first = await store.WriteBatchAsync(events, CancellationToken.None);
        var snapshot = await ReadEventsAsync(source);
        var repeat = await store.WriteBatchAsync(events, CancellationToken.None);

        Assert.Equal(90, first.EventsInserted);
        Assert.Equal(0, repeat.EventsInserted);
        Assert.Equal(snapshot, await ReadEventsAsync(source));
        Assert.Equal(90, snapshot.Count);
    }

    [Fact]
    public async Task Concurrent_batches_with_overlapping_keys_do_not_deadlock()
    {
        var source = NewSource();
        var events = Enumerable.Range(0, 200).Select(i => Event(source, $"e{i:D3}", At(i), n: i)).ToList();
        var reversed = Enumerable.Reverse(events).ToList();

        for (var round = 0; round < 5; round++)
        {
            var batch = round == 0 ? events : events.Select(e => e with { SourceEventId = $"r{round}-{e.SourceEventId}" }).ToList();
            var other = round == 0 ? reversed : Enumerable.Reverse(batch).ToList();
            await Task.WhenAll(Store().WriteBatchAsync(batch, CancellationToken.None),
                Store().WriteBatchAsync(other, CancellationToken.None));
        }

        Assert.Equal(1000, (await ReadEventsAsync(source)).Count);
    }

    [Fact]
    public async Task Correction_of_existing_event_links_original_and_leaves_it_unchanged()
    {
        var source = NewSource();
        var store = Store();
        await store.WriteBatchAsync([Event(source, "orig", At(0))], CancellationToken.None);
        var before = (await ReadEventsAsync(source)).Single();

        var result = await store.WriteBatchAsync([Correction(source, "fix", At(100), "orig", At(0))], CancellationToken.None);

        Assert.Equal(new EventWriteResult(1, 1), result);
        var rows = await ReadEventsAsync(source);
        Assert.Equal(before, rows.Single(r => r.SourceEventId == "orig"));
        var correction = Assert.Single(await ReadCorrectionsAsync(source));
        Assert.Equal(before.EventId, correction.OriginalEventId);
        Assert.Equal(rows.Single(r => r.SourceEventId == "fix").EventId, correction.CorrectionEventId);
        Assert.Equal("ошибочный проход", correction.Reason);
        Assert.Equal(source, correction.Actor);
        Assert.Equal(source, correction.OriginalSource);
    }

    [Fact]
    public async Task Correction_before_original_has_null_link_and_repeat_adds_nothing()
    {
        var source = NewSource();
        var store = Store();
        var correction = Correction(source, "fix", At(100), "orig", At(0), reason: null);

        var first = await store.WriteBatchAsync([correction], CancellationToken.None);
        var repeat = await store.WriteBatchAsync([correction], CancellationToken.None);
        // исходное приходит позже: журнал неизменяем, связь не достраивается
        await store.WriteBatchAsync([Event(source, "orig", At(0))], CancellationToken.None);

        Assert.Equal(new EventWriteResult(1, 1), first);
        Assert.Equal(new EventWriteResult(0, 0), repeat);
        var events = await ReadEventsAsync(source);
        var row = Assert.Single(await ReadCorrectionsAsync(source));
        Assert.Null(row.OriginalEventId);
        Assert.Null(row.Reason);
        Assert.Equal(events.Single(e => e.SourceEventId == "fix").EventId, row.CorrectionEventId);
        Assert.Equal(2, events.Count);
    }

    [Fact]
    public async Task Store_does_not_require_update_privilege()
    {
        // Весь сценарий записи идёт под ktm_app (SELECT, INSERT); UPDATE под ним запрещён - см. DatabaseSetupTests.
        var source = NewSource();
        var store = Store();

        await store.WriteBatchAsync([Event(source, "a", At(0)), Correction(source, "b", At(5), "a", At(0))], CancellationToken.None);
        await store.WriteBatchAsync([Event(source, "a", At(0))], CancellationToken.None);

        Assert.Equal(2, (await ReadEventsAsync(source)).Count);
    }

    [Fact]
    public async Task Database_errors_are_classified_by_sqlstate()
    {
        // 23505: уникальность; 22P02: неверный текстовый формат; 42501: нет прав (реальные ошибки сервера).
        var source = NewSource();
        await using var connection = await db.AppDataSource.OpenConnectionAsync();
        var insert =
            $"""
            INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, external_person_id, trust, payload, schema_version)
            VALUES (gen_random_uuid(), '{source}', 'e1', '2026-03-10Z', now(), 'access.pass', 'p1', 100, jsonb_build_object(), 1)
            """;
        await new NpgsqlCommand(insert, connection).ExecuteNonQueryAsync();

        async Task<Exception> Capture(string sql)
        {
            try
            {
                await new NpgsqlCommand(sql, connection).ExecuteNonQueryAsync();
            }
            catch (NpgsqlException ex)
            {
                return PostgresStoreException.Adapt(ex);
            }

            throw new InvalidOperationException("Expected a database error.");
        }

        var unique = await Capture(insert);
        var format = await Capture("SELECT 'abc'::int");
        var privilege = await Capture($"UPDATE ktm.events SET trust = 1 WHERE source = '{source}'");

        Assert.Equal("23505", ((ISqlStateError)unique).SqlState);
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(unique));
        Assert.Equal("22P02", ((ISqlStateError)format).SqlState);
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(format));
        Assert.Equal("42501", ((ISqlStateError)privilege).SqlState);
        Assert.Equal(WriteErrorClass.FatalConfig, WriteErrorClassifier.Classify(privilege));
        Assert.DoesNotContain(source, unique.Message);
    }

    [Fact]
    public async Task Constraint_violation_in_batch_surfaces_as_data_error_and_writes_nothing()
    {
        var source = NewSource();
        var tooLongSource = new string('x', 40); // ck_events_source: <= 32 символов
        var batch = new List<ParsedEvent> { Event(source, "ok", At(0)), Event(tooLongSource, "bad", At(1)) };

        var ex = await Assert.ThrowsAsync<PostgresStoreException>(() => Store().WriteBatchAsync(batch, CancellationToken.None));

        Assert.Equal("23514", ex.SqlState);
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(ex));
        Assert.Empty(await ReadEventsAsync(source)); // транзакция откатилась целиком
    }

    [Fact]
    public async Task Unreachable_database_is_classified_as_transient()
    {
        var options = new PostgresOptions { Port = 1, Password = "x", ConnectTimeoutSeconds = 2, MinPoolSize = 0 };
        await using var dataSource = options.BuildDataSource();
        var store = new NpgsqlEventStore(dataSource, new FixedTrustProvider(100));

        var ex = await Assert.ThrowsAsync<PostgresStoreException>(
            () => store.WriteBatchAsync([Event(NewSource(), "a", At(0))], CancellationToken.None));

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(ex));
    }

    private sealed record EventRow(Guid EventId, string SourceEventId, DateTimeOffset OccurredAt, DateTimeOffset ReceivedAt,
        string EventType, string? DeviceId, string ExternalPersonId, short Trust, string Payload, short SchemaVersion, string Json);

    private sealed record CorrectionRow(string CorrectionSourceEventId, Guid CorrectionEventId, string OriginalSource,
        string OriginalSourceEventId, DateTimeOffset OriginalOccurredAt, Guid? OriginalEventId, string? Reason, string Actor);
}

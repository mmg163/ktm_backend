using Xunit.Abstractions;
using System.Diagnostics;
using System.Text.Json.Nodes;
using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Infrastructure.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Npgsql;
using static Ktm.DataSaver.IntegrationTests.Postgres.TestEvents;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задачи 5.1-5.5. Хранилище работает под ролью ktm_app (SELECT, INSERT), поэтому все тесты проверяют и права.</summary>
[Collection(PostgresCollection.Name)]
public sealed class NpgsqlEventStoreTests(PostgresFixture db, ITestOutputHelper output)
{
    private static readonly DateTimeOffset Now = new(2026, 3, 1, 12, 0, 0, TimeSpan.Zero);

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
            rows.Add(new EventRow(reader.GetGuid(0), reader.GetString(1), ReadUtc(reader, 2),
                ReadUtc(reader, 3), reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
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
                ReadUtc(reader, 4), reader.IsDBNull(5) ? null : reader.GetGuid(5),
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
        var result = await Store(trust: 90, (oneC, 60)).WriteBatchAsync(events, Now, CancellationToken.None);
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
            // В events.payload - всё тело сообщения (конверт, неизвестные поля), сравнение jsonb-эквивалентности.
            Assert.True(JsonNode.DeepEquals(JsonNode.Parse(expected.Body.GetRawText()), JsonNode.Parse(row.Payload)));
            Assert.Equal(expected.Payload.GetProperty("n").GetInt32(),
                JsonNode.Parse(row.Payload)!["payload"]!["n"]!.GetValue<int>());
            // UUIDv7: 48 бит времени в миллисекундах совпадают с received_at.
            Assert.Equal(row.ReceivedAt.ToUnixTimeMilliseconds(), UuidV7Milliseconds(row.EventId));
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

        var first = await store.WriteBatchAsync(events, Now, CancellationToken.None);
        var snapshot = await ReadEventsAsync(source);
        // Повтор той же пачки приходит с другим моментом получения, но ранее вставленные строки не меняются.
        var repeat = await store.WriteBatchAsync(events, Now.AddMinutes(5), CancellationToken.None);

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
            await Task.WhenAll(Store().WriteBatchAsync(batch, Now, CancellationToken.None),
                Store().WriteBatchAsync(other, Now, CancellationToken.None));
        }

        Assert.Equal(1000, (await ReadEventsAsync(source)).Count);
    }

    [Fact]
    public async Task Correction_of_existing_event_links_original_and_leaves_it_unchanged()
    {
        var source = NewSource();
        var store = Store();
        await store.WriteBatchAsync([Event(source, "orig", At(0))], Now, CancellationToken.None);
        var before = (await ReadEventsAsync(source)).Single();

        var result = await store.WriteBatchAsync([Correction(source, "fix", At(100), "orig", At(0))], Now, CancellationToken.None);

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

        var first = await store.WriteBatchAsync([correction], Now, CancellationToken.None);
        var repeat = await store.WriteBatchAsync([correction], Now, CancellationToken.None);
        // исходное приходит позже: журнал неизменяем, связь не достраивается
        await store.WriteBatchAsync([Event(source, "orig", At(0))], Now, CancellationToken.None);

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

        await store.WriteBatchAsync([Event(source, "a", At(0)), Correction(source, "b", At(5), "a", At(0))], Now, CancellationToken.None);
        await store.WriteBatchAsync([Event(source, "a", At(0))], Now, CancellationToken.None);

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
            VALUES (gen_random_uuid(), '{source}', 'e1', '2026-03-10', now() AT TIME ZONE 'UTC', 'access.pass', 'p1', 100, jsonb_build_object(), 1)
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

        var ex = await Assert.ThrowsAsync<PostgresStoreException>(() => Store().WriteBatchAsync(batch, Now, CancellationToken.None));

        Assert.Equal("23514", ex.SqlState);
        Assert.Equal(WriteErrorClass.Data, WriteErrorClassifier.Classify(ex));
        Assert.Empty(await ReadEventsAsync(source)); // транзакция откатилась целиком
    }

    [Fact]
    public async Task Number_with_boundary_fraction_accepted_by_decoder_is_accepted_by_postgres()
    {
        var id = "num-" + Guid.NewGuid().ToString("N");
        var fraction = "0." + new string('0', 16382) + "1"; // 16383 цифры дробной части: предел numeric и декодера
        var template = Event("skud", id, At());
        var json = template.Body.GetRawText().Replace("\"n\":0", "\"n\":" + fraction).Replace("access.pass", "access_granted");
        Assert.Contains(fraction, json);

        var decoded = new EventDecoder().Decode(System.Text.Encoding.UTF8.GetBytes(json));
        Assert.True(decoded.IsSuccess, decoded.Detail);

        var result = await Store().WriteBatchAsync([decoded.Event!], Now, CancellationToken.None);

        Assert.Equal(1, result.EventsInserted);
        Assert.Contains((await ReadEventsAsync("skud")), r => r.SourceEventId == id);
    }

    [Fact]
    public async Task Decoder_verdict_on_boundary_numbers_matches_postgres_jsonb_outcome()
    {
        var zeros = (int n) => new string('0', n);
        string[] numbers =
        [
            // принимаются
            "0." + zeros(16383), "0." + zeros(16382) + "1", "0e1073741823", "0e-16383", "0.0e-16382", "1e131071", "1e-16383", "-0e5",
            // отвергаются
            "0." + zeros(16384), "0." + zeros(20000), "0e2000000000", "0e1073741824", "0.0e-2000000000", "0e-16384", "1e131072",
            "1e-16384", "0." + zeros(16382) + "1e-1",
        ];
        var template = Event("skud", "num-boundary", At());
        var decoder = new EventDecoder();
        var mismatches = new List<string>();
        var accepted = 0;
        var rejected = 0;

        await using var connection = await db.AppDataSource.OpenConnectionAsync();
        foreach (var number in numbers)
        {
            var json = template.Body.GetRawText().Replace("\"n\":0", "\"n\":" + number).Replace("access.pass", "access_granted");
            Assert.Contains("\"n\":" + number, json);
            var decoderAccepts = decoder.Decode(System.Text.Encoding.UTF8.GetBytes(json)).IsSuccess;

            bool postgresAccepts;
            try
            {
                await using var command = new NpgsqlCommand("SELECT @body::jsonb", connection);
                command.Parameters.AddWithValue("body", NpgsqlTypes.NpgsqlDbType.Text, json);
                await command.ExecuteScalarAsync();
                postgresAccepts = true;
            }
            catch (PostgresException ex) when (ex.SqlState == "22003")
            {
                postgresAccepts = false;
            }

            if (decoderAccepts != postgresAccepts)
            {
                mismatches.Add($"{(number.Length > 40 ? number[..40] + "..." + number.Length : number)}: decoder={decoderAccepts}, postgres={postgresAccepts}");
            }

            _ = postgresAccepts ? accepted++ : rejected++;
        }

        Assert.Empty(mismatches);
        Assert.True(accepted > 0 && rejected > 0);
    }

    [Fact]
    public async Task Correction_body_keeps_correction_block_and_unknown_fields()
    {
        var source = NewSource();
        var correction = Correction(source, "fix", At(100), "orig", At(0));

        await Store().WriteBatchAsync([correction], Now, CancellationToken.None);

        var stored = JsonNode.Parse((await ReadEventsAsync(source)).Single().Payload)!;
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(correction.Body.GetRawText()), stored));
        Assert.Equal("orig", stored["correction"]!["correctsSourceEventId"]!.GetValue<string>());
        Assert.Equal(2, stored["futureField"]!["x"]![1]!.GetValue<int>());
    }

    [Fact]
    public async Task Cancelled_write_surfaces_cancellation_not_a_database_error()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Store().WriteBatchAsync([Event(NewSource(), "a", At(0))], Now, cts.Token));
    }

    [Fact]
    public async Task Duplicate_and_new_events_are_handled_when_the_chunk_is_already_compressed()
    {
        var source = NewSource();
        var day = new DateTimeOffset(2025, 1, 6, 10, 0, 0, TimeSpan.Zero);
        var store = Store();
        await store.WriteBatchAsync([Event(source, "old", day)], Now, CancellationToken.None);

        await using (var migrator = new NpgsqlConnection(db.MigratorConnectionString))
        {
            await migrator.OpenAsync();
            await using var compress = new NpgsqlCommand(
                "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('ktm.events', older_than => '2025-02-01'::timestamp, newer_than => '2025-01-01'::timestamp) c",
                migrator);
            Assert.True((long)(await compress.ExecuteScalarAsync())! >= 1);
            await using var check = new NpgsqlCommand(
                "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'ktm' AND hypertable_name = 'events' AND is_compressed AND range_start < '2025-01-06Z' AND range_end > '2025-01-06Z'",
                migrator);
            Assert.Equal(1L, await check.ExecuteScalarAsync());
        }

        var result = await store.WriteBatchAsync(
            [Event(source, "old", day), Event(source, "new", day.AddMinutes(1))], Now, CancellationToken.None);

        Assert.Equal(new EventWriteResult(1, 0), result);
        Assert.Equal(2, (await ReadEventsAsync(source)).Count);
    }

    [Fact]
    public void Authentication_error_nested_in_npgsql_exception_is_fatal_config()
    {
        var inner = new PostgresException("password authentication failed", "FATAL", "FATAL", "28P01");
        var adapted = PostgresStoreException.Adapt(new NpgsqlException("connect failed", inner));

        Assert.Equal("28P01", ((ISqlStateError)adapted).SqlState);
        Assert.Equal(WriteErrorClass.FatalConfig, WriteErrorClassifier.Classify(adapted));
    }

    private static long UuidV7Milliseconds(Guid id)
    {
        var bytes = id.ToByteArray(bigEndian: true);
        long ms = 0;
        for (var i = 0; i < 6; i++) ms = (ms << 8) | bytes[i];
        return ms;
    }

    [Fact]
    public async Task Unreachable_database_is_classified_as_transient()
    {
        var options = new PostgresOptions
        {
            Host = "127.0.0.1", Database = "d", Username = "u", SslMode = "Disable",
            Port = 1, Password = "x", ConnectTimeoutSeconds = 2, MinPoolSize = 0,
        };
        await using var dataSource = options.BuildDataSource();
        var store = new NpgsqlEventStore(dataSource, new FixedTrustProvider(100));

        var ex = await Assert.ThrowsAsync<PostgresStoreException>(
            () => store.WriteBatchAsync([Event(NewSource(), "a", At(0))], Now, CancellationToken.None));

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(ex));
    }

    [Fact]
    public async Task Poison_constraint_rejects_only_the_poison_event_with_23514_and_ordinary_events_are_written()
    {
        var source = NewSource();
        await using var poison = await PoisonConstraint.AddAsync(db.AdminConnectionString);

        var ex = await Assert.ThrowsAsync<PostgresStoreException>(
            () => Store().WriteBatchAsync([Event(source, "ok", At(0)), Event(source, poison.PoisonId, At(1))], Now, CancellationToken.None));
        await Store().WriteBatchAsync([Event(source, "ok", At(0))], Now, CancellationToken.None);

        Assert.Equal(("23514", PoisonConstraint.Name, WriteErrorClass.Data), (ex.SqlState, ex.ConstraintName, WriteErrorClassifier.Classify(ex)));
        Assert.Equal("Npgsql.PostgresException", DeadLetterInfo.FromException(ex, "write", 4, Now).ExceptionType);
        Assert.Equal(["ok"], (await ReadEventsAsync(source)).Select(r => r.SourceEventId));

        // «Исправление причины»: после снятия ограничения то же событие записывается.
        await poison.DropAsync();
        await Store().WriteBatchAsync([Event(source, poison.PoisonId, At(1))], Now, CancellationToken.None);
        Assert.Equal(["ok", poison.PoisonId], (await ReadEventsAsync(source)).Select(r => r.SourceEventId));
    }

    // Фиксирует итоговый список инфраструктурных и «сообщенческих» исключений на реальном драйвере (ADR-0006 п. 2.2, R-1).
    [Fact]
    public async Task Value_that_cannot_be_serialized_is_unexpected_write_value_error_not_infrastructure()
    {
        var source = NewSource();
        var bad = Event(source, "big-version", At(0)) with { SchemaVersion = 70000 };

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => Store().WriteBatchAsync([bad], Now, CancellationToken.None));

        Assert.Equal(WriteErrorClass.Unexpected, WriteErrorClassifier.Classify(ex));
        Assert.Equal(DeadLetterReasons.WriteValueError, DeadLetterInfo.FromException(ex, "write", 4, Now).Reason);
    }

    [Fact]
    public async Task Exhausted_connection_pool_is_infrastructure_and_never_poison()
    {
        var builder = new NpgsqlConnectionStringBuilder(db.AppConnectionString) { MaxPoolSize = 1, Timeout = 1, Pooling = true };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        await using var held = await dataSource.OpenConnectionAsync();
        var store = new NpgsqlEventStore(dataSource, new FixedTrustProvider(100));

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => store.WriteBatchAsync([Event(NewSource(), "a", At(0))], Now, CancellationToken.None));

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(ex));
    }

    // M5 (review-fixes-1): обрыв соединения посреди выполнения транзакции - инфраструктура, а не свойство сообщения.
    // Запись блокируется незавершённой вставкой того же ключа в другой транзакции, затем бэкенд записи завершается pg_terminate_backend.
    [Fact]
    public async Task Backend_terminated_in_the_middle_of_a_write_is_infrastructure_and_never_poison()
    {
        var source = NewSource();
        var appName = "ktm-m5-" + Guid.NewGuid().ToString("N")[..12];
        var builder = new NpgsqlConnectionStringBuilder(db.AppConnectionString) { ApplicationName = appName, Pooling = false };
        await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);
        var store = new NpgsqlEventStore(dataSource, new FixedTrustProvider(100));
        var blocked = Event(source, "blocked", At(0));

        await using var blocker = await db.AppDataSource.OpenConnectionAsync();
        await using var blockerTransaction = await blocker.BeginTransactionAsync();
        await using (var insert = new NpgsqlCommand(
            """
            INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, device_id, external_person_id, trust, payload, schema_version)
            VALUES (@id, @s, @e, @o, @r, 'access', NULL, 'p', 100, '{}'::jsonb, 1)
            """, blocker, blockerTransaction))
        {
            insert.Parameters.AddWithValue("id", Guid.NewGuid());
            insert.Parameters.AddWithValue("s", blocked.Source);
            insert.Parameters.AddWithValue("e", blocked.SourceEventId);
            insert.Parameters.AddWithValue("o", DateTime.SpecifyKind(blocked.OccurredAt.UtcDateTime, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue("r", DateTime.SpecifyKind(Now.UtcDateTime, DateTimeKind.Unspecified));
            await insert.ExecuteNonQueryAsync();
        }

        var write = store.WriteBatchAsync([blocked], Now, CancellationToken.None);
        await using var admin = new NpgsqlConnection(db.AdminConnectionString);
        await admin.OpenAsync();
        for (var i = 0; i < 50 && !write.IsCompleted; i++)
        {
            await using var terminate = new NpgsqlCommand("SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = @n AND wait_event_type = 'Lock') t", admin);
            terminate.Parameters.AddWithValue("n", appName);
            if ((long)(await terminate.ExecuteScalarAsync())! > 0) break;
            await Task.Delay(100);
        }

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => write);

        Assert.Equal(WriteErrorClass.Transient, WriteErrorClassifier.Classify(ex));
    }

    private static DateTimeOffset ReadUtc(NpgsqlDataReader reader, int ordinal) =>
        new(DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc));

    private sealed record EventRow(Guid EventId, string SourceEventId, DateTimeOffset OccurredAt, DateTimeOffset ReceivedAt,
        string EventType, string? DeviceId, string ExternalPersonId, short Trust, string Payload, short SchemaVersion, string Json);

    private sealed record CorrectionRow(string CorrectionSourceEventId, Guid CorrectionEventId, string OriginalSource,
        string OriginalSourceEventId, DateTimeOffset OriginalOccurredAt, Guid? OriginalEventId, string? Reason, string Actor);
}

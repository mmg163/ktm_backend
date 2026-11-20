using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Замер пропускной способности (задача 8.1, категория Perf: в обычный прогон не входит). Kafka и TimescaleDB в контейнерах,
/// Data Saver in-process с настройками по умолчанию (Batch 500 / 1000 мс), топик на 9 партиций как в ADR-0002.
/// Параметры: <c>KTM_PERF_RATE</c> (соб/с, 500), <c>KTM_PERF_SECONDS</c> (900), <c>KTM_PERF_CAPACITY</c> (300000).
/// Отчёт печатается в вывод теста и пишется в файл <c>perf-*.txt</c> рядом со сборкой (или в <c>KTM_PERF_OUT</c>).
/// </summary>
[Collection(AppCollection.Name)]
[Trait("Category", "Perf")]
public sealed class ThroughputPerfTests(KafkaFixture kafka, PostgresFixture postgres, ITestOutputHelper output)
{
    private const int Partitions = 9;
    private const int Persons = 1000;
    private static readonly string[] Sources = ["skud", "cv", "1c-do"];

    private static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), CultureInfo.InvariantCulture, out var v) && v > 0 ? v : fallback;

    private static void UseDefaultTimings(Dictionary<string, string?> settings)
    {
        foreach (var key in new[] { "DataSaver:Batch:MaxWaitMs", "DataSaver:Backoff:InitialMs", "DataSaver:Backoff:MaxMs", "DataSaver:PollIntervalMs" })
        {
            settings.Remove(key);
        }
    }

    // Тип события и тело по источнику: skud/cv требуют deviceId, 1c-do - нет (схема kafka-event.schema.json).
    private static (string Body, string Key) Message(string prefix, long seq, DateTime occurredAt)
    {
        var source = Sources[seq % Sources.Length];
        var person = $"emp-{seq % Persons}";
        var id = $"{prefix}-{seq.ToString(CultureInfo.InvariantCulture)}";
        var at = occurredAt.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);
        var (type, device, payload) = source switch
        {
            "skud" => ("access_granted", ",\"deviceId\":\"turnstile-01-entry\"", "{\"direction\":\"in\",\"zone\":\"main-gate\"}"),
            "cv" => ("presence_detected", ",\"deviceId\":\"camera-07\"", "{\"confidence\":0.93,\"frameRef\":\"s3://cv/2026/11/20/f-" + seq + "\"}"),
            _ => ("task_assigned", string.Empty, "{\"taskId\":\"T-" + seq + "\",\"priority\":\"normal\"}"),
        };
        var body = $"{{\"schemaVersion\":1,\"source\":\"{source}\",\"sourceEventId\":\"{id}\",\"occurredAt\":\"{at}\",\"eventType\":\"{type}\"{device},\"externalPersonId\":\"{person}\",\"payload\":{payload}}}";
        return (body, $"{source}:{person}");
    }

    private static long NowMicroTicks() => DateTime.UtcNow.Ticks / 10 * 10; // как в timestamptz: микросекунды

    [Fact]
    public async Task Sustained_load_at_target_rate_has_no_lag_growth_and_reports_latency()
    {
        var rate = EnvInt("KTM_PERF_RATE", 500);
        var seconds = EnvInt("KTM_PERF_SECONDS", 900);
        var total = (long)rate * seconds;
        var report = new Report();
        report.Line($"== Sustained load: {rate} ev/s x {seconds} s = {total} events, 3 sources, {Partitions} partitions ==");
        await report.EnvironmentAsync(await ContainerIdsAsync());

        var run = await TestRun.CreateAsync(kafka.BootstrapServers, postgres, Partitions);
        await using var app = DataSaverInstance.Start(run, UseDefaultTimings);
        using var batches = new BatchListener();
        await app.WaitReadyAsync();
        var consumerReady = await WaitAssignedAsync(run);
        report.Line($"Group stable: {consumerReady}");

        var published = new long[total];      // микро-тики публикации (= occurredAt)
        var latencyMs = new double[total];
        Array.Fill(latencyMs, double.NaN);
        var sent = 0L;
        var deliveryErrors = 0L;
        var ledger = new List<(double T, long Written, long Sent, long Committed)>();
        var resources = new List<string>();
        var cpuSamples = new List<(double T, double CpuPct, long WorkingSetMb)>();
        using var stop = new CancellationTokenSource();
        var clock = Stopwatch.StartNew();

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            LingerMs = 5,
            Acks = Acks.All,
        }).Build();

        var generator = Task.Run(async () =>
        {
            var pacing = Stopwatch.StartNew();
            while (Volatile.Read(ref sent) < total)
            {
                var due = Math.Min(total, (long)(pacing.Elapsed.TotalSeconds * rate) + 1);
                for (var seq = Volatile.Read(ref sent); seq < due; seq++)
                {
                    var ticks = NowMicroTicks();
                    published[seq] = ticks;
                    var (body, key) = Message(run.Prefix, seq, new DateTime(ticks, DateTimeKind.Utc));
                    producer.Produce(run.RawTopic, new Message<string, string> { Key = key, Value = body }, r =>
                    {
                        if (r.Error.IsError) Interlocked.Increment(ref deliveryErrors);
                    });
                    Volatile.Write(ref sent, seq + 1);
                }

                await Task.Delay(2);
            }

            producer.Flush(TimeSpan.FromSeconds(60));
        });

        // Задержка "публикация -> строка в БД": опрос журнала каждые 250 мс; момент видимости строки - начало опроса (верхняя оценка, разрешение ~250 мс).
        var latencySampler = Task.Run(async () =>
        {
            await using var connection = new NpgsqlConnection(postgres.AdminConnectionString);
            await connection.OpenAsync();
            var lowest = 0L;
            var seen = new bool[total];
            while (true)
            {
                var visibleAt = NowMicroTicks();
                var available = Volatile.Read(ref sent);
                if (lowest < available)
                {
                    await using var command = new NpgsqlCommand(
                        "SELECT source_event_id FROM ktm.events WHERE occurred_at >= @from AND source_event_id LIKE @p", connection);
                    command.Parameters.AddWithValue("from", new DateTime(published[lowest], DateTimeKind.Utc));
                    command.Parameters.AddWithValue("p", run.Prefix + "-%");
                    await using var reader = await command.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetString(0);
                        var seq = long.Parse(id.AsSpan(id.LastIndexOf('-') + 1), CultureInfo.InvariantCulture);
                        if (seen[seq]) continue;
                        seen[seq] = true;
                        latencyMs[seq] = (visibleAt - published[seq]) / 10_000.0;
                    }

                    while (lowest < available && seen[lowest]) lowest++;
                }

                if (stop.IsCancellationRequested && lowest >= Volatile.Read(ref sent)) break;
                await Task.Delay(250);
            }
        });

        // Счётчики, лаг группы и ресурсы.
        var monitor = Task.Run(async () =>
        {
            var process = Process.GetCurrentProcess();
            var cpuPrev = process.TotalProcessorTime;
            var tPrev = clock.Elapsed;
            var nextLag = TimeSpan.Zero;
            var nextDocker = TimeSpan.Zero;
            var containers = await ContainerIdsAsync();
            long committed = 0;
            while (!stop.IsCancellationRequested)
            {
                await Task.Delay(1000);
                var t = clock.Elapsed;
                if (t >= nextLag)
                {
                    committed = await run.CommittedAsync();
                    nextLag = t + TimeSpan.FromSeconds(5);
                    process.Refresh();
                    var cpu = process.TotalProcessorTime;
                    cpuSamples.Add((t.TotalSeconds, (cpu - cpuPrev).TotalSeconds / (t - tPrev).TotalSeconds * 100, process.WorkingSet64 >> 20));
                    cpuPrev = cpu;
                    tPrev = t;
                }

                ledger.Add((t.TotalSeconds, app.Counters.Written, Volatile.Read(ref sent), committed));
                if (t >= nextDocker && containers.Count > 0)
                {
                    nextDocker = t + TimeSpan.FromSeconds(30);
                    resources.Add($"t={t.TotalSeconds:F0}s " + await DockerStatsAsync(containers));
                }
            }
        });

        await generator;
        var generationSeconds = clock.Elapsed.TotalSeconds;
        var drainStart = clock.Elapsed;
        await Wait.UntilAsync(async () => await run.RowsAsync() == total, $"all {total} rows in the journal", TimeSpan.FromMinutes(5),
            () => $"rows={run.RowsAsync().Result}");
        var drain = clock.Elapsed - drainStart;
        await Wait.UntilAsync(async () => await run.CommittedAsync() == total, "all offsets committed", TimeSpan.FromMinutes(1));
        stop.Cancel();
        await Task.WhenAll(latencySampler, monitor);

        // Итоги.
        var warmup = Math.Min(60, seconds / 5.0);
        var from = ledger.Last(p => p.T <= warmup);
        var to = ledger.Last(p => p.T <= generationSeconds - 10 && p.T >= warmup);
        var steadyRate = (to.Written - from.Written) / (to.T - from.T);
        report.Line($"Generated {Volatile.Read(ref sent)} events in {generationSeconds:F1} s => {sent / generationSeconds:F1} ev/s published; delivery errors: {deliveryErrors}");
        report.Line($"Drain after generation end: {drain.TotalSeconds:F1} s");
        report.Line($"Steady interval t={from.T:F0}..{to.T:F0} s: written {steadyRate:F1} ev/s (counter ktm.datasaver.events.written)");
        report.Line("Per-minute written rate (ev/s): " + string.Join(", ", PerMinute(ledger, generationSeconds)));

        var warmIndex = (long)(warmup * rate);
        var steady = latencyMs.Skip((int)warmIndex).Take((int)Math.Max(0, total - warmIndex - 10L * rate)).Where(v => !double.IsNaN(v)).Order().ToArray();
        var all = latencyMs.Where(v => !double.IsNaN(v)).Order().ToArray();
        report.Line($"Latency publish -> row visible (ms; upper bound, resolution ~250 ms), steady events n={steady.Length}: p50={Pct(steady, 0.50):F0} p95={Pct(steady, 0.95):F0} p99={Pct(steady, 0.99):F0} max={steady.LastOrDefault():F0}");
        report.Line($"Latency all events n={all.Length}: p50={Pct(all, 0.50):F0} p99={Pct(all, 0.99):F0} max={all.LastOrDefault():F0}; events not observed by sampler: {total - all.Length}");

        var lag = ledger.Where(p => p.T >= warmup && p.T <= generationSeconds).Select(p => (p.T, Lag: p.Sent - p.Committed, WrittenLag: p.Sent - p.Written)).ToList();
        var third = Math.Max(1, lag.Count / 3);
        var firstMean = lag.Take(third).Average(p => (double)p.Lag);
        var lastMean = lag.Skip(lag.Count - third).Average(p => (double)p.Lag);
        report.Line($"Consumer lag (published - committed offsets, messages), steady: first third mean={firstMean:F0}, last third mean={lastMean:F0}, max={lag.Max(p => p.Lag)}; written-lag max={lag.Max(p => p.WrittenLag)}");
        report.Line("Lag every ~60 s (published-committed / published-written): " +
                    string.Join(", ", lag.Where((_, i) => i % 60 == 0).Select(p => $"{p.T:F0}s:{p.Lag}/{p.WrittenLag}")));

        var duration = batches.Durations.Order().ToArray();
        var sizes = batches.Sizes.Order().ToArray();
        if (duration.Length > 0)
        {
            report.Line($"Batches: {duration.Length}, size mean={sizes.Average():F0} p50={Pct(sizes.Select(s => (double)s).ToArray(), 0.5):F0} max={sizes.Last()}; write duration ms: p50={Pct(duration, 0.5) * 1000:F0} p99={Pct(duration, 0.99) * 1000:F0} max={duration.Last() * 1000:F0}");
        }

        if (cpuSamples.Count > 0)
        {
            report.Line($"Test process (Data Saver + generator + 3 clients, in-process): CPU mean={cpuSamples.Average(s => s.CpuPct):F0}% of one core (max {cpuSamples.Max(s => s.CpuPct):F0}%), working set mean={cpuSamples.Average(s => s.WorkingSetMb):F0} MB, max={cpuSamples.Max(s => s.WorkingSetMb)} MB; GC heap now={GC.GetTotalMemory(false) >> 20} MB, gen2 collections={GC.CollectionCount(2)}");
        }

        foreach (var line in resources) report.Line("docker stats: " + line);
        report.Line("DB: " + await DbSizeAsync());
        var warnings = app.Logs.GetSnapshot().Count(r => r.Level >= LogLevel.Warning);
        report.Line($"Log records >= Warning: {warnings}; loop terminations: {app.Terminator.Calls}");
        report.Publish(output, "sustained");

        Assert.Equal(0, deliveryErrors);
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        Assert.True(sent / generationSeconds >= rate * 0.98, "generator did not reach the target rate");
        Assert.True(steadyRate >= rate * 0.98, $"written {steadyRate:F1} ev/s < 98% of {rate}");
        Assert.True(lastMean <= firstMean + 2 * 500, $"lag grows: {firstMean:F0} -> {lastMean:F0}");
        Assert.True(drain < TimeSpan.FromSeconds(30), $"backlog drained for {drain}");
        Assert.True(Pct(steady, 0.99) < TimeSpan.FromMinutes(5).TotalMilliseconds, "p99 above the 5 minute NFR");
        Assert.Equal(0, app.Terminator.Calls);
    }

    [Fact]
    public async Task Backlog_drain_gives_maximum_single_instance_rate_and_insert_plan_on_filled_table()
    {
        var n = EnvInt("KTM_PERF_CAPACITY", 300_000);
        var report = new Report();
        report.Line($"== Max single-instance rate: backlog of {n} events, {Partitions} partitions, instance starts after publishing ==");
        await report.EnvironmentAsync(await ContainerIdsAsync());

        var run = await TestRun.CreateAsync(kafka.BootstrapServers, postgres, Partitions);
        using (var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = kafka.BootstrapServers, LingerMs = 20, Acks = Acks.All }).Build())
        {
            for (var seq = 0L; seq < n; seq++)
            {
                var (body, key) = Message(run.Prefix, seq, DateTime.UtcNow);
                while (true)
                {
                    try
                    {
                        producer.Produce(run.RawTopic, new Message<string, string> { Key = key, Value = body });
                        break;
                    }
                    catch (ProduceException<string, string> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
                    {
                        producer.Poll(TimeSpan.FromMilliseconds(50));
                    }
                }
            }

            producer.Flush(TimeSpan.FromMinutes(2));
        }

        await using var app = DataSaverInstance.Start(run, UseDefaultTimings);
        using var batches = new BatchListener();
        var clock = Stopwatch.StartNew();
        var samples = new List<(double T, long Written)>();
        while (app.Counters.Written < n)
        {
            if (clock.Elapsed > TimeSpan.FromMinutes(10)) throw new TimeoutException($"backlog not drained: {app.Counters.Written}/{n}");
            samples.Add((clock.Elapsed.TotalSeconds, app.Counters.Written));
            await Task.Delay(250);
        }

        var total = clock.Elapsed.TotalSeconds;
        var firstWrite = samples.First(s => s.Written > 0).T;
        var p10 = samples.First(s => s.Written >= n * 0.1);
        var p90 = samples.First(s => s.Written >= n * 0.9);
        report.Line($"Drained {n} in {total:F1} s from instance start (includes host start and group join); first write at {firstWrite:F1} s");
        report.Line($"Overall: {n / total:F0} ev/s; from first write: {n / (total - firstWrite):F0} ev/s; 10%..90% interval: {(p90.Written - p10.Written) / (p90.T - p10.T):F0} ev/s");
        var duration = batches.Durations.Order().ToArray();
        var sizes = batches.Sizes.ToArray();
        report.Line($"Batches: {duration.Length}, mean size {sizes.Average():F0}; write duration ms p50={Pct(duration, 0.5) * 1000:F0} p99={Pct(duration, 0.99) * 1000:F0} max={duration.Last() * 1000:F0}");
        await Wait.UntilAsync(async () => await run.RowsAsync() == n, "all rows", TimeSpan.FromMinutes(1));
        report.Line("DB: " + await DbSizeAsync());
        report.Line($"Log records >= Warning: {app.Logs.GetSnapshot().Count(r => r.Level >= LogLevel.Warning)}");

        report.Line(string.Empty);
        report.Line("== EXPLAIN (ANALYZE, BUFFERS) of a 500-row insert into the filled hypertable (rolled back; rows from generate_series, otherwise the INSERT of NpgsqlEventStore) ==");
        var executionTimes = new List<double>();
        string lastPlan = string.Empty;
        await using (var connection = new NpgsqlConnection(postgres.AdminConnectionString))
        {
            await connection.OpenAsync();
            for (var i = 0; i < 7; i++)
            {
                await using var transaction = await connection.BeginTransactionAsync();
                var plan = new StringBuilder();
                await using (var command = new NpgsqlCommand(ExplainSql(Guid.NewGuid().ToString("N")), connection, transaction))
                await using (var reader = await command.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync()) plan.AppendLine(reader.GetString(0));
                }

                await transaction.RollbackAsync();
                lastPlan = plan.ToString();
                var execLine = lastPlan.Split('\n').Last(l => l.StartsWith("Execution Time", StringComparison.Ordinal));
                executionTimes.Add(double.Parse(execLine.Split(':')[1].Replace("ms", string.Empty).Trim(), CultureInfo.InvariantCulture));
            }
        }

        report.Line("Execution Time ms, 7 runs (first is cold): " + string.Join(", ", executionTimes.Select(t => t.ToString("F1", CultureInfo.InvariantCulture))));
        report.Line("Last plan:");
        report.Line(lastPlan);
        report.Publish(output, "capacity");

        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        Assert.Equal(0, app.Terminator.Calls);
    }

    private static string ExplainSql(string runId) => $$"""
        EXPLAIN (ANALYZE, BUFFERS)
        INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, device_id,
                                external_person_id, trust, payload, schema_version)
        SELECT t.event_id, t.source, t.source_event_id, t.occurred_at, t.received_at, t.event_type, t.device_id,
               t.external_person_id, t.trust, t.payload, t.schema_version
        FROM (SELECT gen_random_uuid() AS event_id,
                     (ARRAY['skud','cv','1c-do'])[g % 3 + 1] AS source,
                     'explain-{{runId}}-' || g AS source_event_id,
                     now() - g * interval '2 milliseconds' AS occurred_at,
                     now() AS received_at,
                     'access_granted' AS event_type,
                     'turnstile-01-entry' AS device_id,
                     'emp-' || (g % 1000) AS external_person_id,
                     100::smallint AS trust,
                     '{"direction":"in","zone":"main-gate"}'::jsonb AS payload,
                     1::smallint AS schema_version
              FROM generate_series(1, 500) g) t
        ORDER BY t.source, t.source_event_id, t.occurred_at
        ON CONFLICT (source, source_event_id, occurred_at) DO NOTHING
        """;

    private async Task<bool> WaitAssignedAsync(TestRun run)
    {
        await Wait.UntilAsync(async () =>
        {
            var snapshot = await run.Kafka.DescribeGroupAsync(run.Group, run.RawTopic);
            return snapshot is not null && snapshot.Members.Values.Sum(a => a.Count) == Partitions;
        }, "all partitions assigned", TimeSpan.FromSeconds(60));
        return true;
    }

    private static double Pct(double[] sorted, double q) => sorted.Length == 0 ? double.NaN : sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(q * sorted.Length) - 1)];

    private static IEnumerable<string> PerMinute(List<(double T, long Written, long Sent, long Committed)> ledger, double generationSeconds)
    {
        for (var start = 0.0; start + 60 <= generationSeconds + 1; start += 60)
        {
            var a = ledger.LastOrDefault(p => p.T <= start);
            var b = ledger.LastOrDefault(p => p.T <= start + 60);
            if (b.T > a.T) yield return ((b.Written - a.Written) / (b.T - a.T)).ToString("F0", CultureInfo.InvariantCulture);
        }
    }

    private async Task<string> DbSizeAsync()
    {
        await using var connection = new NpgsqlConnection(postgres.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT (SELECT count(*) FROM ktm.events) || ' rows in ktm.events, hypertable total size ' || pg_size_pretty(hypertable_size('ktm.events')) || ', chunks ' || (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'events')",
            connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    /// <summary>Контейнеры этого прогона (Kafka и TimescaleDB) по опубликованным портам: на машине могут работать чужие контейнеры.</summary>
    private async Task<List<string>> ContainerIdsAsync()
    {
        var kafkaPort = kafka.BootstrapServers[(kafka.BootstrapServers.LastIndexOf(':') + 1)..];
        var dbPort = new NpgsqlConnectionStringBuilder(postgres.AdminConnectionString).Port.ToString(CultureInfo.InvariantCulture);
        var ids = new List<string>();
        foreach (var port in new[] { kafkaPort, dbPort })
        {
            var found = await RunAsync("docker", $"ps --filter publish={port} --format \"{{{{.ID}}}}\"");
            ids.AddRange(found.Split((char)10, StringSplitOptions.RemoveEmptyEntries).Select(l => l.Trim()));
        }

        return ids;
    }

    private static async Task<string> DockerStatsAsync(List<string> ids)
    {
        var stats = await RunAsync("docker", "stats --no-stream --format \"{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}\" " + string.Join(' ', ids));
        return string.Join(" ; ", stats.Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(l => l.Trim()));
    }

    internal static async Task<string> RunAsync(string file, string arguments)
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo(file, arguments) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false })!;
            var text = await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            return text;
        }
        catch (System.ComponentModel.Win32Exception)
        {
            return string.Empty;
        }
    }

    private sealed class BatchListener : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly List<double> _durations = [];
        private readonly List<int> _sizes = [];

        public BatchListener()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == DataSaverMetrics.MeterName &&
                    instrument.Name is "ktm.datasaver.batch.duration" or "ktm.datasaver.batch.size")
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>((_, value, _, _) => { lock (_durations) _durations.Add(value); });
            _listener.SetMeasurementEventCallback<int>((_, value, _, _) => { lock (_sizes) _sizes.Add(value); });
            _listener.Start();
        }

        public double[] Durations { get { lock (_durations) return [.. _durations]; } }

        public int[] Sizes { get { lock (_sizes) return [.. _sizes.Order()]; } }

        public void Dispose() => _listener.Dispose();
    }

    private sealed class Report
    {
        private readonly StringBuilder _text = new();

        public void Line(string line) => _text.AppendLine(line);

        public async Task EnvironmentAsync(List<string> containers)
        {
            Line($"Started {DateTimeOffset.UtcNow:O}; OS {System.Runtime.InteropServices.RuntimeInformation.OSDescription}; .NET {Environment.Version}; logical CPUs {Environment.ProcessorCount}");
            Line("Images: " + string.Join(", ", (await RunAsync("docker", "ps --format \"{{.Image}}\" " + string.Join(' ', containers.Select(c => "--filter id=" + c)))).Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim())));
            Line("Docker: " + (await RunAsync("docker", "info --format \"{{.ServerVersion}}, {{.NCPU}} CPU, {{.MemTotal}} bytes\"")).Trim());
            Line("Batch: MaxSize=500, MaxWaitMs=1000 (defaults), PollIntervalMs=500 (default), Kafka SessionTimeoutMs=10000");
        }

        public void Publish(ITestOutputHelper output, string name)
        {
            var text = _text.ToString();
            output.WriteLine(text);
            Console.WriteLine(text);
            var directory = Environment.GetEnvironmentVariable("KTM_PERF_OUT") ?? AppContext.BaseDirectory;
            File.WriteAllText(Path.Combine(directory, $"perf-{name}.txt"), text);
        }
    }
}

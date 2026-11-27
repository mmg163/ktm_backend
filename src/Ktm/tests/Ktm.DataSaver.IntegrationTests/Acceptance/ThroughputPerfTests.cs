using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Infrastructure.Postgres;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Ktm.HostedServices.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Xunit.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Замер пропускной способности (задача 8.1, категория Perf: в обычный прогон не входит, без параметров пропускается).
/// Kafka и TimescaleDB в контейнерах, Data Saver in-process, топик на 9 партиций как в ADR-0002. Каждый прогон работает в
/// собственной пустой БД (<see cref="PerfDatabase"/>) и засевает в неё фиксированный объём строк (<c>KTM_PERF_SEED</c>,
/// 300 000): объём таблицы не зависит от порядка тестов. count(*) до засева, после засева и после прогона - в отчёте.
/// Параметры: <c>KTM_PERF_RATE</c> (соб/с, 500), <c>KTM_PERF_SECONDS</c> (900), <c>KTM_PERF_CAPACITY</c> (300000),
/// <c>KTM_PERF_SEED</c> (300000, 0 - без засева), <c>KTM_PERF_BATCH_MAXSIZE</c> / <c>KTM_PERF_BATCH_MAXWAITMS</c>
/// (иначе умолчания продукта), <c>KTM_PERF_DRAIN_TIMEOUT_SECONDS</c> (300), <c>KTM_PERF_RUN</c> (метка прогона для
/// каталога выгрузки), <c>KTM_PERF_OUT</c> (каталог выгрузки; иначе <c>docs/test-protocols/data/&lt;дата&gt;-&lt;метка&gt;/</c>).
/// Допуск устойчивой скорости: <see cref="SteadyRateFactorLong"/> для прогонов от <see cref="ShortRunSeconds"/>,
/// <see cref="SteadyRateFactorShort"/> для более коротких (дымовых).
/// </summary>
[Collection(AppCollection.Name)]
[Trait("Category", "Perf")]
public sealed class ThroughputPerfTests(KafkaFixture kafka, PostgresFixture postgres, ITestOutputHelper output)
{
    private const int Partitions = 9;
    private const int Persons = 1000;

    /// <summary>Прогоны короче этого порога (секунд) считаются короткими: прогрев занимает заметную долю, допуск мягче.</summary>
    internal const int ShortRunSeconds = 600;

    internal const double SteadyRateFactorLong = 0.98;
    internal const double SteadyRateFactorShort = 0.95;

    // Строки журнала ищутся по received_at (одна запись пачки за раз, значения не убывают); occurred_at >= cursor - окно
    // нужен для использования индекса по времени: событие не может быть записано позже, чем через это окно после публикации.
    private const int LatencyLookbackSeconds = 15;
    private const int LatencyPollMs = 250;
    private const int LedgerPeriodMs = 1000;

    private static readonly string[] Sources = ["skud", "cv", "1c-do"];

    private static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), CultureInfo.InvariantCulture, out var v) && v > 0 ? v : fallback;

    private static int EnvNonNegative(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), CultureInfo.InvariantCulture, out var v) && v >= 0 ? v : fallback;

    private static void Tune(Dictionary<string, string?> settings)
    {
        // Берём умолчания продукта; значения, заданные стендом (SessionTimeout, SslMode, Shutdown), остаются.
        foreach (var key in new[] { "DataSaver:Batch:MaxWaitMs", "DataSaver:Backoff:InitialMs", "DataSaver:Backoff:MaxMs", "DataSaver:PollIntervalMs" })
        {
            settings.Remove(key);
        }

        if (Environment.GetEnvironmentVariable("KTM_PERF_BATCH_MAXSIZE") is { Length: > 0 } size) settings["DataSaver:Batch:MaxSize"] = size;
        if (Environment.GetEnvironmentVariable("KTM_PERF_BATCH_MAXWAITMS") is { Length: > 0 } wait) settings["DataSaver:Batch:MaxWaitMs"] = wait;
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

    private static long NowMicroTicks() => DateTime.UtcNow.Ticks / 10 * 10; // как в PostgreSQL timestamp: микросекунды

    private static async Task DelayAsync(int milliseconds, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(milliseconds, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Отмена - штатный сигнал остановки фоновой работы, а не ошибка.
        }
    }

    [PerfFact("KTM_PERF_RATE", "KTM_PERF_SECONDS")]
    public async Task Sustained_load_at_target_rate_has_no_lag_growth_and_reports_latency()
    {
        var rate = EnvInt("KTM_PERF_RATE", 500);
        var seconds = EnvInt("KTM_PERF_SECONDS", 900);
        var seedRows = EnvNonNegative("KTM_PERF_SEED", 300_000);
        var drainTimeout = TimeSpan.FromSeconds(EnvNonNegative("KTM_PERF_DRAIN_TIMEOUT_SECONDS", 300));
        var total = (long)rate * seconds;
        var steadyFactor = seconds < ShortRunSeconds ? SteadyRateFactorShort : SteadyRateFactorLong;
        var report = new Report();
        report.Line($"== Sustained load: {rate} ev/s x {seconds} s = {total} events, 3 sources, {Partitions} partitions ==");
        var containerIds = await ContainerIdsAsync();
        await report.EnvironmentAsync(containerIds);
        report.Line($"Bootstrap {kafka.BootstrapServers}; measured containers: {string.Join(',', containerIds)}");

        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        report.Line($"Dedicated database created for this run; ktm.events count(*) before seed: {await database.CountEventsAsync()}");
        await database.SeedAsync(seedRows);
        var rowsBefore = await database.CountEventsAsync();
        report.Line($"ktm.events count(*) after seed of {seedRows}: {rowsBefore}");

        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database, Partitions);
        using var batches = new BatchListener();
        await using var app = DataSaverInstance.Start(run, Tune);
        report.Config(app.Services);
        var batchMaxSize = app.Services.GetRequiredService<IOptions<BatchOptions>>().Value.MaxSize;
        await app.WaitReadyAsync();
        var consumerReady = await WaitAssignedAsync(run);
        report.Line($"Group stable: {consumerReady}");

        var published = new long[total];      // микро-тики публикации (= occurredAt)
        var latencyMs = new double[total];
        Array.Fill(latencyMs, double.NaN);
        var sent = 0L;
        var deliveryErrors = 0L;
        var ledger = new List<LedgerPoint>();
        var resources = new List<string>();
        var cpuSamples = new List<(double T, double CpuPct, long WorkingSetMb)>();
        var iterationSizes = new List<int>();
        var committedReadMs = new List<double>();
        var pollStats = new PollStats();
        long lastRows = 0;
        using var stop = new CancellationTokenSource();   // штатное завершение: монитор и опросчик досчитывают и выходят
        using var abort = new CancellationTokenSource();  // аварийное: всё немедленно останавливается (в finally)
        var clock = Stopwatch.StartNew();
        var runStartedAt = new DateTime(NowMicroTicks(), DateTimeKind.Utc);

        using var producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            LingerMs = 5,
            Acks = Acks.All,
        }).Build();
        using var committedReader = new CommittedReader(kafka.BootstrapServers);

        // Публикатор: по расписанию "должно быть отправлено к моменту t"; размер пачки каждой итерации фиксируется для оценки равномерности.
        var generator = Task.Run(async () =>
        {
            var pacing = Stopwatch.StartNew();
            while (Volatile.Read(ref sent) < total && !abort.IsCancellationRequested)
            {
                var due = Math.Min(total, (long)(pacing.Elapsed.TotalSeconds * rate) + 1);
                var first = Volatile.Read(ref sent);
                for (var seq = first; seq < due; seq++)
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

                if (due > first) iterationSizes.Add((int)(due - first));
                await DelayAsync(2, abort.Token);
            }

            producer.Flush(abort.IsCancellationRequested ? TimeSpan.FromSeconds(5) : TimeSpan.FromSeconds(60));
        });

        // Задержка "публикация -> строка в БД": курсорное чтение, только строки, записанные после предыдущего опроса
        // (received_at строго больше последнего виденного; пачка пишется одним коммитом, пачки - последовательно).
        // Момент видимости - начало опроса, погрешность до LatencyPollMs плюс время запроса.
        var latencySampler = Task.Run(async () =>
        {
            await using var connection = new NpgsqlConnection(database.AdminConnectionString);
            await connection.OpenAsync(abort.Token);
            var cursor = runStartedAt;
            var observed = 0L;
            await using var command = new NpgsqlCommand(
                "SELECT source_event_id, received_at FROM ktm.events WHERE occurred_at >= @lo AND received_at > @cursor AND source_event_id LIKE @p", connection);
            var lo = command.Parameters.Add("lo", NpgsqlTypes.NpgsqlDbType.Timestamp);
            var cur = command.Parameters.Add("cursor", NpgsqlTypes.NpgsqlDbType.Timestamp);
            command.Parameters.AddWithValue("p", run.Prefix + "-%");
            while (!abort.IsCancellationRequested)
            {
                var visibleAt = NowMicroTicks();
                var poll = Stopwatch.StartNew();
                lo.Value = DateTime.SpecifyKind(cursor.AddSeconds(-LatencyLookbackSeconds), DateTimeKind.Unspecified);
                cur.Value = DateTime.SpecifyKind(cursor, DateTimeKind.Unspecified);
                var rows = 0;
                await using (var reader = await command.ExecuteReaderAsync(abort.Token))
                {
                    while (await reader.ReadAsync(abort.Token))
                    {
                        var id = reader.GetString(0);
                        var seq = long.Parse(id.AsSpan(id.LastIndexOf('-') + 1), CultureInfo.InvariantCulture);
                        var receivedAt = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
                        if (receivedAt > cursor) cursor = receivedAt;
                        rows++;
                        if (!double.IsNaN(latencyMs[seq])) continue;
                        latencyMs[seq] = (visibleAt - published[seq]) / 10_000.0;
                        observed++;
                    }
                }

                pollStats.Add(rows, poll.Elapsed.TotalMilliseconds);
                if (stop.IsCancellationRequested && observed >= Volatile.Read(ref sent)) break;
                await DelayAsync(LatencyPollMs, abort.Token);
            }
        });

        // Счётчики, committed offsets и CPU процесса - с одной частотой (LedgerPeriodMs).
        var monitor = Task.Run(async () =>
        {
            var process = Process.GetCurrentProcess();
            var cpuPrev = process.TotalProcessorTime;
            var tPrev = clock.Elapsed;
            var nextCpu = TimeSpan.Zero;
            var tick = 0;
            while (!stop.IsCancellationRequested && !abort.IsCancellationRequested)
            {
                tick++;
                var delay = TimeSpan.FromMilliseconds(tick * (double)LedgerPeriodMs) - clock.Elapsed;
                if (delay > TimeSpan.Zero) await DelayAsync((int)delay.TotalMilliseconds, abort.Token);
                var t = clock.Elapsed;
                var written = app.Counters.Written;
                var sentNow = Volatile.Read(ref sent);
                var read = Stopwatch.StartNew();
                var committed = await committedReader.ReadAsync(run.Group, run.RawTopic);
                committedReadMs.Add(read.Elapsed.TotalMilliseconds);
                ledger.Add(new LedgerPoint(t.TotalSeconds, written, sentNow, committed));
                if (t >= nextCpu)
                {
                    nextCpu = t + TimeSpan.FromSeconds(5);
                    process.Refresh();
                    var cpu = process.TotalProcessorTime;
                    cpuSamples.Add((t.TotalSeconds, (cpu - cpuPrev).TotalSeconds / (t - tPrev).TotalSeconds * 100, process.WorkingSet64 >> 20));
                    cpuPrev = cpu;
                    tPrev = t;
                }
            }
        });

        // docker stats занимает секунды: в отдельной задаче, чтобы не растягивать период журнала счётчиков.
        var dockerStats = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested && !abort.IsCancellationRequested && containerIds.Count > 0)
            {
                resources.Add($"t={clock.Elapsed.TotalSeconds:F0}s " + await DockerStatsAsync(containerIds));
                await DelayAsync(30_000, abort.Token);
            }
        });

        Task[] background = [generator, latencySampler, monitor, dockerStats];
        var generationSeconds = 0.0;
        var drain = TimeSpan.Zero;
        var failed = false;
        try
        {
            await generator;
            generationSeconds = clock.Elapsed.TotalSeconds;
            var drainStart = clock.Elapsed;
            await Wait.UntilAsync(async () => (lastRows = await run.RowsAsync()) == total, $"all {total} rows in the journal", drainTimeout,
                () => $"rows={lastRows}");
            drain = clock.Elapsed - drainStart;
            await Wait.UntilAsync(async () => await run.CommittedAsync() == total, "all offsets committed", TimeSpan.FromMinutes(1));
            stop.Cancel();
            await Task.WhenAll(background).WaitAsync(TimeSpan.FromSeconds(60));
        }
        catch (Exception ex)
        {
            failed = true;
            report.Line($"RUN FAILED: {ex.GetType().Name}: {ex.Message}");
            throw;
        }
        finally
        {
            // Ничего не должно остаться работать после теста: ни при таймауте ожидания, ни при падении любого шага.
            stop.Cancel();
            abort.Cancel();
            var unfinished = await StopBackgroundAsync(background, TimeSpan.FromSeconds(30));
            report.Line($"Background tasks (publisher, latency poller, ledger, docker stats) still running after shutdown: {unfinished}");
            if (failed) report.Publish(output, "sustained-failed");
        }

        // Итоги.
        var rowsAfter = await database.CountEventsAsync();
        report.Line($"ktm.events count(*) after run: {rowsAfter} (seed {rowsBefore} + run {rowsAfter - rowsBefore}, expected run rows {total})");
        var warmup = Math.Min(60, seconds / 5.0);
        var from = ledger.Last(p => p.T <= warmup);
        var to = ledger.Last(p => p.T <= generationSeconds - 10 && p.T >= warmup);
        var steadyRate = (to.Written - from.Written) / (to.T - from.T);
        var sentTotal = Volatile.Read(ref sent);
        report.Line($"Generated {sentTotal} events in {generationSeconds:F1} s => {sentTotal / generationSeconds:F1} ev/s published; delivery errors: {deliveryErrors}");
        report.Line($"Drain after generation end: {drain.TotalSeconds:F1} s");
        report.Line($"Steady interval t={from.T:F0}..{to.T:F0} s: written {steadyRate:F1} ev/s (counter ktm.datasaver.events.written); pass threshold {steadyFactor:P0} of target ({(seconds < ShortRunSeconds ? "short run" : "long run")}, boundary {ShortRunSeconds} s)");
        report.Line("Per-minute written rate (ev/s): " + string.Join(", ", PerMinute(ledger, generationSeconds)));

        // Равномерность отправок: итерации публикатора и скорость по интервалам журнала (Task.Delay(2) на Windows даёт пачки по таймеру ~15 мс).
        var sizes = iterationSizes.Select(s => (double)s).Order().ToArray();
        var perSecond = new List<double>();
        for (var i = 1; i < ledger.Count && ledger[i].T <= generationSeconds; i++)
        {
            perSecond.Add((ledger[i].Sent - ledger[i - 1].Sent) / (ledger[i].T - ledger[i - 1].T));
        }

        var perSecondSorted = perSecond.Order().ToArray();
        report.Line($"Publisher uniformity: {iterationSizes.Count} send iterations, events per iteration p50={Pct(sizes, 0.5):F0} p99={Pct(sizes, 0.99):F0} max={sizes.LastOrDefault():F0}; published ev/s per ledger interval min={perSecondSorted.FirstOrDefault():F0} p5={Pct(perSecondSorted, 0.05):F0} p95={Pct(perSecondSorted, 0.95):F0} max={perSecondSorted.LastOrDefault():F0}");

        var warmIndex = (long)(warmup * rate);
        var steady = latencyMs.Skip((int)warmIndex).Take((int)Math.Max(0, total - warmIndex - 10L * rate)).Where(v => !double.IsNaN(v)).Order().ToArray();
        var all = latencyMs.Where(v => !double.IsNaN(v)).Order().ToArray();
        report.Line($"Latency publish -> row visible (ms; error up to {LatencyPollMs} ms + poll time, see poll stats), steady events n={steady.Length}: p50={Pct(steady, 0.50):F0} p95={Pct(steady, 0.95):F0} p99={Pct(steady, 0.99):F0} max={steady.LastOrDefault():F0}");
        report.Line($"Latency all events n={all.Length}: p50={Pct(all, 0.50):F0} p99={Pct(all, 0.99):F0} max={all.LastOrDefault():F0}; events not observed by poller: {total - all.Length}");
        report.Line($"Latency poller load (cursor read, new rows only): period {LatencyPollMs} ms + query, polls={pollStats.Polls}, mean rows per poll={pollStats.MeanRows:F1}, mean poll duration={pollStats.MeanMs:F1} ms, max={pollStats.MaxMs:F1} ms");

        var lag = ledger.Where(p => p.T >= warmup && p.T <= generationSeconds).ToList();
        var third = Math.Max(1, lag.Count / 3);
        double Mean(IEnumerable<LedgerPoint> points, Func<LedgerPoint, long> f) => points.Average(p => (double)f(p));
        report.Line($"Ledger period {LedgerPeriodMs} ms (counters and committed offsets with the same period), samples={ledger.Count}; committed offsets read: mean {committedReadMs.Average():F0} ms, max {committedReadMs.Max():F0} ms per read (committed value error up to this plus {LedgerPeriodMs} ms)");
        report.Line($"Lag by committed offsets (published - committed, messages), steady: first third mean={Mean(lag.Take(third), p => p.Sent - p.Committed):F0}, last third mean={Mean(lag.Skip(lag.Count - third), p => p.Sent - p.Committed):F0}, max={lag.Max(p => p.Sent - p.Committed)}");
        report.Line($"Lag by writes (published - written, messages), steady: first third mean={Mean(lag.Take(third), p => p.Sent - p.Written):F0}, last third mean={Mean(lag.Skip(lag.Count - third), p => p.Sent - p.Written):F0}, max={lag.Max(p => p.Sent - p.Written)}");
        report.Line("Lag every ~60 s (published-committed / published-written): " +
                    string.Join(", ", lag.Where((_, i) => i % 60 == 0).Select(p => $"{p.T:F0}s:{p.Sent - p.Committed}/{p.Sent - p.Written}")));

        var duration = batches.Durations.Order().ToArray();
        var batchSizes = batches.Sizes;
        if (duration.Length > 0)
        {
            report.Line($"Batches: {duration.Length}, size mean={batchSizes.Average():F0} p50={Pct(batchSizes.Select(s => (double)s).ToArray(), 0.5):F0} max={batchSizes.Last()}; write duration ms: p50={Pct(duration, 0.5) * 1000:F0} p99={Pct(duration, 0.99) * 1000:F0} max={duration.Last() * 1000:F0}");
        }

        if (cpuSamples.Count > 0)
        {
            report.Line($"Test process (Data Saver + generator + 3 clients, in-process): CPU mean={cpuSamples.Average(s => s.CpuPct):F0}% of one core (max {cpuSamples.Max(s => s.CpuPct):F0}%), working set mean={cpuSamples.Average(s => s.WorkingSetMb):F0} MB, max={cpuSamples.Max(s => s.WorkingSetMb)} MB; GC heap now={GC.GetTotalMemory(false) >> 20} MB, gen2 collections={GC.CollectionCount(2)}");
        }

        foreach (var line in resources) report.Line("docker stats: " + line);
        report.Line("DB: " + await DbSizeAsync(database));
        report.WarningRecords(app.Logs.GetSnapshot());
        report.Line($"Loop terminations: {app.Terminator.Calls}");
        report.Publish(output, "sustained");

        Assert.Equal(0, deliveryErrors);
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
        Assert.True(sentTotal / generationSeconds >= rate * SteadyRateFactorLong, "generator did not reach the target rate");
        Assert.True(steadyRate >= rate * steadyFactor, $"written {steadyRate:F1} ev/s < {steadyFactor:P0} of {rate} ({(seconds < ShortRunSeconds ? "short" : "long")} run)");
        Assert.True(Mean(lag.Skip(lag.Count - third), p => p.Sent - p.Committed) <= Mean(lag.Take(third), p => p.Sent - p.Committed) + 2 * batchMaxSize, "committed lag grows");
        Assert.True(drain < TimeSpan.FromSeconds(30), $"backlog drained for {drain}");
        Assert.Equal(0, total - all.Length);
        Assert.True(Pct(steady, 0.99) < TimeSpan.FromMinutes(5).TotalMilliseconds, "p99 above the 5 minute NFR");
        Assert.Equal(0, app.Terminator.Calls);
    }

    [PerfFact("KTM_PERF_CAPACITY")]
    public async Task Backlog_drain_gives_maximum_single_instance_rate_and_insert_plan_on_filled_table()
    {
        var n = EnvInt("KTM_PERF_CAPACITY", 300_000);
        var seedRows = EnvNonNegative("KTM_PERF_SEED", 300_000);
        var report = new Report();
        report.Line($"== Max single-instance rate: backlog of {n} events, {Partitions} partitions, instance starts after publishing ==");
        var containerIds = await ContainerIdsAsync();
        await report.EnvironmentAsync(containerIds);
        report.Line($"Bootstrap {kafka.BootstrapServers}; measured containers: {string.Join(',', containerIds)}");

        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        report.Line($"Dedicated database created for this run; ktm.events count(*) before seed: {await database.CountEventsAsync()}");
        await database.SeedAsync(seedRows);
        var rowsBefore = await database.CountEventsAsync();
        report.Line($"ktm.events count(*) after seed of {seedRows}: {rowsBefore}");

        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database, Partitions);
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

        using var batches = new BatchListener();
        await using var app = DataSaverInstance.Start(run, Tune);
        report.Config(app.Services);
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
        report.Line($"Drained {n} in {total:F1} s from instance start (includes host start and group join; counter sampling 250 ms); first write at {firstWrite:F1} s");
        report.Line($"Overall: {n / total:F0} ev/s; from first write: {n / (total - firstWrite):F0} ev/s; 10%..90% interval: {(p90.Written - p10.Written) / (p90.T - p10.T):F0} ev/s");
        var duration = batches.Durations.Order().ToArray();
        var sizes = batches.Sizes.ToArray();
        report.Line($"Batches: {duration.Length}, mean size {sizes.Average():F0}; write duration ms p50={Pct(duration, 0.5) * 1000:F0} p99={Pct(duration, 0.99) * 1000:F0} max={duration.Last() * 1000:F0}");
        await Wait.UntilAsync(async () => await run.RowsAsync() == n, "all rows", TimeSpan.FromMinutes(1));
        var rowsAfter = await database.CountEventsAsync();
        report.Line($"ktm.events count(*) after drain: {rowsAfter} (seed {rowsBefore} + backlog {rowsAfter - rowsBefore}, expected {n})");
        report.Line("DB: " + await DbSizeAsync(database));
        report.WarningRecords(app.Logs.GetSnapshot());

        report.Line(string.Empty);
        report.Line("== EXPLAIN (ANALYZE, BUFFERS) of a 500-row insert into the filled hypertable under role ktm_app (rolled back). NOT the text of NpgsqlEventStore.InsertEventsSql: rows come from generate_series instead of unnest of parameters, so the figure is a lower bound ==");
        var executionTimes = new List<double>();
        await using (var connection = new NpgsqlConnection(database.AppConnectionString))
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
                var text = plan.ToString();
                report.SaveRaw($"explain-{i + 1}.txt", text);
                var execLine = text.Split('\n').Last(l => l.StartsWith("Execution Time", StringComparison.Ordinal));
                executionTimes.Add(double.Parse(execLine.Split(':')[1].Replace("ms", string.Empty).Trim(), CultureInfo.InvariantCulture));
            }
        }

        report.Line("Execution Time ms, 7 runs (first is cold): " + string.Join(", ", executionTimes.Select(t => t.ToString("F1", CultureInfo.InvariantCulture))));
        report.Line("Full plans of each run: explain-1.txt .. explain-7.txt next to this report");
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
                     (now() AT TIME ZONE 'UTC') - g * interval '2 milliseconds' AS occurred_at,
                     now() AT TIME ZONE 'UTC' AS received_at,
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

    /// <summary>Ждёт фоновые задачи не дольше <paramref name="timeout"/>; ошибки задач уже зафиксированы основным путём. Возвращает число не завершившихся.</summary>
    private static async Task<int> StopBackgroundAsync(Task[] tasks, TimeSpan timeout)
    {
        try
        {
            await Task.WhenAll(tasks).WaitAsync(timeout);
        }
        catch (TimeoutException)
        {
            // Задачи не успели завершиться: число оставшихся возвращается ниже и попадает в отчёт.
        }
        catch (Exception)
        {
            // Ошибки самих задач - в Task.Exception; здесь важно только, что все завершены (проверяется ниже).
        }

        return tasks.Count(t => !t.IsCompleted);
    }

    private static IEnumerable<string> PerMinute(List<LedgerPoint> ledger, double generationSeconds)
    {
        for (var start = 0.0; start + 60 <= generationSeconds + 1; start += 60)
        {
            var a = ledger.LastOrDefault(p => p.T <= start);
            var b = ledger.LastOrDefault(p => p.T <= start + 60);
            if (b.T > a.T) yield return ((b.Written - a.Written) / (b.T - a.T)).ToString("F0", CultureInfo.InvariantCulture);
        }
    }

    private static async Task<string> DbSizeAsync(PerfDatabase database)
    {
        await using var connection = new NpgsqlConnection(database.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT (SELECT count(*) FROM ktm.events) || ' rows in ktm.events, hypertable total size ' || pg_size_pretty(hypertable_size('ktm.events')) || ', chunks ' || (SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'events')",
            connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    /// <summary>Контейнеры этого прогона (Kafka и TimescaleDB) по опубликованным портам: на машине могут работать чужие контейнеры.</summary>
    private async Task<List<string>> ContainerIdsAsync()
    {
        var kafkaPort = new Uri(kafka.BootstrapServers).Port.ToString(CultureInfo.InvariantCulture);
        var dbPort = new NpgsqlConnectionStringBuilder(postgres.AdminConnectionString).Port.ToString(CultureInfo.InvariantCulture);
        // publish= у docker ps фильтрует по порту контейнера, поэтому сопоставляем опубликованный порт хоста по колонке Ports.
        var ps = await RunAsync("docker", "ps --format \"{{.ID}}|{{.Ports}}\"");
        var ids = ps.Split((char)10, StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Trim().Split('|', 2))
            .Where(p => p.Length == 2 && (p[1].Contains($":{kafkaPort}->", StringComparison.Ordinal) || p[1].Contains($":{dbPort}->", StringComparison.Ordinal)))
            .Select(p => p[0])
            .ToList();

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

    private readonly record struct LedgerPoint(double T, long Written, long Sent, long Committed);

    private sealed class PollStats
    {
        private long _polls;
        private long _rows;
        private double _totalMs;
        private double _maxMs;

        public long Polls => Volatile.Read(ref _polls);

        public double MeanRows => _polls == 0 ? 0 : (double)_rows / _polls;

        public double MeanMs => _polls == 0 ? 0 : _totalMs / _polls;

        public double MaxMs => _maxMs;

        // Пишет одна задача (опросчик), читает основной поток после её завершения.
        public void Add(int rows, double ms)
        {
            _rows += rows;
            _totalMs += ms;
            _maxMs = Math.Max(_maxMs, ms);
            Interlocked.Increment(ref _polls);
        }
    }

    /// <summary>Committed offsets группы через долгоживущий админ-клиент: частое чтение (1 с) не должно платить за подключение каждый раз.</summary>
    private sealed class CommittedReader(string bootstrapServers) : IDisposable
    {
        private readonly IAdminClient _admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        private long _last;

        public async Task<long> ReadAsync(string group, string topic)
        {
            try
            {
                var result = (await _admin.ListConsumerGroupOffsetsAsync([new ConsumerGroupTopicPartitions(group, null)])).Single();
                _last = result.Partitions.Where(p => p.Topic == topic && p.Offset.Value >= 0).Sum(p => p.Offset.Value);
            }
            catch (KafkaException)
            {
                // Группа ещё не зафиксировала смещения (или временный сбой чтения): остаётся прошлое значение.
            }

            return _last;
        }

        public void Dispose() => _admin.Dispose();
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
        private readonly string _directory = OutputDirectory();

        public void Line(string line) => _text.AppendLine(line);

        public async Task EnvironmentAsync(List<string> containers)
        {
            Line($"Started {DateTimeOffset.UtcNow:O}; OS {System.Runtime.InteropServices.RuntimeInformation.OSDescription}; .NET {Environment.Version}; logical CPUs {Environment.ProcessorCount}");
            Line("Git: " + (await RunAsync("git", "rev-parse --short HEAD")).Trim() + (string.IsNullOrWhiteSpace(await RunAsync("git", "status --porcelain")) ? string.Empty : " (working tree has uncommitted changes)"));
            Line("Images: " + string.Join(", ", (await RunAsync("docker", "ps --format \"{{.Image}}\" " + string.Join(' ', containers.Select(c => "--filter id=" + c)))).Split('\n', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim())));
            Line("Docker: " + (await RunAsync("docker", "info --format \"{{.ServerVersion}}, {{.NCPU}} CPU, {{.MemTotal}} bytes\"")).Trim());
            Line("Other processes: parallel tasks on the host are NOT controlled by the test; record them in the protocol");
        }

        /// <summary>Фактическая конфигурация экземпляра (то, что видит сервис после слияния настроек и умолчаний), а не копия из кода.</summary>
        public void Config(IServiceProvider services)
        {
            var batch = services.GetRequiredService<IOptions<BatchOptions>>().Value;
            var kafkaOptions = services.GetRequiredService<IOptions<KafkaOptions>>().Value;
            var postgresOptions = services.GetRequiredService<IOptions<PostgresOptions>>().Value;
            var shutdown = services.GetRequiredService<IOptions<ShutdownOptions>>().Value;
            var root = services.GetRequiredService<IOptions<DataSaverOptions>>().Value;
            Line($"Effective config: DataSaver__Batch__MaxSize={batch.MaxSize}, DataSaver__Batch__MaxWaitMs={batch.MaxWaitMs}, DataSaver__PollIntervalMs={root.PollIntervalMs}, " +
                 $"DataSaver__Kafka__SessionTimeoutMs={kafkaOptions.SessionTimeoutMs}, DataSaver__Postgres__SslMode={postgresOptions.SslMode}, " +
                 $"DataSaver__Shutdown__TimeoutSeconds={shutdown.TimeoutSeconds}, Postgres pool max={postgresOptions.MaxPoolSize}");
        }

        /// <summary>Записи лога уровня Warning и выше: уровень, категория, сообщение и тип исключения (без стека и без значений конфигурации).</summary>
        public void WarningRecords(IReadOnlyList<Microsoft.Extensions.Logging.Testing.FakeLogRecord> records)
        {
            var warnings = records.Where(r => r.Level >= LogLevel.Warning).ToList();
            Line($"Log records >= Warning: {warnings.Count}");
            foreach (var r in warnings)
            {
                Line($"  [{r.Level}] {r.Category}: {r.Message}" + (r.Exception is null ? string.Empty : $" (exception {r.Exception.GetType().Name}: {r.Exception.Message})"));
            }
        }

        public void SaveRaw(string fileName, string text)
        {
            Directory.CreateDirectory(_directory);
            File.WriteAllText(Path.Combine(_directory, fileName), text);
        }

        public void Publish(ITestOutputHelper output, string name)
        {
            var text = _text.ToString();
            output.WriteLine(text);
            Console.WriteLine(text);
            SaveRaw($"perf-{name}.txt", text);
            Console.WriteLine($"Raw output saved to {_directory}");
        }

        // docs/test-protocols/data/<дата>-<метка прогона>/: сырые выгрузки, на которые ссылается протокол.
        private static string OutputDirectory()
        {
            if (Environment.GetEnvironmentVariable("KTM_PERF_OUT") is { Length: > 0 } explicitDirectory) return explicitDirectory;
            var label = Environment.GetEnvironmentVariable("KTM_PERF_RUN") is { Length: > 0 } l ? l : "run";
            var folder = $"{DateTime.UtcNow:yyyy-MM-dd}-{label}";
            for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            {
                if (Directory.Exists(Path.Combine(dir.FullName, "docs", "test-protocols"))) return Path.Combine(dir.FullName, "docs", "test-protocols", "data", folder);
            }

            return Path.Combine(AppContext.BaseDirectory, "perf-data", folder);
        }
    }
}

using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Rules;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Npgsql;
using Xunit.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>
/// Задача 6.5 (ADR-0005 п. 2.13): перф-проверка движка правил, категория Perf, без <c>KTM_PERF_RULES_*</c> пропускается.
/// 50 включённых правил (сравнения, диапазоны, вложенные and/or/!), которые на обычном потоке не срабатывают, поток по умолчанию
/// 500 соб/с. (а) обычный поток: задержка «публикация -> строка в журнале»; (б) 100 % отбраковка: сервис держит поток по
/// публикации в rejected, лаг не растёт; (в) стоимость <c>Evaluate</c> на сообщение (в процессе, без Kafka и БД).
/// Параметры: <c>KTM_PERF_RULES_RATE</c> (500), <c>KTM_PERF_RULES_SECONDS</c> (120), <c>KTM_PERF_RUN</c>, <c>KTM_PERF_OUT</c>.
/// Протокол: docs/test-protocols/data-saver-rules-engine-throughput.md.
/// </summary>
[Collection(AppCollection.Name)]
[Trait("Category", "Perf")]
public sealed class RulesEnginePerfTests(KafkaFixture kafka, PostgresFixture postgres, ITestOutputHelper output)
{
    private const int Partitions = 9;
    private const int Persons = 1000;
    private const int RuleCount = 50;
    private const string AllRulesEnv = "KTM_PERF_RULES_RATE";
    private const string SecondsEnv = "KTM_PERF_RULES_SECONDS";

    private static int EnvInt(string name, int fallback) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), CultureInfo.InvariantCulture, out var v) && v > 0 ? v : fallback;

    /// <summary>
    /// 50 правил на три источника и общие. Условия подобраны так, чтобы на сообщениях <see cref="MessageBody"/> не срабатывать
    /// (вычисляются полностью, но дают ложь): значения вне диапазонов, неравенство типа, вложенные and/or/!.
    /// </summary>
    internal static IReadOnlyList<(string Id, string Source, string Condition)> BuildRules()
    {
        var rules = new List<(string, string, string)>();
        for (var i = 0; i < RuleCount; i++)
        {
            var source = (i % 4) switch { 0 => "skud", 1 => "cv", 2 => "1c-do", _ => "*" };
            var condition = (i % 5) switch
            {
                0 => $$"""{"==": [{"var": "eventType"}, "never_{{i}}"]}""",
                1 => $$"""{"<=": [{{900 + i}}, {"var": "payload.confidence"}, {{1000 + i}}]}""",
                2 => $$"""{"and": [{">": [{"var": "payload.confidence"}, {{50 + i}}]}, {"!=": [{"var": "eventType"}, "x"]}]}""",
                3 => $$$"""{"or": [{"==": [{"var": "payload.zone"}, "never-zone-{{{i}}}"]}, {"!": {"!=": [{"var": "eventType"}, "never_{{{i}}}"]}}]}""",
                _ => $$$"""{"and": [{"or": [{"==": [{"var": "eventType"}, "e{{{i}}}"]}, {">=": [{"var": "occurredAt"}, "2099-01-01T00:00:00Z"]}]}, {"!": {"==": [{"var": "payload.direction"}, "in"]}}]}""",
            };
            rules.Add(($"perf-rule-{i:D2}", source, condition));
        }

        return rules;
    }

    private static string MessageBody(string prefix, long seq, DateTimeOffset occurredAt)
    {
        var source = RuleMessages.Sources[seq % RuleMessages.Sources.Length];
        var at = occurredAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture);
        return RuleMessages.Body(source, $"{prefix}-{seq.ToString(CultureInfo.InvariantCulture)}", at, RuleMessages.DefaultType(source),
            RuleMessages.DefaultPayload(source, seq), person: $"emp-{seq % Persons}");
    }

    /// <summary>Узлов <c>regex</c> в наборе: предел по умолчанию <c>MaxRegexNodes</c> (64), набор занимает его целиком.</summary>
    private const int RegexNodeCount = 64;

    /// <summary>
    /// 50 правил с <see cref="RegexNodeCount"/> узлами <c>regex</c> (14 правил по два узла в <c>or</c>, 36 по одному): якорные и неякорные
    /// шаблоны, <c>(?i)</c>, поля <paramref name="fields"/> по кругу. На сообщениях <see cref="MessageBody"/> и на полях из нулей не срабатывают.
    /// </summary>
    internal static IReadOnlyList<(string Id, string Source, string Condition)> BuildRegexRules(params string[] fields)
    {
        string[] patterns =
        [
            "^TEST-\\d+\\z", "PROD-\\d{3}", "(?i)^cam-(\\d+)-test$", "(?i)(admin|root)-\\d{3}", "^restricted-[a-z]+\\z", "(?i)^file://", "^X-\\d+\\z", "[0-9]{12}-unknown",
        ];
        var rules = new List<(string, string, string)>();
        var node = 0;
        string Leaf()
        {
            var n = node++;
            return $$"""{"regex": [{"var": "{{fields[n % fields.Length]}}"}, {{JsonSerializer.Serialize(patterns[n % patterns.Length])}}]}""";
        }

        for (var i = 0; i < RuleCount; i++)
        {
            var source = (i % 4) switch { 0 => "skud", 1 => "cv", 2 => "1c-do", _ => "*" };
            var condition = i < 14 ? $$"""{"or": [{{Leaf()}}, {{Leaf()}}]}""" : Leaf();
            rules.Add(($"perf-regex-{i:D2}", source, condition));
        }

        Assert.Equal(RegexNodeCount, node);
        return rules;
    }

    private static async Task InsertRulesAsync(IDatabaseEndpoint database, bool rejectAll, bool regex = false)
    {
        var set = regex ? BuildRegexRules("deviceId", "externalPersonId", "payload.zone") : BuildRules();
        foreach (var (id, source, condition) in set)
        {
            await RuleTable.InsertAsync(database, id, $"Перф-правило {id}", source, condition, "perf_noise");
        }

        if (rejectAll)
        {
            // Первое по приоритету общее правило, срабатывающее на каждом сообщении: поток целиком идёт в rejected.
            await RuleTable.InsertAsync(database, "perf-reject-all", "Перф: отбраковать всё", "*",
                """{"!=": [{"var": "eventType"}, "__never__"]}""", "perf_all", priority: 1);
        }
    }

    [PerfFact(AllRulesEnv, SecondsEnv)]
    public Task Normal_stream_with_50_rules_reports_latency_and_no_lag_growth() => RunLoadAsync(rejectAll: false);

    [PerfFact(AllRulesEnv, SecondsEnv)]
    public Task Full_rejection_with_50_rules_keeps_rate_on_rejected_publication() => RunLoadAsync(rejectAll: true);

    /// <summary>Задача 9.8 (а): типичные сообщения (поля до 256 символов) при 64 узлах regex: задержка «Kafka -> журнал» против протокола 6.5.</summary>
    [PerfFact(AllRulesEnv, SecondsEnv)]
    public Task Regex_normal_stream_with_64_regex_nodes_reports_latency_and_no_lag_growth() => RunLoadAsync(rejectAll: false, regex: true);

    /// <summary>
    /// Задача 9.8 (б): предельный ввод - поле-строка 64 КиБ в каждом сообщении, 64 узла regex (в 50 правилах) на движке по умолчанию
    /// (NonBacktracking, 200 мс). В процессе, без Kafka и БД: стоимость <c>Evaluate</c> на сообщение и на узел, аллокации, число прерываний по
    /// пределу совпадения (в сервисе каждое из них - <c>rules.eval_errors{kind=regex_timeout}</c>; ожидается 0, ненулевое - в протокол и R-13).
    /// </summary>
    [PerfFact(AllRulesEnv, SecondsEnv)]
    public void Regex_evaluate_cost_on_64KiB_field_is_measured_with_timeouts_counted()
    {
        var defs = BuildRegexRules("payload.s")
            .Select(r => new RuleDefinition(r.Id, r.Id, null, r.Source == "*" ? null : r.Source, true, 100, 1, "perf_noise", JsonDocument.Parse(r.Condition).RootElement.Clone()))
            .ToList();
        var snapshot = new RuleSnapshotBuilder(new ConditionCompiler(), 500).Build("perf", defs, []);
        Assert.Equal(RuleCount, snapshot.Rules.Count);
        Assert.Empty(snapshot.Errors);
        Assert.Equal(RegexNodeCount, snapshot.Rules.Sum(r => r.RegexNodeCount));

        // 64 КиБ без совпадений и без перевода строки: худший случай для неякорных шаблонов - полный просмотр поля.
        var field = string.Concat(Enumerable.Repeat("abcdefghij0123456789-_ ", 3000))[..65536];
        var bodies = RuleMessages.Sources.Select(source => JsonDocument.Parse(
            RuleMessages.Body(source, "ev-1", "2026-11-19T04:58:12Z", RuleMessages.DefaultType(source), $"{{\"s\":\"{field}\"}}")).RootElement.Clone()).ToArray();
        var timeouts = 0;
        var matched = 0;

        void EvaluateAll()
        {
            for (var i = 0; i < bodies.Length; i++)
            {
                foreach (var rule in snapshot.RulesFor(RuleMessages.Sources[i]))
                {
                    try
                    {
                        matched += rule.Evaluate(bodies[i]) ? 1 : 0;
                    }
                    catch (RuleEvaluationAbortedException)
                    {
                        timeouts++;
                    }
                }
            }
        }

        for (var i = 0; i < 5; i++) EvaluateAll(); // прогрев
        timeouts = 0;
        const int Rounds = 50;
        var before = GC.GetAllocatedBytesForCurrentThread();
        var clock = Stopwatch.StartNew();
        for (var round = 0; round < Rounds; round++) EvaluateAll();
        clock.Stop();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        var messages = Rounds * bodies.Length;
        var perMessageUs = clock.Elapsed.TotalMilliseconds * 1000 / messages;
        var nodesPerMessage = snapshot.RulesFor("skud").ToArray().Sum(r => r.RegexNodeCount);
        var text = $"Regex Evaluate cost on a 64 KiB field: {messages} messages (round-robin sources), {nodesPerMessage} regex nodes applicable to skud; " +
                   $"mean {perMessageUs:F1} us per message ({perMessageUs / nodesPerMessage:F2} us per regex node); " +
                   $"allocated {allocated / messages} B per message; timeouts (rules.eval_errors regex_timeout) = {timeouts}; matched={matched}; " +
                   $"{DateTimeOffset.UtcNow:O}, .NET {Environment.Version}, {Environment.ProcessorCount} logical CPUs";
        output.WriteLine(text);
        SaveRaw("perf-rules-regex-evaluate-64kib.txt", text);
        Assert.Equal(0, matched);
    }

    [PerfFact(AllRulesEnv, SecondsEnv)]
    public void Evaluate_cost_per_message_with_50_rules_is_measured()
    {
        var defs = BuildRules()
            .Select(r => new RuleDefinition(r.Id, r.Id, null, r.Source == "*" ? null : r.Source, true, 100, 1, "perf_noise", JsonDocument.Parse(r.Condition).RootElement.Clone()))
            .ToList();
        var snapshot = new RuleSnapshotBuilder(new ConditionCompiler(), 500).Build("perf", defs, []);
        Assert.Equal(RuleCount, snapshot.Rules.Count);
        Assert.Empty(snapshot.Errors);

        var bodies = Enumerable.Range(0, 3000)
            .Select(i => JsonDocument.Parse(MessageBody("ev", i, DateTimeOffset.UtcNow)).RootElement.Clone()).ToArray();
        var sources = bodies.Select(b => b.GetProperty("source").GetString()!).ToArray();
        var matched = 0;
        for (var i = 0; i < bodies.Length; i++) // прогрев JIT
        {
            foreach (var rule in snapshot.RulesFor(sources[i])) matched += rule.Evaluate(bodies[i]) ? 1 : 0;
        }

        const int Rounds = 20;
        var clock = Stopwatch.StartNew();
        for (var round = 0; round < Rounds; round++)
        {
            for (var i = 0; i < bodies.Length; i++)
            {
                foreach (var rule in snapshot.RulesFor(sources[i])) matched += rule.Evaluate(bodies[i]) ? 1 : 0;
            }
        }

        clock.Stop();
        var messages = Rounds * bodies.Length;
        var perMessageUs = clock.Elapsed.TotalMilliseconds * 1000 / messages;
        var applied = snapshot.RulesFor("skud").Length;
        var text = $"Evaluate cost: {messages} messages x rules of own source ({applied} for skud, mixed sources), no match expected; " +
                   $"mean {perMessageUs:F2} us per message ({perMessageUs / applied:F3} us per rule); matched={matched}; " +
                   $"{DateTimeOffset.UtcNow:O}, .NET {Environment.Version}, {Environment.ProcessorCount} logical CPUs";
        output.WriteLine(text);
        SaveRaw("perf-rules-evaluate.txt", text);
        Assert.Equal(0, matched);
    }

    private async Task RunLoadAsync(bool rejectAll, bool regex = false)
    {
        var rate = EnvInt(AllRulesEnv, 500);
        var seconds = EnvInt(SecondsEnv, 120);
        var total = (long)rate * seconds;
        var name = (regex ? "rules-regex-" : "rules-") + (rejectAll ? "reject-all" : "normal");
        var report = new StringBuilder();
        report.AppendLine($"== {name}: {rate} ev/s x {seconds} s = {total} events, {RuleCount} rules{(regex ? $" ({RegexNodeCount} regex nodes)" : string.Empty)}{(rejectAll ? " + 1 match-all rule" : string.Empty)}, {Partitions} partitions ==");

        await using var database = await PerfDatabase.CreateAsync(postgres.AdminConnectionString);
        await InsertRulesAsync(database, rejectAll, regex);
        var run = await TestRun.CreateAsync(kafka.BootstrapServers, database, Partitions);
        await using var app = DataSaverInstance.Start(run, s =>
        {
            // Умолчания продукта (как в замере data-saver-throughput.md), опрос правил - по умолчанию.
            foreach (var key in new[] { "DataSaver:Batch:MaxWaitMs", "DataSaver:Backoff:InitialMs", "DataSaver:Backoff:MaxMs", "DataSaver:PollIntervalMs" }) s.Remove(key);
        });
        await app.WaitReadyAsync();
        await Wait.UntilAsync(() => app.Counters.Gauge("ktm.datasaver.rules.active") >= RuleCount, "all perf rules loaded", TimeSpan.FromSeconds(90));
        Assert.Equal(0, app.Counters.Gauge("ktm.datasaver.rules.invalid"));

        var published = new long[total];
        var latencyMs = new double[total];
        Array.Fill(latencyMs, double.NaN);
        var sent = 0L;
        var deliveryErrors = 0L;
        var ledger = new List<(double T, long Sent, long Committed, long Written, long Rejected)>();
        using var abort = new CancellationTokenSource();
        using var producer = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = kafka.BootstrapServers, LingerMs = 5, Acks = Acks.All }).Build();
        var clock = Stopwatch.StartNew();
        var startedAt = DateTime.UtcNow;

        var generator = Task.Run(async () =>
        {
            var pacing = Stopwatch.StartNew();
            while (Volatile.Read(ref sent) < total && !abort.IsCancellationRequested)
            {
                var due = Math.Min(total, (long)(pacing.Elapsed.TotalSeconds * rate) + 1);
                for (var seq = Volatile.Read(ref sent); seq < due; seq++)
                {
                    var ticks = DateTime.UtcNow.Ticks / 10 * 10;
                    published[seq] = ticks;
                    var source = RuleMessages.Sources[seq % RuleMessages.Sources.Length];
                    producer.Produce(run.RawTopic,
                        new Message<string, string> { Key = $"{source}:emp-{seq % Persons}", Value = MessageBody(run.Prefix, seq, new DateTimeOffset(ticks, TimeSpan.Zero)) },
                        r => { if (r.Error.IsError) Interlocked.Increment(ref deliveryErrors); });
                    Volatile.Write(ref sent, seq + 1);
                }

                try { await Task.Delay(2, abort.Token); } catch (OperationCanceledException) { }
            }

            producer.Flush(TimeSpan.FromSeconds(60));
        });

        // Задержка публикация -> строка: курсорный опрос по received_at (как в ThroughputPerfTests, погрешность до периода опроса).
        var sampler = Task.Run(async () =>
        {
            if (rejectAll) return; // в журнал ничего не пишется
            await using var connection = new NpgsqlConnection(database.AdminConnectionString);
            await connection.OpenAsync(abort.Token);
            var cursor = startedAt;
            await using var command = new NpgsqlCommand("SELECT source_event_id, received_at FROM ktm.events WHERE occurred_at >= @lo AND received_at > @cursor AND source_event_id LIKE @p", connection);
            var lo = command.Parameters.Add("lo", NpgsqlTypes.NpgsqlDbType.TimestampTz);
            var cur = command.Parameters.Add("cursor", NpgsqlTypes.NpgsqlDbType.TimestampTz);
            command.Parameters.AddWithValue("p", run.Prefix + "-%");
            while (!abort.IsCancellationRequested)
            {
                var visibleAt = DateTime.UtcNow.Ticks / 10 * 10;
                lo.Value = cursor.AddSeconds(-15);
                cur.Value = cursor;
                await using (var reader = await command.ExecuteReaderAsync(abort.Token))
                {
                    while (await reader.ReadAsync(abort.Token))
                    {
                        var id = reader.GetString(0);
                        var seq = long.Parse(id.AsSpan(id.LastIndexOf('-') + 1), CultureInfo.InvariantCulture);
                        var receivedAt = reader.GetDateTime(1);
                        if (receivedAt > cursor) cursor = receivedAt;
                        if (double.IsNaN(latencyMs[seq])) latencyMs[seq] = (visibleAt - published[seq]) / 10_000.0;
                    }
                }

                try { await Task.Delay(250, abort.Token); } catch (OperationCanceledException) { }
            }
        });

        try
        {
            while (!generator.IsCompleted)
            {
                var committed = await run.CommittedAsync();
                ledger.Add((clock.Elapsed.TotalSeconds, Volatile.Read(ref sent), committed, app.Counters.Written, app.Counters.Sum("ktm.datasaver.rules.rejected")));
                await Task.Delay(1000);
            }

            await generator;
            var generationSeconds = clock.Elapsed.TotalSeconds;
            await Wait.UntilAsync(async () => await run.CommittedAsync() == total, "all offsets committed", TimeSpan.FromMinutes(5));
            await Task.Delay(1000);
            abort.Cancel();
            await sampler;

            var warmup = Math.Min(30, seconds / 5.0);
            var steady = ledger.Where(p => p.T >= warmup && p.T <= generationSeconds - 5).ToList();
            var rateBy = (Func<(double T, long Sent, long Committed, long Written, long Rejected), long> f) => (f(steady[^1]) - f(steady[0])) / (steady[^1].T - steady[0].T);
            var third = Math.Max(1, steady.Count / 3);
            var lagFirst = steady.Take(third).Average(p => (double)(p.Sent - p.Committed));
            var lagLast = steady.Skip(steady.Count - third).Average(p => (double)(p.Sent - p.Committed));
            report.AppendLine($"Generated {Volatile.Read(ref sent)} in {generationSeconds:F1} s ({Volatile.Read(ref sent) / generationSeconds:F1} ev/s); delivery errors {deliveryErrors}");
            report.AppendLine($"Steady t={steady[0].T:F0}..{steady[^1].T:F0} s: written {rateBy(p => p.Written):F1} ev/s, rules.rejected {rateBy(p => p.Rejected):F1} ev/s, committed {rateBy(p => p.Committed):F1} ev/s");
            report.AppendLine($"Lag by committed offsets: first third mean {lagFirst:F0}, last third mean {lagLast:F0}, max {steady.Max(p => p.Sent - p.Committed)}");
            report.AppendLine($"Rows in journal: {await database.CountEventsAsync()}; rejected topic records published (counter): {app.Counters.Sum("ktm.datasaver.rules.rejected")}");
            if (!rejectAll)
            {
                var lat = latencyMs.Where(v => !double.IsNaN(v)).Order().ToArray();
                report.AppendLine($"Latency publish -> row visible (ms), n={lat.Length}: p50={Pct(lat, 0.50):F0} p95={Pct(lat, 0.95):F0} p99={Pct(lat, 0.99):F0} max={lat.Last():F0}; not observed: {total - lat.Length}");
                Assert.Equal(total, await database.CountEventsAsync());
                Assert.Equal(0, total - lat.Length);
            }
            else
            {
                Assert.Equal(0L, await database.CountEventsAsync());
                Assert.Equal(total, app.Counters.Sum("ktm.datasaver.rules.rejected"));
            }

            report.AppendLine($"Loop terminations: {app.Terminator.Calls}; started {startedAt:O}, .NET {Environment.Version}, {Environment.ProcessorCount} logical CPUs");
            output.WriteLine(report.ToString());
            SaveRaw($"perf-{name}.txt", report.ToString());
            Assert.Equal(0, deliveryErrors);
            Assert.True(rateBy(p => rejectAll ? p.Rejected : p.Written) >= rate * 0.95, "steady rate below 95% of target");
            Assert.True(lagLast <= lagFirst + 1000, "committed lag grows");
            Assert.Equal(0, app.Terminator.Calls);
        }
        finally
        {
            abort.Cancel();
            await Task.WhenAll(generator, sampler).WaitAsync(TimeSpan.FromSeconds(30));
        }
    }

    private static double Pct(double[] sorted, double p) => sorted.Length == 0 ? double.NaN : sorted[Math.Min(sorted.Length - 1, (int)(p * sorted.Length))];

    private static void SaveRaw(string fileName, string text)
    {
        var label = Environment.GetEnvironmentVariable("KTM_PERF_RUN") is { Length: > 0 } l ? l : "rules";
        var directory = Environment.GetEnvironmentVariable("KTM_PERF_OUT") is { Length: > 0 } explicitDirectory
            ? explicitDirectory
            : Path.Combine(RepositoryRoot(), "docs", "test-protocols", "data", $"{DateTime.UtcNow:yyyy-MM-dd}-{label}");
        Directory.CreateDirectory(directory);
        File.WriteAllText(Path.Combine(directory, fileName), text);
    }

    private static string RepositoryRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "docs", "test-protocols"))) return dir.FullName;
        }

        return AppContext.BaseDirectory;
    }
}

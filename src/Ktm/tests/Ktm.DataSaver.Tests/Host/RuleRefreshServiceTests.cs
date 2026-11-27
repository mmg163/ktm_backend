using System.Diagnostics.Metrics;
using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Tests.Decoding;
using Ktm.DataSaver.Tests.Ingestion;
using Ktm.HostedServices.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Time.Testing;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Host;

/// <summary>Первая загрузка, опрос по таймеру и сбои источника (ADR-0005 п. 2.10) на подставном <see cref="IRuleSource"/> и <see cref="FakeTimeProvider"/>.</summary>
public sealed class RuleRefreshServiceTests : IAsyncLifetime
{
    private static readonly TimeSpan Interval = TimeSpan.FromSeconds(60);

    private sealed class ScriptedSource : IRuleSource
    {
        private int _calls;

        public string Revision = "rev-1";
        public List<RuleDefinition> Rules = [];
        public List<RuleProblem> Problems = [];
        public Func<int, Exception?> Fail = _ => null;

        public int Calls => Volatile.Read(ref _calls);

        public ValueTask<RuleSourceResult> ReadAsync(string? previousRevision, CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref _calls);
            if (Fail(call) is { } error)
            {
                throw error;
            }

            return ValueTask.FromResult(previousRevision == Revision
                ? RuleSourceResult.NotChanged(Revision)
                : RuleSourceResult.Changed(Revision, Rules.ToList(), Problems.ToList()));
        }
    }

    private sealed class RecordingTerminator : IProcessTerminator
    {
        public List<int> ExitCodes { get; } = [];

        public void Terminate(int exitCode, string reason) => ExitCodes.Add(exitCode);
    }

    private sealed class Probe : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, long> _counters = [];
        private readonly Dictionary<string, double> _gauges = [];

        public Probe(Meter meter)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, meter)) listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((i, v, tags, _) =>
            {
                var key = $"{i.Name}[{string.Join(",", tags.ToArray().Select(t => t.Value))}]";
                lock (_counters) _counters[key] = _counters.GetValueOrDefault(key) + v;
            });
            _listener.SetMeasurementEventCallback<int>((i, v, _, _) => { lock (_gauges) _gauges[i.Name] = v; });
            _listener.SetMeasurementEventCallback<double>((i, v, _, _) => { lock (_gauges) _gauges[i.Name] = v; });
            _listener.Start();
        }

        public long Count(string result, string trigger) { lock (_counters) return _counters.GetValueOrDefault($"ktm.datasaver.rules.reloads[{result},{trigger}]"); }

        public long CountResult(string result) { lock (_counters) return _counters.Where(c => c.Key.StartsWith($"ktm.datasaver.rules.reloads[{result},", StringComparison.Ordinal)).Sum(c => c.Value); }

        public double Gauge(string name)
        {
            _listener.RecordObservableInstruments();
            lock (_gauges) return _gauges[$"ktm.datasaver.rules.{name}"];
        }

        public void Dispose() => _listener.Dispose();
    }

    private readonly FakeTimeProvider _time = new();
    private readonly ScriptedSource _source = new();
    private readonly RecordingTerminator _terminator = new();
    private readonly FakeLogger<RuleRefreshService> _logger = new();
    private readonly RuleSnapshotHolder _holder = new();
    private readonly Meter _meter = new("test-" + Guid.NewGuid());
    private readonly Probe _probe;
    private readonly CancellationTokenSource _timeout = new(TimeSpan.FromSeconds(60));
    private RuleRefreshService? _service;

    public RuleRefreshServiceTests() => _probe = new Probe(_meter);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        if (_service is not null)
        {
            await _service.StopAsync(CancellationToken.None);
        }

        _probe.Dispose();
        _meter.Dispose();
        _timeout.Dispose();
    }

    private RuleRefreshService CreateService() => _service = new RuleRefreshService(
        new RuleSnapshotRefresher(_source, new RuleSnapshotBuilder(new ConditionCompiler()), _holder, _time),
        _holder,
        Interval,
        new BackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 2, 0, () => 0.5),
        _time,
        new DataSaverMetrics(_meter, _time),
        _terminator,
        _logger);

    private async Task StartAsync() => await CreateService().StartAsync(_timeout.Token);

    private static async Task Eventually(Func<bool> condition)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        while (!condition())
        {
            await Task.Delay(5, limit.Token);
        }
    }

    /// <summary>Двигает часы шагами, пока условие не выполнится; возвращает, насколько сдвинуто.</summary>
    private async Task<TimeSpan> AdvanceUntil(Func<bool> condition, TimeSpan step)
    {
        using var limit = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var total = TimeSpan.Zero;
        while (!condition())
        {
            _time.Advance(step);
            total += step;
            await Task.Delay(2, limit.Token);
        }

        return total;
    }

    /// <summary>Часы сдвигаются на интервал опроса после того, как сервис ждёт таймер; возвращается, когда тик отработал.</summary>
    private async Task TickAsync(Func<bool> done) => await AdvanceUntil(done, Interval);

    private IEnumerable<FakeLogRecord> Records(LogLevel level) => _logger.Collector.GetSnapshot().Where(r => r.Level == level);

    [Fact]
    public async Task FirstLoad_DatabaseDown_RetriesWithGrowingBackoff_IngestionWaits_ThenLoads()
    {
        _source.Rules = [Rule("one")];
        _source.Fail = call => call <= 2 ? new RuleSourceException(RuleSourceFailureKind.Unavailable, "db down") : null;

        await StartAsync();
        await Eventually(() => _source.Calls == 1);

        Assert.False(_holder.HasLoaded);
        Assert.False(_service!.WaitForFirstLoadAsync(CancellationToken.None).IsCompleted);

        var firstPause = await AdvanceUntil(() => _source.Calls >= 2, TimeSpan.FromMilliseconds(100));
        Assert.True(firstPause >= TimeSpan.FromSeconds(1), $"first pause {firstPause}");
        Assert.False(_holder.HasLoaded);

        var secondPause = await AdvanceUntil(() => _source.Calls >= 3, TimeSpan.FromMilliseconds(100));
        Assert.True(secondPause >= TimeSpan.FromSeconds(2), $"second pause {secondPause}");

        await _service.WaitForFirstLoadAsync(_timeout.Token);
        Assert.True(_holder.HasLoaded);
        Assert.Equal(["one"], _holder.Current.Rules.Select(r => r.Definition.Id));
        Assert.Equal(2, _probe.Count("failed", "startup"));
        Assert.Equal(1, _probe.Count("changed", "startup"));
        Assert.Empty(_terminator.ExitCodes);
        // Первая же причина логируется Warning, повторы той же причины подавляются (раз в 10 минут).
        var warning = Assert.Single(Records(LogLevel.Warning));
        Assert.DoesNotContain("password", warning.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FirstLoad_TableMissingOrNoPrivilege_TerminatesWith78_AndNeverReleasesIngestion()
    {
        _source.Fail = _ => new RuleSourceException(RuleSourceFailureKind.Configuration, "Rule table ktm.rejection_rules is not readable (SQLSTATE 42P01)");

        await StartAsync();
        await Eventually(() => _terminator.ExitCodes.Count == 1);

        Assert.Equal([78], _terminator.ExitCodes);
        Assert.Contains(Records(LogLevel.Critical), r => r.Message.Contains("42P01", StringComparison.Ordinal));
        Assert.False(_holder.HasLoaded);
        Assert.False(_service!.WaitForFirstLoadAsync(CancellationToken.None).IsCompleted);
        Assert.Equal(1, _probe.Count("failed", "startup"));
        Assert.Equal(1, _source.Calls);
    }

    [Fact]
    public async Task FirstLoad_EmptyTable_IsSuccessfulWithEmptySet_AndInfo()
    {
        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);

        Assert.True(_holder.HasLoaded);
        Assert.Empty(_holder.Current.Rules);
        Assert.Contains(Records(LogLevel.Information), r => r.Message.Contains("no enabled rules", StringComparison.Ordinal));
        Assert.Empty(Records(LogLevel.Warning));
        Assert.Empty(Records(LogLevel.Error));
        Assert.Equal(0, _probe.Gauge("active"));
    }

    [Fact]
    public async Task Timer_ReplacesSnapshotOnChange_SkipsRebuildWhenUnchanged_LogsDiffAndMissingVersionBump()
    {
        _source.Rules = [Rule("keep"), Rule("edit", version: 1), Rule("gone")];
        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);
        var first = _holder.Current;
        Assert.Equal(3, _probe.Gauge("active"));

        // Таймер без изменений в таблице: набор не пересобирается.
        await TickAsync(() => _probe.Count("unchanged", "timer") >= 1);
        Assert.Same(first, _holder.Current);

        // Правка условия без увеличения version, удаление и добавление.
        _source.Revision = "rev-2";
        _source.Rules = [Rule("keep"), Rule("edit", condition: """{"==":[{"var":"eventType"},"other"]}""", version: 1), Rule("new")];
        await TickAsync(() => _probe.Count("changed", "timer") >= 1);

        Assert.Equal("rev-2", _holder.Current.Revision);
        Assert.Equal(["edit", "keep", "new"], _holder.Current.Rules.Select(r => r.Definition.Id).Order(StringComparer.Ordinal));
        var info = Records(LogLevel.Information).Last().Message;
        Assert.Contains("new@1", info, StringComparison.Ordinal);
        Assert.Contains("edit@1", info, StringComparison.Ordinal);
        Assert.Contains("gone@1", info, StringComparison.Ordinal);
        var warning = Assert.Single(Records(LogLevel.Warning));
        Assert.Contains("edit", warning.Message, StringComparison.Ordinal);
        Assert.Contains("without increasing version", warning.Message, StringComparison.Ordinal);
        Assert.Equal(3, _probe.Gauge("active"));
    }

    [Fact]
    public async Task Timer_RemovingAllRules_LogsRemovedAndRevision_WithEmptySet()
    {
        _source.Rules = [Rule("gone")];
        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);
        Assert.Equal(1, _probe.Gauge("active"));

        _source.Revision = "rev-2";
        _source.Rules = [];
        await TickAsync(() => _probe.Count("changed", "timer") >= 1);

        Assert.Empty(_holder.Current.Rules);
        var info = Records(LogLevel.Information).Last().Message;
        Assert.Contains("no enabled rules", info, StringComparison.Ordinal);
        Assert.Contains("rev-2", info, StringComparison.Ordinal);
        Assert.Contains("removed [gone@1]", info, StringComparison.Ordinal);
        Assert.Equal(0, _probe.Gauge("active"));
        Assert.Empty(Records(LogLevel.Warning));
        Assert.Empty(Records(LogLevel.Error));
    }

    [Fact]
    public async Task Timer_DisablingAllRules_LogsThemAsChanged_WithEmptySet()
    {
        // Выключенное правило остаётся в отпечатках набора: дифф считает его изменённым (не удалённым).
        _source.Rules = [Rule("a"), Rule("b")];
        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);

        _source.Revision = "rev-2";
        _source.Rules = [Rule("a", enabled: false), Rule("b", enabled: false)];
        await TickAsync(() => _probe.Count("changed", "timer") >= 1);

        Assert.Empty(_holder.Current.Rules);
        var info = Records(LogLevel.Information).Last().Message;
        Assert.Contains("no enabled rules", info, StringComparison.Ordinal);
        Assert.Contains("rev-2", info, StringComparison.Ordinal);
        Assert.Contains("changed [a@1, b@1]", info, StringComparison.Ordinal);
        Assert.Contains("removed []", info, StringComparison.Ordinal);
        Assert.Equal(0, _probe.Gauge("active"));
        Assert.Empty(Records(LogLevel.Error));
    }

    [Fact]
    public async Task UnparsedRow_IsLoggedAsError_WithKnownVersionOrUnknown()
    {
        _source.Problems =
        [
            new RuleProblem("bad", "ktm.rejection_rules", "column condition does not contain valid JSON", RuleVersion: 3),
            new RuleProblem("worse", "ktm.rejection_rules", "column name has an unexpected type or is NULL"),
        ];

        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);

        var errors = Records(LogLevel.Error).Select(r => r.Message).ToList();
        Assert.Contains(errors, m => m.Contains("RuleId bad, RuleVersion 3,", StringComparison.Ordinal));
        Assert.Contains(errors, m => m.Contains("RuleId worse, RuleVersion unknown,", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ReadFailureAfterFirstLoad_KeepsPreviousSnapshot_ThrottlesWarning_CountsFailed_AndRecovers()
    {
        _source.Rules = [Rule("one")];
        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);
        var loaded = _holder.Current;
        var failing = true;
        // 42P01 после первой загрузки не завершает процесс: это недоступность источника.
        _source.Fail = _ => failing ? new RuleSourceException(RuleSourceFailureKind.Configuration, "Rule table ktm.rejection_rules is not readable (SQLSTATE 42P01)") : null;

        for (var n = 1; n <= 3; n++)
        {
            var expected = n;
            await TickAsync(() => _probe.CountResult("failed") >= expected);
        }

        Assert.Same(loaded, _holder.Current);
        Assert.Empty(_terminator.ExitCodes);
        Assert.Single(Records(LogLevel.Warning));
        // Возраст снимка растёт: неудачные проверки его не сбрасывают.
        Assert.True(_probe.Gauge("snapshot_age") >= 180);

        // Через 10 минут та же причина логируется снова.
        await TickAsync(() => Records(LogLevel.Warning).Count() >= 2);

        // Восстановление: Unchanged - успешная проверка, возраст снимка сбрасывается.
        failing = false;
        await TickAsync(() => _probe.Count("unchanged", "timer") >= 1);
        Assert.Same(loaded, _holder.Current);
        Assert.True(_probe.Gauge("snapshot_age") < 1);

        _source.Revision = "rev-2";
        _source.Rules = [Rule("one"), Rule("two")];
        await TickAsync(() => _probe.Count("changed", "timer") >= 1);
        Assert.Equal(["one", "two"], _holder.Current.Rules.Select(r => r.Definition.Id));
    }

    [Fact]
    public async Task InvalidRule_IsLoggedAsErrorWithIdVersionAndReason_RepeatedNotMoreOftenThanHourly_AndOthersWork()
    {
        _source.Rules = [Rule("good"), Rule("broken", condition: """{"in":[1,[1,2]]}""", version: 7)];

        await StartAsync();
        await _service!.WaitForFirstLoadAsync(_timeout.Token);

        Assert.Equal(["good"], _holder.Current.Rules.Select(r => r.Definition.Id));
        Assert.Equal(1, _probe.Gauge("invalid"));
        Assert.Equal(1, _probe.Gauge("active"));
        var error = Assert.Single(Records(LogLevel.Error));
        Assert.Contains("broken", error.Message, StringComparison.Ordinal);
        Assert.Contains("7", error.Message, StringComparison.Ordinal);
        Assert.Contains(_holder.Current.Errors.Single().Reason, error.Message, StringComparison.Ordinal);

        // Десять тиков по минуте (набор не меняется): напоминания нет.
        for (var n = 1; n <= 10; n++)
        {
            var expected = n;
            await TickAsync(() => _probe.Count("unchanged", "timer") >= expected);
        }

        Assert.Single(Records(LogLevel.Error));

        // Через час та же ошибка напоминается ещё раз.
        await AdvanceUntil(() => Records(LogLevel.Error).Count() >= 2, Interval);

        // Правка исправляет правило: оно вступает в силу в следующем снимке, ошибок больше нет.
        _source.Revision = "rev-2";
        _source.Rules = [Rule("good"), Rule("broken", version: 8)];
        await TickAsync(() => _probe.Count("changed", "timer") >= 1);
        Assert.Equal(["broken", "good"], _holder.Current.Rules.Select(r => r.Definition.Id).Order(StringComparer.Ordinal));
        Assert.Equal(0, _probe.Gauge("invalid"));
    }

    [Fact]
    public async Task Worker_DoesNotConsumeUntilFirstRuleLoad_ThenStartsTheLoop()
    {
        _source.Rules = [Rule("one")];
        _source.Fail = call => call <= 1 ? new RuleSourceException(RuleSourceFailureKind.Unavailable, "db down") : null;
        var service = CreateService();
        var h = new Harness();
        h.StopWhenQueueDrained();
        var consumer = new CountingConsumer();
        var options = new KafkaOptions { BootstrapServers = "localhost:1" };
        var throttle = new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval);
        var handler = new KafkaFatalErrorHandler(_terminator, new FakeLogger<KafkaFatalErrorHandler>(), throttle);
        var worker = new DataSaverWorker(
            h.Loop,
            consumer,
            new KafkaRejectionPublisher(options, handler, new FakeLogger<KafkaRejectionPublisher>(), throttle),
            new KafkaDeadLetterPublisher(options, handler, new FakeLogger<KafkaDeadLetterPublisher>(), throttle),
            h.Breaker,
            new DataSaverMetrics(_meter),
            _terminator,
            new FakeLogger<DataSaverWorker>(),
            service);

        await service.StartAsync(_timeout.Token);
        await worker.StartAsync(h.Stop.Token);
        await Eventually(() => _source.Calls == 1);
        await Task.Delay(100);

        Assert.Equal(0, h.Source.ConsumeCalls);
        Assert.Equal(0, consumer.Starts);

        await AdvanceUntil(() => h.Source.ConsumeCalls > 0, TimeSpan.FromMilliseconds(100));
        await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.True(_holder.HasLoaded);
        Assert.Equal(1, consumer.Starts);
        Assert.Equal(1, consumer.Disposals);
        Assert.Empty(_terminator.ExitCodes);
        await worker.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Worker_StoppedBeforeFirstRuleLoad_NeverStartsTheConsumer_AndDisposesIt()
    {
        _source.Fail = _ => new RuleSourceException(RuleSourceFailureKind.Unavailable, "db down");
        var service = CreateService();
        var h = new Harness();
        var consumer = new CountingConsumer();
        var options = new KafkaOptions { BootstrapServers = "localhost:1" };
        var throttle = new LogThrottle(TimeProvider.System, LogThrottle.DefaultInterval);
        var handler = new KafkaFatalErrorHandler(_terminator, new FakeLogger<KafkaFatalErrorHandler>(), throttle);
        var workerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, _) =>
            {
                if (ReferenceEquals(instrument.Meter, _meter) && instrument.Name == "ktm.datasaver.loop.state")
                {
                    workerStarted.TrySetResult();
                }
            }
        };
        listener.Start();
        using var worker = new DataSaverWorker(
            h.Loop,
            consumer,
            new KafkaRejectionPublisher(options, handler, new FakeLogger<KafkaRejectionPublisher>(), throttle),
            new KafkaDeadLetterPublisher(options, handler, new FakeLogger<KafkaDeadLetterPublisher>(), throttle),
            h.Breaker,
            new DataSaverMetrics(_meter),
            _terminator,
            new FakeLogger<DataSaverWorker>(),
            service);

        await service.StartAsync(_timeout.Token);
        await worker.StartAsync(CancellationToken.None);
        try
        {
            await Eventually(() => _source.Calls == 1);
            await workerStarted.Task.WaitAsync(TimeSpan.FromSeconds(15));
            await worker.StopAsync(CancellationToken.None);

            Assert.Equal(0, consumer.Starts);
            Assert.Equal(1, consumer.Disposals);
            Assert.Empty(_terminator.ExitCodes);
        }
        finally
        {
            await worker.StopAsync(CancellationToken.None);
        }
    }

    private sealed class CountingConsumer : IConsumerLifecycle
    {
        public int Starts { get; private set; }

        public int Disposals { get; private set; }

        public void Start() => Starts++;

        public void Dispose() => Disposals++;
    }

    [Fact]
    public void Stage_TakesOneSnapshotPerMessage_ReplacementAppliesToTheNextMessage()
    {
        var before = new RuleSnapshotBuilder(new ConditionCompiler()).Build("a", [], []);
        var after = new RuleSnapshotBuilder(new ConditionCompiler()).Build(
            "b", [Rule("zone", condition: """{"==":[{"var":"eventType"},"access_granted"]}""")], []);
        var provider = new SwappingProvider(before, after);
        var stage = new RuleRejectionStage(provider);
        var bytes = EventSamples.Bytes(EventSamples.Skud);
        var message = EventSamples.Message(bytes);
        var parsed = new EventDecoder().Decode(bytes).Event!;

        var first = stage.Evaluate(message, parsed);
        var second = stage.Evaluate(message, parsed);

        Assert.True(first.IsAccepted);
        Assert.Equal("zone", second.Rejection!.RuleId);
        Assert.Equal(2, provider.Reads);
    }

    private sealed class SwappingProvider(RuleSnapshot first, RuleSnapshot next) : IRuleSnapshotProvider
    {
        public int Reads { get; private set; }

        // Снимок заменяется сразу после выдачи: стадия, читающая Current дважды за сообщение, увидела бы второй набор.
        public RuleSnapshot Current => Reads++ == 0 ? first : next;
    }
}

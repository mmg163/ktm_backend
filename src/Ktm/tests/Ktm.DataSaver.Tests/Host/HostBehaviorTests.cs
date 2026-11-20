using System.Diagnostics.Metrics;
using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Health;
using Ktm.DataSaver.Hosting;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Infrastructure.Postgres;
using Ktm.DataSaver.Tests.Decoding;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Host;

public class HostBehaviorTests
{
    private sealed class FakeSignals : IHealthSignals
    {
        public DateTimeOffset LastHeartbeat { get; set; }
        public IngestionState State { get; set; } = IngestionState.Running;
        public bool IsConsumerFatal { get; set; }
        public bool HasAssignment { get; set; } = true;
        public bool DatabaseUp { get; set; } = true;

        public ValueTask<bool> IsDatabaseAvailableAsync(CancellationToken cancellationToken) => ValueTask.FromResult(DatabaseUp);
    }

    private static (HealthEvaluator Evaluator, FakeSignals Signals, FakeTimeProvider Time) Health()
    {
        var time = new FakeTimeProvider();
        var signals = new FakeSignals { LastHeartbeat = time.GetUtcNow() };
        return (new HealthEvaluator(signals, Options.Create(new HealthOptions { LiveStaleSeconds = 60 }), time), signals, time);
    }

    [Fact]
    public async Task Liveness_IgnoresDatabaseAndDegraded_ButFailsOnStalledLoopOrFatalConsumer()
    {
        var (health, signals, time) = Health();
        signals.DatabaseUp = false;
        signals.State = IngestionState.Degraded;
        signals.HasAssignment = false;

        Assert.True(health.Live().IsHealthy);

        time.Advance(TimeSpan.FromSeconds(61));
        Assert.Equal("loop_stalled", health.Live().Reason);

        signals.LastHeartbeat = time.GetUtcNow();
        signals.IsConsumerFatal = true;
        Assert.Equal("consumer_fatal_error", health.Live().Reason);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Readiness_RequiresAssignmentRunningStateAndDatabase()
    {
        var (health, signals, _) = Health();
        Assert.True((await health.ReadyAsync(default)).IsHealthy);

        signals.HasAssignment = false;
        Assert.Equal("no_partitions_assigned", (await health.ReadyAsync(default)).Reason);

        signals.HasAssignment = true;
        signals.State = IngestionState.Degraded;
        Assert.Equal("ingestion_degraded", (await health.ReadyAsync(default)).Reason);

        signals.State = IngestionState.Running;
        signals.DatabaseUp = false;
        var result = await health.ReadyAsync(default);
        Assert.False(result.IsHealthy);
        Assert.Equal("database_unavailable", result.Reason);
    }

    [Fact]
    public void Terminator_IsIdempotent_FirstExitCodeWinsAndStopsHostOnce()
    {
        var lifetime = new CountingLifetime();
        var codes = new List<int>();
        var terminator = new HostProcessTerminator(lifetime, new FakeLogger<HostProcessTerminator>(), codes.Add);

        terminator.Terminate(70, "kafka fatal");
        terminator.Terminate(1, "loop failed");

        Assert.Equal([70], codes);
        Assert.Equal(1, lifetime.StopCalls);
    }

    private sealed class CountingLifetime : IHostApplicationLifetime
    {
        public int StopCalls { get; private set; }
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() => StopCalls++;
    }

    private sealed class FakeBatchWriter : IBatchWriter
    {
        public int Inserted { get; set; }
        public int CorrectionsInserted { get; set; }
        public Exception? Error { get; set; }

        public Task<EventWriteResult> WriteBatchAsync(IReadOnlyList<ParsedEvent> events, CancellationToken cancellationToken) =>
            Error is { } error ? Task.FromException<EventWriteResult>(error) : Task.FromResult(new EventWriteResult(Inserted, CorrectionsInserted));
    }

    private sealed class Collector : IDisposable
    {
        private readonly MeterListener _listener = new();
        public Dictionary<string, long> Totals { get; } = [];
        public Dictionary<string, long> ByTag { get; } = [];

        public Collector(Meter meter)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, meter)) listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((i, v, tags, _) =>
            {
                Totals[i.Name] = Totals.GetValueOrDefault(i.Name) + v;
                foreach (var tag in tags) ByTag[$"{i.Name}[{tag.Value}]"] = ByTag.GetValueOrDefault($"{i.Name}[{tag.Value}]") + v;
            });
            _listener.Start();
        }

        public void Dispose() => _listener.Dispose();
    }

    private static ParsedEvent[] Events(int count) =>
        Enumerable.Range(0, count).Select(i => new ParsedEvent(1, "skud", $"e{i}", DateTimeOffset.UnixEpoch, "t", null, "p", default, null, default)).ToArray();

    [Fact]
    public async Task StoreMetrics_CountWrittenAndDuplicatesAndCorrections_AndRetryOnFailure()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var writer = new FakeBatchWriter { Inserted = 90, CorrectionsInserted = 2 };
        var store = new MeteredEventStore(writer, new DataSaverMetrics(meter));

        await store.WriteAsync(Events(100), default);

        Assert.Equal(90, collector.Totals["ktm.datasaver.events.written"]);
        Assert.Equal(10, collector.Totals["ktm.datasaver.events.duplicates"]);
        Assert.Equal(2, collector.Totals["ktm.datasaver.corrections.written"]);

        writer.Error = new IOException("down");
        await Assert.ThrowsAsync<IOException>(async () => await store.WriteAsync(Events(5), default));
        Assert.Equal(1, collector.ByTag["ktm.datasaver.retries[write]"]);
        Assert.Equal(90, collector.Totals["ktm.datasaver.events.written"]);
    }

    private sealed class FakePublisher(RejectionPublishResult result) : IRejectionPublisher
    {
        public ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken) =>
            ValueTask.FromResult(result);
    }

    [Fact]
    public async Task RejectionPublisher_CountsByReason_LogsErrorWithCoordinatesAndNoBody()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var logger = new FakeLogger<ObservedRejectionPublisher>();
        var message = EventSamples.Message(EventSamples.Bytes("""{"secret-body":"ФИО Иванов"}"""), partition: 3, offset: 42);
        var rejected = new[] { new RejectedMessage(message, new RejectionInfo(RejectionKind.Format, "malformed_json", "bad json")) };

        var failing = new ObservedRejectionPublisher(new FakePublisher(RejectionPublishResult.Failure("broker down")), new DataSaverMetrics(meter), logger);
        await failing.PublishAsync(rejected, default);
        Assert.Empty(logger.Collector.GetSnapshot());
        Assert.Equal(1, collector.ByTag["ktm.datasaver.retries[publish]"]);
        Assert.False(collector.Totals.ContainsKey("ktm.datasaver.messages.rejected"));

        var ok = new ObservedRejectionPublisher(new FakePublisher(RejectionPublishResult.Success), new DataSaverMetrics(meter), logger);
        await ok.PublishAsync(rejected, default);

        Assert.Equal(1, collector.ByTag["ktm.datasaver.messages.rejected[malformed_json]"]);
        var record = Assert.Single(logger.Collector.GetSnapshot());
        Assert.Equal(LogLevel.Error, record.Level);
        Assert.Contains("topic=ktm.events.raw partition=3 offset=42", record.Message);
        Assert.DoesNotContain("Иванов", record.Message);
    }

    [Fact]
    public void AdminNotifier_LogsErrorAndCounts()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var logger = new FakeLogger<LoggingAdminNotifier>();

        new LoggingAdminNotifier(new DataSaverMetrics(meter), logger).NotifyFatalConfig(new InvalidOperationException("password=abc"), 3);

        Assert.Equal(1, collector.Totals["ktm.datasaver.admin_notifications"]);
        var record = Assert.Single(logger.Collector.GetSnapshot());
        Assert.Equal(LogLevel.Error, record.Level);
        Assert.DoesNotContain("password", record.Message);
    }
}

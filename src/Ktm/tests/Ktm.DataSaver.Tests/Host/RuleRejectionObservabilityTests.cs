using System.Diagnostics.Metrics;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Tests.Decoding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Host;

public sealed class RuleRejectionObservabilityTests
{
    private sealed class FakePublisher(RejectionPublishResult result) : IRejectionPublisher
    {
        public ValueTask<RejectionPublishResult> PublishAsync(IReadOnlyList<RejectedMessage> rejected, CancellationToken cancellationToken) =>
            ValueTask.FromResult(result);
    }

    private sealed class Collector : IDisposable
    {
        private readonly MeterListener _listener = new();
        public Dictionary<string, long> ByKey { get; } = [];
        public Dictionary<string, double> Gauges { get; } = [];

        public Collector(Meter meter)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (ReferenceEquals(instrument.Meter, meter)) listener.EnableMeasurementEvents(instrument);
            };
            _listener.SetMeasurementEventCallback<long>((i, v, tags, _) =>
            {
                var key = i.Name + "[" + string.Join(",", tags.ToArray().Select(t => $"{t.Key}={t.Value}")) + "]";
                ByKey[key] = ByKey.GetValueOrDefault(key) + v;
            });
            _listener.SetMeasurementEventCallback<int>((i, v, _, _) => Gauges[i.Name] = v);
            _listener.SetMeasurementEventCallback<double>((i, v, _, _) => Gauges[i.Name] = v);
            _listener.Start();
        }

        public void Poll() => _listener.RecordObservableInstruments();

        public void Dispose() => _listener.Dispose();
    }

    private static RejectedMessage RuleRejected(string ruleId = "skud-test-event", string source = "skud") => new(
        EventSamples.Message(EventSamples.Bytes("""{"externalPersonId":"Иванов Иван"}"""), partition: 2, offset: 7),
        new RejectionInfo(RejectionKind.Rule, "test_data", "Сработало правило", ruleId, RuleName: "n", RuleVersion: 4),
        new EventOrigin(source, "evt-1"));

    [Fact]
    public async Task Rule_rejection_is_counted_per_source_and_rule_after_confirmed_publish_and_logged_as_warning_without_message_values()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var logger = new FakeLogger<ObservedRejectionPublisher>();
        var metrics = new DataSaverMetrics(meter);
        RejectedMessage[] batch = [RuleRejected(), RuleRejected(), RuleRejected()];

        await new ObservedRejectionPublisher(new FakePublisher(RejectionPublishResult.Failure("down")), metrics, logger).PublishAsync(batch, default);
        Assert.DoesNotContain(collector.ByKey.Keys, k => k.StartsWith("ktm.datasaver.rules.rejected", StringComparison.Ordinal));

        await new ObservedRejectionPublisher(new FakePublisher(RejectionPublishResult.Success), metrics, logger).PublishAsync(batch, default);

        Assert.Equal(3, collector.ByKey["ktm.datasaver.rules.rejected[source=skud,rule_id=skud-test-event]"]);
        Assert.Equal(3, collector.ByKey["ktm.datasaver.messages.rejected[reason=rule_matched]"]);
        var records = logger.Collector.GetSnapshot();
        Assert.Equal(3, records.Count);
        var record = records[0];
        Assert.Equal(LogLevel.Warning, record.Level);
        var fields = record.StructuredState!.ToDictionary(p => p.Key, p => p.Value);
        Assert.Equal(
            ("skud-test-event", "4", "test_data", "skud", "evt-1", "ktm.events.raw", "2", "7"),
            (fields["RuleId"], fields["RuleVersion"], fields["ReasonCode"], fields["Source"], fields["SourceEventId"], fields["Topic"], fields["Partition"], fields["Offset"]));
        Assert.DoesNotContain("Иванов", record.Message);
        Assert.All(record.StructuredState!, p => Assert.DoesNotContain("Иванов", p.Value ?? string.Empty));
    }

    [Fact]
    public async Task Format_rejection_still_logs_error_and_does_not_increase_rule_counter()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var logger = new FakeLogger<ObservedRejectionPublisher>();
        var format = new RejectedMessage(EventSamples.Message(EventSamples.Bytes("{}")), new RejectionInfo(RejectionKind.Format, "invalid_schema", "d"));

        await new ObservedRejectionPublisher(new FakePublisher(RejectionPublishResult.Success), new DataSaverMetrics(meter), logger).PublishAsync([format], default);

        Assert.Equal(LogLevel.Error, Assert.Single(logger.Collector.GetSnapshot()).Level);
        Assert.Equal(1, collector.ByKey["ktm.datasaver.messages.rejected[reason=invalid_schema]"]);
        Assert.DoesNotContain(collector.ByKey.Keys, k => k.StartsWith("ktm.datasaver.rules.rejected", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Rule_rejection_without_origin_does_not_fail_logs_warning_with_rule_id_and_skips_rule_counter()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var logger = new FakeLogger<ObservedRejectionPublisher>();
        var item = new RejectedMessage(EventSamples.Message(EventSamples.Bytes("{}")), new RejectionInfo(RejectionKind.Rule, "test_data", "d", "r-1", RuleVersion: 2));

        await new ObservedRejectionPublisher(new FakePublisher(RejectionPublishResult.Success), new DataSaverMetrics(meter), logger).PublishAsync([item], default);

        var record = Assert.Single(logger.Collector.GetSnapshot());
        Assert.Equal(LogLevel.Warning, record.Level);
        var fields = record.StructuredState!.ToDictionary(p => p.Key, p => p.Value);
        Assert.Equal("r-1", fields["RuleId"]);
        Assert.Null(fields["Source"]);
        Assert.Null(fields["SourceEventId"]);
        Assert.Equal(1, collector.ByKey["ktm.datasaver.messages.rejected[reason=rule_matched]"]);
        Assert.DoesNotContain(collector.ByKey.Keys, k => k.StartsWith("ktm.datasaver.rules.rejected", StringComparison.Ordinal));
    }

    [Fact]
    public void Snapshot_age_is_reset_by_changed_and_unchanged_reloads_but_not_by_failed_and_absent_before_first_success()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var time = new FakeTimeProvider();
        var metrics = new DataSaverMetrics(meter, time);
        const string age = "ktm.datasaver.rules.snapshot_age";

        metrics.RulesReloaded("failed", "startup");
        collector.Poll();
        Assert.False(collector.Gauges.ContainsKey(age));

        metrics.RulesReloaded("unchanged", "timer");
        collector.Poll();
        Assert.Equal(0.0, collector.Gauges[age]);

        time.Advance(TimeSpan.FromSeconds(30));
        metrics.RulesReloaded("failed", "timer");
        collector.Poll();
        Assert.Equal(30.0, collector.Gauges[age]);

        metrics.RulesReloaded("changed", "timer");
        collector.Poll();
        Assert.Equal(0.0, collector.Gauges[age]);
        Assert.Equal(1, collector.ByKey["ktm.datasaver.rules.reloads[result=failed,trigger=timer]"]);

        time.Advance(TimeSpan.FromSeconds(5));
        metrics.RulesReloaded("unchanged", "timer");
        time.Advance(TimeSpan.FromSeconds(7));
        collector.Poll();
        Assert.Equal(7.0, collector.Gauges[age]);
    }

    [Fact]
    public void Rule_gauges_report_active_and_invalid_counts()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        using var collector = new Collector(meter);
        var metrics = new DataSaverMetrics(meter);

        metrics.SetRulesState(active: 5, invalid: 2);
        collector.Poll();

        Assert.Equal((5.0, 2.0), (collector.Gauges["ktm.datasaver.rules.active"], collector.Gauges["ktm.datasaver.rules.invalid"]));
    }
}

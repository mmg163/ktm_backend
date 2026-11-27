using System.Diagnostics.Metrics;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Tests.Decoding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;

namespace Ktm.DataSaver.Tests.Host;

public sealed class DeadLetterObservabilityTests
{
    private sealed class FakeDeadLetters(DeadLetterPublishResult result) : IDeadLetterPublisher
    {
        public ValueTask<DeadLetterPublishResult> PublishAsync(IReadOnlyList<DeadLetteredMessage> messages, CancellationToken cancellationToken) =>
            ValueTask.FromResult(result);
    }

    private static DeadLetteredMessage Item() => new(
        EventSamples.Message(EventSamples.Bytes("""{"externalPersonId":"Иванов Иван"}"""), partition: 2, offset: 7),
        new EventOrigin("skud", "evt-1"),
        new DeadLetterInfo(DeadLetterReasons.WriteDataError, "write", "Npgsql.PostgresException", "23514", "ck_poison", "events", 4, DateTimeOffset.UtcNow));

    private static Dictionary<string, long> Collect(Meter meter, out MeterListener listener)
    {
        var totals = new Dictionary<string, long>();
        listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (ReferenceEquals(instrument.Meter, meter)) l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((i, v, tags, _) =>
        {
            var key = i.Name + "[" + string.Join(",", tags.ToArray().Select(t => $"{t.Key}={t.Value}")) + "]";
            totals[key] = totals.GetValueOrDefault(key) + v;
        });
        listener.Start();
        return totals;
    }

    [Fact]
    public async Task ConfirmedPublish_IsCountedAndLoggedWithCoordinates_FailureOnlyCountsARetry_AndNoBodyReachesTheLog()
    {
        using var meter = new Meter("test-" + Guid.NewGuid());
        var totals = Collect(meter, out var listener);
        using var _ = listener;
        var logger = new FakeLogger<ObservedDeadLetterPublisher>();
        var metrics = new DataSaverMetrics(meter);

        await new ObservedDeadLetterPublisher(new FakeDeadLetters(DeadLetterPublishResult.Failure("down")), metrics, logger).PublishAsync([Item()], default);
        Assert.Empty(logger.Collector.GetSnapshot());
        Assert.DoesNotContain(totals.Keys, k => k.StartsWith("ktm.datasaver.messages.dead_lettered", StringComparison.Ordinal));
        Assert.Equal(1, totals["ktm.datasaver.retries[stage=dlq_publish]"]);

        await new ObservedDeadLetterPublisher(new FakeDeadLetters(DeadLetterPublishResult.Success), metrics, logger).PublishAsync([Item()], default);

        Assert.Equal(1, totals["ktm.datasaver.messages.dead_lettered[reason=write_data_error,stage=write]"]);
        var record = Assert.Single(logger.Collector.GetSnapshot());
        Assert.Equal(LogLevel.Error, record.Level);
        var fields = record.StructuredState!.ToDictionary(p => p.Key, p => p.Value);
        Assert.Equal(
            ("ktm.events.raw", "2", "7", "skud", "evt-1", "write_data_error", "write", "Npgsql.PostgresException", "4", "23514", "ck_poison", "events"),
            (fields["Topic"], fields["Partition"], fields["Offset"], fields["Source"], fields["SourceEventId"], fields["Reason"], fields["Stage"],
                fields["ExceptionType"], fields["Attempts"], fields["SqlState"], fields["ConstraintName"], fields["TableName"]));
        Assert.DoesNotContain("Иванов", record.Message);
        Assert.All(record.StructuredState!, p => Assert.DoesNotContain("Иванов", p.Value ?? string.Empty));
        Assert.Null(record.Exception);
    }
}

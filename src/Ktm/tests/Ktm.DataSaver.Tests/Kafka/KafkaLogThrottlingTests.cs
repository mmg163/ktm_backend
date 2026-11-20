using Confluent.Kafka;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaLogThrottlingTests
{
    private sealed class RecordingTerminator : IProcessTerminator
    {
        public List<int> ExitCodes { get; } = [];

        public void Terminate(int exitCode, string reason) => ExitCodes.Add(exitCode);
    }

    [Fact]
    public void NonFatalErrorCallbacks_AreLoggedOncePer30Seconds_WithSuppressedCount_FatalIsNeverThrottled()
    {
        var time = new FakeTimeProvider();
        var logger = new FakeLogger<KafkaFatalErrorHandler>();
        var terminator = new RecordingTerminator();
        var handler = new KafkaFatalErrorHandler(terminator, logger, time);
        var down = new Error(ErrorCode.Local_Transport, "broker down", isFatal: false);
        var other = new Error(ErrorCode.Local_AllBrokersDown, "all brokers down", isFatal: false);

        for (var i = 0; i < 50; i++)
        {
            handler.Handle(down);
            time.Advance(TimeSpan.FromMilliseconds(100));
        }

        handler.Handle(other);
        Assert.Equal(2, logger.Collector.Count);

        time.Advance(TimeSpan.FromSeconds(30));
        handler.Handle(down);

        var records = logger.Collector.GetSnapshot();
        Assert.Equal(3, records.Count);
        Assert.Equal("49", records[2].StructuredState!.Single(p => p.Key == "Suppressed").Value);

        var fatal = new Error(ErrorCode.Local_Fatal, "fenced", isFatal: true);
        Assert.True(handler.Handle(fatal));
        Assert.True(handler.Handle(fatal));
        Assert.Equal(2, logger.Collector.GetSnapshot().Count(r => r.Level == LogLevel.Critical));
        Assert.Equal([KafkaFatalErrorHandler.FatalExitCode], terminator.ExitCodes);
    }

    [Fact]
    public void NativeLogs_TransportNoiseThrottled_OtherErrorsAndCriticalAreKept_InfoDemoted()
    {
        var time = new FakeTimeProvider();
        var logger = new FakeLogger<KafkaMessageSource>();
        var bridge = new KafkaNativeLogBridge(logger, time);

        for (var i = 0; i < 100; i++)
        {
            bridge.Handle(new LogMessage("rdkafka#consumer-1", SyslogLevel.Error, "FAIL", "127.0.0.1:1/bootstrap: Connect to ipv4#127.0.0.1:1 failed"));
        }

        bridge.Handle(new LogMessage("rdkafka#consumer-1", SyslogLevel.Error, "SSL", "handshake failed"));
        bridge.Handle(new LogMessage("rdkafka#consumer-1", SyslogLevel.Critical, "FAIL", "fatal"));
        bridge.Handle(new LogMessage("rdkafka#consumer-1", SyslogLevel.Info, "JOIN", "joined group"));
        bridge.Handle(new LogMessage("rdkafka#consumer-1", SyslogLevel.Debug, "SEND", "request"));

        var records = logger.Collector.GetSnapshot();
        // Из 100 транспортных Error проходит одно (Warning); Error другого фасета и Critical транспортного не подавляются.
        Assert.Contains(records, r => r.Level == LogLevel.Critical && r.Message.Contains("fatal", StringComparison.Ordinal));
        Assert.Contains(records, r => r.Level == LogLevel.Error && r.Message.Contains("handshake failed", StringComparison.Ordinal));
        Assert.Single(records, r => r.Message.Contains("Connect to", StringComparison.Ordinal));
        Assert.Contains(records, r => r.Level == LogLevel.Debug && r.Message.Contains("joined group", StringComparison.Ordinal));
        Assert.Contains(records, r => r.Level == LogLevel.Trace && r.Message.Contains("request", StringComparison.Ordinal));
        Assert.DoesNotContain(records, r => r.Level == LogLevel.Information);
    }
}

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
        var handler = new KafkaFatalErrorHandler(terminator, logger, new LogThrottle(time, LogThrottle.DefaultInterval));
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
        var bridge = new KafkaNativeLogBridge(logger, new LogThrottle(time, LogThrottle.DefaultInterval));

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

    [Fact]
    public void OneThrottle_LimitsConsumerAndProducerTogether_AndLetsTheKeyThroughAfterTheWindow()
    {
        var time = new FakeTimeProvider();
        var throttle = new LogThrottle(time, LogThrottle.DefaultInterval);
        var consumerLog = new FakeLogger<KafkaMessageSource>();
        var producerLog = new FakeLogger<KafkaRejectionPublisher>();
        var consumer = new KafkaNativeLogBridge(consumerLog, throttle);
        var producer = new KafkaNativeLogBridge(producerLog, throttle);
        var fail = new LogMessage("rdkafka", SyslogLevel.Error, "FAIL", "Connect to ipv4#127.0.0.1:1 failed");

        for (var i = 0; i < 10; i++)
        {
            consumer.Handle(fail);
            producer.Handle(fail);
        }

        // Источник (consumer, producer) в ключ не входит: на процесс одна строка за окно.
        Assert.Equal(1, consumerLog.Collector.Count + producerLog.Collector.Count);

        time.Advance(LogThrottle.DefaultInterval);
        producer.Handle(fail);

        Assert.Equal(2, consumerLog.Collector.Count + producerLog.Collector.Count);
        Assert.Equal("19", producerLog.Collector.GetSnapshot().Single().StructuredState!.Single(p => p.Key == "Suppressed").Value);
    }

    [Fact]
    public void StaleKeysAreEvicted_FreshOnesAreKept()
    {
        var time = new FakeTimeProvider();
        var throttle = new LogThrottle(time, LogThrottle.DefaultInterval);
        for (var i = 0; i < LogThrottle.EvictionThreshold; i++)
        {
            Assert.True(throttle.TryPass($"k{i}", out _));
        }

        // Окно ещё не истекло: словарь растёт, ключ по-прежнему подавляется.
        Assert.True(throttle.TryPass("fresh", out _));
        Assert.Equal(LogThrottle.EvictionThreshold + 1, throttle.TrackedKeys);
        Assert.False(throttle.TryPass("k0", out _));

        time.Advance(LogThrottle.DefaultInterval);
        Assert.True(throttle.TryPass("after", out _));

        Assert.Equal(1, throttle.TrackedKeys);
        Assert.True(throttle.TryPass("k0", out var suppressed));
        Assert.Equal(0, suppressed);
    }

    [Fact]
    public async Task ManyThreadsOnOneKey_ExactlyOnePasses()
    {
        var throttle = new LogThrottle(new FakeTimeProvider(), LogThrottle.DefaultInterval);
        var passed = 0;
        using var start = new ManualResetEventSlim();
        var tasks = Enumerable.Range(0, 32).Select(_ => Task.Run(() =>
        {
            start.Wait();
            for (var i = 0; i < 1000; i++)
            {
                if (throttle.TryPass("native:FAIL", out _))
                {
                    Interlocked.Increment(ref passed);
                }
            }
        })).ToArray();

        start.Set();
        await Task.WhenAll(tasks);

        Assert.Equal(1, passed);
    }
}

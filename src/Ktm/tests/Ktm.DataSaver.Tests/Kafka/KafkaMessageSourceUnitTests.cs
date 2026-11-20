using Confluent.Kafka;
using Ktm.DataSaver.Infrastructure.Kafka;
using Microsoft.Extensions.Logging.Testing;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaMessageSourceUnitTests
{
    private sealed class NoopTerminator : IProcessTerminator
    {
        public void Terminate(int exitCode, string reason)
        {
        }
    }

    private static KafkaMessageSource Create() => new(
        new KafkaOptions { BootstrapServers = "localhost:1" },
        new KafkaFatalErrorHandler(new NoopTerminator(), new FakeLogger<KafkaFatalErrorHandler>()),
        new FakeLogger<KafkaMessageSource>());

    [Fact]
    public void HasAssignment_FollowsRebalanceHandlers_WithoutAskingTheConsumer()
    {
        using var source = Create();
        TopicPartition P(int n) => new("ktm.events.raw", n);

        Assert.False(source.HasAssignment);

        // Клиент не подключён к брокеру, его Assignment пуст: ответ может прийти только из кэша обработчиков.
        source.OnAssigned([P(0), P(1)]);
        Assert.True(source.HasAssignment);
        Assert.Empty(source.Assignment);

        source.OnRevoked([P(0)]);
        Assert.True(source.HasAssignment);

        // Потеря партиций обрабатывается тем же путём, что и отзыв.
        source.OnRevoked([P(1)]);
        Assert.False(source.HasAssignment);

        source.OnAssigned([P(2)]);
        Assert.True(source.HasAssignment);
    }

    [Fact]
    public void Dispose_ClearsAssignmentCache_EvenWithoutRevokeCallback()
    {
        var source = Create();
        source.OnAssigned([new TopicPartition("ktm.events.raw", 0)]);
        Assert.True(source.HasAssignment);

        source.Dispose();

        Assert.False(source.HasAssignment);
    }

    [Fact]
    public void ProbeBroker_AfterDispose_ThrowsObjectDisposed()
    {
        var source = Create();
        source.Dispose();

        Assert.Throws<ObjectDisposedException>(() => source.ProbeBroker(TimeSpan.FromMilliseconds(100)));
    }
}

using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Health;
using Microsoft.Extensions.Options;

namespace Ktm.DataSaver.Tests.Ingestion;

/// <summary>
/// Liveness (D-10): решение принимает <see cref="HealthEvaluator"/> по отметке цикла; медленные, но завершающиеся
/// ожидания (запись, публикация, commit), каждое в пределах порога, не должны давать ложный loop_stalled.
/// </summary>
public sealed class IngestionLoopHeartbeatTests
{
    private const int LiveStaleSeconds = 60;

    private sealed class LoopSignals(IngestionLoop loop) : IHealthSignals
    {
        public DateTimeOffset LastHeartbeat => loop.LastHeartbeat;

        public IngestionState State => loop.State;

        public bool IsConsumerFatal => false;

        public bool HasAssignment => true;

        public ValueTask<bool> IsBrokerAvailableAsync(CancellationToken cancellationToken) => ValueTask.FromResult(true);

        public ValueTask<bool> IsDatabaseAvailableAsync(CancellationToken cancellationToken) => ValueTask.FromResult(true);
    }

    private static HealthEvaluator Evaluator(Harness h) =>
        new(new LoopSignals(h.Loop), Options.Create(new HealthOptions { LiveStaleSeconds = LiveStaleSeconds }), h.Time);

    [Fact]
    public async Task WriteThenPublishThenCommit_EachWaitJustUnderThreshold_LivenessStaysHealthy()
    {
        var h = new Harness();
        var health = Evaluator(h);
        var wait = TimeSpan.FromSeconds(LiveStaleSeconds - 1);
        var verdicts = new List<(string Point, HealthResult Result)>();
        h.Source.Enqueue(Harness.Valid(0, 0), Harness.Valid(0, 1), Harness.Garbage(0, 2));
        h.Store.OnCall = _ =>
        {
            h.Time.Advance(wait);
            verdicts.Add(("after write", health.Live()));
        };
        h.Publisher.Result = _ =>
        {
            h.Time.Advance(wait);
            verdicts.Add(("after publish", health.Live()));
            return RejectionPublishResult.Success;
        };
        h.Committer.Fail = _ =>
        {
            h.Time.Advance(wait);
            verdicts.Add(("after commit", health.Live()));
            return null;
        };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        // Ожидания не суммируются: после каждого успешного шага цикл отмечается заново.
        Assert.Equal(["after write", "after publish", "after commit"], verdicts.Select(v => v.Point));
        Assert.All(verdicts, v => Assert.True(v.Result.IsHealthy, $"liveness is {v.Result.Reason} {v.Point}"));
    }

    [Fact]
    public async Task HangingWrite_LivenessReportsLoopStalled()
    {
        var h = new Harness();
        var health = Evaluator(h);
        h.Source.Enqueue(Harness.Valid(0, 0), Harness.Valid(0, 1), Harness.Valid(0, 2));
        h.Store.Hang = true;
        var run = Task.Run(h.RunAsync);
        try
        {
            Assert.True(SpinWait.SpinUntil(() => h.Store.Writes.Count > 0, TimeSpan.FromSeconds(10)), "write was not started");

            h.Time.Advance(TimeSpan.FromSeconds(LiveStaleSeconds + 1));

            var result = health.Live();
            Assert.False(result.IsHealthy);
            Assert.Equal("loop_stalled", result.Reason);
        }
        finally
        {
            // Остановка: предел ShutdownTimeout отсчитывается по часам FakeTimeProvider.
            h.Stop.Cancel();
            h.Time.Advance(TimeSpan.FromMinutes(1));
            await run;
        }
    }

    [Fact]
    public async Task ConsumeWaitThenWrite_EachJustUnderThreshold_LivenessStaysHealthy()
    {
        // Первое сообщение запускает отсчёт закрытия пачки (59 с): Consume ждёт 59 с и возвращает null, затем запись 59 с.
        var wait = TimeSpan.FromSeconds(LiveStaleSeconds - 1);
        var h = new Harness(wait, wait, batchSize: 100);
        var health = Evaluator(h);
        HealthResult? afterWrite = null;
        h.Source.Enqueue(Harness.Valid(0, 0));
        h.Store.OnCall = _ =>
        {
            h.Time.Advance(wait);
            afterWrite = health.Live();
            h.Stop.Cancel();
        };

        await h.RunAsync();

        // Ожидание Consume и запись не суммируются: после Consume цикл отмечается до записи.
        Assert.NotNull(afterWrite);
        Assert.True(afterWrite.IsHealthy, $"liveness is {afterWrite.Reason} after write");
    }

    [Fact]
    public async Task HangingConsume_LivenessReportsLoopStalled()
    {
        var h = new Harness();
        var health = Evaluator(h);
        h.Source.Hang = true;
        var run = Task.Run(h.RunAsync);
        try
        {
            Assert.True(SpinWait.SpinUntil(() => h.Source.ConsumeCalls > 0, TimeSpan.FromSeconds(10)), "consume was not started");

            h.Time.Advance(TimeSpan.FromSeconds(LiveStaleSeconds + 1));

            var result = health.Live();
            Assert.False(result.IsHealthy);
            Assert.Equal("loop_stalled", result.Reason);
        }
        finally
        {
            h.Stop.Cancel();
            await run;
        }
    }
}

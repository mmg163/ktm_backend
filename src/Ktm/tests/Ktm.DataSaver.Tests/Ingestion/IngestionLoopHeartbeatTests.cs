namespace Ktm.DataSaver.Tests.Ingestion;

/// <summary>Liveness (D-10): отметка цикла не должна устаревать из-за медленной, но завершающейся операции ввода-вывода.</summary>
public sealed class IngestionLoopHeartbeatTests
{
    private static readonly TimeSpan SlowIo = TimeSpan.FromSeconds(45);

    [Fact]
    public async Task SlowButSuccessfulWrite_FreshHeartbeatByTheTimeOfCommit()
    {
        var h = new Harness();
        h.Source.Enqueue(Harness.Valid(0, 0), Harness.Valid(0, 1), Harness.Valid(0, 2));
        h.Store.OnCall = _ => h.Time.Advance(SlowIo);
        TimeSpan? ageAtCommit = null;
        h.Committer.Fail = _ =>
        {
            ageAtCommit = h.Time.GetUtcNow() - h.Loop.LastHeartbeat;
            return null;
        };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.NotNull(ageAtCommit);
        Assert.True(ageAtCommit < TimeSpan.FromSeconds(1), $"heartbeat is {ageAtCommit} old after a successful write");
    }

    [Fact]
    public async Task SlowButSuccessfulRejectedPublish_FreshHeartbeatByTheTimeOfCommit()
    {
        var h = new Harness();
        h.Source.Enqueue(Harness.Garbage(0, 0), Harness.Garbage(0, 1), Harness.Garbage(0, 2));
        h.Publisher.Result = _ =>
        {
            h.Time.Advance(SlowIo);
            return Ktm.DataSaver.Core.Ingestion.RejectionPublishResult.Success;
        };
        TimeSpan? ageAtCommit = null;
        h.Committer.Fail = _ =>
        {
            ageAtCommit = h.Time.GetUtcNow() - h.Loop.LastHeartbeat;
            return null;
        };
        h.StopWhenQueueDrained();

        await h.RunAsync();

        Assert.NotNull(ageAtCommit);
        Assert.True(ageAtCommit < TimeSpan.FromSeconds(1), $"heartbeat is {ageAtCommit} old after a successful publish");
    }

    [Fact]
    public async Task HangingWrite_HeartbeatGoesStale()
    {
        var h = new Harness();
        h.Source.Enqueue(Harness.Valid(0, 0), Harness.Valid(0, 1), Harness.Valid(0, 2));
        h.Store.Hang = true;
        var run = Task.Run(h.RunAsync);

        while (h.Store.Writes.Count == 0)
        {
            await Task.Delay(5);
        }

        h.Time.Advance(TimeSpan.FromMinutes(2));
        var age = h.Time.GetUtcNow() - h.Loop.LastHeartbeat;

        Assert.True(age >= TimeSpan.FromMinutes(2), $"heartbeat is only {age} old while the write hangs");

        // Остановка: предел ShutdownTimeout отсчитывается по часам FakeTimeProvider.
        h.Stop.Cancel();
        h.Time.Advance(TimeSpan.FromMinutes(1));
        await run;
    }
}

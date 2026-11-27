using Ktm.DataSaver.Application.Batching;
using Microsoft.Extensions.Time.Testing;

namespace Ktm.DataSaver.Tests.Batching;

public class BatcherTests
{
    private static readonly TimeSpan MaxWait = TimeSpan.FromSeconds(2);

    [Fact]
    public void Closes_by_size_and_starts_new_batch()
    {
        var batcher = new Batcher<int>(3, MaxWait, new FakeTimeProvider());

        batcher.Add(1);
        batcher.Add(2);
        Assert.False(batcher.IsReady);
        Assert.Null(batcher.TryTakeReady());

        batcher.Add(3);
        Assert.Equal([1, 2, 3], batcher.TryTakeReady());
        Assert.Equal(0, batcher.Count);
    }

    [Fact]
    public void Closes_by_time_counted_from_first_message_not_from_last()
    {
        var time = new FakeTimeProvider();
        var batcher = new Batcher<int>(100, MaxWait, time);

        batcher.Add(1);
        time.Advance(TimeSpan.FromSeconds(1.5));
        batcher.Add(2);
        Assert.False(batcher.IsReady);
        Assert.Equal(TimeSpan.FromSeconds(0.5), batcher.TimeUntilDeadline());

        time.Advance(TimeSpan.FromSeconds(0.5));

        Assert.Equal([1, 2], batcher.TryTakeReady());
    }

    [Fact]
    public void Expired_deadline_gives_zero_not_negative_wait()
    {
        var time = new FakeTimeProvider();
        var batcher = new Batcher<int>(100, MaxWait, time);
        batcher.Add(1);

        time.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(TimeSpan.Zero, batcher.TimeUntilDeadline());
    }

    [Fact]
    public void Empty_batch_is_never_ready_regardless_of_time()
    {
        var time = new FakeTimeProvider();
        var batcher = new Batcher<int>(3, MaxWait, time);

        time.Advance(TimeSpan.FromMinutes(10));

        Assert.False(batcher.IsReady);
        Assert.Null(batcher.TryTakeReady());
        Assert.Null(batcher.TimeUntilDeadline());
        Assert.Null(batcher.TakeAll());
    }

    [Fact]
    public void Wait_is_restarted_by_first_message_of_next_batch()
    {
        var time = new FakeTimeProvider();
        var batcher = new Batcher<int>(100, MaxWait, time);
        batcher.Add(1);
        time.Advance(MaxWait);
        Assert.NotNull(batcher.TryTakeReady());

        time.Advance(TimeSpan.FromMinutes(1));
        batcher.Add(2);

        Assert.False(batcher.IsReady);
    }

    [Fact]
    public void TakeAll_returns_incomplete_batch_for_shutdown()
    {
        var batcher = new Batcher<int>(100, MaxWait, new FakeTimeProvider());
        batcher.Add(1);

        Assert.Equal([1], batcher.TakeAll());
    }
}

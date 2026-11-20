using Ktm.DataSaver.Core.Resilience;

namespace Ktm.DataSaver.Tests.Resilience;

public class BackoffPolicyTests
{
    private static BackoffPolicy Create(Func<double> random) =>
        new(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 2.0, 0.2, random);

    [Fact]
    public void Delays_double_up_to_ceiling_and_stay_there()
    {
        var policy = Create(() => 0.5); // нулевой джиттер

        var delays = Enumerable.Range(0, 9).Select(_ => policy.NextDelay().TotalSeconds).ToList();

        Assert.Equal([1d, 2, 4, 8, 16, 30, 30, 30, 30], delays);
    }

    [Fact]
    public void Jitter_stays_within_twenty_percent_at_both_extremes()
    {
        var low = Create(() => 0.0).NextDelay();
        var high = Create(() => 0.999999).NextDelay();

        Assert.Equal(800, low.TotalMilliseconds, 3);
        Assert.InRange(high.TotalMilliseconds, 1199, 1200);
    }

    [Fact]
    public void Jitter_applies_to_ceiling_too()
    {
        var policy = Create(() => 0.999999);
        for (var i = 0; i < 10; i++) policy.NextDelay();

        Assert.InRange(policy.NextDelay().TotalSeconds, 24, 36);
    }

    [Fact]
    public void Reset_restarts_from_initial_delay()
    {
        var policy = Create(() => 0.5);
        policy.NextDelay();
        policy.NextDelay();
        policy.NextDelay();

        policy.Reset();

        Assert.Equal(TimeSpan.FromSeconds(1), policy.NextDelay());
    }
}

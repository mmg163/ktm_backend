using Ktm.DataSaver.Application.Resilience;

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
    public void Ceiling_is_hard_after_jitter()
    {
        var high = Create(() => 0.999999);
        var low = Create(() => 0.0);
        for (var i = 0; i < 10; i++)
        {
            high.NextDelay();
            low.NextDelay();
        }

        Assert.Equal(30, high.NextDelay().TotalSeconds, 6);
        Assert.Equal(24, low.NextDelay().TotalSeconds, 6);
    }

    [Fact]
    public void Zero_jitter_and_multiplier_one_give_constant_delay_even_after_many_attempts()
    {
        var policy = new BackoffPolicy(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30), 1.0, 0.0, () => 0.9);

        for (var i = 0; i < 100_000; i++)
        {
            Assert.Equal(TimeSpan.FromSeconds(1), policy.NextDelay());
        }
    }

    [Fact]
    public void Huge_number_of_attempts_stays_at_ceiling_without_overflow()
    {
        var policy = Create(() => 0.5);

        for (var i = 0; i < 10_000; i++) policy.NextDelay();

        Assert.Equal(TimeSpan.FromSeconds(30), policy.NextDelay());
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

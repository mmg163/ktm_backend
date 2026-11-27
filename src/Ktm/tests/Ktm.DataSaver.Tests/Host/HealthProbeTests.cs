using Ktm.HostedServices.Health;
using Microsoft.Extensions.Time.Testing;
using Npgsql;

namespace Ktm.DataSaver.Tests.Host;

public class HealthProbeTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(2);

    /// <summary>Часы теста не идут сами: двигаем их, пока задача не завершится (таймеры создаются на пуле потоков).</summary>
    private static async Task<T> PumpAsync<T>(FakeTimeProvider time, Task<T> task, TimeSpan step)
    {
        while (!task.IsCompleted)
        {
            time.Advance(step);
            await Task.Delay(1);
        }

        return await task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task BrokerProbe_CachesResultWithinTtl_AndRepeatsAfterIt()
    {
        var time = new FakeTimeProvider();
        var calls = 0;
        var probe = new CachedAvailabilityProbe(_ => { Interlocked.Increment(ref calls); return Task.CompletedTask; }, time, Ttl, Limit);

        Assert.True(await probe.IsAvailableAsync(default));
        time.Advance(TimeSpan.FromSeconds(4));
        Assert.True(await probe.IsAvailableAsync(default));
        Assert.Equal(1, calls);

        time.Advance(TimeSpan.FromSeconds(2));
        Assert.True(await probe.IsAvailableAsync(default));
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task BrokerProbe_ConcurrentCallersShareOneRequest()
    {
        var time = new FakeTimeProvider();
        var calls = 0;
        var release = new TaskCompletionSource();
        var probe = new CachedAvailabilityProbe(async _ => { Interlocked.Increment(ref calls); await release.Task; }, time, Ttl, Limit);

        var callers = Enumerable.Range(0, 5).Select(_ => probe.IsAvailableAsync(default).AsTask()).ToArray();
        release.SetResult();

        Assert.All(await Task.WhenAll(callers), Assert.True);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task BrokerProbe_HungCheckIsUnavailableWithinLimit_AndCancelledCallerDoesNotBreakSharedProbe()
    {
        var time = new FakeTimeProvider();
        var started = time.GetUtcNow();
        // Проверка не реагирует на токен, как блокирующий вызов клиента.
        var probe = new CachedAvailabilityProbe(_ => new TaskCompletionSource().Task, time, Ttl, Limit);

        using var gone = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await probe.IsAvailableAsync(gone.Token));

        var result = await PumpAsync(time, probe.IsAvailableAsync(default).AsTask(), TimeSpan.FromMilliseconds(100));

        Assert.False(result);
        Assert.True(time.GetUtcNow() - started <= Limit + TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public async Task BrokerProbe_FailureAndDisposedClientAreUnavailable_AndRecoveryIsSeenAfterTtl()
    {
        var time = new FakeTimeProvider();
        Exception? failure = new ObjectDisposedException("consumer");
        var seen = new List<Exception>();
        var probe = new CachedAvailabilityProbe(
            _ => failure is null ? Task.CompletedTask : Task.FromException(failure), time, Ttl, Limit, seen.Add);

        Assert.False(await probe.IsAvailableAsync(default));
        Assert.IsType<ObjectDisposedException>(Assert.Single(seen));

        failure = null;
        Assert.False(await probe.IsAvailableAsync(default));

        time.Advance(Ttl);
        Assert.True(await probe.IsAvailableAsync(default));
    }

    [Fact]
    public async Task DatabaseProbe_DisposedDataSourceIsUnavailable_ButClientCancellationIsRethrown()
    {
        var failures = new List<Exception>();

        var disposed = await DatabaseProbe.RunAsync(_ => throw new ObjectDisposedException("data source"), Limit, failures.Add, default);
        var broken = await DatabaseProbe.RunAsync(_ => throw new NpgsqlException("host db.internal"), Limit, failures.Add, default);

        Assert.False(disposed);
        Assert.False(broken);
        Assert.Equal(2, failures.Count);

        using var clientGone = new CancellationTokenSource();
        await clientGone.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await DatabaseProbe.RunAsync(token => Task.Delay(Timeout.Infinite, token), Limit, failures.Add, clientGone.Token));
        Assert.Equal(2, failures.Count);
    }

    [Fact]
    public async Task DatabaseProbe_ExceedingItsOwnLimitIsUnavailable()
    {
        var result = await DatabaseProbe.RunAsync(token => Task.Delay(Timeout.Infinite, token), TimeSpan.FromMilliseconds(50), _ => { }, default);

        Assert.False(result);
    }

    [Fact]
    public async Task DatabaseProbe_ProbeIgnoringTheToken_IsUnavailableWithinLimit_AndClientCancellationStillWins()
    {
        var failures = new List<Exception>();
        var started = System.Diagnostics.Stopwatch.StartNew();

        var result = await DatabaseProbe.RunAsync(_ => Task.Delay(Timeout.Infinite), TimeSpan.FromMilliseconds(100), failures.Add, default)
            .AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        Assert.False(result);
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(5));
        Assert.IsType<TimeoutException>(Assert.Single(failures));

        using var clientGone = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await DatabaseProbe.RunAsync(_ => Task.Delay(Timeout.Infinite), TimeSpan.FromSeconds(30), failures.Add, clientGone.Token));
        Assert.Single(failures);
    }
}

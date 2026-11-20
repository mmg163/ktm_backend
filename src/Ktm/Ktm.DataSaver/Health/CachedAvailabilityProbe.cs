namespace Ktm.DataSaver.Health;

/// <summary>
/// Проверка доступности зависимости для readiness: предел ожидания, кэш результата и единственный проверочный запрос
/// в полёте (single-flight). Сама проверка выполняется на пуле потоков, а не на потоке запроса или цикла приёма.
/// Любой сбой проверки, в том числе <see cref="ObjectDisposedException"/> при остановке, означает «недоступно».
/// </summary>
public sealed class CachedAvailabilityProbe(
    Func<CancellationToken, Task> check,
    TimeProvider time,
    TimeSpan cacheTtl,
    TimeSpan timeout,
    Action<Exception>? onFailure = null)
{
    private readonly Lock _gate = new();
    private Task<bool>? _inFlight;
    private bool _cachedValue;
    private DateTimeOffset _cachedUntil = DateTimeOffset.MinValue;

    public async ValueTask<bool> IsAvailableAsync(CancellationToken cancellationToken)
    {
        Task<bool> task;
        lock (_gate)
        {
            if (_inFlight is null && time.GetUtcNow() < _cachedUntil)
            {
                return _cachedValue;
            }

            // Запрос клиента отменяет только его ожидание, но не общую проверку.
            task = _inFlight ??= Task.Run(ProbeAsync, CancellationToken.None);
        }

        return await task.WaitAsync(cancellationToken);
    }

    private async Task<bool> ProbeAsync()
    {
        bool value;
        try
        {
            using var cts = new CancellationTokenSource(timeout, time);
            var probe = Task.Run(() => check(cts.Token), CancellationToken.None);
            // Проверка может не реагировать на токен (блокирующий вызов клиента): предел держим сами.
            using var limitCts = new CancellationTokenSource();
            var limit = Task.Delay(timeout, time, limitCts.Token);
            var first = await Task.WhenAny(probe, limit);
            await limitCts.CancelAsync();
            if (first != probe)
            {
                await cts.CancelAsync();
                // Наблюдаем исход брошенной проверки, чтобы её исключение не осталось необработанным.
                _ = probe.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);
                value = false;
            }
            else
            {
                await probe;
                value = true;
            }
        }
        catch (Exception ex)
        {
            onFailure?.Invoke(ex);
            value = false;
        }

        lock (_gate)
        {
            _cachedValue = value;
            _cachedUntil = time.GetUtcNow() + cacheTtl;
            _inFlight = null;
        }

        return value;
    }
}

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Пропускает не чаще одного сообщения на ключ за интервал; число подавленных между пропусками возвращается
/// вместе со следующим пропущенным. Потокобезопасен (вызывается с потоков librdkafka).
/// </summary>
internal sealed class LogThrottle(TimeProvider timeProvider, TimeSpan interval)
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, (long Passed, int Suppressed)> _state = [];

    /// <summary>true - сообщение нужно записать; <paramref name="suppressed"/> - сколько таких же было подавлено до него.</summary>
    public bool TryPass(string key, out int suppressed)
    {
        var now = timeProvider.GetTimestamp();
        lock (_gate)
        {
            if (_state.TryGetValue(key, out var entry) && timeProvider.GetElapsedTime(entry.Passed, now) < interval)
            {
                _state[key] = (entry.Passed, entry.Suppressed + 1);
                suppressed = 0;
                return false;
            }

            suppressed = _state.TryGetValue(key, out entry) ? entry.Suppressed : 0;
            _state[key] = (now, 0);
            return true;
        }
    }
}

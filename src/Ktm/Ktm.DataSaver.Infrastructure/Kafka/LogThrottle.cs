namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Пропускает не чаще одного сообщения на ключ за интервал; число подавленных между пропусками возвращается
/// вместе со следующим пропущенным. Потокобезопасен (вызывается с потоков librdkafka). Один экземпляр на процесс
/// (singleton в composition root): consumer, producer и обработчики ошибок делят общий лимит на ключ.
/// Ключи составляются из источника и кода (например, <c>native:FAIL</c>, <c>error:Local_Transport</c>).
/// </summary>
public sealed class LogThrottle(TimeProvider timeProvider, TimeSpan interval)
{
    /// <summary>Интервал по умолчанию: не чаще раза в 30 с на ключ.</summary>
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromSeconds(30);

    /// <summary>Размер словаря, с которого устаревшие ключи вытесняются (ключи - малый набор кодов, рост не ожидается).</summary>
    internal const int EvictionThreshold = 256;

    /// <summary>Число отслеживаемых ключей (для проверки вытеснения).</summary>
    internal int TrackedKeys
    {
        get
        {
            lock (_gate)
            {
                return _state.Count;
            }
        }
    }

    private readonly Lock _gate = new();
    private readonly Dictionary<string, (long Passed, int Suppressed)> _state = [];

    /// <summary>true - сообщение нужно записать; <paramref name="suppressed"/> - сколько таких же было подавлено до него.</summary>
    public bool TryPass(string key, out int suppressed)
    {
        lock (_gate)
        {
            var now = timeProvider.GetTimestamp();
            if (_state.TryGetValue(key, out var entry) && timeProvider.GetElapsedTime(entry.Passed, now) < interval)
            {
                _state[key] = (entry.Passed, entry.Suppressed + 1);
                suppressed = 0;
                return false;
            }

            suppressed = _state.TryGetValue(key, out entry) ? entry.Suppressed : 0;
            if (_state.Count >= EvictionThreshold)
            {
                EvictStale(now);
            }

            _state[key] = (now, 0);
            return true;
        }
    }

    /// <summary>Устаревший ключ (окно истекло) проходит как новый, поэтому его запись можно забыть; счётчик подавленных теряется.</summary>
    private void EvictStale(long now)
    {
        var stale = _state.Where(e => timeProvider.GetElapsedTime(e.Value.Passed, now) >= interval).Select(e => e.Key).ToList();
        foreach (var key in stale)
        {
            _state.Remove(key);
        }
    }
}

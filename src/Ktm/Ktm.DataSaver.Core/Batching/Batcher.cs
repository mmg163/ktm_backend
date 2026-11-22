namespace Ktm.DataSaver.Core.Batching;

/// <summary>
/// Накопитель пачки: закрывается по размеру или по истечении максимального времени ожидания,
/// отсчитываемого от первого сообщения пачки. Пустая пачка не закрывается. Не потокобезопасен.
/// </summary>
public sealed class Batcher<T>
{
    private readonly int _maxSize;
    private readonly TimeSpan _maxWait;
    private readonly TimeProvider _timeProvider;
    private readonly List<T> _items = [];
    private DateTimeOffset _firstAddedAt;

    public Batcher(int maxSize, TimeSpan maxWait, TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSize, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(maxWait, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _maxSize = maxSize;
        _maxWait = maxWait;
        _timeProvider = timeProvider;
    }

    public int Count => _items.Count;

    public void Add(T item)
    {
        if (_items.Count == 0)
        {
            _firstAddedAt = _timeProvider.GetUtcNow();
        }

        _items.Add(item);
    }

    /// <summary>Пачка готова к закрытию: достигнут размер или истекло время от первого сообщения.</summary>
    public bool IsReady =>
        _items.Count > 0
        && (_items.Count >= _maxSize || _timeProvider.GetUtcNow() - _firstAddedAt >= _maxWait);

    /// <summary>Сколько ещё ждать до закрытия по времени (для таймаута Consume); null, если пачка пуста.</summary>
    public TimeSpan? TimeUntilDeadline()
    {
        if (_items.Count == 0)
        {
            return null;
        }

        var left = _maxWait - (_timeProvider.GetUtcNow() - _firstAddedAt);
        return left > TimeSpan.Zero ? left : TimeSpan.Zero;
    }

    /// <summary>Забирает накопленное и начинает новую пачку. Возвращает null, если пачка не готова.</summary>
    public IReadOnlyList<T>? TryTakeReady() => IsReady ? Drain() : null;

    /// <summary>Принудительно забирает накопленное (graceful shutdown). Пустую пачку не отдаёт.</summary>
    public IReadOnlyList<T>? TakeAll() => _items.Count == 0 ? null : Drain();

    /// <summary>Убирает накопленные элементы по условию (отзыв партиций). Время закрытия пачки не меняется:
    /// отсчёт идёт от первого добавленного сообщения (индивидуальные времена не хранятся); это лишь раньше закроет пачку,
    /// что безопасно. Пустой накопитель начинает отсчёт заново при следующем Add.</summary>
    public int RemoveWhere(Predicate<T> match) => _items.RemoveAll(match);

    private IReadOnlyList<T> Drain()
    {
        var batch = _items.ToArray();
        _items.Clear();
        return batch;
    }
}

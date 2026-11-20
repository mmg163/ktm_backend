namespace Ktm.DataSaver.Core.Batching;

/// <summary>
/// Трекер offset пачки. Учитывает все сообщения пачки, включая отбракованные (иначе отравленное сообщение
/// блокирует партицию). Не потокобезопасен: используется из одного потока чтения.
/// </summary>
public sealed class OffsetTracker
{
    private readonly Dictionary<TopicPartitionKey, long> _lastOffsets = new();

    public bool IsEmpty => _lastOffsets.Count == 0;

    public void Track(PartitionOffset position)
    {
        var key = new TopicPartitionKey(position.Topic, position.Partition);
        if (!_lastOffsets.TryGetValue(key, out var last) || position.Offset > last)
        {
            _lastOffsets[key] = position.Offset;
        }
    }

    /// <summary>Offset для commit: по каждой партиции последний offset пачки + 1.</summary>
    public IReadOnlyList<PartitionOffset> GetOffsetsToCommit() =>
        _lastOffsets
            .Select(p => new PartitionOffset(p.Key.Topic, p.Key.Partition, p.Value + 1))
            .ToList();

    /// <summary>Сброс после успешного commit.</summary>
    public void Reset() => _lastOffsets.Clear();

    /// <summary>Удаление партиций при отзыве (rebalance): их offset больше не коммитится этим экземпляром.</summary>
    public void Revoke(IEnumerable<TopicPartitionKey> partitions)
    {
        foreach (var partition in partitions)
        {
            _lastOffsets.Remove(partition);
        }
    }
}

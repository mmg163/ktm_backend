using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Ingestion;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>Синхронный commit offset+1 через consumer источника. Ошибка (в т.ч. потеря партиции) - исключение; логирует цикл.</summary>
public sealed class KafkaOffsetCommitter(KafkaMessageSource source) : IOffsetCommitter
{
    public ValueTask CommitAsync(IReadOnlyList<PartitionOffset> offsets, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (offsets.Count > 0)
        {
            source.Commit(offsets);
        }

        return ValueTask.CompletedTask;
    }
}

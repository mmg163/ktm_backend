using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>Декоратор порта чтения: считает прочитанные сообщения; остальное делегирует.</summary>
public sealed class MeteredMessageSource(IMessageSource inner, DataSaverMetrics metrics) : IMessageSource
{
    public IReadOnlyCollection<TopicPartitionKey> Assignment => inner.Assignment;

    public async ValueTask<ConsumedMessage?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var message = await inner.ConsumeAsync(timeout, cancellationToken);
        if (message is not null)
        {
            metrics.MessageRead();
        }

        return message;
    }

    public AssignmentChanges TakeAssignmentChanges() => inner.TakeAssignmentChanges();

    public void Pause(IReadOnlyCollection<TopicPartitionKey> partitions) => inner.Pause(partitions);

    public void Resume(IReadOnlyCollection<TopicPartitionKey> partitions) => inner.Resume(partitions);
}

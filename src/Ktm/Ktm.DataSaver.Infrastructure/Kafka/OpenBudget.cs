using Ktm.DataSaver.Core.Reprocessing;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Общий срок открытия источника (ADR-0006 п. 2.7): метаданные, watermark по партициям, поиск по времени и чтение первых
/// сообщений делят один бюджет, а не ждут по <c>N * perCall</c>. Токен проверяется перед каждым блокирующим вызовом;
/// сам вызов librdkafka прервать нельзя, поэтому отмена видна не позднее чем через <c>perCall</c>.
/// </summary>
internal sealed class OpenBudget(TimeProvider time, TimeSpan total, TimeSpan perCall, CancellationToken token)
{
    private readonly long _started = time.GetTimestamp();

    /// <summary>Таймаут для следующего блокирующего вызова: не больше <c>perCall</c> и не больше остатка общего срока.</summary>
    /// <exception cref="OperationCanceledException">Токен отменён.</exception>
    /// <exception cref="ReplaySourceException">Общий срок исчерпан.</exception>
    public TimeSpan NextTimeout()
    {
        token.ThrowIfCancellationRequested();
        var left = total - time.GetElapsedTime(_started);
        if (left <= TimeSpan.Zero)
        {
            throw new ReplaySourceException("Kafka: open limit exceeded");
        }

        return left < perCall ? left : perCall;
    }
}

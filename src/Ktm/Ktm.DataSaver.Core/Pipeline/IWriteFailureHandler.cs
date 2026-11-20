using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Core.Pipeline;

public enum WriteFailureKind
{
    Retry,
    DeadLetter,
}

/// <summary>Решение порта C6: повторить пачку либо увести перечисленные сообщения в отдельное хранилище.</summary>
public sealed record WriteFailureDecision
{
    private WriteFailureDecision(WriteFailureKind kind, IReadOnlyList<ConsumedMessage> items)
    {
        Kind = kind;
        DeadLetterItems = items;
    }

    public static WriteFailureDecision Retry { get; } = new(WriteFailureKind.Retry, []);

    public WriteFailureKind Kind { get; }

    /// <summary>Сообщения, признанные потерянными (только при <see cref="WriteFailureKind.DeadLetter"/>).</summary>
    public IReadOnlyList<ConsumedMessage> DeadLetterItems { get; }

    public static WriteFailureDecision DeadLetter(IReadOnlyList<ConsumedMessage> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        return new WriteFailureDecision(WriteFailureKind.DeadLetter, items);
    }
}

/// <summary>Контекст ошибки записи класса «данные»: пачка, ошибка, номер попытки (с 1).</summary>
public sealed record WriteFailureContext(IReadOnlyList<ConsumedMessage> Batch, Exception Error, int Attempt);

/// <summary>Порт C6 (обработка ошибок записи). Вызывается только для класса «данные» (ADR-0002, D-7).</summary>
public interface IWriteFailureHandler
{
    ValueTask<WriteFailureDecision> HandleAsync(WriteFailureContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Заглушка до появления C6: всегда <see cref="WriteFailureDecision.Retry"/>. Ничего не теряет и не пропускает,
/// блокирует партицию до устранения причины. Пауза, <c>Degraded</c> и лог Error - ответственность цикла.
/// </summary>
public sealed class BlockingWriteFailureHandler : IWriteFailureHandler
{
    public ValueTask<WriteFailureDecision> HandleAsync(WriteFailureContext context, CancellationToken cancellationToken) =>
        ValueTask.FromResult(WriteFailureDecision.Retry);
}

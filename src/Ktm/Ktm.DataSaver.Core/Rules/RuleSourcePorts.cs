using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>Ошибка отдельного правила, найденная источником (строка не отобразилась в <see cref="RuleDefinition"/>).</summary>
public sealed record RuleProblem(string? RuleId, string Location, string Message, int? RuleVersion = null);

/// <summary>
/// Результат чтения источника. <c>Unchanged = true</c> - набор совпадает с ревизией <c>previousRevision</c>
/// (<see cref="Rules"/> и <see cref="Problems"/> пусты); иначе - полный новый набор (ADR-0005 п. 2.1).
/// </summary>
public sealed record RuleSourceResult(
    bool Unchanged,
    string Revision,
    IReadOnlyList<RuleDefinition> Rules,
    IReadOnlyList<RuleProblem> Problems)
{
    public static RuleSourceResult NotChanged(string revision) => new(true, revision, [], []);

    public static RuleSourceResult Changed(string revision, IReadOnlyList<RuleDefinition> rules, IReadOnlyList<RuleProblem>? problems = null) =>
        new(false, revision, rules, problems ?? []);
}

/// <summary>Класс сбоя источника правил: решает, повторять ли чтение (<c>Unavailable</c>) или это ошибка развёртывания.</summary>
public enum RuleSourceFailureKind
{
    /// <summary>Источник временно недоступен (сеть, таймаут): повторить, действует прежний набор.</summary>
    Unavailable,

    /// <summary>Источник вернул непригодный набор целиком.</summary>
    Invalid,

    /// <summary>Ошибка конфигурации/развёртывания (нет таблицы, нет прав): на первой загрузке - код выхода 78.</summary>
    Configuration,
}

/// <summary>Невозможность получить пригодный набор правил. Сообщение не содержит строки подключения и значений секретов.</summary>
public sealed class RuleSourceException(RuleSourceFailureKind kind, string message, Exception? inner = null) : Exception(message, inner)
{
    public RuleSourceFailureKind Kind { get; } = kind;
}

/// <summary>Порт источника правил: движок знает только <see cref="RuleDefinition"/>, а не хранилище.</summary>
public interface IRuleSource
{
    /// <param name="previousRevision">Ревизия последнего применённого набора; <c>null</c> на первой загрузке.</param>
    /// <exception cref="RuleSourceException">Источник недоступен или набор непригоден.</exception>
    ValueTask<RuleSourceResult> ReadAsync(string? previousRevision, CancellationToken cancellationToken);
}

/// <summary>Подсказка сигнала: «возможно, правила изменились» (причину ставит реализация, для лога).</summary>
public sealed record RuleChangeHint(string Reason);

/// <summary>Необязательный ускоритель перезагрузки (например, <c>LISTEN/NOTIFY</c>); в первой итерации реализации нет.</summary>
public interface IRuleChangeSignal
{
    IAsyncEnumerable<RuleChangeHint> WatchAsync(CancellationToken cancellationToken);
}

/// <summary>Что вызвало очередную загрузку (тег метрики <c>trigger</c>).</summary>
public enum RuleRefreshTrigger
{
    Timer,
    Signal,
}

/// <summary>Поток поводов для загрузки: таймер плюс необязательный сигнал; сигналы за время загрузки схлопываются в один.</summary>
public static class RuleRefreshTriggers
{
    /// <summary>
    /// Выдаёт повод по истечении <paramref name="interval"/> либо по сигналу (что раньше). Завершается исключением отмены.
    /// Сбой сигнала передаётся потребителю исключением: ускоритель не должен падать молча.
    /// </summary>
    public static async IAsyncEnumerable<RuleRefreshTrigger> WatchAsync(
        TimeSpan interval,
        TimeProvider time,
        IRuleChangeSignal? signal,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<RuleRefreshTrigger>(new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
        });
        using var pumpStop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task? pump = null;
        if (signal is not null)
        {
            pump = Task.Run(async () =>
            {
                try
                {
                    await foreach (var _ in signal.WatchAsync(pumpStop.Token))
                    {
                        channel.Writer.TryWrite(RuleRefreshTrigger.Signal);
                    }
                }
                catch (OperationCanceledException) when (pumpStop.IsCancellationRequested)
                {
                    // остановка потребителя
                }
                catch (Exception ex)
                {
                    channel.Writer.TryComplete(ex);
                }
            }, CancellationToken.None);
        }

        try
        {
            while (true)
            {
                RuleRefreshTrigger trigger;
                using (var timeout = new CancellationTokenSource(interval, time))
                using (var wait = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token))
                {
                    try
                    {
                        trigger = await channel.Reader.ReadAsync(wait.Token);
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        trigger = RuleRefreshTrigger.Timer;
                    }
                }

                yield return trigger;
            }
        }
        finally
        {
            await pumpStop.CancelAsync();
            if (pump is not null)
            {
                await pump;
            }
        }
    }
}

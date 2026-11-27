using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Resilience;

/// <summary>Ядовитый элемент: ошибка записи класса Data или Unexpected на одиночном сообщении.</summary>
public sealed record IsolatedPoison<T>(T Item, WriteErrorClass Class, Exception Error);

public enum IsolationOutcome
{
    /// <summary>Каждый элемент либо записан, либо признан ядовитым.</summary>
    Completed,

    /// <summary>Ошибка инфраструктуры (не Data и не Unexpected): <c>Remaining</c> нужно передать повторно после восстановления.</summary>
    Interrupted,

    /// <summary>Отмена токеном: <c>Remaining</c> не обработан.</summary>
    Cancelled,
}

/// <summary>Результат изоляции: записанное, ядовитое и необработанный остаток (при прерывании).</summary>
public sealed record IsolationResult<T>(
    IReadOnlyList<T> Written,
    IReadOnlyList<IsolatedPoison<T>> Poison,
    IReadOnlyList<T> Remaining,
    IsolationOutcome Outcome,
    Exception? InterruptedBy);

/// <summary>
/// Изоляция «ядовитого» сообщения (ADR-0006 п. 2.3, design Р-4): пачка, которую БД не принимает, делится пополам; каждая половина
/// пишется отдельной транзакцией тем же <see cref="IEventStore"/> и тем же <c>receivedAt</c> (идемпотентность делает успешные
/// части безопасными). Успешная часть записана и дальше не пишется, упавшая делится до одиночных сообщений; одиночное сообщение
/// с ошибкой класса Data или Unexpected - ядовитое. При одном ядовитом из N записей не больше <c>2 * ceil(log2 N)</c>
/// (корневая пачка заведомо упала и целиком не пишется; одиночное сообщение подтверждается записью). Ошибка инфраструктуры
/// прерывает изоляцию. Ничего не логирует и не знает о Kafka.
/// </summary>
public sealed class PoisonIsolator(IEventStore store)
{
    private sealed record Segment<T>(IReadOnlyList<T> Items, bool KnownFailed);

    /// <param name="items">Сообщения пачки, которую БД отвергла.</param>
    /// <param name="eventOf">Событие для записи.</param>
    /// <param name="onWritten">Вызывается после каждой успешной записи части (до следующей записи).</param>
    /// <param name="touch">Вызывается после каждой записи (успешной или нет): отметка живости цикла.</param>
    /// <param name="onPoison">
    /// Вызывается сразу после признания сообщения ядовитым (до следующей записи). Порядок обхода совпадает с порядком пачки,
    /// поэтому <paramref name="onWritten"/> и <paramref name="onPoison"/> приходят в том порядке, в котором сообщения идут в пачке.
    /// </param>
    /// <param name="failedWrites">
    /// Счётчик неудавшихся записей на событие (ключ - ссылка на событие): увеличивается для каждого события неудавшейся части
    /// класса Data/Unexpected. Инфраструктурная ошибка и отмена не считаются. Словарь можно передавать в следующий вызов
    /// (изоляция, прерванная инфраструктурой, не теряет счёт).
    /// </param>
    public async ValueTask<IsolationResult<T>> IsolateAsync<T>(
        IReadOnlyList<T> items,
        Func<T, ParsedEvent> eventOf,
        DateTimeOffset receivedAt,
        Action<IReadOnlyList<T>>? onWritten,
        Action? touch,
        CancellationToken cancellationToken,
        Action<IsolatedPoison<T>>? onPoison = null,
        IDictionary<ParsedEvent, int>? failedWrites = null)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(eventOf);

        var written = new List<T>();
        var poison = new List<IsolatedPoison<T>>();
        var pending = new Stack<Segment<T>>();
        if (items.Count > 0)
        {
            pending.Push(new Segment<T>(items, KnownFailed: items.Count > 1));
        }

        while (pending.Count > 0)
        {
            var segment = pending.Pop();
            if (segment.KnownFailed)
            {
                Split(segment.Items, pending);
                continue;
            }

            Exception error;
            try
            {
                await store.WriteAsync(segment.Items.Select(eventOf).ToList(), receivedAt, cancellationToken);
                touch?.Invoke();
                written.AddRange(segment.Items);
                onWritten?.Invoke(segment.Items);
                continue;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return Interrupted(segment, pending, written, poison, IsolationOutcome.Cancelled, null);
            }
            catch (Exception ex)
            {
                error = ex;
            }

            touch?.Invoke();
            var errorClass = WriteErrorClassifier.Classify(error);
            if (errorClass is not (WriteErrorClass.Data or WriteErrorClass.Unexpected))
            {
                return Interrupted(segment, pending, written, poison, IsolationOutcome.Interrupted, error);
            }

            if (failedWrites is not null)
            {
                foreach (var item in segment.Items)
                {
                    var key = eventOf(item);
                    failedWrites[key] = failedWrites.TryGetValue(key, out var count) ? count + 1 : 1;
                }
            }

            if (segment.Items.Count == 1)
            {
                var isolated = new IsolatedPoison<T>(segment.Items[0], errorClass, error);
                poison.Add(isolated);
                onPoison?.Invoke(isolated);
            }
            else
            {
                Split(segment.Items, pending);
            }
        }

        return new IsolationResult<T>(written, poison, [], IsolationOutcome.Completed, null);
    }

    private static IsolationResult<T> Interrupted<T>(
        Segment<T> current,
        Stack<Segment<T>> pending,
        List<T> written,
        List<IsolatedPoison<T>> poison,
        IsolationOutcome outcome,
        Exception? interruptedBy)
    {
        var remaining = new List<T>(current.Items);
        foreach (var rest in pending)
        {
            remaining.AddRange(rest.Items);
        }

        return new IsolationResult<T>(written, poison, remaining, outcome, interruptedBy);
    }

    private static void Split<T>(IReadOnlyList<T> items, Stack<Segment<T>> pending)
    {
        var middle = items.Count / 2;
        // Вторая половина кладётся первой: первая обрабатывается раньше (порядок записи как в пачке).
        pending.Push(new Segment<T>(items.Skip(middle).ToList(), KnownFailed: false));
        pending.Push(new Segment<T>(items.Take(middle).ToList(), KnownFailed: false));
    }
}

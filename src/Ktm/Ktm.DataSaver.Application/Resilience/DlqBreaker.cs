namespace Ktm.DataSaver.Core.Resilience;

/// <summary>
/// Предохранитель dlq (ADR-0006 п. 2.3, 6): считает сообщения, ушедшие в dlq подряд, без успешной записи события между ними.
/// Достигнув порога, считается открытым: dlq не используется, пока одна запись не пройдёт успешно. Потокобезопасен
/// (читается из потока метрик).
/// </summary>
public sealed class DlqBreaker
{
    private readonly int _threshold;
    private int _count;

    public DlqBreaker(int threshold)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(threshold, 1);
        _threshold = threshold;
    }

    /// <summary>Открыт: подряд ушло не меньше порога сообщений.</summary>
    public bool IsOpen => Volatile.Read(ref _count) >= _threshold;

    /// <summary>Сколько сообщений подряд ушло в dlq с последней успешной записи.</summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>Учитывает <paramref name="count"/> сообщений, признанных ядовитыми; 0 и меньше состояние не меняют.</summary>
    public void Register(int count)
    {
        if (count <= 0)
        {
            return;
        }

        int current, next;
        do
        {
            current = Volatile.Read(ref _count);
            next = (int)Math.Min((long)current + count, int.MaxValue);
        }
        while (Interlocked.CompareExchange(ref _count, next, current) != current);
    }

    /// <summary>
    /// Успешная запись <paramref name="events"/> событий закрывает предохранитель и обнуляет счётчик. Запись пустой пачки
    /// (одни отбракованные) - не успех записи и счётчик не сбрасывает.
    /// </summary>
    public void OnWriteSucceeded(int events)
    {
        if (events > 0)
        {
            Volatile.Write(ref _count, 0);
        }
    }
}

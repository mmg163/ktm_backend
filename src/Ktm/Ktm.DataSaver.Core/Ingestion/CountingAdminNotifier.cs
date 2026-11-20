namespace Ktm.DataSaver.Core.Ingestion;

/// <summary>Заглушка: только считает оповещения; видимость обеспечивает Error-лог цикла.</summary>
public sealed class CountingAdminNotifier : IAdminNotifier
{
    private long _count;

    public long Count => Interlocked.Read(ref _count);

    public void NotifyFatalConfig(Exception error, int attempt) => Interlocked.Increment(ref _count);
}

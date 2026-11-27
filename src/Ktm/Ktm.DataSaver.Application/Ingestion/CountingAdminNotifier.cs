namespace Ktm.DataSaver.Application.Ingestion;

/// <summary>Заглушка: только считает оповещения по видам; видимость обеспечивает Error-лог цикла.</summary>
public sealed class CountingAdminNotifier : IAdminNotifier
{
    private long _fatalConfigCalls;
    private long _breakerCalls;

    /// <summary>Оповещения о фатальной ошибке конфигурации/прав.</summary>
    public long FatalConfigCalls => Interlocked.Read(ref _fatalConfigCalls);

    /// <summary>Оповещения об открытом предохранителе dlq.</summary>
    public long BreakerCalls => Interlocked.Read(ref _breakerCalls);

    public void NotifyFatalConfig(Exception error, int attempt) => Interlocked.Increment(ref _fatalConfigCalls);

    public void NotifyDeadLetterBreakerOpen(Exception error, int attempt) => Interlocked.Increment(ref _breakerCalls);
}

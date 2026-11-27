using Ktm.DataSaver.Application.Ingestion;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Observability;

/// <summary>Заглушка канала оповещений (6.5): Error-лог и счётчик; реальный канал заменит реализацию порта.</summary>
public sealed class LoggingAdminNotifier(DataSaverMetrics metrics, ILogger<LoggingAdminNotifier> logger) : IAdminNotifier
{
    public void NotifyFatalConfig(Exception error, int attempt)
    {
        metrics.AdminNotified();
        logger.LogError("Administrator notification: fatal configuration/permission write error {ErrorType}, attempt {Attempt}", error.GetType().Name, attempt);
    }

    public void NotifyDeadLetterBreakerOpen(Exception error, int attempt)
    {
        metrics.AdminNotified();
        logger.LogError(
            "Administrator notification: dead letter breaker is open, messages are retried without limit, {ErrorType}, attempt {Attempt}",
            error.GetType().Name, attempt);
    }
}

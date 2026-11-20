using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Фатальная ошибка клиента Kafka (<see cref="Error.IsFatal"/>) не лечится ретраями: процесс завершается с
/// ненулевым кодом, оркестратор перезапускает его. Нефатальные ошибки только логируются (клиент восстановится сам).
/// Регистрируется как singleton: один экземпляр на consumer и producer, защита от повторного вызова - внутри.
/// </summary>
public sealed class KafkaFatalErrorHandler(IProcessTerminator terminator, ILogger<KafkaFatalErrorHandler> logger)
{
    public const int FatalExitCode = 70;

    private int _terminated;

    /// <summary>true - ошибка была фатальной и запрошено завершение.</summary>
    public bool Handle(Error error)
    {
        if (!error.IsFatal)
        {
            logger.LogWarning("Kafka client error: {Code} {Reason}", error.Code, error.Reason);
            return false;
        }

        logger.LogCritical("Kafka fatal error: {Code} {Reason}; process is terminating", error.Code, error.Reason);
        if (Interlocked.Exchange(ref _terminated, 1) == 0)
        {
            terminator.Terminate(FatalExitCode, $"Kafka fatal error {error.Code}: {error.Reason}");
        }

        return true;
    }
}

using Ktm.DataSaver.Infrastructure.Kafka;

namespace Ktm.HostedServices.Hosting;

/// <summary>
/// Завершение процесса при фатальной ошибке: выставляет код выхода и просит host остановиться штатно (дописать пачку,
/// закрыть consumer). Идемпотентна: первый вызов определяет код выхода, остальные игнорируются (вызов возможен из
/// потоков librdkafka).
/// </summary>
public sealed class HostProcessTerminator(
    IHostApplicationLifetime lifetime,
    ILogger<HostProcessTerminator> logger,
    Action<int>? setExitCode = null) : IProcessTerminator
{
    private readonly Action<int> _setExitCode = setExitCode ?? (code => Environment.ExitCode = code);
    private int _requested;

    public void Terminate(int exitCode, string reason)
    {
        if (Interlocked.Exchange(ref _requested, 1) != 0)
        {
            return;
        }

        logger.LogCritical("Process termination requested with exit code {ExitCode}: {Reason}", exitCode, reason);
        _setExitCode(exitCode);
        lifetime.StopApplication();
    }
}

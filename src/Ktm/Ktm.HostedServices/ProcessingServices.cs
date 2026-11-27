namespace Ktm.Hosting;

internal sealed class BindingService(ILogger<BindingService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Binding worker registered");
        return Task.Delay(Timeout.Infinite, stoppingToken);
    }
}

internal sealed class InterpretationService(ILogger<InterpretationService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        logger.LogInformation("Interpretation worker registered");
        return Task.Delay(Timeout.Infinite, stoppingToken);
    }
}

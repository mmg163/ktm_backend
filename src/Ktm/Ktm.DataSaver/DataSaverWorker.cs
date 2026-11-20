namespace Ktm.DataSaver;

/// <summary>Каркас фонового обработчика; цикл приёма добавляется задачами группы 3-6.</summary>
public sealed class DataSaverWorker : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.Delay(Timeout.Infinite, stoppingToken);
}

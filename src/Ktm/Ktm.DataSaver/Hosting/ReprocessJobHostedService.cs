using Ktm.DataSaver.Core.Reprocessing;

namespace Ktm.DataSaver.Hosting;

/// <summary>
/// Привязка задач повторной обработки к жизненному циклу host (ADR-0006 п. 2.6): при остановке отменяет активную задачу и ждёт её
/// завершения в пределах <c>HostOptions.ShutdownTimeout</c>. Offset при повторной обработке не коммитится, поэтому отмена безопасна.
/// Запуск задач - только по HTTP; при старте ничего не делает.
/// </summary>
public sealed class ReprocessJobHostedService(ReprocessJobManager manager) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => manager.StopAsync(cancellationToken);
}

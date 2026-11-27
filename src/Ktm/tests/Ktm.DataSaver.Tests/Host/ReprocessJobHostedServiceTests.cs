using Ktm.DataSaver.Application.Reprocessing;
using Ktm.DataSaver.Tests.Admin;
using Ktm.HostedServices.Hosting;

namespace Ktm.DataSaver.Tests.Host;

/// <summary>Остановка host отменяет активную задачу повторной обработки и дожидается её (ADR-0006 п. 2.6).</summary>
public sealed class ReprocessJobHostedServiceTests
{
    [Fact]
    public async Task StopAsync_CancelsActiveJob_AndWaitsForIt()
    {
        var (open, release) = AdminTestHost.BlockedSource(messages: 6);
        await using var host = await AdminTestHost.StartAsync(open);
        var id = await host.StartJobAsync();
        var service = new ReprocessJobHostedService(host.Manager);

        // Читатель отпускается после запроса остановки: задача должна завершиться отменой, а не доработать источник.
        var stopping = service.StopAsync(CancellationToken.None);
        release.SetResult();
        await stopping.WaitAsync(TimeSpan.FromSeconds(10));

        var job = host.Manager.Get(id)!;
        Assert.Equal(ReprocessJobState.Cancelled, job.State);
        Assert.Equal(ReprocessOutcome.Cancelled, job.Outcome);
    }
}

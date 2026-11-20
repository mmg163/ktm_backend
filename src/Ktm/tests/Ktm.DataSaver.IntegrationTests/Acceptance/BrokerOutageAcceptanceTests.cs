using System.Net;
using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Health;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.IntegrationTests.Host;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Testcontainers.Kafka;
using Xunit.Abstractions;

namespace Ktm.DataSaver.IntegrationTests.Acceptance;

/// <summary>Собственный брокер для тестов, которые его замораживают (docker pause): общий контейнер остальным тестам нужен живым.</summary>
public sealed class PausableKafkaFixture : IAsyncLifetime
{
    private readonly KafkaContainer _container = new KafkaBuilder("apache/kafka:3.9.1").Build();

    public string BootstrapServers => _container.GetBootstrapAddress();

    public Task InitializeAsync() => _container.StartAsync();

    public async Task DisposeAsync() => await _container.DisposeAsync();

    public Task PauseAsync() => _container.PauseAsync();

    public Task UnpauseAsync() => _container.UnpauseAsync();
}

/// <summary>Недоступность и возвращение брокера Kafka: readiness и поведение commit.</summary>
[Collection(AppCollection.Name)]
public sealed class BrokerOutageAcceptanceTests(PausableKafkaFixture broker, PostgresFixture postgres, ITestOutputHelper output) : IClassFixture<PausableKafkaFixture>
{
    // Хвост группы 6: брокер возвращается -> ready снова 200, приём продолжается. Заморозка контейнера сохраняет адрес и порт.
    [Fact]
    public async Task BrokerPausedThenResumed_ReadinessGoesBrokerUnavailable_ThenBack200_AndIngestionContinues()
    {
        var run = await TestRun.CreateAsync(broker.BootstrapServers, postgres);
        // Назначение должно пережить заморозку дольше окна проверки: иначе ready скажет no_partitions_assigned.
        await using var app = DataSaverInstance.Start(run, s => s["DataSaver:Kafka:SessionTimeoutMs"] = "60000");
        await app.WaitReadyAsync();
        await run.PublishEventsAsync(0, 1);
        await Wait.UntilAsync(async () => await run.RowsAsync() == 1, "event before the outage");

        await broker.PauseAsync();
        try
        {
            var window = HealthSignals.BrokerCacheTtl + HealthSignals.ProbeTimeout + TimeSpan.FromSeconds(10);
            var body = string.Empty;
            await Wait.UntilAsync(async () =>
            {
                using var response = await app.Client.GetAsync("/health/ready");
                body = await response.Content.ReadAsStringAsync();
                return response.StatusCode == HttpStatusCode.ServiceUnavailable;
            }, "readiness 503 while the broker is frozen", window);
            Assert.Contains("broker_unavailable", body);
            Assert.Equal(HttpStatusCode.OK, (await app.Client.GetAsync("/health/live")).StatusCode);
        }
        finally
        {
            await broker.UnpauseAsync();
        }

        await app.WaitReadyAsync(TimeSpan.FromSeconds(60));
        await run.PublishEventsAsync(1, 1);
        await Wait.UntilAsync(async () => await run.RowsAsync() == 2 && await run.CommittedAsync() == 2, "ingestion continues after the broker is back", TimeSpan.FromSeconds(60));
        Assert.Equal(0, app.Terminator.Calls);
    }

    // Хвост 7.5/7.6: Commit в Kafka блокирующий и без собственного таймаута. Замороженный брокер подвешивает цикл
    // (на замере >150 с, пока брокер не ответит). Проверяем то, что обязано выполняться при этом: зависание видно
    // как liveness 503, остановка host ограничена его пределом, события не теряются и не дублируются.
    // Ограничение Commit бюджетом остановки (D-8) отложено решением лида: тест фиксирует границы, а не его отсутствие.
    [Fact]
    [Trait("Category", "Long")]
    public async Task CommitAgainstFrozenBroker_HangIsVisibleAsLiveness503_HostStopIsBounded_NoLossOnRestart()
    {
        var run = await TestRun.CreateAsync(broker.BootstrapServers, postgres);
        var gate = new CommitGate();
        await using var app = DataSaverInstance.Start(run,
            tune: s =>
            {
                s["DataSaver:Health:LiveStaleSeconds"] = "10";
                s["DataSaver:Postgres:ConnectTimeoutSeconds"] = "2";
                s["DataSaver:Postgres:CommandTimeoutSeconds"] = "5";
            },
            services: s =>
            {
                s.RemoveAll<IOffsetCommitter>();
                s.AddSingleton<IOffsetCommitter>(sp => new GatedCommitter(new KafkaOffsetCommitter(sp.GetRequiredService<KafkaMessageSource>()), gate));
            });
        await app.WaitReadyAsync();

        await run.PublishEventsAsync(0, 1);
        await gate.Reached.Task.WaitAsync(Wait.Default);
        Assert.Equal(1L, await run.RowsAsync());

        await broker.PauseAsync();
        try
        {
            gate.Release.SetResult();
            await Wait.UntilAsync(async () => (await app.Client.GetAsync("/health/live")).StatusCode == HttpStatusCode.ServiceUnavailable,
                "liveness 503 while Commit hangs on the frozen broker", TimeSpan.FromSeconds(60));
            Assert.False(gate.Finished.Task.IsCompleted, "Commit must still be blocked while the broker is frozen");

            // Предел host = Shutdown__TimeoutSeconds (10) + запас (5); остановка не висит дольше.
            var stop = await app.StopAsync();
            output.WriteLine($"Host stop with a hung Commit took {stop}");
            Assert.InRange(stop, TimeSpan.Zero, TimeSpan.FromSeconds(20));
        }
        finally
        {
            await broker.UnpauseAsync();
        }

        await gate.Finished.Task.WaitAsync(TimeSpan.FromSeconds(120));
        output.WriteLine($"Commit outcome after the broker came back: {gate.Outcome}");

        // Повторный запуск в той же группе: событие записано один раз, offset зафиксирован.
        await using var restarted = DataSaverInstance.Start(run);
        await Wait.UntilAsync(async () => await run.CommittedAsync() == 1, "offset committed after restart", TimeSpan.FromSeconds(60));
        Assert.Equal(1L, await run.RowsAsync());
        Assert.Equal(0L, await run.DuplicateKeyGroupsAsync());
    }

    private sealed class CommitGate
    {
        public TaskCompletionSource Reached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Finished { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string Outcome { get; set; } = "?";
    }

    private sealed class GatedCommitter(IOffsetCommitter inner, CommitGate gate) : IOffsetCommitter
    {
        public async ValueTask CommitAsync(IReadOnlyList<PartitionOffset> offsets, CancellationToken cancellationToken)
        {
            gate.Reached.TrySetResult();
            await gate.Release.Task;
            try
            {
                await inner.CommitAsync(offsets, cancellationToken);
                gate.Outcome = "success";
            }
            catch (Exception ex)
            {
                gate.Outcome = ex.GetType().Name + ": " + ex.Message;
                throw;
            }
            finally
            {
                gate.Finished.TrySetResult();
            }
        }
    }
}

using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.IntegrationTests.Host;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>
/// Экземпляр Data Saver in-process (фабрика экземпляра, 7.1). Метрики, логи и вызовы завершения процесса перехватываются
/// с момента старта: счётчики не теряют сообщения, прочитанные до подключения слушателя. Остановка - штатная (host StopAsync).
/// </summary>
internal sealed class DataSaverInstance : IAsyncDisposable
{
    private readonly DataSaverAppFactory _factory;
    private readonly Meter _meter = new(DataSaverMetrics.MeterName);
    private int _disposed;
    private int _stopped;

    private DataSaverInstance(TestRun run, Action<Dictionary<string, string?>>? tune, Action<IServiceCollection>? services)
    {
        Counters = new MetricCounters(_meter);
        Logs = new FakeLogCollector();
        Terminator = new RecordingTerminator();

        var settings = DataSaverAppFactory.Settings(
            run.Bootstrap, run.RawTopic, run.RejectedTopic, run.Group, new NpgsqlConnectionStringBuilder(run.Db.AppConnectionString));
        tune?.Invoke(settings);

        _factory = new DataSaverAppFactory(settings, collection =>
        {
            collection.AddLogging(builder => builder.AddProvider(new FakeLoggerProvider(Logs)));
            collection.RemoveAll<DataSaverMetrics>();
            collection.AddSingleton(new DataSaverMetrics(_meter));
            collection.RemoveAll<IProcessTerminator>();
            collection.AddSingleton<IProcessTerminator>(Terminator);
            services?.Invoke(collection);
        });
        Client = _factory.CreateClient();
    }

    public HttpClient Client { get; }

    public MetricCounters Counters { get; }

    public FakeLogCollector Logs { get; }

    public RecordingTerminator Terminator { get; }

    public IServiceProvider Services => _factory.Services;

    /// <summary>Запускает экземпляр: host стартует, цикл приёма вступает в группу теста.</summary>
    public static DataSaverInstance Start(TestRun run, Action<Dictionary<string, string?>>? tune = null, Action<IServiceCollection>? services = null) =>
        new(run, tune, services);

    public Task WaitReadyAsync(TimeSpan? timeout = null) =>
        Wait.UntilAsync(async () => (await ReadyStatusAsync()) == HttpStatusCode.OK, "readiness 200", timeout);

    public async Task<HttpStatusCode> ReadyStatusAsync()
    {
        using var response = await Client.GetAsync("/health/ready");
        return response.StatusCode;
    }

    /// <summary>
    /// Штатная остановка host (порядок D-8: дописать пачку, commit, Close, flush); счётчики и логи остаются доступными.
    /// Возвращает время остановки. Повторный вызов ничего не делает.
    /// </summary>
    public async Task<TimeSpan> StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0) return TimeSpan.Zero;
        var elapsed = Stopwatch.StartNew();
        Client.Dispose();
        await Services.GetRequiredService<IHost>().StopAsync(cancellationToken);
        return elapsed.Elapsed;
    }

    /// <summary>Останавливает (если ещё не остановлен) и освобождает ресурсы теста.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
        {
            await StopAsync(cts.Token);
        }

        await _factory.DisposeAsync();
        Counters.Dispose();
        _meter.Dispose();
    }

    internal sealed class RecordingTerminator : IProcessTerminator
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public void Terminate(int exitCode, string reason) => Interlocked.Increment(ref _calls);
    }
}

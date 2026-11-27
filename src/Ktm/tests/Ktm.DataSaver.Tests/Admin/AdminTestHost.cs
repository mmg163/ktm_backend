using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Reprocessing;
using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Tests.Reprocessing;
using Ktm.HostedServices.Admin;
using Ktm.HostedServices.Configuration;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;

namespace Ktm.DataSaver.Tests.Admin;

/// <summary>Источник повторной обработки из делегата: открытие подменяется тестом (читатель, отказ, ошибка периода, сбой).</summary>
internal sealed class FakeReplaySource(Func<ReprocessRequest, ReplayOpenResult> open) : IReplaySource
{
    public int Opened { get; private set; }

    public ValueTask<ReplayOpenResult> OpenAsync(ReprocessRequest request, CancellationToken cancellationToken)
    {
        Opened++;
        return ValueTask.FromResult(open(request));
    }
}

/// <summary>
/// Admin-область на настоящем Kestrel (порт 0) с настоящими <see cref="ReprocessJobManager"/> и раннером; подменены только
/// источник Kafka и запись в БД. Без Docker. Часы - системные: раннер с <c>maxRate</c> 200 на нескольких сообщениях не ждёт.
/// </summary>
internal sealed class AdminTestHost : IAsyncDisposable
{
    private readonly WebApplication _app;

    private AdminTestHost(WebApplication app, HttpClient client, FakeReplaySource source, FakeWriter writer, FakeLogCollector logs, ReprocessJobManager manager)
    {
        _app = app;
        Client = client;
        Source = source;
        Writer = writer;
        Logs = logs;
        Manager = manager;
    }

    public HttpClient Client { get; }

    public FakeReplaySource Source { get; }

    public FakeWriter Writer { get; }

    public FakeLogCollector Logs { get; }

    public ReprocessJobManager Manager { get; }

    public IServiceProvider Services => _app.Services;

    public static async Task<AdminTestHost> StartAsync(
        Func<ReprocessRequest, ReplayOpenResult> open,
        Dictionary<string, string?>? admin = null,
        Action<IServiceCollection>? configure = null)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        var logs = new FakeLogCollector();
        builder.Logging.ClearProviders();
        builder.Logging.AddProvider(new FakeLoggerProvider(logs));

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(admin ?? []).Build();
        var source = new FakeReplaySource(open);
        var writer = new FakeWriter();
        var settings = new ReprocessSettings(400, TimeSpan.FromMinutes(10), 1000, 3, 3);
        var runner = new ReprocessRunner(
            new IngestionPipeline(new FormatStage(new EventDecoder()), []),
            writer,
            settings,
            () => new BackoffPolicy(TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(50), 2, 0, () => 0.5),
            TimeProvider.System,
            new FakeLogger<ReprocessRunner>());
        var manager = new ReprocessJobManager(source, runner, settings, TimeProvider.System, new FakeLogger<ReprocessJobManager>());

        builder.Services.AddSingleton(TimeProvider.System);
        builder.Services.AddSingleton(manager);
        builder.Services.AddSingleton(Microsoft.Extensions.Options.Options.Create(new ReprocessOptions()));
        builder.Services.AddSingleton(Microsoft.Extensions.Options.Options.Create(new Ktm.DataSaver.Infrastructure.Kafka.KafkaOptions()));
        builder.Services.AddAdmin(configuration);
        builder.Services.AddBuiltInAdminSections();
        configure?.Invoke(builder.Services);

        var app = builder.Build();
        app.MapAdmin();
        app.MapGet("/health/live", () => "alive");
        await app.StartAsync();
        var address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return new AdminTestHost(app, new HttpClient { BaseAddress = new Uri(address) }, source, writer, logs, manager);
    }

    public async Task<HttpResponseMessage> PostJobAsync(string json, string? requestedBy = null)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/admin/v1/reprocessing/jobs")
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json"),
        };
        if (requestedBy is not null)
        {
            request.Headers.TryAddWithoutValidation("X-Requested-By", requestedBy);
        }

        return await Client.SendAsync(request);
    }

    public async Task<Guid> StartJobAsync(string json = """{"source":"dlq"}""")
    {
        using var response = await PostJobAsync(json);
        Assert.Equal(System.Net.HttpStatusCode.Accepted, response.StatusCode);
        return (await Json.OfAsync(response)).GetProperty("id").GetGuid();
    }

    public async Task<JsonElement> GetJsonAsync(string path)
    {
        using var response = await Client.GetAsync(path);
        return await Json.OfAsync(response);
    }

    public async Task WaitFinishedAsync(Guid id) => await Manager.WhenFinishedAsync(id).WaitAsync(TimeSpan.FromSeconds(10));

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.DisposeAsync();
    }

    /// <summary>Читатель, который не отдаёт первую пачку, пока тест не разрешит: задача остаётся активной.</summary>
    public static (Func<ReprocessRequest, ReplayOpenResult> Open, TaskCompletionSource Release) BlockedSource(int messages = 3)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = Enumerable.Range(0, messages).Select(i => ReprocessFixture.Valid("e" + i, i)).ToArray();
        return (_ => ReplayOpenResult.Opened(new FakeReader(batch) { BeforeRead = async call => { if (call == 1) await release.Task; } }), release);
    }

    public static Func<ReprocessRequest, ReplayOpenResult> Messages(params ReplayMessage[] messages) =>
        _ => ReplayOpenResult.Opened(new FakeReader(messages));
}

internal static class Json
{
    /// <summary>Тело ответа как JSON; текст читается один раз, поэтому ответ можно разбирать повторно.</summary>
    public static async Task<JsonElement> OfAsync(HttpResponseMessage response)
    {
        using var document = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        return document.RootElement.Clone();
    }
}

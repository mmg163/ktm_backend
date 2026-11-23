using Ktm.DataSaver.IntegrationTests.Kafka;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Host;

[CollectionDefinition(Name)]
public sealed class AppCollection : ICollectionFixture<KafkaFixture>, ICollectionFixture<PostgresFixture>
{
    public const string Name = "data-saver-app";
}

/// <summary>Data Saver in-process (WebApplicationFactory): настройки приходят так же, как в бою, - через конфигурацию <c>DataSaver:*</c>.</summary>
internal sealed class DataSaverAppFactory(Dictionary<string, string?> settings, Action<IServiceCollection>? configureServices = null) : WebApplicationFactory<Program>
{
    public static Dictionary<string, string?> Settings(string bootstrapServers, string topic, string rejectedTopic, string group, NpgsqlConnectionStringBuilder db) => new()
    {
        ["DataSaver:Kafka:BootstrapServers"] = bootstrapServers,
        ["DataSaver:Kafka:Topic"] = topic,
        ["DataSaver:Kafka:RejectedTopic"] = rejectedTopic,
        ["DataSaver:Kafka:GroupId"] = group,
        ["DataSaver:Kafka:SessionTimeoutMs"] = "10000",
        ["DataSaver:Postgres:Host"] = db.Host,
        ["DataSaver:Postgres:Port"] = db.Port.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["DataSaver:Postgres:Database"] = db.Database,
        ["DataSaver:Postgres:Username"] = db.Username,
        ["DataSaver:Postgres:Password"] = db.Password,
        ["DataSaver:Postgres:SslMode"] = "Disable",
        ["DataSaver:Batch:MaxWaitMs"] = "200",
        ["DataSaver:Backoff:InitialMs"] = "200",
        ["DataSaver:Backoff:MaxMs"] = "1000",
        ["DataSaver:Shutdown:TimeoutSeconds"] = "10",
        ["DataSaver:PollIntervalMs"] = "200",
    };

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        // Консольный и EventLog-провайдеры (на Windows их добавляет WebApplication.CreateBuilder) в тестах не нужны: логи
        // перехватывает FakeLogCollector, а EventLog уничтожается вместе с host, и запоздалая запись из ещё работающего
        // BackgroundService (остановка по таймауту при зависшем commit) падает с ObjectDisposedException.
        builder.ConfigureLogging(logging => logging.ClearProviders());
        builder.ConfigureAppConfiguration(configuration => configuration.AddInMemoryCollection(settings));
        // Composition root читает часть настроек при регистрации служб (DataSaver:Rules:Enabled), до применения
        // ConfigureAppConfiguration: UseSetting кладёт их в конфигурацию host-а с самого начала.
        foreach (var (key, value) in settings)
        {
            if (value is not null) builder.UseSetting(key, value);
        }

        if (configureServices is not null) builder.ConfigureTestServices(configureServices);
    }
}

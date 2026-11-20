using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Health;
using Ktm.DataSaver.Hosting;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Observability;
using Ktm.DataSaver.Infrastructure.Postgres;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Diagnostics.Metrics;

namespace Ktm.DataSaver;

/// <summary>Composition root Data Saver: опции с проверкой при старте, компоненты цикла, health и worker.</summary>
public static class DataSaverServiceCollectionExtensions
{
    public static IServiceCollection AddDataSaver(this IServiceCollection services, IConfiguration configuration)
    {
        var root = configuration.GetSection(DataSaverOptions.SectionName);

        AddOptions<KafkaOptions>(services, root.GetSection("Kafka"), DataSaverChecks.Kafka)
            .PostConfigure(options => options.ResolveSaslPasswordFile());
        AddOptions<PostgresOptions>(services, root.GetSection("Postgres"), DataSaverChecks.Postgres);
        AddOptions<BatchOptions>(services, root.GetSection("Batch"), DataSaverChecks.Batch);
        AddOptions<BackoffOptions>(services, root.GetSection("Backoff"), DataSaverChecks.Backoff);
        AddOptions<ShutdownOptions>(services, root.GetSection("Shutdown"), DataSaverChecks.Shutdown);
        AddOptions<HealthOptions>(services, root.GetSection("Health"), DataSaverChecks.Health);
        services.AddSingleton<IValidateOptions<HealthOptions>, LiveStaleConsistencyValidator>();
        AddOptions<TrustOptions>(services, root.GetSection("Trust"), DataSaverChecks.Trust);
        AddOptions<DataSaverOptions>(services, root, DataSaverChecks.Root);

        // Предел остановки host согласован с Shutdown__TimeoutSeconds: запас на Close consumer и flush producer.
        services.AddOptions<HostOptions>().Configure<IOptions<ShutdownOptions>>((host, shutdown) =>
            host.ShutdownTimeout = TimeSpan.FromSeconds(shutdown.Value.TimeoutSeconds + ShutdownOptions.HostMarginSeconds));
        services.AddOptions<KestrelServerOptions>().Configure<IOptions<HealthOptions>>((kestrel, health) =>
            kestrel.ListenAnyIP(health.Value.Port));

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(sp => new DataSaverMetrics(sp.GetRequiredService<IMeterFactory>().Create(DataSaverMetrics.MeterName)));

        services.AddSingleton<IProcessTerminator>(sp => new HostProcessTerminator(
            sp.GetRequiredService<IHostApplicationLifetime>(), sp.GetRequiredService<ILogger<HostProcessTerminator>>()));
        services.AddSingleton<KafkaFatalErrorHandler>();
        services.AddSingleton<EventDecoder>();

        // Конвейер: формат (встроенная стадия) + заглушка C5 (принимает всё).
        services.AddSingleton<FormatStage>();
        services.AddSingleton<IEventValidator, AcceptAllEventValidator>();
        services.AddSingleton<IIngestionStage, EventValidationStage>();
        services.AddSingleton<IngestionPipeline>();
        services.AddSingleton<IWriteFailureHandler, BlockingWriteFailureHandler>();
        services.AddSingleton<IAdminNotifier, LoggingAdminNotifier>();

        services.AddSingleton<ISourceTrustProvider>(sp => sp.GetRequiredService<IOptions<TrustOptions>>().Value.BuildProvider());
        services.AddSingleton<NpgsqlDataSource>(sp => sp.GetRequiredService<IOptions<PostgresOptions>>().Value.BuildDataSource());
        services.AddSingleton<NpgsqlEventStore>();
        services.AddSingleton<IBatchWriter>(sp => sp.GetRequiredService<NpgsqlEventStore>());
        services.AddSingleton<IEventStore, MeteredEventStore>();

        services.AddSingleton<KafkaMessageSource>(sp => new KafkaMessageSource(
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<KafkaFatalErrorHandler>(),
            sp.GetRequiredService<ILogger<KafkaMessageSource>>()));
        services.AddSingleton<IMessageSource>(sp => new MeteredMessageSource(
            sp.GetRequiredService<KafkaMessageSource>(), sp.GetRequiredService<DataSaverMetrics>()));
        services.AddSingleton<IOffsetCommitter, KafkaOffsetCommitter>();
        services.AddSingleton<KafkaRejectionPublisher>(sp => new KafkaRejectionPublisher(
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<KafkaFatalErrorHandler>(),
            sp.GetRequiredService<ILogger<KafkaRejectionPublisher>>()));
        services.AddSingleton<IRejectionPublisher>(sp => new ObservedRejectionPublisher(
            sp.GetRequiredService<KafkaRejectionPublisher>(),
            sp.GetRequiredService<DataSaverMetrics>(),
            sp.GetRequiredService<ILogger<ObservedRejectionPublisher>>()));

        services.AddSingleton(sp =>
        {
            var batch = sp.GetRequiredService<IOptions<BatchOptions>>().Value;
            return new Batcher<ConsumedMessage>(batch.MaxSize, TimeSpan.FromMilliseconds(batch.MaxWaitMs), sp.GetRequiredService<TimeProvider>());
        });
        services.AddSingleton(sp =>
        {
            var backoff = sp.GetRequiredService<IOptions<BackoffOptions>>().Value;
            return new BackoffPolicy(
                TimeSpan.FromMilliseconds(backoff.InitialMs),
                TimeSpan.FromMilliseconds(backoff.MaxMs),
                backoff.Multiplier,
                backoff.JitterFraction,
                Random.Shared.NextDouble);
        });
        services.AddSingleton(sp => new IngestionLoopOptions(
            TimeSpan.FromMilliseconds(sp.GetRequiredService<IOptions<DataSaverOptions>>().Value.PollIntervalMs),
            TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<ShutdownOptions>>().Value.TimeoutSeconds)));
        services.AddSingleton<IngestionLoop>();

        services.AddSingleton<IHealthSignals, HealthSignals>();
        services.AddSingleton<HealthEvaluator>();
        services.AddHostedService<DataSaverWorker>();
        return services;
    }

    private static OptionsBuilder<T> AddOptions<T>(IServiceCollection services, IConfiguration section, Func<T, IEnumerable<string>> check)
        where T : class
    {
        services.AddSingleton<IValidateOptions<T>>(new OptionsValidator<T>(check));
        return services.AddOptions<T>().Bind(section).ValidateOnStart();
    }
}

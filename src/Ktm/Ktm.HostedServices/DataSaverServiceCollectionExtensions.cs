using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Core.Batching;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Admin;
using Ktm.DataSaver.Admin.Reprocessing;
using Ktm.DataSaver.Core.Reprocessing;
using Ktm.DataSaver.Core.Rules;
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
        AddOptions<DlqOptions>(services, root.GetSection("Dlq"), DataSaverChecks.Dlq);
        AddOptions<ReprocessOptions>(services, root.GetSection("Reprocess"), DataSaverChecks.Reprocess);
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
        // Один троттлинг логов Kafka на процесс: consumer, producer и error-callback делят лимит на ключ.
        services.AddSingleton(sp => new LogThrottle(sp.GetRequiredService<TimeProvider>(), LogThrottle.DefaultInterval));
        services.AddSingleton(sp => new DataSaverMetrics(sp.GetRequiredService<IMeterFactory>().Create(DataSaverMetrics.MeterName)));

        services.AddSingleton<IProcessTerminator>(sp => new HostProcessTerminator(
            sp.GetRequiredService<IHostApplicationLifetime>(), sp.GetRequiredService<ILogger<HostProcessTerminator>>()));
        services.AddSingleton<KafkaFatalErrorHandler>();
        services.AddSingleton<EventDecoder>();

        // Конвейер: формат (встроенная стадия) + заглушка C5 (принимает всё).
        services.AddSingleton<FormatStage>();
        services.AddSingleton<IEventValidator, AcceptAllEventValidator>();
        services.AddSingleton<IIngestionStage, EventValidationStage>();
        // Правила отбраковки: строго после C5 (ADR-0005 п. 2.8); Enabled=false - ничего из них не регистрируется.
        var rulesEnabled = RulesEnabled(root.GetSection("Rules"));
        if (rulesEnabled)
        {
            AddRules(services, root.GetSection("Rules"));
        }
        else
        {
            // Публикатор отбракованных читает предел совпадения для записи в лог; без правил - значения по умолчанию, без проверки.
            services.AddSingleton(RegexPolicy.Default);
        }

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
            sp.GetRequiredService<ILogger<KafkaMessageSource>>(),
            sp.GetRequiredService<LogThrottle>()));
        services.AddSingleton<IConsumerLifecycle>(sp => sp.GetRequiredService<KafkaMessageSource>());
        services.AddSingleton<IMessageSource>(sp => new MeteredMessageSource(
            sp.GetRequiredService<KafkaMessageSource>(), sp.GetRequiredService<DataSaverMetrics>()));
        services.AddSingleton<IOffsetCommitter, KafkaOffsetCommitter>();
        services.AddSingleton<KafkaDeadLetterPublisher>(sp => new KafkaDeadLetterPublisher(
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<KafkaFatalErrorHandler>(),
            sp.GetRequiredService<ILogger<KafkaDeadLetterPublisher>>(),
            sp.GetRequiredService<LogThrottle>()));
        services.AddSingleton<IDeadLetterPublisher>(sp => new ObservedDeadLetterPublisher(
            sp.GetRequiredService<KafkaDeadLetterPublisher>(),
            sp.GetRequiredService<DataSaverMetrics>(),
            sp.GetRequiredService<ILogger<ObservedDeadLetterPublisher>>()));
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<DlqOptions>>().Value.ToPolicy());
        services.AddSingleton(sp => new DlqBreaker(sp.GetRequiredService<IOptions<DlqOptions>>().Value.BreakerThreshold));
        services.AddSingleton<IIngestionObserver>(sp => sp.GetRequiredService<DataSaverMetrics>());
        services.AddSingleton<KafkaRejectionPublisher>(sp => new KafkaRejectionPublisher(
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<KafkaFatalErrorHandler>(),
            sp.GetRequiredService<ILogger<KafkaRejectionPublisher>>(),
            sp.GetRequiredService<LogThrottle>()));
        services.AddSingleton<IRejectionPublisher>(sp => new ObservedRejectionPublisher(
            sp.GetRequiredService<KafkaRejectionPublisher>(),
            sp.GetRequiredService<DataSaverMetrics>(),
            sp.GetRequiredService<ILogger<ObservedRejectionPublisher>>(),
            sp.GetRequiredService<RegexPolicy>(),
            sp.GetRequiredService<TimeProvider>()));

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
        // Загрузка правил регистрируется раньше worker-а приёма (ADR-0005 п. 2.10).
        if (rulesEnabled)
        {
            services.AddHostedService(sp => sp.GetRequiredService<RuleRefreshService>());
        }

        services.AddHostedService<DataSaverWorker>();

        AddReprocessing(services);
        // Admin-область на порту health: разделы регистрируются строкой AddAdminSection (design Р-8).
        services.AddAdmin(root.GetSection("Admin"));
        services.AddBuiltInAdminSections();
        return services;
    }

    // Повторная обработка (ADR-0006 п. 2.6): задача идёт в процессе сервиса, тот же конвейер и та же идемпотентная запись;
    // MaxAttempts и BatchSize - те же настройки, что у рабочего цикла (Dlq__MaxAttempts, Batch__MaxSize).
    private static void AddReprocessing(IServiceCollection services)
    {
        services.AddSingleton(sp =>
        {
            var reprocess = sp.GetRequiredService<IOptions<ReprocessOptions>>().Value;
            return new ReprocessSettings(
                reprocess.MaxRateLimit,
                TimeSpan.FromSeconds(reprocess.MaxUnavailableSeconds),
                ReprocessSettings.DefaultMaxReportEntries,
                sp.GetRequiredService<IOptions<BatchOptions>>().Value.MaxSize,
                sp.GetRequiredService<IOptions<DlqOptions>>().Value.MaxAttempts);
        });
        services.AddSingleton<IReplaySource>(sp => new KafkaReplaySource(
            sp.GetRequiredService<IOptions<KafkaOptions>>().Value,
            sp.GetRequiredService<ILogger<KafkaReplaySource>>(),
            sp.GetRequiredService<LogThrottle>()));
        services.AddSingleton<IReplayEventWriter>(sp => new CountingEventStore(sp.GetRequiredService<IBatchWriter>()));
        services.AddSingleton(sp =>
        {
            var backoff = sp.GetRequiredService<IOptions<BackoffOptions>>().Value;
            return new ReprocessRunner(
                sp.GetRequiredService<IngestionPipeline>(),
                sp.GetRequiredService<IReplayEventWriter>(),
                sp.GetRequiredService<ReprocessSettings>(),
                // Политика не потокобезопасна: на каждый запуск задачи своя.
                () => new BackoffPolicy(
                    TimeSpan.FromMilliseconds(backoff.InitialMs),
                    TimeSpan.FromMilliseconds(backoff.MaxMs),
                    backoff.Multiplier,
                    backoff.JitterFraction,
                    Random.Shared.NextDouble),
                sp.GetRequiredService<TimeProvider>(),
                sp.GetRequiredService<ILogger<ReprocessRunner>>());
        });
        services.AddSingleton<ReprocessJobManager>(sp => new ReprocessJobManager(
            sp.GetRequiredService<IReplaySource>(),
            sp.GetRequiredService<ReprocessRunner>(),
            sp.GetRequiredService<ReprocessSettings>(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<ReprocessJobManager>>()));
        // Регистрируется после DataSaverWorker: останавливается раньше него (обратный порядок), задача отменяется до закрытия consumer/producer.
        services.AddHostedService<ReprocessJobHostedService>();
    }

    // Значение, которое не разбирается как bool, не отключает правила: об ошибке скажет проверка опций при старте.
    private static bool RulesEnabled(IConfiguration rules) =>
        !bool.TryParse(rules["Enabled"], out var enabled) || enabled;

    private static void AddRules(IServiceCollection services, IConfiguration section)
    {
        AddOptions<RulesOptions>(services, section, DataSaverChecks.Rules);
        services.AddSingleton(sp => new RegexPolicy(TimeSpan.FromMilliseconds(sp.GetRequiredService<IOptions<RulesOptions>>().Value.RegexTimeoutMs)));
        services.AddSingleton(sp => new ConditionCompiler(OperatorRegistry.CreateDefault(sp.GetRequiredService<RegexPolicy>())));
        services.AddSingleton(sp =>
        {
            var rules = sp.GetRequiredService<IOptions<RulesOptions>>().Value;
            return new RuleSnapshotBuilder(sp.GetRequiredService<ConditionCompiler>(), rules.MaxRules, rules.MaxRegexNodes);
        });
        services.AddSingleton<RuleSnapshotHolder>();
        services.AddSingleton<IRuleSnapshotProvider>(sp => sp.GetRequiredService<RuleSnapshotHolder>());
        services.AddSingleton<IRuleSource, NpgsqlRuleSource>();
        services.AddSingleton<RuleSnapshotRefresher>();
        services.AddSingleton<IIngestionStage, RuleRejectionStage>();
        services.AddSingleton(sp =>
        {
            var backoff = sp.GetRequiredService<IOptions<BackoffOptions>>().Value;
            return new RuleRefreshService(
                sp.GetRequiredService<RuleSnapshotRefresher>(),
                sp.GetRequiredService<RuleSnapshotHolder>(),
                TimeSpan.FromSeconds(sp.GetRequiredService<IOptions<RulesOptions>>().Value.RefreshIntervalSeconds),
                // Своя политика: общая с циклом приёма не потокобезопасна.
                new BackoffPolicy(
                    TimeSpan.FromMilliseconds(backoff.InitialMs),
                    TimeSpan.FromMilliseconds(backoff.MaxMs),
                    backoff.Multiplier,
                    backoff.JitterFraction,
                    Random.Shared.NextDouble),
                sp.GetRequiredService<TimeProvider>(),
                sp.GetRequiredService<DataSaverMetrics>(),
                sp.GetRequiredService<IProcessTerminator>(),
                sp.GetRequiredService<ILogger<RuleRefreshService>>(),
                sp.GetService<IRuleChangeSignal>());
        });
    }

    private static OptionsBuilder<T> AddOptions<T>(IServiceCollection services, IConfiguration section, Func<T, IEnumerable<string>> check)
        where T : class
    {
        services.AddSingleton<IValidateOptions<T>>(new OptionsValidator<T>(check));
        return services.AddOptions<T>().Bind(section).ValidateOnStart();
    }
}

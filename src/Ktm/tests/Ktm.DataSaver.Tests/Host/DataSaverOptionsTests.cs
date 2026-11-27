using Ktm.DataSaver.Application.Reprocessing;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.HostedServices;
using Ktm.HostedServices.Admin;
using Ktm.HostedServices.Configuration;
using Ktm.HostedServices.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Ktm.DataSaver.Tests.Host;

public class DataSaverOptionsTests
{
    private const string Secret = "s3cret-value-123";

    private static Dictionary<string, string?> Valid() => new()
    {
        ["DataSaver:Kafka:BootstrapServers"] = "kafka:9092",
        ["DataSaver:Postgres:Host"] = "db",
        ["DataSaver:Postgres:Database"] = "ktm",
        ["DataSaver:Postgres:Username"] = "ktm_app",
        ["DataSaver:Postgres:Password"] = Secret,
    };

    private sealed class StubLifetime : IHostApplicationLifetime
    {
        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => CancellationToken.None;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication()
        {
        }
    }

    private static ServiceProvider Build(Dictionary<string, string?> values)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMetrics();
        services.AddSingleton<IHostApplicationLifetime, StubLifetime>();
        services.AddDataSaver(configuration);
        return services.BuildServiceProvider();
    }

    private static string StartupFailure(Dictionary<string, string?> values)
    {
        using var provider = Build(values);
        var ex = Assert.ThrowsAny<Exception>(() => provider.GetRequiredService<IStartupValidator>().Validate());
        // Несколько опций с ошибками агрегируются в AggregateException.
        var failures = ex is AggregateException aggregate ? aggregate.InnerExceptions : [ex];
        // Только ошибка валидации: иное исключение (например, из PostConfigure) - не штатная диагностика старта.
        Assert.All(failures, f => Assert.IsType<OptionsValidationException>(f));
        return string.Join("; ", failures.Select(f => f.Message));
    }

    [Fact]
    public void ValidConfiguration_PassesAndUsesAdrDefaults()
    {
        using var provider = Build(Valid());

        provider.GetRequiredService<IStartupValidator>().Validate();

        var kafka = provider.GetRequiredService<IOptions<KafkaOptions>>().Value;
        Assert.Equal(("ktm.events.raw", "ktm.events.rejected", "ktm-data-saver"), (kafka.Topic, kafka.RejectedTopic, kafka.GroupId));
        var batch = provider.GetRequiredService<IOptions<BatchOptions>>().Value;
        Assert.Equal((500, 1000), (batch.MaxSize, batch.MaxWaitMs));
        var backoff = provider.GetRequiredService<IOptions<BackoffOptions>>().Value;
        Assert.Equal((1000, 30000, 2.0, 0.2), (backoff.InitialMs, backoff.MaxMs, backoff.Multiplier, backoff.JitterFraction));
        var health = provider.GetRequiredService<IOptions<HealthOptions>>().Value;
        Assert.Equal((8080, 60), (health.Port, health.LiveStaleSeconds));
        // Предел остановки host = Shutdown__TimeoutSeconds (по умолчанию 30) + запас.
        Assert.Equal(TimeSpan.FromSeconds(30 + ShutdownOptions.HostMarginSeconds), provider.GetRequiredService<IOptions<HostOptions>>().Value.ShutdownTimeout);
    }

    [Fact]
    public void HostShutdownTimeout_FollowsConfiguredShutdownTimeout()
    {
        var values = Valid();
        values["DataSaver:Shutdown:TimeoutSeconds"] = "90";
        using var provider = Build(values);

        Assert.Equal(TimeSpan.FromSeconds(90 + ShutdownOptions.HostMarginSeconds), provider.GetRequiredService<IOptions<HostOptions>>().Value.ShutdownTimeout);
    }

    [Fact]
    public void MissingRequiredSettings_FailStartupNamingEachSettingWithoutSecrets()
    {
        var values = Valid();
        values.Remove("DataSaver:Kafka:BootstrapServers");
        values.Remove("DataSaver:Postgres:Host");

        var message = StartupFailure(values);

        Assert.Contains("DataSaver__Kafka__BootstrapServers", message);
        Assert.Contains("DataSaver__Postgres__Host", message);
        Assert.DoesNotContain(Secret, message);

        values["DataSaver:Postgres:Host"] = "db";
        values["DataSaver:Postgres:Password"] = null;
        Assert.Contains("DataSaver__Postgres__Password", StartupFailure(values));
    }

    [Fact]
    public void InvalidValues_FailWithSettingNames_AndNeverEchoSecrets()
    {
        var values = Valid();
        values["DataSaver:Batch:MaxSize"] = "0";
        values["DataSaver:Backoff:InitialMs"] = "5000";
        values["DataSaver:Backoff:MaxMs"] = "1000";
        values["DataSaver:Trust:Default"] = "150";
        values["DataSaver:Kafka:SecurityProtocol"] = "TELNET";

        var message = StartupFailure(values);

        Assert.Contains("DataSaver__Batch__MaxSize", message);
        Assert.Contains("DataSaver__Backoff__MaxMs", message);
        Assert.Contains("DataSaver__Trust__Default", message);
        Assert.Contains("DataSaver__Kafka__SecurityProtocol", message);
        Assert.DoesNotContain(Secret, message);
    }

    [Fact]
    public void PasswordFile_IsAccepted_AndUnreadableFileFailsWithPathOnly()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, Secret + "\n");
            var values = Valid();
            values["DataSaver:Postgres:Password"] = null;
            values["DataSaver:Postgres:PasswordFile"] = file;
            using (var provider = Build(values))
            {
                provider.GetRequiredService<IStartupValidator>().Validate();
            }

            values["DataSaver:Postgres:PasswordFile"] = file + ".missing";
            var message = StartupFailure(values);
            Assert.Contains("DataSaver__Postgres__PasswordFile", message);
            Assert.DoesNotContain(Secret, message);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void KafkaSaslPasswordFile_IsReadIntoOptions()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, Secret + "\r\n");
            var values = Valid();
            values["DataSaver:Kafka:SaslPasswordFile"] = file;
            using var provider = Build(values);

            Assert.Equal(Secret, provider.GetRequiredService<IOptions<KafkaOptions>>().Value.SaslPassword);
        }
        finally
        {
            File.Delete(file);
        }
    }

    // Ожидания Consume (ceil(Poll / 1000)), записи в БД (Connect + 3 * Command: BeginTransaction, ExecuteNonQuery и Commit,
    // каждое со своим CommandTimeout) и публикации (ceil(P / 1000)) не суммируются: сравнивается максимум. Равенство порогу -
    // отказ. Столбцы: Connect, Command, ProducerMessageTimeoutMs, PollIntervalMs, LiveStaleSeconds, валидно ли.
    [Theory]
    [InlineData(15, 10, 30000, 1000, 46, true)]      // по умолчанию: БД 15 + 30 = 45 с < 46
    [InlineData(15, 10, 30000, 1000, 45, false)]     // БД 45 с = порогу
    [InlineData(15, 14, 30000, 1000, 58, true)]      // БД 57 с на 1 с меньше порога
    [InlineData(15, 14, 30000, 1000, 57, false)]     // БД 57 с = порогу
    [InlineData(14, 15, 30000, 1000, 60, true)]      // БД 59 с на 1 с меньше порога
    [InlineData(15, 15, 30000, 1000, 60, false)]     // БД 60 с = порогу
    [InlineData(15, 20, 30000, 1000, 60, false)]     // БД 75 с: зелёный по формуле Connect + Command (35 с)
    [InlineData(15, 10, 59000, 1000, 60, true)]      // Kafka 59 с < 60
    [InlineData(15, 10, 60000, 1000, 60, false)]     // Kafka 60 с = порогу
    [InlineData(15, 10, 59001, 1000, 60, false)]     // ceil(59.001) = 60
    [InlineData(1, 1, 60000, 1000, 60, false)]       // максимум - Kafka при мелких Connect и Command
    [InlineData(15, 10, 45000, 1000, 60, true)]      // сумма 90 больше порога, максимум 45 - меньше: проходит
    [InlineData(15, 10, int.MaxValue, 1000, 60, false)] // без переполнения
    [InlineData(15, int.MaxValue, 1000, 1000, 60, false)] // без переполнения на Command
    [InlineData(1, 1, 1000, 60000, 60, false)]       // Poll 60 с = порогу
    [InlineData(1, 1, 1000, 59000, 60, true)]        // Poll 59 с на 1 с меньше порога
    [InlineData(1, 1, 1000, 59001, 60, false)]       // ceil(59.001) = 60
    [InlineData(1, 1, 1000, 90000, 60, false)]       // Poll 90 с при пороге 60
    [InlineData(1, 1, 1000, 4500, 5, false)]         // ceil(4.5) = 5 = порогу
    [InlineData(1, 1, 1000, 4500, 6, true)]
    [InlineData(29, 10, 1000, 2000, 60, true)]       // Poll 2 при Connect + 3 * Command = 59 и пороге 60
    public void LiveStaleSeconds_MustExceedTheLongestBlockingWait_MaxOfPollDatabaseAndPublish(
        int connect, int command, int producerMs, int pollMs, int liveStaleSeconds, bool valid)
    {
        var values = Valid();
        values["DataSaver:Postgres:ConnectTimeoutSeconds"] = connect.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["DataSaver:Postgres:CommandTimeoutSeconds"] = command.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["DataSaver:Kafka:ProducerMessageTimeoutMs"] = producerMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["DataSaver:PollIntervalMs"] = pollMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        values["DataSaver:Health:LiveStaleSeconds"] = liveStaleSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);

        if (valid)
        {
            using var provider = Build(values);
            provider.GetRequiredService<IStartupValidator>().Validate();
            return;
        }

        var message = StartupFailure(values);
        Assert.Contains("DataSaver__Health__LiveStaleSeconds", message);
        Assert.Contains("DataSaver__PollIntervalMs", message);
        Assert.Contains("DataSaver__Postgres__ConnectTimeoutSeconds", message);
        Assert.Contains("DataSaver__Postgres__CommandTimeoutSeconds", message);
        Assert.Contains("DataSaver__Kafka__ProducerMessageTimeoutMs", message);
        Assert.DoesNotContain(Secret, message);
    }

    [Theory]
    [InlineData("DataSaver:Dlq:MaxAttempts", "0", "DataSaver__Dlq__MaxAttempts")]
    [InlineData("DataSaver:Dlq:MaxAttempts", "21", "DataSaver__Dlq__MaxAttempts")]
    [InlineData("DataSaver:Dlq:BreakerThreshold", "0", "DataSaver__Dlq__BreakerThreshold")]
    [InlineData("DataSaver:Dlq:BreakerThreshold", "10001", "DataSaver__Dlq__BreakerThreshold")]
    [InlineData("DataSaver:Kafka:DlqTopic", " ", "DataSaver__Kafka__DlqTopic")]
    public void InvalidDlqSettings_FailStartupNamingTheSettingWithoutSecrets(string key, string value, string expectedName)
    {
        var values = Valid();
        values[key] = value;

        var message = StartupFailure(values);

        Assert.Contains(expectedName, message);
        Assert.DoesNotContain(Secret, message);
    }

    [Theory]
    [InlineData("DataSaver:Dlq:MaxAttempts", "1")]
    [InlineData("DataSaver:Dlq:MaxAttempts", "20")]
    [InlineData("DataSaver:Dlq:BreakerThreshold", "1")]
    [InlineData("DataSaver:Dlq:BreakerThreshold", "10000")]
    public void DlqSettingsAtRangeBoundaries_AreAccepted(string key, string value)
    {
        var values = Valid();
        values[key] = value;
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();
    }

    [Theory]
    [InlineData("DataSaver:Reprocess:MaxRateLimit", "0", "DataSaver__Reprocess__MaxRateLimit")]
    [InlineData("DataSaver:Reprocess:MaxRateLimit", "10001", "DataSaver__Reprocess__MaxRateLimit")]
    [InlineData("DataSaver:Reprocess:MaxRatePerSecond", "0", "DataSaver__Reprocess__MaxRatePerSecond")]
    [InlineData("DataSaver:Reprocess:MaxRatePerSecond", "401", "DataSaver__Reprocess__MaxRatePerSecond")]
    [InlineData("DataSaver:Reprocess:MaxUnavailableSeconds", "0", "DataSaver__Reprocess__MaxUnavailableSeconds")]
    [InlineData("DataSaver:Reprocess:MaxUnavailableSeconds", "86401", "DataSaver__Reprocess__MaxUnavailableSeconds")]
    public void InvalidReprocessSettings_FailStartupNamingTheSettingWithoutSecrets(string key, string value, string expectedName)
    {
        var values = Valid();
        values[key] = value;

        var message = StartupFailure(values);

        Assert.Contains(expectedName, message);
        Assert.DoesNotContain(Secret, message);
    }

    [Fact]
    public void ReprocessSettings_TakeMaxAttemptsFromDlqAndBatchSizeFromBatch_AndDefaultsAreFromAdr()
    {
        var values = Valid();
        values["DataSaver:Dlq:MaxAttempts"] = "7";
        values["DataSaver:Batch:MaxSize"] = "123";
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();
        var settings = provider.GetRequiredService<ReprocessSettings>();
        var options = provider.GetRequiredService<IOptions<ReprocessOptions>>().Value;

        Assert.Equal((7, 123), (settings.MaxAttempts, settings.BatchSize));
        Assert.Equal((200, 400, 600), (options.MaxRatePerSecond, options.MaxRateLimit, options.MaxUnavailableSeconds));
        Assert.Equal((400, TimeSpan.FromSeconds(600)), (settings.MaxRateLimit, settings.MaxUnavailable));
        Assert.True(provider.GetRequiredService<IOptions<AdminOptions>>().Value.SwaggerEnabled);
        // Сервис и его останов зарегистрированы: задача отменяется вместе с host.
        Assert.NotNull(provider.GetRequiredService<ReprocessJobManager>());
        Assert.Contains(provider.GetServices<IHostedService>(), s => s is ReprocessJobHostedService);
        Assert.Contains(provider.GetServices<IAdminSection>(), s => s.Id == "reprocessing");
    }

    [Fact]
    public void NonPositiveConnectTimeout_ReportsOnlyItself_NotLiveStaleConflict()
    {
        var values = Valid();
        values["DataSaver:Postgres:ConnectTimeoutSeconds"] = "0";

        var message = StartupFailure(values);

        Assert.Contains("DataSaver__Postgres__ConnectTimeoutSeconds", message);
        Assert.DoesNotContain("DataSaver__Health__LiveStaleSeconds", message);
        Assert.DoesNotContain("longest blocking wait", message);
    }

    [Fact]
    public void NonPositivePollInterval_ReportsOnlyItself_NotLiveStaleConflict()
    {
        var values = Valid();
        values["DataSaver:PollIntervalMs"] = "0";

        var message = StartupFailure(values);

        Assert.Contains("DataSaver__PollIntervalMs", message);
        Assert.DoesNotContain("longest blocking wait", message);
    }

    [Fact]
    public void KafkaSaslPassword_ConflictAndUnreadableFile_AreValidationFailuresWithoutSecrets()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "from-file");
            var values = Valid();
            values["DataSaver:Kafka:SaslPassword"] = Secret;
            values["DataSaver:Kafka:SaslPasswordFile"] = file;
            var conflict = StartupFailure(values);
            Assert.Contains("DataSaver__Kafka__SaslPassword", conflict);
            Assert.Contains("DataSaver__Kafka__SaslPasswordFile", conflict);
            Assert.DoesNotContain(Secret, conflict);
            Assert.DoesNotContain("from-file", conflict);

            values["DataSaver:Kafka:SaslPassword"] = null;
            values["DataSaver:Kafka:SaslPasswordFile"] = file + ".missing";
            var unreadable = StartupFailure(values);
            Assert.Contains("DataSaver__Kafka__SaslPasswordFile", unreadable);
            Assert.Contains(".missing", unreadable);

            // Читаемый пустой файл - не ошибка чтения.
            File.WriteAllText(file, string.Empty);
            values["DataSaver:Kafka:SaslPasswordFile"] = file;
            using var provider = Build(values);
            provider.GetRequiredService<IStartupValidator>().Validate();
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void PostgresSettingErrors_NameSettingsInEnvironmentForm()
    {
        var values = Valid();
        values["DataSaver:Postgres:Port"] = "70000";
        var port = StartupFailure(values);

        values["DataSaver:Postgres:Port"] = "5432";
        values["DataSaver:Postgres:SslMode"] = "Sometimes";
        var sslMode = StartupFailure(values);

        Assert.Contains("DataSaver__Postgres__Port", port);
        Assert.Contains("DataSaver__Postgres__SslMode", sslMode);
        Assert.DoesNotContain("Postgres:", port + sslMode);
    }

    [Fact]
    public void PoolSizeError_NamesTheSettings()
    {
        var values = Valid();
        values["DataSaver:Postgres:MinPoolSize"] = "20";
        values["DataSaver:Postgres:MaxPoolSize"] = "10";

        var message = StartupFailure(values);

        Assert.Contains("DataSaver__Postgres__MinPoolSize", message);
        Assert.Contains("DataSaver__Postgres__MaxPoolSize", message);
    }

    [Fact]
    public void ResolveSaslPasswordFile_IsIdempotent_NoFalseConflictOnSecondCall()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "from-file\n");
            var options = new KafkaOptions { SaslPasswordFile = file };

            options.ResolveSaslPasswordFile();
            options.ResolveSaslPasswordFile();

            Assert.Null(options.SaslPasswordError);
            Assert.Equal("from-file", options.SaslPassword);

            // Явный пароль вместе с файлом остаётся конфликтом и при повторном вызове.
            var conflicting = new KafkaOptions { SaslPassword = Secret, SaslPasswordFile = file };
            conflicting.ResolveSaslPasswordFile();
            conflicting.ResolveSaslPasswordFile();
            Assert.NotNull(conflicting.SaslPasswordError);
            Assert.Equal(Secret, conflicting.SaslPassword);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [Fact]
    public void SaslPasswordError_CannotBeInjectedThroughConfiguration()
    {
        var values = Valid();
        values["DataSaver:Kafka:SaslPasswordError"] = "injected";
        using var provider = Build(values);

        provider.GetRequiredService<IStartupValidator>().Validate();

        Assert.Null(provider.GetRequiredService<IOptions<KafkaOptions>>().Value.SaslPasswordError);
    }

    [Fact]
    public void LogThrottle_IsRegisteredAsSingleton()
    {
        using var provider = Build(Valid());

        Assert.Same(provider.GetRequiredService<LogThrottle>(), provider.GetRequiredService<LogThrottle>());
    }
}

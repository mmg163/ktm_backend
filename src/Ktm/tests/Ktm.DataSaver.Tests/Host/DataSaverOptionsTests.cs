using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Infrastructure.Kafka;
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

    private static ServiceProvider Build(Dictionary<string, string?> values)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddDataSaver(configuration);
        return services.BuildServiceProvider();
    }

    private static string StartupFailure(Dictionary<string, string?> values)
    {
        using var provider = Build(values);
        var ex = Assert.ThrowsAny<Exception>(() => provider.GetRequiredService<IStartupValidator>().Validate());
        // Несколько опций с ошибками агрегируются в AggregateException.
        var failures = ex is AggregateException aggregate ? aggregate.InnerExceptions : [ex];
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

    [Theory]
    [InlineData(45, false)]
    [InlineData(46, true)]
    public void LiveStaleSeconds_MustExceedConnectPlusCommandTimeout(int liveStaleSeconds, bool valid)
    {
        var values = Valid();
        values["DataSaver:Postgres:ConnectTimeoutSeconds"] = "15";
        values["DataSaver:Postgres:CommandTimeoutSeconds"] = "30";
        values["DataSaver:Health:LiveStaleSeconds"] = liveStaleSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture);

        if (valid)
        {
            using var provider = Build(values);
            provider.GetRequiredService<IStartupValidator>().Validate();
            return;
        }

        var message = StartupFailure(values);
        Assert.Contains("DataSaver__Health__LiveStaleSeconds", message);
        Assert.Contains("DataSaver__Postgres__ConnectTimeoutSeconds", message);
        Assert.Contains("DataSaver__Postgres__CommandTimeoutSeconds", message);
        Assert.DoesNotContain(Secret, message);
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
}

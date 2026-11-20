using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Postgres;
using Microsoft.Extensions.Options;

namespace Ktm.DataSaver.Configuration;

/// <summary>Корневая секция <c>DataSaver</c> (переменные <c>DataSaver__*</c>, ADR-0002 D-11).</summary>
public sealed class DataSaverOptions
{
    public const string SectionName = "DataSaver";

    /// <summary>Предел ожидания одного Consume; он же период опроса группы в ожидании backoff.</summary>
    public int PollIntervalMs { get; set; } = 500;
}

/// <summary><c>DataSaver__Batch__*</c>: пачка закрывается по размеру либо по времени от первого сообщения.</summary>
public sealed class BatchOptions
{
    public int MaxSize { get; set; } = 500;

    public int MaxWaitMs { get; set; } = 1000;
}

/// <summary><c>DataSaver__Backoff__*</c>: min(Initial x Multiplier^n, Max) с джиттером.</summary>
public sealed class BackoffOptions
{
    public int InitialMs { get; set; } = 1000;

    public int MaxMs { get; set; } = 30000;

    public double Multiplier { get; set; } = 2.0;

    public double JitterFraction { get; set; } = 0.2;
}

/// <summary><c>DataSaver__Shutdown__TimeoutSeconds</c>: предел дописывания пачки от сигнала остановки (D-8).</summary>
public sealed class ShutdownOptions
{
    /// <summary>Запас сверх предела на Close consumer и flush producer при выставлении <c>HostOptions.ShutdownTimeout</c>.</summary>
    public const int HostMarginSeconds = 5;

    public int TimeoutSeconds { get; set; } = 30;
}

/// <summary><c>DataSaver__Health__*</c>: отдельный порт Kestrel (D-10).</summary>
public sealed class HealthOptions
{
    public int Port { get; set; } = 8080;

    public int LiveStaleSeconds { get; set; } = 60;
}

/// <summary>
/// <c>DataSaver__Trust__Default</c> и <c>DataSaver__Trust__Source__&lt;source&gt;</c>: временный источник коэффициентов
/// доверия (0..100) до появления хранилища настроек.
/// </summary>
public sealed class TrustOptions
{
    public short Default { get; set; } = EnvironmentSourceTrustProvider.DefaultTrust;

    public Dictionary<string, string> Source { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Единственный источник проверки значений - сам провайдер (бросает <see cref="InvalidOperationException"/>).</summary>
    public EnvironmentSourceTrustProvider BuildProvider()
    {
        var variables = new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            [EnvironmentSourceTrustProvider.DefaultVariable] = Default.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        foreach (var (source, value) in Source)
        {
            variables[EnvironmentSourceTrustProvider.SourcePrefix + source] = value;
        }

        return new EnvironmentSourceTrustProvider(variables);
    }
}

/// <summary>Проверка опций при старте: сообщение содержит имя настройки в форме окружения и никогда - значения секретов.</summary>
internal sealed class OptionsValidator<T>(Func<T, IEnumerable<string>> check) : IValidateOptions<T>
    where T : class
{
    public ValidateOptionsResult Validate(string? name, T options)
    {
        var failures = check(options).ToList();
        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}

internal static class DataSaverChecks
{
    public static IEnumerable<string> Kafka(KafkaOptions o)
    {
        const string p = "DataSaver__Kafka__";
        if (string.IsNullOrWhiteSpace(o.BootstrapServers)) yield return $"{p}BootstrapServers is required.";
        if (string.IsNullOrWhiteSpace(o.Topic)) yield return $"{p}Topic must not be empty.";
        if (string.IsNullOrWhiteSpace(o.RejectedTopic)) yield return $"{p}RejectedTopic must not be empty.";
        if (string.IsNullOrWhiteSpace(o.GroupId)) yield return $"{p}GroupId must not be empty.";
        if (o.SessionTimeoutMs < 1) yield return $"{p}SessionTimeoutMs must be positive.";
        if (o.ProducerMessageTimeoutMs < 1) yield return $"{p}ProducerMessageTimeoutMs must be positive.";
        if (o.SecurityProtocol is not null && Rejects(() => KafkaConfigExtensions.ParseSecurityProtocol(o.SecurityProtocol)))
        {
            yield return $"{p}SecurityProtocol has an unsupported value (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL).";
        }

        if (o.SaslMechanism is not null && Rejects(() => KafkaConfigExtensions.ParseSaslMechanism(o.SaslMechanism)))
        {
            yield return $"{p}SaslMechanism has an unsupported value (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER).";
        }
    }

    public static IEnumerable<string> Postgres(PostgresOptions o)
    {
        // BuildConnectionString проверяет всё сразу (в т.ч. читаемость PasswordFile); сообщения не содержат значений секретов.
        string? error = null;
        try
        {
            _ = o.BuildConnectionString();
        }
        catch (InvalidOperationException ex)
        {
            error = ex.Message.Replace("Postgres:", "DataSaver__Postgres__", StringComparison.Ordinal);
        }

        if (error is not null) yield return error;
    }

    public static IEnumerable<string> Batch(BatchOptions o)
    {
        if (o.MaxSize < 1) yield return "DataSaver__Batch__MaxSize must be at least 1.";
        if (o.MaxWaitMs < 1) yield return "DataSaver__Batch__MaxWaitMs must be positive.";
    }

    public static IEnumerable<string> Backoff(BackoffOptions o)
    {
        const string p = "DataSaver__Backoff__";
        if (o.InitialMs < 1) yield return $"{p}InitialMs must be positive.";
        if (o.MaxMs < o.InitialMs) yield return $"{p}MaxMs must not be less than {p}InitialMs.";
        if (o.Multiplier < 1.0) yield return $"{p}Multiplier must be at least 1.";
        if (o.JitterFraction is < 0.0 or > 1.0) yield return $"{p}JitterFraction must be in range 0..1.";
    }

    public static IEnumerable<string> Shutdown(ShutdownOptions o)
    {
        if (o.TimeoutSeconds < 1) yield return "DataSaver__Shutdown__TimeoutSeconds must be at least 1.";
    }

    public static IEnumerable<string> Root(DataSaverOptions o)
    {
        if (o.PollIntervalMs < 1) yield return "DataSaver__PollIntervalMs must be positive.";
    }

    public static IEnumerable<string> Health(HealthOptions o)
    {
        if (o.Port is < 1 or > 65535) yield return "DataSaver__Health__Port must be in range 1..65535.";
        if (o.LiveStaleSeconds < 1) yield return "DataSaver__Health__LiveStaleSeconds must be positive.";
    }

    public static IEnumerable<string> Trust(TrustOptions o)
    {
        string? error = null;
        try
        {
            _ = o.BuildProvider();
        }
        catch (InvalidOperationException ex)
        {
            // Сообщение провайдера называет переменную (DataSaver__Trust__...), значение в него не входит.
            error = ex.Message;
        }

        if (error is not null) yield return error;
    }

    private static bool Rejects(Action parse)
    {
        try
        {
            parse();
            return false;
        }
        catch (ArgumentException)
        {
            return true;
        }
    }
}

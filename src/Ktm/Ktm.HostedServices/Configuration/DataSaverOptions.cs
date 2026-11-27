using Ktm.DataSaver.Application.Resilience;
using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.Infrastructure.Kafka;
using Ktm.DataSaver.Infrastructure.Postgres;
using Microsoft.Extensions.Options;

namespace Ktm.HostedServices.Configuration;

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

/// <summary>
/// <c>DataSaver__Dlq__*</c>: уход сообщений, которые не удалось обработать, в dlq (ADR-0006 п. 2.3).
/// <c>Enabled = false</c> - аварийный возврат к повтору без предела.
/// </summary>
public sealed class DlqOptions
{
    public const int MinMaxAttempts = 1;
    public const int MaxMaxAttempts = 20;
    public const int MinBreakerThreshold = 1;
    public const int MaxBreakerThreshold = 10000;

    public bool Enabled { get; set; } = true;

    /// <summary>Число неудачных попыток по ошибке, привязанной к сообщению, до изоляции и dlq.</summary>
    public int MaxAttempts { get; set; } = DlqPolicy.DefaultMaxAttempts;

    /// <summary>Сколько сообщений подряд без успешной записи открывают предохранитель (dlq не используется).</summary>
    public int BreakerThreshold { get; set; } = DlqPolicy.DefaultBreakerThreshold;

    public DlqPolicy ToPolicy() => new(Enabled, MaxAttempts, BreakerThreshold);
}

/// <summary>
/// <c>DataSaver__Reprocess__*</c>: пределы повторной обработки (ADR-0006 п. 2.6, В-8). <c>MaxRatePerSecond</c> - скорость, если запрос
/// её не задал; <c>MaxRateLimit</c> - жёсткий верхний предел <c>maxRate</c> запроса; <c>MaxUnavailableSeconds</c> - сколько задача ждёт БД.
/// </summary>
public sealed class ReprocessOptions
{
    public const int MinMaxRate = 1;
    public const int MaxMaxRateLimit = 10000;
    public const int MinMaxUnavailableSeconds = 1;
    public const int MaxMaxUnavailableSeconds = 86400;

    public int MaxRatePerSecond { get; set; } = 200;

    public int MaxRateLimit { get; set; } = 400;

    public int MaxUnavailableSeconds { get; set; } = 600;
}

/// <summary><c>DataSaver__Admin__*</c>: admin-область на порту health (ADR-0006 п. 2.6).</summary>
public sealed class AdminOptions
{
    /// <summary><c>false</c> - документ OpenAPI <c>/admin/openapi/v1.json</c> и Swagger UI <c>/admin/swagger</c> не отдаются (404).</summary>
    public bool SwaggerEnabled { get; set; } = true;
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

/// <summary><c>DataSaver__Rules__*</c>: правила отбраковки (ADR-0005 п. 2.10).</summary>
public sealed class RulesOptions
{
    public const int MinRefreshIntervalSeconds = 5;
    public const int MaxRefreshIntervalSeconds = 3600;

    /// <summary><c>false</c> - стадия правил и загрузчик не регистрируются, таблица не читается (аварийное отключение).</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Период опроса таблицы; изменение правил вступает в силу не позже него.</summary>
    public int RefreshIntervalSeconds { get; set; } = 60;

    /// <summary>Предел числа применяемых правил (ограничение вычислений и кардинальности метрик).</summary>
    public int MaxRules { get; set; } = 500;

    public const int MinRegexTimeoutMs = 1;
    public const int MaxRegexTimeoutMs = 1000;
    public const int MinMaxRegexNodes = 1;
    public const int MaxMaxRegexNodes = 1000;

    /// <summary>Предел времени одного совпадения regex, мс; превышение отбраковывает сообщение с кодом <c>rule_eval_timeout</c> (ADR-0005 п. 2.7).</summary>
    public int RegexTimeoutMs { get; set; } = RegexPolicy.DefaultMatchTimeoutMs;

    /// <summary>Предел числа узлов regex в наборе применяемых правил.</summary>
    public int MaxRegexNodes { get; set; } = RuleSnapshotBuilder.DefaultMaxRegexNodes;
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
        if (string.IsNullOrWhiteSpace(o.DlqTopic)) yield return $"{p}DlqTopic must not be empty.";
        if (string.IsNullOrWhiteSpace(o.GroupId)) yield return $"{p}GroupId must not be empty.";
        if (o.SessionTimeoutMs < 1) yield return $"{p}SessionTimeoutMs must be positive.";
        if (o.ProducerMessageTimeoutMs < 1) yield return $"{p}ProducerMessageTimeoutMs must be positive.";
        if (o.SaslPasswordError is not null) yield return o.SaslPasswordError;
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
            error = ex.Message;
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

    public static IEnumerable<string> Dlq(DlqOptions o)
    {
        const string p = "DataSaver__Dlq__";
        if (o.MaxAttempts is < DlqOptions.MinMaxAttempts or > DlqOptions.MaxMaxAttempts)
        {
            yield return $"{p}MaxAttempts must be in range {DlqOptions.MinMaxAttempts}..{DlqOptions.MaxMaxAttempts}.";
        }

        if (o.BreakerThreshold is < DlqOptions.MinBreakerThreshold or > DlqOptions.MaxBreakerThreshold)
        {
            yield return $"{p}BreakerThreshold must be in range {DlqOptions.MinBreakerThreshold}..{DlqOptions.MaxBreakerThreshold}.";
        }
    }

    public static IEnumerable<string> Reprocess(ReprocessOptions o)
    {
        const string p = "DataSaver__Reprocess__";
        if (o.MaxRateLimit is < ReprocessOptions.MinMaxRate or > ReprocessOptions.MaxMaxRateLimit)
        {
            yield return $"{p}MaxRateLimit must be in range {ReprocessOptions.MinMaxRate}..{ReprocessOptions.MaxMaxRateLimit}.";
        }

        if (o.MaxRatePerSecond < ReprocessOptions.MinMaxRate || o.MaxRatePerSecond > o.MaxRateLimit)
        {
            yield return $"{p}MaxRatePerSecond must be in range {ReprocessOptions.MinMaxRate}..{p}MaxRateLimit ({o.MaxRateLimit}).";
        }

        if (o.MaxUnavailableSeconds is < ReprocessOptions.MinMaxUnavailableSeconds or > ReprocessOptions.MaxMaxUnavailableSeconds)
        {
            yield return $"{p}MaxUnavailableSeconds must be in range {ReprocessOptions.MinMaxUnavailableSeconds}..{ReprocessOptions.MaxMaxUnavailableSeconds}.";
        }
    }

    public static IEnumerable<string> Shutdown(ShutdownOptions o)
    {
        if (o.TimeoutSeconds < 1) yield return "DataSaver__Shutdown__TimeoutSeconds must be at least 1.";
    }

    public static IEnumerable<string> Rules(RulesOptions o)
    {
        const string p = "DataSaver__Rules__";
        if (o.RefreshIntervalSeconds is < RulesOptions.MinRefreshIntervalSeconds or > RulesOptions.MaxRefreshIntervalSeconds)
        {
            yield return $"{p}RefreshIntervalSeconds must be in range {RulesOptions.MinRefreshIntervalSeconds}..{RulesOptions.MaxRefreshIntervalSeconds}.";
        }

        if (o.MaxRules < 1) yield return $"{p}MaxRules must be at least 1.";
        if (o.RegexTimeoutMs is < RulesOptions.MinRegexTimeoutMs or > RulesOptions.MaxRegexTimeoutMs)
        {
            yield return $"{p}RegexTimeoutMs must be in range {RulesOptions.MinRegexTimeoutMs}..{RulesOptions.MaxRegexTimeoutMs}.";
        }

        if (o.MaxRegexNodes is < RulesOptions.MinMaxRegexNodes or > RulesOptions.MaxMaxRegexNodes)
        {
            yield return $"{p}MaxRegexNodes must be in range {RulesOptions.MinMaxRegexNodes}..{RulesOptions.MaxMaxRegexNodes}.";
        }
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

    /// <summary>
    /// Три блокирующих ожидания цикла, во время которых heartbeat не отмечается: Consume (до PollInterval), запись пачки
    /// в БД (Connect + 3 * Command секунд) и публикация в rejected или dlq (до ProducerMessageTimeoutMs). Запись - это
    /// BeginTransaction, ExecuteNonQuery и Commit: три обращения к БД, и CommandTimeout действует на каждое (допущение по
    /// документации Npgsql, замера нет, tasks 10.16). После каждого ожидания цикл отмечается (Touch), поэтому они не
    /// суммируются - худший случай максимум из трёх. Commit offset в Kafka - исключение: отметки вокруг него нет. Если
    /// liveness-порог не больше этого максимума, штатно долгое ожидание убило бы процесс перезапуском, который его не лечит.
    /// </summary>
    public static IEnumerable<string> LiveStale(HealthOptions health, PostgresOptions postgres, KafkaOptions kafka, int pollIntervalMs)
    {
        if (health.LiveStaleSeconds < 1 || postgres.ConnectTimeoutSeconds < 1 || postgres.CommandTimeoutSeconds < 1
            || kafka.ProducerMessageTimeoutMs < 1 || pollIntervalMs < 1)
        {
            // Сами значения некорректны: об этом скажут проверки своих опций.
            yield break;
        }

        var pollWait = (long)Math.Ceiling(pollIntervalMs / 1000.0);
        var databaseWait = (long)postgres.ConnectTimeoutSeconds + 3L * postgres.CommandTimeoutSeconds;
        var publishWait = (long)Math.Ceiling(kafka.ProducerMessageTimeoutMs / 1000.0);
        var worst = Math.Max(pollWait, Math.Max(databaseWait, publishWait));
        if (worst >= health.LiveStaleSeconds)
        {
            yield return "DataSaver__Health__LiveStaleSeconds must be greater than the longest blocking wait of the loop: " +
                         $"max(ceil(DataSaver__PollIntervalMs / 1000) = {pollWait} s, " +
                         $"DataSaver__Postgres__ConnectTimeoutSeconds + 3 * DataSaver__Postgres__CommandTimeoutSeconds = {databaseWait} s, " +
                         $"ceil(DataSaver__Kafka__ProducerMessageTimeoutMs / 1000) = {publishWait} s) = {worst} s. " +
                         "Consume, a database write and a publish to the rejected or dead letter topic are blocking waits that do not touch the liveness heartbeat.";
        }
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

/// <summary>Согласованность <see cref="HealthOptions"/> с таймаутами БД: проверка нескольких секций при старте.</summary>
internal sealed class LiveStaleConsistencyValidator(
    IOptions<PostgresOptions> postgres,
    IOptions<KafkaOptions> kafka,
    IOptions<DataSaverOptions> root) : IValidateOptions<HealthOptions>
{
    public ValidateOptionsResult Validate(string? name, HealthOptions options)
    {
        PostgresOptions postgresValue;
        KafkaOptions kafkaValue;
        int pollIntervalMs;
        try
        {
            postgresValue = postgres.Value;
            kafkaValue = kafka.Value;
            pollIntervalMs = root.Value.PollIntervalMs;
        }
        catch (OptionsValidationException)
        {
            // Некорректные настройки Postgres или Kafka сообщит их собственная проверка.
            return ValidateOptionsResult.Success;
        }

        var failures = DataSaverChecks.LiveStale(options, postgresValue, kafkaValue, pollIntervalMs).ToList();
        return failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
    }
}

using Confluent.Kafka;

namespace Ktm.DataSaver.Infrastructure.Kafka;

public static class KafkaConfigExtensions
{
    private static readonly Dictionary<string, SecurityProtocol> SecurityProtocols = new(StringComparer.OrdinalIgnoreCase)
    {
        ["PLAINTEXT"] = SecurityProtocol.Plaintext,
        ["SSL"] = SecurityProtocol.Ssl,
        ["SASL_PLAINTEXT"] = SecurityProtocol.SaslPlaintext,
        ["SASL_SSL"] = SecurityProtocol.SaslSsl,
    };

    private static readonly Dictionary<string, SaslMechanism> SaslMechanisms = new(StringComparer.OrdinalIgnoreCase)
    {
        ["PLAIN"] = SaslMechanism.Plain,
        ["SCRAM-SHA-256"] = SaslMechanism.ScramSha256,
        ["SCRAM-SHA-512"] = SaslMechanism.ScramSha512,
        ["GSSAPI"] = SaslMechanism.Gssapi,
        ["OAUTHBEARER"] = SaslMechanism.OAuthBearer,
    };

    /// <summary>Документированное имя протокола (без учёта регистра) в enum; иначе понятная ошибка с именем настройки.</summary>
    public static SecurityProtocol ParseSecurityProtocol(string value) =>
        Lookup(SecurityProtocols, value, "Kafka__SecurityProtocol");

    /// <summary>Документированное имя механизма SASL (без учёта регистра) в enum; иначе понятная ошибка с именем настройки.</summary>
    public static SaslMechanism ParseSaslMechanism(string value) =>
        Lookup(SaslMechanisms, value, "Kafka__SaslMechanism");

    /// <summary>Общие для consumer и producer параметры подключения и безопасности.</summary>
    internal static T ApplyConnection<T>(this T config, KafkaOptions options)
        where T : ClientConfig
    {
        config.BootstrapServers = options.BootstrapServers;
        if (options.ClientId is not null) config.ClientId = options.ClientId;
        if (options.SecurityProtocol is not null) config.SecurityProtocol = ParseSecurityProtocol(options.SecurityProtocol);
        if (options.SaslMechanism is not null) config.SaslMechanism = ParseSaslMechanism(options.SaslMechanism);
        if (options.SaslUsername is not null) config.SaslUsername = options.SaslUsername;
        if (options.SaslPassword is not null) config.SaslPassword = options.SaslPassword;
        return config;
    }

    private static TEnum Lookup<TEnum>(Dictionary<string, TEnum> table, string value, string settingName)
    {
        if (table.TryGetValue(value.Trim(), out var result))
        {
            return result;
        }

        throw new ArgumentException(
            $"Unsupported value of {settingName}: '{value}'. Allowed: {string.Join(", ", table.Keys)} (case-insensitive).",
            settingName);
    }
}

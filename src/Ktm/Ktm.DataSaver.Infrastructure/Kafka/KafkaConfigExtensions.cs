using Confluent.Kafka;

namespace Ktm.DataSaver.Infrastructure.Kafka;

internal static class KafkaConfigExtensions
{
    /// <summary>Общие для consumer и producer параметры подключения и безопасности.</summary>
    public static T ApplyConnection<T>(this T config, KafkaOptions options)
        where T : ClientConfig
    {
        config.BootstrapServers = options.BootstrapServers;
        if (options.ClientId is not null) config.ClientId = options.ClientId;
        if (options.SecurityProtocol is not null) config.SecurityProtocol = Enum.Parse<SecurityProtocol>(options.SecurityProtocol, ignoreCase: true);
        if (options.SaslMechanism is not null) config.SaslMechanism = Enum.Parse<SaslMechanism>(options.SaslMechanism, ignoreCase: true);
        if (options.SaslUsername is not null) config.SaslUsername = options.SaslUsername;
        if (options.SaslPassword is not null) config.SaslPassword = options.SaslPassword;
        return config;
    }
}

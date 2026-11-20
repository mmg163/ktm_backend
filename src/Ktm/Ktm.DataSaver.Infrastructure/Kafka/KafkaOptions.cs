namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Параметры подключения к Kafka (секция конфигурации <c>Kafka</c>, переменные <c>Kafka__*</c>).
/// Пароль не попадает в логи: объект целиком не логируется, <see cref="ToString"/> его скрывает.
/// </summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>Единый входной топик всех источников (D-1).</summary>
    public string Topic { get; set; } = "ktm.events.raw";

    /// <summary>Топик отбракованных по умолчанию.</summary>
    public string RejectedTopic { get; set; } = "ktm.events.rejected";

    public string GroupId { get; set; } = "ktm-data-saver";

    public string? ClientId { get; set; }

    /// <summary>
    /// <c>group.instance.id</c> (static membership: перезапуск реплики не вызывает ребаланс в пределах session timeout).
    /// Необязательный, уникален на реплику; пустое значение не задаётся.
    /// </summary>
    public string? GroupInstanceId { get; set; }

    /// <summary>PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL; null - по умолчанию библиотеки.</summary>
    public string? SecurityProtocol { get; set; }

    public string? SaslMechanism { get; set; }

    public string? SaslUsername { get; set; }

    public string? SaslPassword { get; set; }

    /// <summary>Файл с паролем SASL (секрет из файла); взаимоисключающе с <see cref="SaslPassword"/>.</summary>
    public string? SaslPasswordFile { get; set; }

    public int SessionTimeoutMs { get; set; } = 45000;

    /// <summary>Предел ожидания подтверждения публикации отбракованного брокером.</summary>
    public int ProducerMessageTimeoutMs { get; set; } = 30000;

    public override string ToString() => $"Kafka({BootstrapServers}, topic={Topic}, group={GroupId})";
}

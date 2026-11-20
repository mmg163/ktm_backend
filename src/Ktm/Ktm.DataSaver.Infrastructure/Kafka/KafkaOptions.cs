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

    /// <summary>
    /// Ошибка разрешения пароля SASL (конфликт с <see cref="SaslPassword"/> или нечитаемый файл); не биндится из
    /// конфигурации (закрытый сеттер). Текст называет настройку и путь, но не значение секрета; сообщает её валидатор.
    /// </summary>
    public string? SaslPasswordError { get; private set; }

    private string? _passwordReadFromFile;

    public int SessionTimeoutMs { get; set; } = 45000;

    /// <summary>Предел ожидания подтверждения публикации отбракованного брокером.</summary>
    public int ProducerMessageTimeoutMs { get; set; } = 30000;

    /// <summary>
    /// Читает пароль SASL из <see cref="SaslPasswordFile"/>, если файл задан. Не бросает: ошибка записывается в
    /// <see cref="SaslPasswordError"/>, чтобы её превратила в сообщение валидация настроек при старте. Конфликт
    /// определяется по исходным значениям; читаемый пустой файл ошибкой чтения не считается.
    /// </summary>
    public void ResolveSaslPasswordFile()
    {
        SaslPasswordError = null;
        if (_passwordReadFromFile is not null && SaslPassword == _passwordReadFromFile)
        {
            // Повторный вызов: пароль, подставленный из файла предыдущим вызовом, не считается заданным пользователем.
            SaslPassword = null;
        }

        _passwordReadFromFile = null;
        if (string.IsNullOrWhiteSpace(SaslPasswordFile))
        {
            return;
        }

        if (!string.IsNullOrEmpty(SaslPassword))
        {
            SaslPasswordError = "DataSaver__Kafka__SaslPassword and DataSaver__Kafka__SaslPasswordFile are mutually exclusive.";
            return;
        }

        try
        {
            SaslPassword = File.ReadAllText(SaslPasswordFile).TrimEnd('\r', '\n');
            _passwordReadFromFile = SaslPassword;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Только имя настройки и путь; содержимое файла в сообщение не попадает.
            SaslPasswordError = $"DataSaver__Kafka__SaslPasswordFile '{SaslPasswordFile}' cannot be read: {ex.GetType().Name}.";
        }
    }

    public override string ToString() => $"Kafka({BootstrapServers}, topic={Topic}, group={GroupId})";
}

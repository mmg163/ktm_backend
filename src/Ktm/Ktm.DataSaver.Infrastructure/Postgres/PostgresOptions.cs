using Npgsql;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Настройки подключения к БД журнала (секция <c>DataSaver:Postgres</c>, окружение <c>DataSaver__Postgres__*</c>).
/// Пароль - переменная <see cref="Password"/> либо файл <see cref="PasswordFile"/> (секрет Docker/Kubernetes); не оба сразу.
/// Пароль не попадает в <see cref="ToString"/> и в сообщения исключений.
/// </summary>
public sealed class PostgresOptions
{
    public string Host { get; set; } = "localhost";

    public int Port { get; set; } = 5432;

    public string Database { get; set; } = "ktm_revisor";

    public string Username { get; set; } = "ktm_app";

    public string? Password { get; set; }

    public string? PasswordFile { get; set; }

    public int CommandTimeoutSeconds { get; set; } = 30;

    public int ConnectTimeoutSeconds { get; set; } = 15;

    public int MinPoolSize { get; set; } = 1;

    public int MaxPoolSize { get; set; } = 10;

    public int KeepAliveSeconds { get; set; } = 30;

    /// <summary>Проверяет настройки и собирает строку подключения (содержит пароль: в лог не выводить).</summary>
    public string BuildConnectionString()
    {
        if (string.IsNullOrWhiteSpace(Host)) throw new InvalidOperationException("Postgres:Host is required.");
        if (string.IsNullOrWhiteSpace(Database)) throw new InvalidOperationException("Postgres:Database is required.");
        if (string.IsNullOrWhiteSpace(Username)) throw new InvalidOperationException("Postgres:Username is required.");
        if (Port is < 1 or > 65535) throw new InvalidOperationException("Postgres:Port must be in range 1..65535.");
        if (CommandTimeoutSeconds < 1) throw new InvalidOperationException("Postgres:CommandTimeoutSeconds must be positive.");
        if (ConnectTimeoutSeconds < 1) throw new InvalidOperationException("Postgres:ConnectTimeoutSeconds must be positive.");
        if (MinPoolSize < 0 || MaxPoolSize < 1 || MinPoolSize > MaxPoolSize)
        {
            throw new InvalidOperationException("Postgres pool sizes must satisfy 0 <= MinPoolSize <= MaxPoolSize and MaxPoolSize >= 1.");
        }

        if (KeepAliveSeconds < 0) throw new InvalidOperationException("Postgres:KeepAliveSeconds must not be negative.");

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = Host,
            Port = Port,
            Database = Database,
            Username = Username,
            Password = ResolvePassword(),
            CommandTimeout = CommandTimeoutSeconds,
            Timeout = ConnectTimeoutSeconds,
            MinPoolSize = MinPoolSize,
            MaxPoolSize = MaxPoolSize,
            KeepAlive = KeepAliveSeconds,
            ApplicationName = "ktm-data-saver",
        };
        return builder.ConnectionString;
    }

    public NpgsqlDataSource BuildDataSource() => NpgsqlDataSource.Create(BuildConnectionString());

    public override string ToString() =>
        $"Postgres {Username}@{Host}:{Port}/{Database} (CommandTimeout={CommandTimeoutSeconds}s, Pool={MinPoolSize}..{MaxPoolSize}, KeepAlive={KeepAliveSeconds}s, Password=***)";

    private string? ResolvePassword()
    {
        var hasPassword = !string.IsNullOrEmpty(Password);
        var hasFile = !string.IsNullOrWhiteSpace(PasswordFile);
        if (hasPassword && hasFile)
        {
            throw new InvalidOperationException("Postgres:Password and Postgres:PasswordFile are mutually exclusive.");
        }

        if (!hasFile) return Password;

        try
        {
            // Завершающий перевод строки в файле секрета не часть пароля.
            return File.ReadAllText(PasswordFile!).TrimEnd('\r', '\n');
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Сообщение содержит только путь к файлу, не его содержимое.
            throw new InvalidOperationException($"Postgres:PasswordFile '{PasswordFile}' cannot be read: {ex.GetType().Name}.");
        }
    }
}

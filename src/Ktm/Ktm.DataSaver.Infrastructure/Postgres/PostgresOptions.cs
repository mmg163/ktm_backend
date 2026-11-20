using Npgsql;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Настройки подключения к БД журнала (секция <c>DataSaver:Postgres</c>, окружение <c>DataSaver__Postgres__*</c>).
/// Host, Database, Username и пароль обязательны (ADR-0002 D-11: без значений по умолчанию). Пароль - переменная
/// <see cref="Password"/> либо файл <see cref="PasswordFile"/>, ровно один из двух. Пароль не попадает в
/// <see cref="ToString"/> и в сообщения исключений.
/// </summary>
public sealed class PostgresOptions
{
    /// <summary>Допустимые значения <see cref="SslMode"/>.</summary>
    public static readonly IReadOnlyList<string> AllowedSslModes = ["Disable", "Prefer", "Require", "VerifyCA", "VerifyFull"];

    public string Host { get; set; } = string.Empty;

    public int Port { get; set; } = 5432;

    public string Database { get; set; } = string.Empty;

    public string Username { get; set; } = string.Empty;

    public string? Password { get; set; }

    public string? PasswordFile { get; set; }

    /// <summary>Шифрование канала (docs/07 п. 7.3): по умолчанию Require; Disable - только для тестов и локальной разработки.</summary>
    public string SslMode { get; set; } = "Require";

    /// <summary>Предел одной команды. Запись пачки - три обращения (BeginTransaction, ExecuteNonQuery, Commit), см. проверку LiveStaleSeconds.</summary>
    public int CommandTimeoutSeconds { get; set; } = 10;

    public int ConnectTimeoutSeconds { get; set; } = 15;

    public int MinPoolSize { get; set; } = 1;

    public int MaxPoolSize { get; set; } = 10;

    public int KeepAliveSeconds { get; set; } = 30;

    /// <summary>Проверяет настройки и собирает строку подключения (содержит пароль: в лог не выводить).</summary>
    public string BuildConnectionString()
    {
        if (string.IsNullOrWhiteSpace(Host)) throw new InvalidOperationException("DataSaver__Postgres__Host is required.");
        if (string.IsNullOrWhiteSpace(Database)) throw new InvalidOperationException("DataSaver__Postgres__Database is required.");
        if (string.IsNullOrWhiteSpace(Username)) throw new InvalidOperationException("DataSaver__Postgres__Username is required.");
        if (Port is < 1 or > 65535) throw new InvalidOperationException("DataSaver__Postgres__Port must be in range 1..65535.");
        if (CommandTimeoutSeconds < 1) throw new InvalidOperationException("DataSaver__Postgres__CommandTimeoutSeconds must be positive.");
        if (ConnectTimeoutSeconds < 1) throw new InvalidOperationException("DataSaver__Postgres__ConnectTimeoutSeconds must be positive.");
        if (MinPoolSize < 0 || MaxPoolSize < 1 || MinPoolSize > MaxPoolSize)
        {
            throw new InvalidOperationException("DataSaver__Postgres__MinPoolSize and DataSaver__Postgres__MaxPoolSize must satisfy 0 <= MinPoolSize <= MaxPoolSize and MaxPoolSize >= 1.");
        }

        if (KeepAliveSeconds < 0) throw new InvalidOperationException("DataSaver__Postgres__KeepAliveSeconds must not be negative.");

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = Host,
            Port = Port,
            Database = Database,
            Username = Username,
            Password = ResolvePassword(),
            SslMode = ParseSslMode(),
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
        $"Postgres {Username}@{Host}:{Port}/{Database} (SslMode={SslMode}, CommandTimeout={CommandTimeoutSeconds}s, Pool={MinPoolSize}..{MaxPoolSize}, KeepAlive={KeepAliveSeconds}s, Password=***)";

    private Npgsql.SslMode ParseSslMode()
    {
        var match = AllowedSslModes.FirstOrDefault(m => string.Equals(m, SslMode, StringComparison.OrdinalIgnoreCase));
        if (match is null)
        {
            throw new InvalidOperationException(
                $"DataSaver__Postgres__SslMode '{SslMode}' is not supported. Allowed values: {string.Join(", ", AllowedSslModes)}.");
        }

        return Enum.Parse<Npgsql.SslMode>(match);
    }

    private string ResolvePassword()
    {
        var hasPassword = !string.IsNullOrEmpty(Password);
        var hasFile = !string.IsNullOrWhiteSpace(PasswordFile);
        if (hasPassword && hasFile)
        {
            throw new InvalidOperationException("DataSaver__Postgres__Password and DataSaver__Postgres__PasswordFile are mutually exclusive.");
        }

        if (!hasPassword && !hasFile)
        {
            throw new InvalidOperationException("DataSaver__Postgres__Password or DataSaver__Postgres__PasswordFile is required.");
        }

        if (hasPassword) return Password!;

        string password;
        try
        {
            // Завершающий перевод строки в файле секрета не часть пароля.
            password = File.ReadAllText(PasswordFile!).TrimEnd('\r', '\n');
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Сообщение содержит только путь к файлу, не его содержимое.
            throw new InvalidOperationException($"DataSaver__Postgres__PasswordFile '{PasswordFile}' cannot be read: {ex.GetType().Name}.");
        }

        if (password.Length == 0)
        {
            throw new InvalidOperationException($"DataSaver__Postgres__PasswordFile '{PasswordFile}' is empty.");
        }

        return password;
    }
}

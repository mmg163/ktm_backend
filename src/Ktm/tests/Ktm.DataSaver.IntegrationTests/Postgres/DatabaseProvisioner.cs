using Npgsql;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Конечная точка БД журнала для тестов: строки подключения администратора и ktm_app.</summary>
public interface IDatabaseEndpoint
{
    string AdminConnectionString { get; }

    string AppConnectionString { get; }
}

/// <summary>
/// Подготовка контейнера TimescaleDB скриптами из репозитория (не копиями): <c>db/init</c> под суперпользователем,
/// <c>db/migrations</c> под <c>ktm_migrator</c>. Файлы выполняются через Npgsql как есть (без метакоманд psql);
/// пароли ролям задаются отдельно (в репозиторий не попадают).
/// </summary>
internal static class DatabaseProvisioner
{
    public const string Image = "timescale/timescaledb:2.32.1-pg16";
    public const string Database = "ktm_revisor";
    public const string MigratorPassword = "migrator-test-password";
    public const string AppPassword = "app-test-password";

    public static PostgreSqlBuilder NewBuilder() => new PostgreSqlBuilder(Image).WithDatabase(Database);

    public static string ConnectionFor(string baseConnectionString, string user, string password) =>
        new NpgsqlConnectionStringBuilder(baseConnectionString) { Username = user, Password = password, SslMode = SslMode.Disable }.ConnectionString;

    public static async Task ProvisionAsync(string adminConnectionString)
    {
        await ApplyInitAsync(adminConnectionString);
        await SetRolePasswordAsync(adminConnectionString, "ktm_migrator", MigratorPassword);
        await SetRolePasswordAsync(adminConnectionString, "ktm_app", AppPassword);
        await ApplyMigrationsAsync(ConnectionFor(adminConnectionString, "ktm_migrator", MigratorPassword));
    }

    /// <summary>Только <c>db/init</c> и пароли ролей, без миграций (их накатывает проверяемый скрипт).</summary>
    public static async Task ProvisionWithoutMigrationsAsync(string adminConnectionString, string migratorPassword)
    {
        await ApplyInitAsync(adminConnectionString);
        await SetRolePasswordAsync(adminConnectionString, "ktm_migrator", migratorPassword);
    }

    public static Task ApplyInitAsync(string adminConnectionString) => ApplyFilesAsync("db/init", adminConnectionString);

    public static Task ApplyMigrationsAsync(string migratorConnectionString) => ApplyFilesAsync("db/migrations", migratorConnectionString);

    private static async Task SetRolePasswordAsync(string adminConnectionString, string role, string password)
    {
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync();
        await using var format = new NpgsqlCommand("SELECT format('ALTER ROLE %I PASSWORD %L', @role, @password)", connection);
        format.Parameters.AddWithValue("role", role);
        format.Parameters.AddWithValue("password", password);
        var alter = (string)(await format.ExecuteScalarAsync())!;
        await using var execute = new NpgsqlCommand(alter, connection);
        await execute.ExecuteNonQueryAsync();
    }

    // Файлы выполняются как есть, без метакоманд psql, в порядке имён.
    private static async Task ApplyFilesAsync(string relativeDirectory, string connectionString)
    {
        var directory = Path.Combine(RepositoryRoot(), relativeDirectory);
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (var file in Directory.GetFiles(directory, "*.sql").Order(StringComparer.Ordinal))
        {
            await using var command = new NpgsqlCommand(await File.ReadAllTextAsync(file), connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    internal static string RepositoryRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "db", "migrations"))) return dir.FullName;
        }

        throw new DirectoryNotFoundException("Repository root with db/migrations was not found above the test output directory.");
    }
}

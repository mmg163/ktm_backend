using Npgsql;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres";
}

/// <summary>
/// Контейнер TimescaleDB (тег закреплён) с БД, подготовленной скриптами из репозитория (не копиями): <c>db/init</c> под
/// суперпользователем, <c>db/migrations</c> под <c>ktm_migrator</c>; пароли ролям задаются отдельно (в репозиторий не попадают).
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private const string Image = "timescale/timescaledb:2.32.1-pg16";
    private const string Database = "ktm_revisor";
    private const string MigratorPassword = "migrator-test-password";
    private const string AppPassword = "app-test-password";

    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder(Image).WithDatabase(Database).Build();

    public string AdminConnectionString => _container.GetConnectionString();

    public string MigratorConnectionString => ConnectionFor("ktm_migrator", MigratorPassword);

    public string AppConnectionString => ConnectionFor("ktm_app", AppPassword);

    /// <summary>Источник данных под ролью ktm_app: под ней работает Data Saver.</summary>
    public NpgsqlDataSource AppDataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        await ApplyInitAsync();
        await SetRolePasswordAsync("ktm_migrator", MigratorPassword);
        await SetRolePasswordAsync("ktm_app", AppPassword);
        await ApplyMigrationsAsync();
        AppDataSource = NpgsqlDataSource.Create(AppConnectionString);
    }

    public async Task DisposeAsync()
    {
        if (AppDataSource is not null) await AppDataSource.DisposeAsync();
        await _container.DisposeAsync().AsTask();
    }

    public Task ApplyInitAsync() => ApplyFilesAsync("db/init", AdminConnectionString);

    public Task ApplyMigrationsAsync() => ApplyFilesAsync("db/migrations", MigratorConnectionString);

    private string ConnectionFor(string user, string password) =>
        new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Username = user, Password = password, SslMode = SslMode.Disable }.ConnectionString;

    private async Task SetRolePasswordAsync(string role, string password)
    {
        await using var connection = new NpgsqlConnection(AdminConnectionString);
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

    private static string RepositoryRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "db", "migrations"))) return dir.FullName;
        }

        throw new DirectoryNotFoundException("Repository root with db/migrations was not found above the test output directory.");
    }
}

using Npgsql;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "postgres";
}

/// <summary>Контейнер TimescaleDB (тег закреплён) с БД, подготовленной скриптами из репозитория, см. <see cref="DatabaseProvisioner"/>.</summary>
public sealed class PostgresFixture : IAsyncLifetime, IDatabaseEndpoint
{
    private readonly PostgreSqlContainer _container = DatabaseProvisioner.NewBuilder().Build();

    public string AdminConnectionString => _container.GetConnectionString();

    public string MigratorConnectionString => DatabaseProvisioner.ConnectionFor(AdminConnectionString, "ktm_migrator", DatabaseProvisioner.MigratorPassword);

    public string AppConnectionString => DatabaseProvisioner.ConnectionFor(AdminConnectionString, "ktm_app", DatabaseProvisioner.AppPassword);

    /// <summary>Источник данных под ролью ktm_app: под ней работает Data Saver.</summary>
    public NpgsqlDataSource AppDataSource { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        await DatabaseProvisioner.ProvisionAsync(AdminConnectionString);
        AppDataSource = NpgsqlDataSource.Create(AppConnectionString);
    }

    public async Task DisposeAsync()
    {
        if (AppDataSource is not null) await AppDataSource.DisposeAsync();
        await _container.DisposeAsync().AsTask();
    }

    public Task ApplyInitAsync() => DatabaseProvisioner.ApplyInitAsync(AdminConnectionString);

    public Task ApplyMigrationsAsync() => DatabaseProvisioner.ApplyMigrationsAsync(MigratorConnectionString);
}

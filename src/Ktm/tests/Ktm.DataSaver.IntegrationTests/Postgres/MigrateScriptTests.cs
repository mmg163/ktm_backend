using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Ktm.DataSaver.IntegrationTests.Support;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Контейнер TimescaleDB в общей сети с роботом миграций: роли и пароль есть, миграций нет (их накатывает скрипт).</summary>
public sealed class MigrateScriptFixture : IAsyncLifetime
{
    // Образ клиента - тот же тег, что у сервиса migrate в deploy/data-saver/compose.yml: при смене тега менять в обоих местах.
    public const string ClientImage = "postgres:16.16-alpine3.24";

    // Пароль со спецсимволами: ':' и '\' требуют экранирования в .pgpass, пробел проверяет отсутствие обрезки.
    public const string Password = @"p:a\ss w0rd";

    private readonly INetwork _network = new NetworkBuilder().Build();
    private PostgreSqlContainer _db = null!;
    private int _databaseCounter;

    public string AdminConnectionString => _db.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _network.CreateAsync();
        _db = DatabaseProvisioner.NewBuilder().WithNetwork(_network).WithNetworkAliases("db").Build();
        await _db.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _db.DisposeAsync().AsTask();
        await _network.DeleteAsync();
    }

    /// <summary>Новая пустая база с выполненным db/init (роли общие на кластер).</summary>
    public async Task<string> CreateDatabaseAsync()
    {
        var name = $"mig{Interlocked.Increment(ref _databaseCounter)}";
        await using (var connection = new NpgsqlConnection(AdminConnectionString))
        {
            await connection.OpenAsync();
            await using var create = new NpgsqlCommand($"CREATE DATABASE {name}", connection);
            await create.ExecuteNonQueryAsync();
        }

        await DatabaseProvisioner.ProvisionWithoutMigrationsAsync(AdminFor(name), Password);
        return name;
    }

    /// <summary>Запуск migrate.sh в контейнере с uid 1654, каталогами и секретом только на чтение (без read_only/tmpfs, как задано в compose).</summary>
    public async Task<(long ExitCode, string Output)> RunMigrateAsync(string database, string migrationsDirectory)
    {
        var secretDirectory = Directory.CreateTempSubdirectory("migrate-secret-");
        var secretFile = Path.Combine(secretDirectory.FullName, "ktm_migrator_password");
        await File.WriteAllTextAsync(secretFile, Password + "\n");
        try
        {
            var script = Path.Combine(DatabaseProvisioner.RepositoryRoot(), "deploy", "data-saver", "migrate");
            await using var container = new ContainerBuilder(ClientImage)
                .WithNetwork(_network)
                .WithEntrypoint("sh", "-c", "sh /migrate/migrate.sh; echo \"MIGRATE_EXIT=$?\"")
                .WithCreateParameterModifier(p => p.User = "1654:1654")
                .WithBindMount(migrationsDirectory, "/db/migrations", AccessMode.ReadOnly)
                .WithBindMount(script, "/migrate", AccessMode.ReadOnly)
                .WithBindMount(secretFile, "/run/secrets/ktm_migrator_password", AccessMode.ReadOnly)
                .WithEnvironment("DataSaver__Postgres__Host", "db")
                .WithEnvironment("DataSaver__Postgres__Database", database)
                .WithEnvironment("DataSaver__Postgres__SslMode", "Disable")
                .WithEnvironment("DataSaver__Postgres__Username", "ktm_app")
                .Build();
            await container.StartAsync();
            // Код выхода скрипта печатается обёрткой sh -c: состояние контейнера Testcontainers для быстро завершающихся не обновляет.
            string stdout = string.Empty, stderr = string.Empty;
            await Support.Wait.UntilAsync(async () =>
            {
                (stdout, stderr) = await container.GetLogsAsync();
                return stdout.Contains("MIGRATE_EXIT=");
            }, "migrate script finishes", TimeSpan.FromMinutes(3));
            var marker = stdout.LastIndexOf("MIGRATE_EXIT=", StringComparison.Ordinal);
            return (long.Parse(stdout[(marker + "MIGRATE_EXIT=".Length)..].Trim()), stdout + stderr);
        }
        finally
        {
            secretDirectory.Delete(recursive: true);
        }
    }

    public async Task<string> DumpSchemaAsync(string database)
    {
        var result = await _db.ExecAsync(["pg_dump", "--schema-only", "-n", "ktm", "-U", "postgres", database]);
        Assert.Equal(0, result.ExitCode);
        // Строки \restrict/\unrestrict содержат случайный токен: в сравнении не участвуют.
        return string.Join('\n', result.Stdout.Split('\n').Where(l => !l.StartsWith("\\restrict") && !l.StartsWith("\\unrestrict")));
    }

    public async Task<bool> TableExistsAsync(string database, string qualifiedName)
    {
        await using var connection = new NpgsqlConnection(AdminFor(database));
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT to_regclass(@name) IS NOT NULL", connection);
        command.Parameters.AddWithValue("name", qualifiedName);
        return (bool)(await command.ExecuteScalarAsync())!;
    }

    private string AdminFor(string database) =>
        new NpgsqlConnectionStringBuilder(AdminConnectionString) { Database = database, Pooling = false }.ConnectionString;
}

/// <summary>Поведение deploy/data-saver/migrate/migrate.sh (ADR-0004, раздел 5) на реальной БД.</summary>
public sealed class MigrateScriptTests(MigrateScriptFixture fixture) : IClassFixture<MigrateScriptFixture>
{
    private static string RealMigrations => Path.Combine(DatabaseProvisioner.RepositoryRoot(), "db", "migrations");

    // Двукратный прогон реального каталога: заодно метатест состава - каждый db/migrations/V*.sql при втором прогоне
    // выполняется поверх уже накатанной схемы; неидемпотентный файл (CREATE TABLE без IF NOT EXISTS) даст ненулевой код.
    [Fact]
    public async Task Running_all_migrations_twice_succeeds_and_leaves_schema_unchanged()
    {
        var database = await fixture.CreateDatabaseAsync();

        var first = await fixture.RunMigrateAsync(database, RealMigrations);
        Assert.True(first.ExitCode == 0, first.Output);
        Assert.True(await fixture.TableExistsAsync(database, "ktm.events"));
        var schemaAfterFirst = await fixture.DumpSchemaAsync(database);

        var second = await fixture.RunMigrateAsync(database, RealMigrations);
        Assert.True(second.ExitCode == 0, second.Output);
        Assert.Equal(schemaAfterFirst, await fixture.DumpSchemaAsync(database));
        Assert.DoesNotContain(MigrateScriptFixture.Password, first.Output + second.Output);
    }

    [Fact]
    public async Task Invalid_sql_fails_with_file_name_and_error_stops_the_run_and_does_not_leak_password()
    {
        var database = await fixture.CreateDatabaseAsync();
        var directory = Directory.CreateTempSubdirectory("migrate-sql-");
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "V001__ok.sql"), "CREATE TABLE ktm.first_ok (a int);");
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "V002__broken.sql"), "SELEC 1;");
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "V003__after.sql"), "CREATE TABLE ktm.must_not_exist (a int);");

            var result = await fixture.RunMigrateAsync(database, directory.FullName);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("V002__broken.sql", result.Output);
            Assert.Contains("syntax error", result.Output);
            Assert.DoesNotContain(MigrateScriptFixture.Password, result.Output);
            Assert.True(await fixture.TableExistsAsync(database, "ktm.first_ok"));
            Assert.False(await fixture.TableExistsAsync(database, "ktm.must_not_exist"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task File_with_psql_meta_command_is_rejected_before_anything_is_applied()
    {
        var database = await fixture.CreateDatabaseAsync();
        var directory = Directory.CreateTempSubdirectory("migrate-meta-");
        try
        {
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "V001__ok.sql"), "CREATE TABLE ktm.must_not_exist (a int);");
            await File.WriteAllTextAsync(Path.Combine(directory.FullName, "V002__meta.sql"), "SELECT 1;\n  \\! echo pwned\n");

            var result = await fixture.RunMigrateAsync(database, directory.FullName);

            Assert.Equal(1, result.ExitCode);
            Assert.Contains("V002__meta.sql", result.Output);
            Assert.False(await fixture.TableExistsAsync(database, "ktm.must_not_exist"));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }
}

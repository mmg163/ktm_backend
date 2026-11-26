using Ktm.DataSaver.IntegrationTests.Postgres;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>
/// Perf-тест не запускается голым <c>dotnet test</c>: без явно заданной переменной из списка он пропускается с понятной причиной.
/// Нагрузочный прогон занимает минуты и не должен стартовать случайно (в том числе по фильтру <c>Category=Perf</c> без параметров).
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class PerfFactAttribute : FactAttribute
{
    public PerfFactAttribute(params string[] requiredAnyOf)
    {
        if (requiredAnyOf.Any(name => !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(name)))) return;
        Skip = $"Perf-замер запускается только явно: задайте хотя бы одну из переменных {string.Join(", ", requiredAnyOf)} " +
               "(порядок запуска - docs/test-protocols/data-saver-throughput.md).";
    }
}

/// <summary>
/// Отдельная БД под один Perf-прогон в контейнере TimescaleDB: пустая, подготовленная теми же скриптами, что и боевая
/// (<c>db/init</c>, <c>db/migrations</c>). Объём таблицы поэтому не зависит от порядка тестов и от соседних прогонов;
/// засев фиксированного объёма делает сам прогон, фактический count(*) выводится в отчёт.
/// </summary>
internal sealed class PerfDatabase : IAsyncDisposable, IDatabaseEndpoint
{
    private readonly string _serverAdminConnectionString;
    private readonly string _name;

    private PerfDatabase(string serverAdminConnectionString, string name)
    {
        _serverAdminConnectionString = serverAdminConnectionString;
        _name = name;
        AdminConnectionString = new NpgsqlConnectionStringBuilder(serverAdminConnectionString) { Database = name, Pooling = false }.ConnectionString;
    }

    public string AdminConnectionString { get; }

    public string AppConnectionString => DatabaseProvisioner.ConnectionFor(AdminConnectionString, "ktm_app", DatabaseProvisioner.AppPassword);

    public static async Task<PerfDatabase> CreateAsync(string serverAdminConnectionString)
    {
        var name = "ktm_perf_" + Guid.NewGuid().ToString("N")[..12];
        await ExecuteAsync(serverAdminConnectionString, $"CREATE DATABASE \"{name}\"");
        var database = new PerfDatabase(serverAdminConnectionString, name);
        await DatabaseProvisioner.ProvisionAsync(database.AdminConnectionString);
        return database;
    }

    /// <summary>Точное число строк журнала (count(*), а не оценка планировщика).</summary>
    public async Task<long> CountEventsAsync()
    {
        await using var connection = new NpgsqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM ktm.events", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    /// <summary>Засев <paramref name="rows"/> строк с префиксом <c>seed-</c>, разнесённых на сутки назад (отдельно от строк прогона).</summary>
    public async Task SeedAsync(long rows)
    {
        if (rows <= 0) return;
        await using var connection = new NpgsqlConnection(AdminConnectionString);
        await connection.OpenAsync();
        await using (var insert = new NpgsqlCommand("""
            INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, device_id,
                                    external_person_id, trust, payload, schema_version)
            SELECT gen_random_uuid(), (ARRAY['skud','cv','1c-do'])[g % 3 + 1], 'seed-' || g,
                   (now() AT TIME ZONE 'UTC') - g * interval '250 milliseconds', now() AT TIME ZONE 'UTC', 'access_granted', 'turnstile-01-entry',
                   'emp-' || (g % 1000), 100::smallint, '{"direction":"in","zone":"main-gate"}'::jsonb, 1::smallint
            FROM generate_series(1, @rows) g
            """, connection) { CommandTimeout = 300 })
        {
            insert.Parameters.AddWithValue("rows", rows);
            await insert.ExecuteNonQueryAsync();
        }

        await using var analyze = new NpgsqlCommand("ANALYZE ktm.events", connection) { CommandTimeout = 300 };
        await analyze.ExecuteNonQueryAsync();
    }

    public async ValueTask DisposeAsync() =>
        await ExecuteAsync(_serverAdminConnectionString, $"DROP DATABASE IF EXISTS \"{_name}\" WITH (FORCE)");

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}

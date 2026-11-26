using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 4A.3 / 7.1: скрипты из db/ на реальной БД: идемпотентность и права ktm_app.</summary>
[Collection(PostgresCollection.Name)]
public sealed class DatabaseSetupTests(PostgresFixture db)
{
    private const string SchemaSnapshotSql = """
        SELECT line FROM (
            SELECT 'column ' || table_name || '.' || column_name || ' ' || data_type || ' null=' || is_nullable
                   || ' default=' || coalesce(column_default, '') AS line
            FROM information_schema.columns WHERE table_schema = 'ktm'
            UNION ALL
            SELECT 'constraint ' || conrelid::regclass || ' ' || conname || ' ' || pg_get_constraintdef(oid)
            FROM pg_constraint WHERE connamespace = 'ktm'::regnamespace
            UNION ALL
            SELECT 'index ' || indexdef FROM pg_indexes WHERE schemaname = 'ktm'
            UNION ALL
            SELECT 'trigger ' || pg_get_triggerdef(oid) FROM pg_trigger
            WHERE NOT tgisinternal AND tgrelid::regclass::text IN ('ktm.events', 'ktm.event_corrections')
            UNION ALL
            SELECT 'grant ' || grantee || ' ' || table_name || ' ' || privilege_type
            FROM information_schema.role_table_grants WHERE table_schema = 'ktm'
            UNION ALL
            SELECT 'owner ' || tablename || ' ' || tableowner FROM pg_tables WHERE schemaname = 'ktm'
            UNION ALL
            SELECT 'hypertable ' || hypertable_name || ' compression=' || compression_enabled || ' dims=' || num_dimensions
            FROM timescaledb_information.hypertables WHERE hypertable_schema = 'ktm'
            UNION ALL
            SELECT 'job ' || proc_name || ' ' || coalesce(config::text, '') || ' ' || schedule_interval
            FROM timescaledb_information.jobs WHERE hypertable_schema = 'ktm'
            UNION ALL
            SELECT 'role ' || rolname FROM pg_roles WHERE rolname IN ('ktm_app', 'ktm_migrator')
        ) s ORDER BY line
        """;

    private async Task<List<string>> SnapshotSchemaAsync()
    {
        await using var connection = new NpgsqlConnection(db.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(SchemaSnapshotSql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var lines = new List<string>();
        while (await reader.ReadAsync()) lines.Add(reader.GetString(0));
        return lines;
    }

    [Fact]
    public async Task Reapplying_scripts_leaves_schema_objects_constraints_triggers_and_grants_unchanged()
    {
        var before = await SnapshotSchemaAsync();

        await db.ApplyInitAsync();
        await db.ApplyMigrationsAsync();

        var after = await SnapshotSchemaAsync();
        Assert.Contains(before, l => l.StartsWith("trigger ") && l.Contains("trg_events_immutable"));
        Assert.Contains(before, l => l == "grant ktm_app events INSERT");
        Assert.Contains(before, l => l.StartsWith("job policy_compression"));
        Assert.Equal(before, after);
    }

    [Fact]
    public async Task App_role_can_insert_but_cannot_update_or_delete()
    {
        var source = TestEvents.NewSource();
        await using var connection = await db.AppDataSource.OpenConnectionAsync();

        await using (var insert = new NpgsqlCommand(
            $"""
            INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, external_person_id, trust, payload, schema_version)
            VALUES (gen_random_uuid(), '{source}', 'e1', now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC', 'access.pass', 'p1', 100, jsonb_build_object(), 1)
            """, connection))
        {
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        foreach (var statement in new[]
                 {
                     $"UPDATE ktm.events SET trust = 0 WHERE source = '{source}'",
                     $"DELETE FROM ktm.events WHERE source = '{source}'",
                     $"UPDATE ktm.event_corrections SET reason = 'x' WHERE source = '{source}'",
                     $"DELETE FROM ktm.event_corrections WHERE source = '{source}'",
                 })
        {
            await using var command = new NpgsqlCommand(statement, connection);
            var ex = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync());
            Assert.Equal("42501", ex.SqlState);
            Assert.Contains("permission denied", ex.MessageText);
        }
    }

    [Fact]
    public async Task Owner_cannot_modify_journal_either_because_of_trigger()
    {
        await using var connection = new NpgsqlConnection(db.MigratorConnectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
            INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, external_person_id, trust, payload, schema_version)
            VALUES (gen_random_uuid(), '{TestEvents.NewSource()}', 'e1', now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC', 'access.pass', 'p1', 100, jsonb_build_object(), 1)
            """, connection);
        await insert.ExecuteNonQueryAsync();

        await using var update = new NpgsqlCommand("UPDATE ktm.events SET trust = 0", connection);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => update.ExecuteNonQueryAsync());
        Assert.Equal("42501", ex.SqlState);
        Assert.Contains("append-only", ex.MessageText);
    }
}

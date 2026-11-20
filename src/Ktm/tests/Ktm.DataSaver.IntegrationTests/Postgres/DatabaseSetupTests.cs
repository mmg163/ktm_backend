using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 4A.3 / 7.1: скрипты из db/ на реальной БД: идемпотентность и права ktm_app.</summary>
[Collection(PostgresCollection.Name)]
public sealed class DatabaseSetupTests(PostgresFixture db)
{
    [Fact]
    public async Task Scripts_are_idempotent_and_keep_privileges()
    {
        await db.ApplyInitAsync();
        await db.ApplyMigrationsAsync();

        await using var connection = await db.AppDataSource.OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_schema = 'ktm' AND hypertable_name = 'events'",
            connection);
        Assert.Equal(1L, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task App_role_can_insert_but_cannot_update_or_delete()
    {
        var source = TestEvents.NewSource();
        await using var connection = await db.AppDataSource.OpenConnectionAsync();

        await using (var insert = new NpgsqlCommand(
            $"""
            INSERT INTO ktm.events (event_id, source, source_event_id, occurred_at, received_at, event_type, external_person_id, trust, payload, schema_version)
            VALUES (gen_random_uuid(), '{source}', 'e1', now(), now(), 'access.pass', 'p1', 100, jsonb_build_object(), 1)
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
            VALUES (gen_random_uuid(), '{TestEvents.NewSource()}', 'e1', now(), now(), 'access.pass', 'p1', 100, jsonb_build_object(), 1)
            """, connection);
        await insert.ExecuteNonQueryAsync();

        await using var update = new NpgsqlCommand("UPDATE ktm.events SET trust = 0", connection);
        var ex = await Assert.ThrowsAsync<PostgresException>(() => update.ExecuteNonQueryAsync());
        Assert.Equal("42501", ex.SqlState);
        Assert.Contains("append-only", ex.MessageText);
    }
}

using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 1.3: таблица правил отбраковки на реальной БД - права ktm_app, CHECK-и, повторный прогон примеров.</summary>
[Collection(PostgresCollection.Name)]
public sealed class RejectionRulesSchemaTests(PostgresFixture db)
{
    private const string ValidCondition = """{"==": [{"var": "eventType"}, "x"]}""";

    private const string ChangedCondition = """{"==": [{"var": "eventType"}, "changed"]}""";

    private static string Insert(string ruleId = "t-rule", string name = "name", string source = "'skud'", string condition = ValidCondition,
        string reason = "test_reason", string priority = "100", string version = "1") =>
        $"""
        INSERT INTO ktm.rejection_rules (rule_id, name, source, condition, reason_code, enabled, priority, version)
        VALUES ('{ruleId}', '{name}', {source}, '{condition}'::jsonb, '{reason}', false, {priority}, {version})
        """;

    private async Task<NpgsqlConnection> OpenMigratorAsync()
    {
        var connection = new NpgsqlConnection(db.MigratorConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    private static async Task<int> ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteNonQueryAsync();
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture)!;
    }

    [Fact]
    public async Task App_role_can_select_rules_but_cannot_insert_update_or_delete()
    {
        await using var connection = await db.AppDataSource.OpenConnectionAsync();

        Assert.True(Convert.ToInt64(await ScalarAsync(connection, "SELECT count(*) FROM ktm.rejection_rules"), System.Globalization.CultureInfo.InvariantCulture) >= 6);

        foreach (var statement in new[]
                 {
                     Insert("app-insert"),
                     "UPDATE ktm.rejection_rules SET enabled = true",
                     "DELETE FROM ktm.rejection_rules",
                 })
        {
            await using var command = new NpgsqlCommand(statement, connection);
            var ex = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync());
            Assert.Equal("42501", ex.SqlState);
        }
    }

    [Fact]
    public async Task Check_constraints_reject_reserved_reason_bad_id_non_object_condition_and_out_of_range_numbers()
    {
        await using var connection = await OpenMigratorAsync();

        var invalid = new (string Sql, string Constraint)[]
        {
            (Insert("c-reason", reason: "invalid_schema"), "ck_rejection_rules_reason"),
            (Insert("c-reason2", reason: "malformed_json"), "ck_rejection_rules_reason"),
            (Insert("c-reason3", reason: "Bad-Code"), "ck_rejection_rules_reason"),
            (Insert("bad id"), "ck_rejection_rules_id"),
            (Insert("c-cond-array", condition: "[1]"), "ck_rejection_rules_cond"),
            (Insert("c-cond-scalar", condition: "5"), "ck_rejection_rules_cond"),
            (Insert("c-prio", priority: "-1"), "ck_rejection_rules_prio"),
            (Insert("c-prio-high", priority: "10001"), "ck_rejection_rules_prio"),
            (Insert("c-version", version: "0"), "ck_rejection_rules_version"),
            (Insert("c-name", name: " "), "ck_rejection_rules_name"),
        };

        foreach (var (sql, constraint) in invalid)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            var ex = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync());
            Assert.Equal("23514", ex.SqlState);
            Assert.Equal(constraint, ex.ConstraintName);
        }

        Assert.Equal("0", await ScalarAsync(connection, "SELECT count(*) FROM ktm.rejection_rules WHERE rule_id LIKE 'c-%' OR rule_id = 'bad id'"));
    }

    [Fact]
    public async Task Condition_size_limit_is_logical_text_length_not_storage_size()
    {
        await using var connection = await OpenMigratorAsync();

        // jsonb-текст нормализован: {"k": "<pad>"}; доводим до ровно 16384 байт UTF-8.
        static string Condition(int textBytes) => "{\"k\": \"" + new string('a', textBytes - 9) + "\"}";
        // сжимаемое (хранится в TOAST-сжатом виде заметно меньше 16 КиБ), но по тексту больше лимита
        var exact = Condition(16384);
        var over = Condition(16385);

        Assert.Equal("16384", await ScalarAsync(connection, $"SELECT octet_length('{exact}'::jsonb::text)"));
        await ExecuteAsync(connection, Insert("sz-exact", condition: exact));

        var ex = await Assert.ThrowsAsync<PostgresException>(async () => await ExecuteAsync(connection, Insert("sz-over", condition: over)));
        Assert.Equal("23514", ex.SqlState);
        Assert.Equal("ck_rejection_rules_cond", ex.ConstraintName);

        await ExecuteAsync(connection, "DELETE FROM ktm.rejection_rules WHERE rule_id = 'sz-exact'");
    }

    [Fact]
    public async Task Insert_without_enabled_or_without_source_is_rejected_because_there_is_no_default()
    {
        await using var connection = await OpenMigratorAsync();

        foreach (var (sql, column) in new[]
                 {
                     ("INSERT INTO ktm.rejection_rules (rule_id, name, source, condition, reason_code) VALUES ('n-enabled', 'n', 'skud', '{}'::jsonb, 'r')", "enabled"),
                     ("INSERT INTO ktm.rejection_rules (rule_id, name, condition, reason_code, enabled) VALUES ('n-source', 'n', '{}'::jsonb, 'r', false)", "source"),
                 })
        {
            await using var command = new NpgsqlCommand(sql, connection);
            var ex = await Assert.ThrowsAsync<PostgresException>(() => command.ExecuteNonQueryAsync());
            Assert.Equal("23502", ex.SqlState);
            Assert.Equal(column, ex.ColumnName);
        }
    }

    [Fact]
    public async Task Rerunning_migrations_does_not_overwrite_a_hand_edited_example()
    {
        await using var connection = await OpenMigratorAsync();
        const string id = "skud-test-event";
        var original = await ScalarAsync(connection, $"SELECT condition::text FROM ktm.rejection_rules WHERE rule_id = '{id}'");

        try
        {
            await ExecuteAsync(connection,
                $"""UPDATE ktm.rejection_rules SET enabled = true, version = 2, condition = '{ChangedCondition}'::jsonb WHERE rule_id = '{id}'""");

            await db.ApplyMigrationsAsync();

            Assert.Equal("True", await ScalarAsync(connection, $"SELECT enabled FROM ktm.rejection_rules WHERE rule_id = '{id}'"));
            Assert.Equal("2", await ScalarAsync(connection, $"SELECT version FROM ktm.rejection_rules WHERE rule_id = '{id}'"));
            Assert.Contains("changed", await ScalarAsync(connection, $"SELECT condition::text FROM ktm.rejection_rules WHERE rule_id = '{id}'"));
        }
        finally
        {
            await ExecuteAsync(connection,
                $"UPDATE ktm.rejection_rules SET enabled = false, version = 1, condition = '{original}'::jsonb WHERE rule_id = '{id}'");
        }
    }

    [Fact]
    public async Task A_deleted_example_comes_back_after_rerun_and_stays_disabled()
    {
        await using var connection = await OpenMigratorAsync();
        await ExecuteAsync(connection, "DELETE FROM ktm.rejection_rules WHERE rule_id = 'common-test-flag'");

        await db.ApplyMigrationsAsync();

        Assert.Equal("False", await ScalarAsync(connection, "SELECT enabled FROM ktm.rejection_rules WHERE rule_id = 'common-test-flag'"));
        Assert.Equal("*", await ScalarAsync(connection, "SELECT source FROM ktm.rejection_rules WHERE rule_id = 'common-test-flag'"));
    }
}

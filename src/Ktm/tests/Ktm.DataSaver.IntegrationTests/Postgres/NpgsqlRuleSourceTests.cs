using System.Text.Json;
using Ktm.DataSaver.Core.Rules;
using Ktm.DataSaver.Infrastructure.Postgres;
using Npgsql;
using NpgsqlTypes;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 4.1: чтение таблицы правил под ролью ktm_app на реальной БД.</summary>
[Collection(PostgresCollection.Name)]
public sealed class NpgsqlRuleSourceTests(PostgresFixture db)
{
    private const string BigNumber = "12345678901234567890.123456789";

    private const string RichCondition =
        """{"and": [{"<=": [0, {"var": "payload.confidence"}, 0.3]}, {"==": [{"var": "payload.counter"}, 12345678901234567890.123456789]}, {"!": {"==": [{"var": "deviceId"}, null]}}]}""";

    private const string SimpleCondition = """{"==": [{"var": "eventType"}, "x"]}""";

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private Task AsMigratorAsync(string sql) => ExecuteAsync(db.MigratorConnectionString, sql);

    private async Task InsertAsync(string id, string name, string? description, string source, string condition, string reason, bool enabled, int priority = 100, int version = 1)
    {
        await using var connection = new NpgsqlConnection(db.MigratorConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO ktm.rejection_rules (rule_id, name, description, source, condition, reason_code, enabled, priority, version)
            VALUES (@id, @name, @description, @source, @condition, @reason, @enabled, @priority, @version)
            """, connection);
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("name", name);
        command.Parameters.AddWithValue("description", (object?)description ?? DBNull.Value);
        command.Parameters.AddWithValue("source", source);
        command.Parameters.Add(new NpgsqlParameter("condition", NpgsqlDbType.Jsonb) { Value = condition });
        command.Parameters.AddWithValue("reason", reason);
        command.Parameters.AddWithValue("enabled", enabled);
        command.Parameters.AddWithValue("priority", priority);
        command.Parameters.AddWithValue("version", version);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task ReadAsync_ReturnsAllRowsIncludingDisabled_MapsColumns_AndKeepsJsonStructureAndNumberPrecision()
    {
        await InsertAsync("src-own", "Свой источник", "описание", "cv", RichCondition, "low_confidence", enabled: true, priority: 7, version: 3);
        await InsertAsync("src-common", "Общее", null, "*", SimpleCondition, "common_reason", enabled: false);
        try
        {
            var source = new NpgsqlRuleSource(db.AppDataSource);

            var result = await source.ReadAsync(null, CancellationToken.None);

            Assert.False(result.Unchanged);
            Assert.Empty(result.Problems);
            // Примеры V003 (выключенные) читаются так же, как и пользовательские строки.
            Assert.Contains(result.Rules, r => r.Id == "skud-test-event" && !r.Enabled);
            var own = Assert.Single(result.Rules, r => r.Id == "src-own");
            Assert.Equal(("Свой источник", "описание", "cv", true, 7, 3, "low_confidence"),
                (own.Name, own.Description, own.Source, own.Enabled, own.Priority, own.Version, own.ReasonCode));
            var operands = own.Condition.GetProperty("and");
            Assert.Equal(3, operands.GetArrayLength());
            Assert.Equal("payload.confidence", operands[0].GetProperty("<=")[1].GetProperty("var").GetString());
            Assert.Equal(BigNumber, operands[1].GetProperty("==")[1].GetRawText());
            Assert.Equal(JsonValueKind.Null, operands[2].GetProperty("!").GetProperty("==")[1].ValueKind);
            var common = Assert.Single(result.Rules, r => r.Id == "src-common");
            Assert.Null(common.Source);
            Assert.Null(common.Description);
            Assert.False(common.Enabled);
            // Порядок выборки: priority, затем rule_id.
            var order = result.Rules.Select(r => (r.Priority, r.Id)).ToList();
            Assert.Equal(order.OrderBy(o => o.Priority).ThenBy(o => o.Id, StringComparer.Ordinal), order);
        }
        finally
        {
            await AsMigratorAsync("DELETE FROM ktm.rejection_rules WHERE rule_id IN ('src-own', 'src-common')");
        }
    }

    /// <summary>Допустимое условие (or из 40 сравнений) ровно в <paramref name="bytes"/> байт UTF-8 в jsonb-форме, как condition::text в БД.</summary>
    private static string JsonbConditionOfBytes(int bytes)
    {
        const int leaves = 40;
        const int literalCap = 1000;
        const string head = "{\"==\": [{\"var\": \"eventType\"}, \"";
        const string tail = "\"]}";
        const string separator = ", ";
        var structure = "{\"or\": [".Length + "]}".Length + (leaves - 1) * separator.Length + leaves * (head.Length + tail.Length);
        var chars = new string('a', bytes - structure);
        var parts = new List<string>();
        for (var i = 0; i < leaves; i++)
        {
            var start = Math.Min(i * literalCap, chars.Length);
            parts.Add(head + chars.Substring(start, Math.Min(literalCap, chars.Length - start)) + tail);
        }

        return "{\"or\": [" + string.Join(separator, parts) + "]}";
    }

    [Fact]
    public async Task Condition_of_exactly_16384_jsonb_bytes_is_stored_read_and_compiled()
    {
        var condition = JsonbConditionOfBytes(ConditionCompiler.MaxConditionBytes);
        Assert.Equal(16384, ConditionCompiler.MaxConditionBytes);
        await using (var connection = new NpgsqlConnection(db.MigratorConnectionString))
        {
            await connection.OpenAsync();
            await using var check = new NpgsqlCommand($"SELECT octet_length('{condition}'::jsonb::text) = 16384", connection);
            Assert.True((bool)(await check.ExecuteScalarAsync())!);
        }

        await InsertAsync("max-size-rule", "n", null, "skud", condition, "r", enabled: true);
        try
        {
            var result = await new NpgsqlRuleSource(db.AppDataSource).ReadAsync(null, CancellationToken.None);

            var rule = Assert.Single(result.Rules, r => r.Id == "max-size-rule");
            var snapshot = new RuleSnapshotBuilder(new ConditionCompiler()).Build(result.Revision, [rule], []);
            Assert.Empty(snapshot.Errors);
            Assert.Equal("max-size-rule", Assert.Single(snapshot.Rules).Definition.Id);
        }
        finally
        {
            await AsMigratorAsync("DELETE FROM ktm.rejection_rules WHERE rule_id = 'max-size-rule'");
        }
    }

    [Fact]
    public async Task Revision_IsStableWithoutChanges_AndChangesOnUpdate()
    {
        await InsertAsync("rev-rule", "n", null, "skud", SimpleCondition, "r", enabled: false);
        try
        {
            var source = new NpgsqlRuleSource(db.AppDataSource);
            var first = await source.ReadAsync(null, CancellationToken.None);

            var same = await source.ReadAsync(first.Revision, CancellationToken.None);

            Assert.True(same.Unchanged);
            Assert.Equal(first.Revision, same.Revision);
            Assert.Empty(same.Rules);

            await AsMigratorAsync("UPDATE ktm.rejection_rules SET enabled = true, version = version + 1 WHERE rule_id = 'rev-rule'");
            var changed = await source.ReadAsync(first.Revision, CancellationToken.None);

            Assert.False(changed.Unchanged);
            Assert.NotEqual(first.Revision, changed.Revision);
            var rule = Assert.Single(changed.Rules, r => r.Id == "rev-rule");
            Assert.True(rule.Enabled);
            Assert.Equal(2, rule.Version);

            // Правка только условия без смены version тоже меняет ревизию (иначе правка не была бы замечена).
            await AsMigratorAsync("""UPDATE ktm.rejection_rules SET condition = '{"==": [{"var": "eventType"}, "y"]}'::jsonb WHERE rule_id = 'rev-rule'""");
            var edited = await source.ReadAsync(changed.Revision, CancellationToken.None);
            Assert.False(edited.Unchanged);
        }
        finally
        {
            await AsMigratorAsync("DELETE FROM ktm.rejection_rules WHERE rule_id = 'rev-rule'");
        }
    }
}

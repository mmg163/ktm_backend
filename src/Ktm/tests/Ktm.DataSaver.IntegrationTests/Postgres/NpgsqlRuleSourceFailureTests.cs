using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.Infrastructure.Postgres;
using Ktm.DataSaver.IntegrationTests.Support;
using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

/// <summary>Задача 4.1: класс сбоя чтения таблицы правил. Свой контейнер: тест ломает таблицу, права и саму БД.</summary>
public sealed class NpgsqlRuleSourceFailureTests
{
    private static async Task AsAdminAsync(StoppableDatabase db, string sql)
    {
        await using var connection = new NpgsqlConnection(db.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<RuleSourceException> FailureAsync(NpgsqlRuleSource source) =>
        await Assert.ThrowsAsync<RuleSourceException>(async () => await source.ReadAsync(null, CancellationToken.None));

    [Fact]
    public async Task UnmappableRow_BecomesProblem_NoPrivilegeOrTable_IsConfiguration_DatabaseDown_IsUnavailable_WithoutSecrets()
    {
        await using var db = await StoppableDatabase.StartAsync();
        await using var dataSource = NpgsqlDataSource.Create(db.AppConnectionString);
        var source = new NpgsqlRuleSource(dataSource);

        // Строка, которая не отображается (NULL в обязательном столбце), не мешает остальным.
        await AsAdminAsync(db, "ALTER TABLE ktm.rejection_rules ALTER COLUMN name DROP NOT NULL");
        await AsAdminAsync(db, """
            INSERT INTO ktm.rejection_rules (rule_id, name, source, condition, reason_code, enabled)
            VALUES ('broken-row', NULL, 'skud', '{"==": [{"var": "eventType"}, "x"]}'::jsonb, 'r', true)
            """);
        var result = await source.ReadAsync(null, CancellationToken.None);
        var problem = Assert.Single(result.Problems);
        Assert.Equal("broken-row", problem.RuleId);
        Assert.Contains("name", problem.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(result.Rules, r => r.Id == "broken-row");
        Assert.Contains(result.Rules, r => r.Id == "skud-test-event");

        await AsAdminAsync(db, "REVOKE SELECT ON ktm.rejection_rules FROM ktm_app");
        var noPrivilege = await FailureAsync(source);
        Assert.Equal(RuleSourceFailureKind.Configuration, noPrivilege.Kind);
        Assert.Contains("42501", noPrivilege.Message, StringComparison.Ordinal);
        await AsAdminAsync(db, "GRANT SELECT ON ktm.rejection_rules TO ktm_app");

        await AsAdminAsync(db, "ALTER TABLE ktm.rejection_rules RENAME TO rejection_rules_moved");
        var noTable = await FailureAsync(source);
        Assert.Equal(RuleSourceFailureKind.Configuration, noTable.Kind);
        Assert.Contains("42P01", noTable.Message, StringComparison.Ordinal);
        await AsAdminAsync(db, "ALTER TABLE ktm.rejection_rules_moved RENAME TO rejection_rules");
        Assert.NotEmpty((await source.ReadAsync(null, CancellationToken.None)).Rules);

        await db.StopAsync();
        var down = await FailureAsync(source);
        Assert.Equal(RuleSourceFailureKind.Unavailable, down.Kind);
        // Ни строки подключения, ни пароля, ни адреса в тексте ошибки.
        Assert.DoesNotContain(DatabaseProvisioner.AppPassword, down.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Host=", down.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("127.0.0.1", down.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("localhost", down.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Null(down.InnerException);
    }
}

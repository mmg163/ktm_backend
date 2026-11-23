using System.Globalization;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Npgsql;
using NpgsqlTypes;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>Правки таблицы правил под ролью <c>ktm_migrator</c> (так их вносит администратор, ADR-0005 п. 2.3): Data Saver под <c>ktm_app</c> правила только читает.</summary>
internal static class RuleTable
{
    public static async Task ExecuteAsync(IDatabaseEndpoint db, string sql, params NpgsqlParameter[] parameters)
    {
        var connectionString = DatabaseProvisioner.ConnectionFor(db.AdminConnectionString, "ktm_migrator", DatabaseProvisioner.MigratorPassword);
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddRange(parameters);
        await command.ExecuteNonQueryAsync();
    }

    public static Task InsertAsync(
        IDatabaseEndpoint db, string ruleId, string name, string source, string conditionJson, string reasonCode,
        bool enabled = true, int priority = 100, int version = 1) =>
        ExecuteAsync(
            db,
            """
            INSERT INTO ktm.rejection_rules (rule_id, name, source, condition, reason_code, enabled, priority, version)
            VALUES (@id, @name, @source, @condition, @reason, @enabled, @priority, @version)
            """,
            new NpgsqlParameter("id", ruleId),
            new NpgsqlParameter("name", name),
            new NpgsqlParameter("source", source),
            new NpgsqlParameter("condition", NpgsqlDbType.Jsonb) { Value = conditionJson },
            new NpgsqlParameter("reason", reasonCode),
            new NpgsqlParameter("enabled", enabled),
            new NpgsqlParameter("priority", priority),
            new NpgsqlParameter("version", version));

    public static Task UpdateAsync(IDatabaseEndpoint db, string ruleId, string setClause, params NpgsqlParameter[] parameters) =>
        ExecuteAsync(db, $"UPDATE ktm.rejection_rules SET {setClause} WHERE rule_id = @id", [.. parameters, new NpgsqlParameter("id", ruleId)]);

    public static Task SetConditionAsync(IDatabaseEndpoint db, string ruleId, string conditionJson, string extraSet = "") =>
        UpdateAsync(db, ruleId, "condition = @condition" + extraSet, new NpgsqlParameter("condition", NpgsqlDbType.Jsonb) { Value = conditionJson });

    /// <summary>Число строк журнала целиком (в БД теста других событий нет).</summary>
    public static async Task<long> EventCountAsync(IDatabaseEndpoint db)
    {
        await using var connection = new NpgsqlConnection(db.AdminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM ktm.events", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }
}

/// <summary>Тела сообщений конверта v1 для трёх источников (поля как в <c>kafka-event.schema.json</c>: skud/cv требуют deviceId).</summary>
internal static class RuleMessages
{
    public static readonly string[] Sources = ["skud", "cv", "1c-do"];

    public static string DeviceFor(string source) => source switch
    {
        "skud" => "turnstile-01-entry",
        "cv" => "camera-07",
        _ => string.Empty,
    };

    public static string DefaultType(string source) => source switch
    {
        "skud" => "access_granted",
        "cv" => "presence_detected",
        _ => "task_assigned",
    };

    public static string DefaultPayload(string source, long seq) => source switch
    {
        "skud" => "{\"direction\":\"in\",\"zone\":\"main-gate\"}",
        "cv" => "{\"confidence\":0.93,\"frameRef\":\"s3://cv/2026/11/20/f-" + seq.ToString(CultureInfo.InvariantCulture) + "\"}",
        _ => "{\"taskId\":\"T-" + seq.ToString(CultureInfo.InvariantCulture) + "\",\"priority\":\"normal\"}",
    };

    public static string Body(string source, string sourceEventId, string occurredAt, string eventType, string payloadJson, string? extraTopLevel = null, string person = "card:0012345678")
    {
        var device = DeviceFor(source);
        var deviceJson = device.Length == 0 ? string.Empty : $",\"deviceId\":\"{device}\"";
        var extra = extraTopLevel is null ? string.Empty : "," + extraTopLevel;
        return $"{{\"schemaVersion\":1,\"source\":\"{source}\",\"sourceEventId\":\"{sourceEventId}\",\"occurredAt\":\"{occurredAt}\",\"eventType\":\"{eventType}\"{deviceJson},\"externalPersonId\":\"{person}\",\"payload\":{payloadJson}{extra}}}";
    }
}

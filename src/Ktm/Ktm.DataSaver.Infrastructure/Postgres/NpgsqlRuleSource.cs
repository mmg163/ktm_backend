using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Ktm.DataSaver.Application.Rules;
using Npgsql;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Источник правил из таблицы <c>ktm.rejection_rules</c> под ролью <c>ktm_app</c> (ADR-0005 п. 2.10): читает все строки
/// (включая выключенные), ревизия - SHA-256 упорядоченной выборки. Совпала с прежней - строки не разбираются.
/// Нужно только право SELECT. Тексты ошибок не содержат строки подключения и значений из таблицы.
/// </summary>
public sealed class NpgsqlRuleSource(NpgsqlDataSource dataSource) : IRuleSource
{
    private const string SelectSql = """
        SELECT rule_id, name, description, source, condition, reason_code, enabled, priority, version
        FROM ktm.rejection_rules
        ORDER BY priority, rule_id
        """;

    private const string Location = "ktm.rejection_rules";
    private const string UndefinedTable = "42P01";
    private const string InsufficientPrivilege = "42501";

    public async ValueTask<RuleSourceResult> ReadAsync(string? previousRevision, CancellationToken cancellationToken)
    {
        List<object?[]> rows;
        try
        {
            rows = await ReadRowsAsync(cancellationToken);
        }
        catch (Exception ex) when ((ex is NpgsqlException or TimeoutException or IOException) && !cancellationToken.IsCancellationRequested)
        {
            throw Classify(ex);
        }

        var revision = Revision(rows);
        if (revision == previousRevision)
        {
            return RuleSourceResult.NotChanged(revision);
        }

        var rules = new List<RuleDefinition>(rows.Count);
        var problems = new List<RuleProblem>();
        foreach (var row in rows)
        {
            try
            {
                rules.Add(Map(row));
            }
            catch (RowMappingException ex)
            {
                problems.Add(new RuleProblem(row[0] as string, Location, ex.Message, row[8] is int version ? version : null));
            }
        }

        return RuleSourceResult.Changed(revision, rules, problems);
    }

    private async Task<List<object?[]>> ReadRowsAsync(CancellationToken cancellationToken)
    {
        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(SelectSql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(values.Select(v => v is DBNull ? null : v).ToArray());
        }

        return rows;
    }

    /// <summary>SHA-256 по значениям строк в порядке выборки; jsonb приходит нормализованным текстом, поэтому ревизия стабильна.</summary>
    private static string Revision(List<object?[]> rows)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var row in rows)
        {
            foreach (var value in row)
            {
                var text = value is null ? "\u0002" : Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
                hash.AppendData(Encoding.UTF8.GetBytes(text));
                hash.AppendData("\u0001"u8);
            }

            hash.AppendData("\u0003"u8);
        }

        return Convert.ToHexString(hash.GetHashAndReset());
    }

    private static RuleDefinition Map(object?[] row)
    {
        var source = Text(row[3], "source");
        JsonElement condition;
        try
        {
            // Числа остаются текстом JSON: JsonElement не приводит их к double.
            using var document = JsonDocument.Parse(Text(row[4], "condition"));
            condition = document.RootElement.Clone();
        }
        catch (JsonException)
        {
            throw new RowMappingException("column condition does not contain valid JSON");
        }

        return new RuleDefinition(
            Text(row[0], "rule_id"),
            Text(row[1], "name"),
            row[2] is null ? null : Text(row[2], "description"),
            source == "*" ? null : source,
            row[6] is bool enabled ? enabled : throw new RowMappingException("column enabled has an unexpected type or is NULL"),
            Number(row[7], "priority"),
            Number(row[8], "version"),
            Text(row[5], "reason_code"),
            condition);
    }

    private static string Text(object? value, string column) =>
        value as string ?? throw new RowMappingException($"column {column} has an unexpected type or is NULL");

    private static int Number(object? value, string column) =>
        value is int number ? number : throw new RowMappingException($"column {column} has an unexpected type or is NULL");

    /// <summary>42P01/42501 - ошибка развёртывания (нет таблицы, нет права), прочее - временная недоступность.</summary>
    private static RuleSourceException Classify(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException { SqlState: UndefinedTable or InsufficientPrivilege } pg)
            {
                return new RuleSourceException(
                    RuleSourceFailureKind.Configuration,
                    $"Rule table {Location} is not readable (SQLSTATE {pg.SqlState}): apply migration V002 and grant SELECT to the service role.");
            }
        }

        var state = (PostgresStoreException.Adapt(exception) as PostgresStoreException)?.SqlState;
        return new RuleSourceException(
            RuleSourceFailureKind.Unavailable,
            state is null
                ? $"Rule table {Location} is unavailable: {exception.GetType().Name}."
                : $"Rule table {Location} is unavailable: SQLSTATE {state}.");
    }

    private sealed class RowMappingException(string message) : Exception(message);
}

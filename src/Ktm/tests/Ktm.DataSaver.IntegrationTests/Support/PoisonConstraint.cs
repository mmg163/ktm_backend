using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>
/// Создаёт тестовое CHECK-ограничение, которое вызывает SQLSTATE 23514 для одного
/// сгенерированного идентификатора. Уникальный идентификатор изолирует тест от
/// событий, записанных предыдущими сценариями на общей базе.
/// </summary>
internal sealed class PoisonConstraint : IAsyncDisposable
{
    public const string Name = "ck_test_poison";
    public string PoisonId { get; } = $"poison-{Guid.NewGuid():N}";

    private readonly string _adminConnectionString;
    private bool _active;

    private PoisonConstraint(string adminConnectionString) => _adminConnectionString = adminConnectionString;

    public static async Task<PoisonConstraint> AddAsync(string adminConnectionString)
    {
        var constraint = new PoisonConstraint(adminConnectionString);
        await constraint.ExecuteAsync($"ALTER TABLE ktm.events ADD CONSTRAINT {Name} CHECK (source_event_id <> '{constraint.PoisonId}')");
        constraint._active = true;
        return constraint;
    }

    /// <summary>Снимает ограничение; повторный вызов безвреден.</summary>
    public async Task DropAsync()
    {
        if (!_active)
        {
            return;
        }

        await ExecuteAsync($"ALTER TABLE ktm.events DROP CONSTRAINT IF EXISTS {Name}");
        _active = false;
    }

    public async ValueTask DisposeAsync() => await DropAsync();

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(_adminConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}

using Npgsql;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>
/// Способ получить в интеграционных тестах «ядовитое» сообщение (ADR-0006 п. 2.3, design Р-9): тестовое CHECK-ограничение на
/// <c>ktm.events</c> (<c>source_event_id &lt;&gt; 'poison'</c>), добавляемое владельцем БД (администратор тестового контейнера, как
/// <see cref="Postgres.DatabaseProvisioner"/>). Запись события с <c>sourceEventId = poison</c> даёт SQLSTATE 23514, остальные пишутся.
/// Снятие ограничения - «исправление причины». Ограничение действует на всю БД: тесты, делящие её, используют <c>poison</c> только
/// в своём источнике и под своей коллекцией. Не часть продукта.
/// </summary>
internal sealed class PoisonConstraint : IAsyncDisposable
{
    public const string Name = "ck_test_poison";
    public const string PoisonId = "poison";

    private readonly string _adminConnectionString;
    private bool _active;

    private PoisonConstraint(string adminConnectionString) => _adminConnectionString = adminConnectionString;

    public static async Task<PoisonConstraint> AddAsync(string adminConnectionString)
    {
        var constraint = new PoisonConstraint(adminConnectionString);
        await constraint.ExecuteAsync($"ALTER TABLE ktm.events ADD CONSTRAINT {Name} CHECK (source_event_id <> '{PoisonId}')");
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

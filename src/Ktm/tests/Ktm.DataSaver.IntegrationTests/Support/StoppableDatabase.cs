using System.Net;
using System.Net.Sockets;
using Ktm.DataSaver.IntegrationTests.Postgres;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Ktm.DataSaver.IntegrationTests.Support;

/// <summary>
/// Собственная БД теста с фиксированным host-портом: после остановки и запуска контейнера адрес для Data Saver
/// не меняется (данные контейнера при stop/start сохраняются).
/// </summary>
internal sealed class StoppableDatabase : IAsyncDisposable, IDatabaseEndpoint
{
    private readonly PostgreSqlContainer _container;

    private StoppableDatabase(PostgreSqlContainer container) => _container = container;

    public string AdminConnectionString => _container.GetConnectionString();

    public string AppConnectionString => DatabaseProvisioner.ConnectionFor(AdminConnectionString, "ktm_app", DatabaseProvisioner.AppPassword);

    public static async Task<StoppableDatabase> StartAsync()
    {
        var container = DatabaseProvisioner.NewBuilder().WithPortBinding(FreePort(), 5432).Build();
        await container.StartAsync();
        await DatabaseProvisioner.ProvisionAsync(container.GetConnectionString());
        return new StoppableDatabase(container);
    }

    public Task StopAsync() => _container.StopAsync();

    /// <summary>Запуск после остановки: порт тот же; возврат - когда БД принимает подключения.</summary>
    public async Task RestartAsync()
    {
        await _container.StartAsync();
        await Wait.UntilAsync(async () =>
        {
            try
            {
                await using var connection = new NpgsqlConnection(AdminConnectionString);
                await connection.OpenAsync();
                return true;
            }
            catch (NpgsqlException)
            {
                return false;
            }
        }, "database accepts connections after restart", TimeSpan.FromSeconds(60));
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}

using Npgsql;

namespace Ktm.DataSaver.Health;

/// <summary>Короткая проверка БД для readiness с пределом ожидания; недоступность - результат, а не исключение.</summary>
public static class DatabaseProbe
{
    /// <summary>
    /// false при сбое драйвера, превышении предела и <see cref="ObjectDisposedException"/> (остановка). Отмена токеном
    /// клиента health-запроса перебрасывается: клиент ушёл, это не недоступность БД. В <paramref name="onFailure"/>
    /// уходит исключение целиком: решать, что логировать (текст может содержать хост БД), должен вызывающий.
    /// </summary>
    public static async ValueTask<bool> RunAsync(
        Func<CancellationToken, Task> probe,
        TimeSpan timeout,
        Action<Exception> onFailure,
        CancellationToken cancellationToken)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        limit.CancelAfter(timeout);
        try
        {
            // WaitAsync держит предел и для пробы, которая не слушает токен.
            await probe(limit.Token).WaitAsync(timeout, cancellationToken);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex) when (ex is NpgsqlException or TimeoutException or OperationCanceledException or InvalidOperationException or IOException or ObjectDisposedException)
        {
            onFailure(ex);
            return false;
        }
    }
}

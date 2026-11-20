using Ktm.DataSaver.Core.Resilience;
using Npgsql;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Адаптер ошибки Npgsql к <see cref="ISqlStateError"/>, чтобы Core классифицировал ошибку по SQLSTATE, не зная драйвера.
/// Для <see cref="PostgresException"/> исходное исключение НЕ сохраняется: его сообщение и Detail могут содержать значения
/// из событий (ПДн). Остаются SQLSTATE, таблица и ограничение. Прочие <see cref="NpgsqlException"/> (сеть, таймаут) сохраняются
/// как внутренние: они значений не несут, а Core классифицирует по ним сетевые сбои.
/// </summary>
public sealed class PostgresStoreException : Exception, ISqlStateError
{
    public PostgresStoreException(string message, string? sqlState, string? constraintName, Exception? inner)
        : base(message, inner)
    {
        SqlState = sqlState;
        ConstraintName = constraintName;
    }

    public string? SqlState { get; }

    public string? ConstraintName { get; }

    /// <summary>Оборачивает ошибки драйвера; прочие (в том числе отмену) возвращает как есть.</summary>
    public static Exception Adapt(Exception exception)
    {
        switch (exception)
        {
            case PostgresException pg:
                return new PostgresStoreException(
                    $"PostgreSQL error SQLSTATE {pg.SqlState}" +
                    (pg.TableName is null ? "" : $" on {pg.TableName}") +
                    (pg.ConstraintName is null ? "" : $" (constraint {pg.ConstraintName})"),
                    pg.SqlState, pg.ConstraintName, inner: null);
            case NpgsqlException npg:
                return new PostgresStoreException($"Npgsql error: {npg.GetType().Name}", sqlState: null, constraintName: null, npg);
            default:
                return exception;
        }
    }
}

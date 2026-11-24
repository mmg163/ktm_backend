using Ktm.DataSaver.Core.Resilience;
using Npgsql;

namespace Ktm.DataSaver.Infrastructure.Postgres;

/// <summary>
/// Адаптер ошибки Npgsql к <see cref="ISqlStateError"/>, чтобы Core классифицировал ошибку по SQLSTATE, не зная драйвера.
/// Для <see cref="PostgresException"/> исходное исключение НЕ сохраняется: его сообщение и Detail могут содержать значения
/// из событий (ПДн). Остаются SQLSTATE, таблица и ограничение. Прочие <see cref="NpgsqlException"/> (сеть, таймаут) сохраняются
/// как внутренние: они значений не несут, а Core классифицирует по ним сетевые сбои. Исключение без SQLSTATE - отказ
/// инфраструктуры (<see cref="IInfrastructureFailure"/>): повтор без предела, в dlq не уходит.
/// </summary>
public sealed class PostgresStoreException : Exception, ISqlStateError, IDbErrorLocation, IInfrastructureFailure, IReportedExceptionType
{
    private const string PostgresExceptionTypeName = "Npgsql.PostgresException";

    public PostgresStoreException(string message, string? sqlState, string? constraintName, Exception? inner, string? tableName = null, string? reportedTypeName = null)
        : base(message, inner)
    {
        SqlState = sqlState;
        ConstraintName = constraintName;
        TableName = tableName;
        ReportedTypeName = reportedTypeName ?? inner?.GetType().FullName ?? nameof(PostgresStoreException);
    }

    public string? SqlState { get; }

    public string? ConstraintName { get; }

    public string? TableName { get; }

    /// <summary>Тип исходного исключения драйвера для <c>ktm-dlq-exception-type</c>.</summary>
    public string ReportedTypeName { get; }

    /// <summary>
    /// Оборачивает ошибки драйвера; прочие (в том числе отмену) возвращает как есть. <see cref="PostgresException"/>
    /// ищется по всей цепочке InnerException (например, ошибка аутентификации 28P01 внутри <see cref="NpgsqlException"/>).
    /// </summary>
    public static Exception Adapt(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is not PostgresException pg) continue;
            return new PostgresStoreException(
                $"PostgreSQL error SQLSTATE {pg.SqlState}" +
                (pg.TableName is null ? "" : $" on {pg.TableName}") +
                (pg.ConstraintName is null ? "" : $" (constraint {pg.ConstraintName})"),
                pg.SqlState, pg.ConstraintName, inner: null, pg.TableName, PostgresExceptionTypeName);
        }

        return exception is NpgsqlException npg
            ? new PostgresStoreException($"Npgsql error: {npg.GetType().Name}", sqlState: null, constraintName: null, npg)
            : exception;
    }

    /// <summary>
    /// Ошибка открытия соединения, не относящаяся к <see cref="NpgsqlException"/> (исчерпание пула, уничтоженный источник данных):
    /// отказ инфраструктуры, а не свойство сообщения. Текст исходного исключения не копируется.
    /// </summary>
    public static PostgresStoreException ConnectionUnavailable(Exception inner) =>
        new($"Database connection is unavailable: {inner.GetType().Name}", sqlState: null, constraintName: null, inner);
}

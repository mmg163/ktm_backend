using System.Data.Common;
using System.Net.Sockets;

namespace Ktm.DataSaver.Application.Resilience;

/// <summary>
/// Чистая функция классификации ошибок (ADR-0002 D-7, ADR-0006 п. 2.2). Порядок: инфраструктура (повтор без предела)
/// -> SQLSTATE 22/23 (<see cref="WriteErrorClass.Data"/>) -> всё остальное (<see cref="WriteErrorClass.Unexpected"/>).
/// Прежнее правило «неизвестное исключение - Transient» отменено решением лида: повтор без предела оставлен только за инфраструктурой.
/// </summary>
public static class WriteErrorClassifier
{
    public static WriteErrorClass Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return ClassifyChain(exception);
    }

    /// <summary>
    /// Ошибка значения, привязанная к сообщению (ADR-0006 п. 2.2 п. 3): не удалось отобразить значение события в параметр записи.
    /// Единый источник списка типов для классификации причины dlq и для адаптера хранилища (неизвестное исключение при работе с
    /// соединением - отказ инфраструктуры, а эти типы - свойство сообщения).
    /// </summary>
    public static bool IsMessageBoundValueError(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception is InvalidCastException or FormatException or OverflowException or ArgumentException;
    }

    private static WriteErrorClass ClassifyChain(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            switch (current)
            {
                case AggregateException aggregate:
                    return ClassifyAggregate(aggregate);
                case ISqlStateError { SqlState: { Length: > 0 } sqlState }:
                    return ClassifySqlState(sqlState);
                case IInfrastructureFailure:
                    return WriteErrorClass.Transient;
                case DbException db:
                    // Исключение драйвера БД: с SQLSTATE - по коду; без него (нет соединения, сбой протокола, таймаут) - инфраструктура.
                    return string.IsNullOrEmpty(db.SqlState) ? WriteErrorClass.Transient : ClassifySqlState(db.SqlState);
                case TimeoutException or SocketException or IOException or ObjectDisposedException or OperationCanceledException:
                    return WriteErrorClass.Transient;
            }
        }

        return WriteErrorClass.Unexpected;
    }

    private static WriteErrorClass ClassifyAggregate(AggregateException aggregate)
    {
        var classes = aggregate.InnerExceptions.Select(ClassifyChain).ToList();
        if (classes.Contains(WriteErrorClass.FatalConfig)) return WriteErrorClass.FatalConfig;
        // Сбой сети не должен превращать валидные события в отбраковку: инфраструктура сильнее ошибок данных.
        if (classes.Contains(WriteErrorClass.Transient)) return WriteErrorClass.Transient;
        if (classes.Contains(WriteErrorClass.Unexpected)) return WriteErrorClass.Unexpected;
        return classes.Count > 0 ? WriteErrorClass.Data : WriteErrorClass.Unexpected;
    }

    /// <summary>
    /// SQLSTATE первой найденной в цепочке исключения ошибки БД: <see cref="ISqlStateError"/> либо
    /// <see cref="DbException"/> (драйвер Core не знает). Для лога вместо текста исключения.
    /// </summary>
    public static string? FindSqlState(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        for (var current = exception; current is not null; current = current.InnerException)
        {
            var state = current switch
            {
                ISqlStateError e => e.SqlState,
                DbException e => e.SqlState,
                AggregateException a => a.InnerExceptions.Select(FindSqlState).FirstOrDefault(x => !string.IsNullOrEmpty(x)),
                _ => null,
            };
            if (!string.IsNullOrEmpty(state))
            {
                return state;
            }
        }

        return null;
    }

    /// <summary>Объект БД (ограничение, таблица) первой найденной в цепочке ошибки; значений из строки не содержит.</summary>
    public static IDbErrorLocation? FindLocation(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is IDbErrorLocation location)
            {
                return location;
            }

            if (current is AggregateException aggregate && aggregate.InnerExceptions.Select(FindLocation).FirstOrDefault(x => x is not null) is { } inner)
            {
                return inner;
            }
        }

        return null;
    }

    /// <summary>
    /// Классификация по SQLSTATE: 22/23 - <see cref="WriteErrorClass.Data"/>; 28/42/3D/3F/0A - <see cref="WriteErrorClass.FatalConfig"/>;
    /// пустой или короткий код и любой иной код (ресурсы, соединение, откат, отмена запроса) - <see cref="WriteErrorClass.Transient"/>:
    /// ошибка БД, не доказанная как свойство сообщения, не должна уводить сообщения в dlq.
    /// </summary>
    public static WriteErrorClass ClassifySqlState(string? sqlState)
    {
        if (string.IsNullOrEmpty(sqlState) || sqlState.Length < 2)
        {
            return WriteErrorClass.Transient;
        }

        var classCode = sqlState.AsSpan(0, 2);

        // Ошибки конфигурации/программы, повтор не лечит: аутентификация, нет БД/схемы/таблицы/прав, синтаксис, не поддерживается.
        if (classCode is "28" or "42" or "3D" or "3F" or "0A")
        {
            return WriteErrorClass.FatalConfig;
        }

        if (classCode is "22" or "23")
        {
            return WriteErrorClass.Data;
        }

        return WriteErrorClass.Transient;
    }
}

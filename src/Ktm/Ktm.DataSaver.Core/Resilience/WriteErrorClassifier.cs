using System.Net.Sockets;

namespace Ktm.DataSaver.Core.Resilience;

/// <summary>Чистая функция классификации ошибок записи (ADR-0002, D-7).</summary>
public static class WriteErrorClassifier
{
    public static WriteErrorClass Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is AggregateException aggregate)
            {
                var classes = aggregate.InnerExceptions.Select(Classify).ToList();
                if (classes.Contains(WriteErrorClass.FatalConfig)) return WriteErrorClass.FatalConfig;
                // Сбой сети не должен превращать валидные события в отбраковку: Transient сильнее Data.
                if (classes.Contains(WriteErrorClass.Transient)) return WriteErrorClass.Transient;
                return classes.Count > 0 ? WriteErrorClass.Data : WriteErrorClass.Transient;
            }

            if (current is ISqlStateError { SqlState: { Length: > 0 } sqlState })
            {
                return ClassifySqlState(sqlState);
            }

            if (current is TimeoutException or SocketException or IOException)
            {
                return WriteErrorClass.Transient;
            }
        }

        // Неизвестная ошибка: ничего не теряем, повторяем.
        return WriteErrorClass.Transient;
    }

    /// <summary>
    /// Классификация по SQLSTATE. Неизвестный или пустой код = Transient: повтор безопасен для данных
    /// (ничего не теряется и не отбраковывается).
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

        // Явно временные: 57P01-03 (остановка/перезапуск сервера), 53xxx (ресурсы), 40001, 40P01, 08xxx (соединение);
        // всё прочее тоже Transient по умолчанию.
        return WriteErrorClass.Transient;
    }
}

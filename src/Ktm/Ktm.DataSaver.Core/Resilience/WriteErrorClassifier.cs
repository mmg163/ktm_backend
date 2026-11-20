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
                if (classes.Contains(WriteErrorClass.Data)) return WriteErrorClass.Data;
                return WriteErrorClass.Transient;
            }

            if (current is ISqlStateError { SqlState: { Length: > 0 } sqlState })
            {
                return Classify(sqlState);
            }

            if (current is TimeoutException or SocketException or IOException)
            {
                return WriteErrorClass.Transient;
            }
        }

        // Неизвестная ошибка: ничего не теряем, повторяем.
        return WriteErrorClass.Transient;
    }

    public static WriteErrorClass Classify(string? sqlState)
    {
        if (string.IsNullOrEmpty(sqlState) || sqlState.Length < 2)
        {
            return WriteErrorClass.Transient;
        }

        return sqlState switch
        {
            "42501" or "42P01" => WriteErrorClass.FatalConfig,
            _ when sqlState.StartsWith("28", StringComparison.Ordinal) => WriteErrorClass.FatalConfig,
            _ when sqlState.StartsWith("22", StringComparison.Ordinal) => WriteErrorClass.Data,
            _ when sqlState.StartsWith("23", StringComparison.Ordinal) => WriteErrorClass.Data,
            _ => WriteErrorClass.Transient,
        };
    }
}

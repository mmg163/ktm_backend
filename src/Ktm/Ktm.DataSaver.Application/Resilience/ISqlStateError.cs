namespace Ktm.DataSaver.Application.Resilience;

/// <summary>
/// Исключение, несущее SQLSTATE. Infrastructure адаптирует исключение драйвера БД к этому контракту,
/// чтобы Core не ссылался на драйвер.
/// </summary>
public interface ISqlStateError
{
    string? SqlState { get; }
}

/// <summary>
/// Необязательное дополнение к <see cref="ISqlStateError"/>: объект БД, на котором произошла ошибка
/// (имя ограничения и таблицы; значений из строки не содержит). Попадает в лог вместо текста исключения.
/// </summary>
public interface IDbErrorLocation
{
    string? ConstraintName { get; }

    string? TableName { get; }
}

namespace Ktm.DataSaver.Core.Resilience;

/// <summary>
/// Исключение, несущее SQLSTATE. Infrastructure адаптирует исключение драйвера БД к этому контракту,
/// чтобы Core не ссылался на драйвер.
/// </summary>
public interface ISqlStateError
{
    string? SqlState { get; }
}

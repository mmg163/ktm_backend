namespace Ktm.DataSaver.Application.Resilience;

public enum WriteErrorClass
{
    /// <summary>Инфраструктура недоступна или перегружена: пауза и повтор той же пачки без предела.</summary>
    Transient,

    /// <summary>Ошибка конфигурации/прав: повтор как Transient, но с Error-логом и ready = false.</summary>
    FatalConfig,

    /// <summary>Ошибка данных (SQLSTATE 22/23): передаётся в IWriteFailureHandler, затем бюджет попыток (ADR-0006).</summary>
    Data,

    /// <summary>
    /// Неожиданное исключение, не относящееся к инфраструктуре и не к SQLSTATE 22/23 (ADR-0006 п. 2.2): считается свойством
    /// сообщения, расходует тот же бюджет попыток, что и <see cref="Data"/>, порт C6 не вызывается.
    /// </summary>
    Unexpected,
}

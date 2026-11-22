namespace Ktm.DataSaver.Core.Resilience;

public enum WriteErrorClass
{
    /// <summary>Временный сбой: пауза и повтор той же пачки.</summary>
    Transient,

    /// <summary>Ошибка конфигурации/прав: повтор как Transient, но с Error-логом и ready = false.</summary>
    FatalConfig,

    /// <summary>Ошибка данных: передаётся в IWriteFailureHandler.</summary>
    Data,
}

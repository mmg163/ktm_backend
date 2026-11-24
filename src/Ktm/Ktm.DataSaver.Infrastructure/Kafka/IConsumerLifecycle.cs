namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>Жизненный цикл источника сообщений: вход в группу (<see cref="Start"/>) отделён от создания, закрытие - <see cref="IDisposable.Dispose"/>.</summary>
public interface IConsumerLifecycle : IDisposable
{
    /// <summary>Подписывает consumer на топик. Идемпотентен.</summary>
    void Start();
}

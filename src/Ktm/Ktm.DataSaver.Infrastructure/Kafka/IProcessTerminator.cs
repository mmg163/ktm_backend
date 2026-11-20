namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Абстракция завершения процесса; реализацию (ненулевой код выхода + остановка host) предоставляет host (группа 6).
/// Реализация должна быть идемпотентной: вызов возможен из потоков librdkafka.
/// </summary>
public interface IProcessTerminator
{
    void Terminate(int exitCode, string reason);
}

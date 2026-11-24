namespace Ktm.DataSaver.Core.Resilience;

/// <summary>
/// Маркер исключения, которое Infrastructure распознала как отказ инфраструктуры (исчерпание пула соединений,
/// уничтоженный источник данных и т.п.), хотя тип исключения драйвера этого не говорит. Core классифицирует его как
/// <see cref="WriteErrorClass.Transient"/>: повтор без предела, сообщение в dlq не уходит.
/// </summary>
public interface IInfrastructureFailure;

/// <summary>
/// Необязательное дополнение: исключение-адаптер сообщает имя типа исходного исключения драйвера для заголовка
/// <c>ktm-dlq-exception-type</c> (адаптер намеренно не хранит исходное исключение: его текст может нести значения из строки).
/// </summary>
public interface IReportedExceptionType
{
    string ReportedTypeName { get; }
}

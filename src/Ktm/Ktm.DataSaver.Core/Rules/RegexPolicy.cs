namespace Ktm.DataSaver.Core.Rules;

/// <summary>
/// Пределы оператора <c>regex</c> (ADR-0005 п. 2.7): предел времени одного совпадения и лимиты шаблона. Значения времени и
/// числа узлов на набор приходят из настроек (<c>DataSaver__Rules__*</c>), длина шаблона и число узлов в правиле фиксированы.
/// </summary>
public sealed record RegexPolicy(TimeSpan MatchTimeout)
{
    public const int MaxPatternLength = 512;
    public const int MaxRegexNodesPerRule = 8;
    public const int DefaultMatchTimeoutMs = 200;

    public static RegexPolicy Default { get; } = new(TimeSpan.FromMilliseconds(DefaultMatchTimeoutMs));
}

/// <summary>
/// Вычисление правила прервано (сейчас - только превышение предела совпадения регулярного выражения). Внутреннее исключение Core:
/// его перехватывает стадия правил и превращает в отбраковку; во внешний контракт оно не входит. Текст не содержит данных сообщения и шаблона.
/// </summary>
internal sealed class RuleEvaluationAbortedException(string kind, TimeSpan limit)
    : Exception("Rule evaluation aborted: " + kind)
{
    public const string RegexTimeout = "regex_timeout";

    public string Kind { get; } = kind;

    public TimeSpan Limit { get; } = limit;
}

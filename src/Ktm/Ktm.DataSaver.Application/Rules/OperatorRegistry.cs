using System.Text.Json;

namespace Ktm.DataSaver.Application.Rules;

/// <summary>
/// Оператор листового сравнения. Новый оператор добавляется классом и <see cref="OperatorRegistry.Register"/>, без правок
/// узлов <c>and</c>/<c>or</c>/<c>!</c>, компилятора и стадии (ADR-0005 п. 2.4).
/// </summary>
public interface IRuleOperator
{
    /// <summary>Ключ оператора в условии (<c>==</c>, <c>&lt;=</c> ...).</summary>
    string Key { get; }

    /// <summary>Имя для трассировки (используется в <see cref="IPredicate.Describe"/>).</summary>
    string TraceName { get; }

    /// <summary>
    /// Разбор и проверка операндов при загрузке. <paramref name="path"/> - путь узла в дереве (<c>condition.and[0]</c>).
    /// Ошибки добавляются в <paramref name="errors"/> в формате <c>&lt;путь&gt;: &lt;причина&gt;</c> (см. <see cref="RuleErrors.Format"/>);
    /// при ошибках возвращается <c>null</c>.
    /// </summary>
    IPredicate? Compile(JsonElement operands, string path, List<string> errors);
}

/// <summary>Формат ошибок правила.</summary>
public static class RuleErrors
{
    public static string Format(string path, string reason) => $"{path}: {reason}";
}

/// <summary>Реестр операторов листового сравнения. Логические <c>and</c>/<c>or</c>/<c>!</c> в реестр не входят.</summary>
public sealed class OperatorRegistry
{
    internal static readonly string[] ReservedKeys = ["and", "or", "!"];

    private readonly Dictionary<string, IRuleOperator> _operators = new(StringComparer.Ordinal);

    /// <summary>Пределы <c>regex</c>, с которыми собран реестр; их же читает компилятор для лимита узлов в правиле.</summary>
    public RegexPolicy RegexPolicy { get; private init; } = RegexPolicy.Default;

    /// <summary>Реестр по умолчанию: <c>==</c>, <c>!=</c>, <c>&lt;</c>, <c>&lt;=</c> (два операнда или диапазон), <c>&gt;</c>, <c>&gt;=</c>, <c>regex</c>.</summary>
    public static OperatorRegistry CreateDefault(RegexPolicy? regexPolicy = null) => CreateDefault(regexPolicy, null);

    /// <summary>Тестовый шов: фабрика <see cref="System.Text.RegularExpressions.Regex"/> (откатный движок для проверки таймаута).</summary>
    internal static OperatorRegistry CreateDefault(
        RegexPolicy? regexPolicy,
        Func<string, TimeSpan, System.Text.RegularExpressions.Regex>? regexFactory)
    {
        var policy = regexPolicy ?? RegexPolicy.Default;
        var registry = new OperatorRegistry { RegexPolicy = policy };
        registry.Register(new EqualityOperator("==", negate: false));
        registry.Register(new EqualityOperator("!=", negate: true));
        registry.Register(new OrderOperator("<", OrderKind.Less));
        registry.Register(new OrderOperator("<=", OrderKind.LessOrEqual));
        registry.Register(new OrderOperator(">", OrderKind.Greater));
        registry.Register(new OrderOperator(">=", OrderKind.GreaterOrEqual));
        registry.Register(new RegexOperator(policy, regexFactory));
        return registry;
    }

    public void Register(IRuleOperator op)
    {
        ArgumentNullException.ThrowIfNull(op);
        if (ReservedKeys.Contains(op.Key, StringComparer.Ordinal))
        {
            throw new ArgumentException($"Key \"{op.Key}\" is reserved for a logical node.", nameof(op));
        }

        if (!_operators.TryAdd(op.Key, op))
        {
            throw new ArgumentException($"Operator \"{op.Key}\" is already registered.", nameof(op));
        }
    }

    public bool TryGet(string key, out IRuleOperator? op) => _operators.TryGetValue(key, out op);
}

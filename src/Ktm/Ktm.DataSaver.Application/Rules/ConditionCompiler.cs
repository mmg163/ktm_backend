using System.Text.Json;
using System.Text.RegularExpressions;
using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Rules;

/// <summary>Результат компиляции условия: корень либо список ошибок (формат <c>&lt;путь&gt;: &lt;причина&gt;</c>).</summary>
public sealed record ConditionCompileResult(IConditionNode? Root, IReadOnlyList<string> Errors, int RegexNodeCount = 0)
{
    public bool Succeeded => Errors.Count == 0 && Root is not null;
}

/// <summary>Результат компиляции правила целиком.</summary>
public sealed record RuleCompileResult(CompiledRule? Rule, IReadOnlyList<string> Errors)
{
    public bool Succeeded => Errors.Count == 0 && Rule is not null;
}

/// <summary>
/// JSON-условие -> дерево узлов (ADR-0005 п. 2.4). Ничего не бросает: все ошибки правила возвращаются списком.
/// Лимиты: глубина 8, узлов 64. Выключенные правила не пропускаются: компилируется то, что передано.
/// </summary>
public sealed class ConditionCompiler(OperatorRegistry registry)
{
    public const int MaxDepth = 8;
    public const int MaxNodes = 64;

    /// <summary>Логический размер условия: байты UTF-8 текста JSON. Та же мера в БД: <c>octet_length(condition::text)</c> (V002).</summary>
    public const int MaxConditionBytes = 16384;
    public const string RootPath = "condition";

    public const int MaxNameLength = 200;

    private static readonly Regex RuleIdFormat = new("^[A-Za-z0-9._:-]{1,128}\\z", RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);
    private static readonly Regex ReasonCodeFormat = new("^[a-z][a-z0-9_]{0,63}\\z", RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    public ConditionCompiler() : this(OperatorRegistry.CreateDefault())
    {
    }

    /// <summary>
    /// Проверяет поля правила (rule_id, название, источник, код причины; ADR-0005 п. 2.3, 2.9) и компилирует условие.
    /// Источник <c>null</c> - все источники (<c>"*"</c> из БД адаптер отображает в <c>null</c>); строка <c>"*"</c> здесь - ошибка.
    /// </summary>
    public RuleCompileResult Compile(RuleDefinition rule)
    {
        var errors = new List<string>();
        if (rule.Id is null || !RuleIdFormat.IsMatch(rule.Id))
        {
            errors.Add(RuleErrors.Format("rule_id", "допустимы символы A-Z a-z 0-9 . _ : - длиной 1-128"));
        }

        var nameLength = rule.Name?.Trim(' ').Length ?? 0;
        if (nameLength is < 1 or > MaxNameLength)
        {
            errors.Add(RuleErrors.Format("name", $"длина названия без крайних пробелов должна быть от 1 до {MaxNameLength}"));
        }

        if (rule.Source is not null && !RuleDefinition.KnownSources.Contains(rule.Source, StringComparer.Ordinal))
        {
            errors.Add(RuleErrors.Format("source", $"неизвестный источник \"{rule.Source}\" (допустимы: skud, cv, 1c-do, *)"));
        }

        if (rule.ReasonCode is RejectReasons.MalformedJson or RejectReasons.UnknownSchemaVersion or RejectReasons.InvalidSchema or RejectReasons.RuleEvalTimeout)
        {
            errors.Add(RuleErrors.Format("reason_code", "код причины зарезервирован системой (проверка формата, прерывание вычисления)"));
        }
        else if (rule.ReasonCode is null || !ReasonCodeFormat.IsMatch(rule.ReasonCode))
        {
            errors.Add(RuleErrors.Format("reason_code", "формат кода причины [a-z][a-z0-9_]{0,63}"));
        }

        var condition = Compile(rule.Condition);
        errors.AddRange(condition.Errors);
        return errors.Count == 0
            ? new RuleCompileResult(new CompiledRule(rule, condition.Root!, condition.RegexNodeCount), errors)
            : new RuleCompileResult(null, errors);
    }

    public ConditionCompileResult Compile(JsonElement condition)
    {
        var state = new State();
        if (condition.ValueKind != JsonValueKind.Undefined
            && System.Text.Encoding.UTF8.GetByteCount(condition.GetRawText()) > MaxConditionBytes)
        {
            // Содержимое условия в сообщение не попадает (ПДн/секреты в значениях).
            state.Errors.Add(RuleErrors.Format(RootPath, $"размер условия превышает {MaxConditionBytes} байт (UTF-8 текст JSON)"));
            return new ConditionCompileResult(null, state.Errors);
        }


        var root = CompileNode(condition, RootPath, 1, state);
        return state.Errors.Count == 0 && root is not null
            ? new ConditionCompileResult(root, state.Errors, state.RegexNodes)
            : new ConditionCompileResult(null, state.Errors);
    }

    private IConditionNode? CompileNode(JsonElement element, string path, int depth, State state)
    {
        if (depth > MaxDepth)
        {
            state.ReportOnce(ref state.DepthReported, RuleErrors.Format(path, $"глубина вложенности превышает {MaxDepth}"));
            return null;
        }

        if (++state.Nodes > MaxNodes)
        {
            state.ReportOnce(ref state.NodesReported, RuleErrors.Format(path, $"число узлов условия превышает {MaxNodes}"));
            return null;
        }

        if (element.ValueKind != JsonValueKind.Object)
        {
            state.Errors.Add(RuleErrors.Format(path, "узел условия должен быть объектом"));
            return null;
        }

        var count = element.GetPropertyCount();
        if (count != 1)
        {
            state.Errors.Add(RuleErrors.Format(path, $"узел должен содержать ровно один оператор, найдено {count}"));
            return null;
        }

        JsonProperty property = default;
        foreach (var p in element.EnumerateObject()) property = p;
        var key = property.Name;
        var operands = property.Value;

        switch (key)
        {
            case "and":
            case "or":
                return CompileLogical(key, operands, path, depth, state);
            case "!":
                return CompileNot(operands, path, depth, state);
        }

        if (!registry.TryGet(key, out var op))
        {
            state.Errors.Add(RuleErrors.Format(path, $"оператор \"{key}\" не поддерживается"));
            return null;
        }

        var predicate = op!.Compile(operands, path, state.Errors);
        if (predicate is null) return null;
        if (predicate is IRegexPredicate && ++state.RegexNodes > RegexPolicy.MaxRegexNodesPerRule)
        {
            state.ReportOnce(
                ref state.RegexNodesReported,
                RuleErrors.Format(path, $"число узлов regex в правиле превышает {RegexPolicy.MaxRegexNodesPerRule}"));
            return null;
        }

        return new LeafNode(predicate);
    }

    private IConditionNode? CompileLogical(string key, JsonElement operands, string path, int depth, State state)
    {
        if (operands.ValueKind != JsonValueKind.Array)
        {
            state.Errors.Add(RuleErrors.Format(path, $"операнды \"{key}\" должны быть массивом узлов"));
            return null;
        }

        var length = operands.GetArrayLength();
        if (length == 0)
        {
            state.Errors.Add(RuleErrors.Format(path, $"массив \"{key}\" не может быть пустым"));
            return null;
        }

        var children = new IConditionNode?[length];
        var index = 0;
        foreach (var item in operands.EnumerateArray())
        {
            children[index] = CompileNode(item, $"{path}.{key}[{index}]", depth + 1, state);
            index++;
        }

        if (children.Any(c => c is null)) return null;
        var nodes = children!.ToArray<IConditionNode>();
        return key == "and" ? new AndNode(nodes) : new OrNode(nodes);
    }

    private IConditionNode? CompileNot(JsonElement operand, string path, int depth, State state)
    {
        if (operand.ValueKind == JsonValueKind.Array)
        {
            if (operand.GetArrayLength() != 1)
            {
                state.Errors.Add(RuleErrors.Format(path, "массив оператора \"!\" должен содержать ровно один узел"));
                return null;
            }

            var inner = CompileNode(operand[0], $"{path}.![0]", depth + 1, state);
            return inner is null ? null : new NotNode(inner);
        }

        var child = CompileNode(operand, $"{path}.!", depth + 1, state);
        return child is null ? null : new NotNode(child);
    }

    private sealed class State
    {
        public readonly List<string> Errors = [];
        public int Nodes;
        public bool DepthReported;
        public bool NodesReported;
        public int RegexNodes;
        public bool RegexNodesReported;

        public void ReportOnce(ref bool flag, string message)
        {
            if (flag) return;
            flag = true;
            Errors.Add(message);
        }
    }
}

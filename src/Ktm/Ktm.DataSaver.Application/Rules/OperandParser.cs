using System.Text.Json;

namespace Ktm.DataSaver.Application.Rules;

public enum OperandKind
{
    Invalid,
    Var,
    Literal,
}

/// <summary>
/// Разбор операндов листовых операторов: <c>{"var": "путь"}</c> либо литерал (строка до 1024 символов, число, <c>true</c>/<c>false</c>,
/// <c>null</c>). Значение по умолчанию у <c>var</c>, путь-выражение, вложенные операторы и массивы-литералы - ошибка правила.
/// Публичен, чтобы операторы, добавляемые в реестр, разбирали операнды единообразно.
/// </summary>
public static class OperandParser
{
    public const int MaxLiteralLength = 1024;

    /// <summary>Определяет вид операнда; ошибки добавляет в <paramref name="errors"/> с путём <paramref name="path"/>.</summary>
    public static OperandKind Classify(JsonElement operand, string path, List<string> errors, out FieldPath? field)
    {
        field = null;
        switch (operand.ValueKind)
        {
            case JsonValueKind.Object:
                if (operand.GetPropertyCount() != 1 || !operand.TryGetProperty("var", out var varValue))
                {
                    errors.Add(RuleErrors.Format(path, "операнд должен быть {\"var\": \"путь\"} или литералом (вложенные операторы не поддерживаются)"));
                    return OperandKind.Invalid;
                }

                if (varValue.ValueKind == JsonValueKind.Array)
                {
                    errors.Add(RuleErrors.Format(path, "var со значением по умолчанию не поддерживается"));
                    return OperandKind.Invalid;
                }

                if (varValue.ValueKind != JsonValueKind.String)
                {
                    errors.Add(RuleErrors.Format(path, "путь var должен быть строкой"));
                    return OperandKind.Invalid;
                }

                if (!FieldPath.TryParse(varValue.GetString(), out field, out var pathError))
                {
                    errors.Add(RuleErrors.Format(path, pathError!));
                    return OperandKind.Invalid;
                }

                return OperandKind.Var;
            case JsonValueKind.Array:
                errors.Add(RuleErrors.Format(path, "массив не может быть литералом операнда"));
                return OperandKind.Invalid;
            case JsonValueKind.String when operand.GetString()!.Length > MaxLiteralLength:
                errors.Add(RuleErrors.Format(path, $"строковый литерал длиннее {MaxLiteralLength} символов"));
                return OperandKind.Invalid;
            default:
                return OperandKind.Literal;
        }
    }

    /// <summary>
    /// Форма «два операнда»: ровно один <c>var</c> и один литерал, порядок любой. <paramref name="literalFirst"/> - литерал слева
    /// (сравнение читается зеркально).
    /// </summary>
    public static bool TryParseBinary(
        JsonElement operands,
        string path,
        string key,
        List<string> errors,
        out FieldPath? field,
        out JsonElement literal,
        out bool literalFirst)
    {
        field = null;
        literal = default;
        literalFirst = false;

        if (operands.ValueKind != JsonValueKind.Array)
        {
            errors.Add(RuleErrors.Format(path, $"операнды оператора \"{key}\" должны быть массивом"));
            return false;
        }

        var count = operands.GetArrayLength();
        if (count != 2)
        {
            errors.Add(RuleErrors.Format(path, $"оператор \"{key}\" принимает ровно два операнда, получено {count}"));
            return false;
        }

        var errorsBefore = errors.Count;
        var first = Classify(operands[0], $"{path}.{key}[0]", errors, out var firstField);
        var second = Classify(operands[1], $"{path}.{key}[1]", errors, out var secondField);
        if (errors.Count != errorsBefore) return false;

        if (first == OperandKind.Var && second == OperandKind.Var)
        {
            errors.Add(RuleErrors.Format(path, $"оба операнда \"{key}\" - var: сравнение двух полей не поддерживается"));
            return false;
        }

        if (first == OperandKind.Literal && second == OperandKind.Literal)
        {
            errors.Add(RuleErrors.Format(path, $"оба операнда \"{key}\" - литералы: нужен ровно один var"));
            return false;
        }

        literalFirst = first == OperandKind.Literal;
        field = literalFirst ? secondField : firstField;
        literal = literalFirst ? operands[0] : operands[1];
        return true;
    }

    /// <summary>Число-литерал как <c>decimal</c>; число вне диапазона <c>decimal</c> - ошибка правила.</summary>
    internal static bool TryGetDecimalLiteral(JsonElement literal, string path, List<string> errors, out decimal value)
    {
        if (literal.TryGetDecimal(out value)) return true;
        errors.Add(RuleErrors.Format(path, "числовой литерал вне диапазона decimal"));
        return false;
    }
}

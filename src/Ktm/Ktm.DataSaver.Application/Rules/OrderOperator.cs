using System.Text.Json;

namespace Ktm.DataSaver.Application.Rules;

internal enum OrderKind
{
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

/// <summary>
/// <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c>, <c>&gt;=</c> (ADR-0005 п. 2.4, 2.6). Два операнда: число (<c>decimal</c>) или дата RFC 3339
/// с зоной (сравнение по моменту времени); литерал слева читается зеркально. Три операнда - только у <c>&lt;=</c>: диапазон
/// <c>[нижняя, var, верхняя]</c>, обе границы включительно. Отсутствующее поле, <c>null</c>, другой тип, строка вместо числа,
/// неразбираемая дата и число вне <c>decimal</c> дают ложь.
/// </summary>
internal sealed class OrderOperator(string key, OrderKind kind) : IRuleOperator
{
    public string Key => key;

    public string TraceName => key;

    public IPredicate? Compile(JsonElement operands, string path, List<string> errors)
    {
        if (operands.ValueKind == JsonValueKind.Array && operands.GetArrayLength() == 3)
        {
            return kind == OrderKind.LessOrEqual
                ? CompileRange(operands, path, errors)
                : Fail(errors, path, $"трёхаргументная форма (диапазон) поддерживается только для оператора \"<=\", не для \"{key}\"");
        }

        if (!OperandParser.TryParseBinary(operands, path, key, errors, out var field, out var literal, out var literalFirst))
        {
            return null;
        }

        var effective = literalFirst ? Mirror(kind) : kind;
        if (!TryReadOrdered(literal, $"{path}.{key}[{(literalFirst ? 0 : 1)}]", errors, out var bound)) return null;

        return new ComparePredicate(field!, effective, bound, $"{field!.Text} {Symbol(effective)}");
    }

    private IPredicate? CompileRange(JsonElement operands, string path, List<string> errors)
    {
        var errorsBefore = errors.Count;
        var kinds = new OperandKind[3];
        FieldPath? field = null;
        for (var i = 0; i < 3; i++)
        {
            kinds[i] = OperandParser.Classify(operands[i], $"{path}.{key}[{i}]", errors, out var f);
            if (i == 1) field = f;
        }

        if (errors.Count != errorsBefore) return null;
        if (kinds[1] != OperandKind.Var || kinds[0] != OperandKind.Literal || kinds[2] != OperandKind.Literal)
        {
            return Fail(errors, path, "в диапазоне var должен стоять посередине, границы - литералы: [нижняя, {\"var\": ...}, верхняя]");
        }

        if (!TryReadOrdered(operands[0], $"{path}.{key}[0]", errors, out var low)
            | !TryReadOrdered(operands[2], $"{path}.{key}[2]", errors, out var high))
        {
            return null;
        }

        if (low.IsDate != high.IsDate)
        {
            return Fail(errors, path, "границы диапазона должны быть одного вида: обе числа либо обе даты");
        }

        if (low.CompareTo(high) > 0)
        {
            return Fail(errors, path, "нижняя граница диапазона больше верхней");
        }

        return new RangePredicate(field!, low, high, $"{field!.Text} <=..<=");
    }

    private static IPredicate? Fail(List<string> errors, string path, string reason)
    {
        errors.Add(RuleErrors.Format(path, reason));
        return null;
    }

    private static bool TryReadOrdered(JsonElement literal, string path, List<string> errors, out Ordered value)
    {
        switch (literal.ValueKind)
        {
            case JsonValueKind.Number:
                if (!OperandParser.TryGetDecimalLiteral(literal, path, errors, out var number))
                {
                    value = default;
                    return false;
                }

                value = Ordered.FromNumber(number);
                return true;
            case JsonValueKind.String:
                if (!Rfc3339.TryParse(literal.GetString(), out var date))
                {
                    errors.Add(RuleErrors.Format(path, "строка не является датой RFC 3339 с обязательной зоной (Z или ±hh:mm)"));
                    value = default;
                    return false;
                }

                value = Ordered.FromDate(date);
                return true;
            default:
                errors.Add(RuleErrors.Format(path, "для операций порядка литерал должен быть числом или датой RFC 3339 с зоной"));
                value = default;
                return false;
        }
    }

    private static OrderKind Mirror(OrderKind k) => k switch
    {
        OrderKind.Less => OrderKind.Greater,
        OrderKind.LessOrEqual => OrderKind.GreaterOrEqual,
        OrderKind.Greater => OrderKind.Less,
        _ => OrderKind.LessOrEqual,
    };

    private static string Symbol(OrderKind k) => k switch
    {
        OrderKind.Less => "<",
        OrderKind.LessOrEqual => "<=",
        OrderKind.Greater => ">",
        _ => ">=",
    };

    /// <summary>Значение порядка: число либо момент времени. Сравнивать можно только значения одного вида.</summary>
    private readonly record struct Ordered(bool IsDate, decimal Number, DateTimeOffset Date)
    {
        public static Ordered FromNumber(decimal n) => new(false, n, default);

        public static Ordered FromDate(DateTimeOffset d) => new(true, 0m, d);

        public int CompareTo(Ordered other) => IsDate ? Date.CompareTo(other.Date) : Number.CompareTo(other.Number);

        /// <summary>Читает значение поля того же вида, что и граница; <c>false</c> - тип не совпал либо дата не разобралась.</summary>
        public bool TryReadSameKind(JsonElement element, out Ordered value)
        {
            value = default;
            if (IsDate)
            {
                if (element.ValueKind != JsonValueKind.String || !Rfc3339.TryParse(element.GetString(), out var date)) return false;
                value = FromDate(date);
                return true;
            }

            if (element.ValueKind != JsonValueKind.Number || !element.TryGetDecimal(out var number)) return false;
            value = FromNumber(number);
            return true;
        }
    }

    private sealed class ComparePredicate(FieldPath field, OrderKind kind, Ordered bound, string describe) : IPredicate
    {
        public bool Evaluate(JsonElement body)
        {
            if (!field.TryGet(body, out var element) || !bound.TryReadSameKind(element, out var value)) return false;
            var cmp = value.CompareTo(bound);
            return kind switch
            {
                OrderKind.Less => cmp < 0,
                OrderKind.LessOrEqual => cmp <= 0,
                OrderKind.Greater => cmp > 0,
                _ => cmp >= 0,
            };
        }

        public string Describe() => describe;
    }

    private sealed class RangePredicate(FieldPath field, Ordered low, Ordered high, string describe) : IPredicate
    {
        public bool Evaluate(JsonElement body)
        {
            if (!field.TryGet(body, out var element) || !low.TryReadSameKind(element, out var value)) return false;
            return value.CompareTo(low) >= 0 && value.CompareTo(high) <= 0;
        }

        public string Describe() => describe;
    }
}

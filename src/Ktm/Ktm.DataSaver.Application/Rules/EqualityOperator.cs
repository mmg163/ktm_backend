using System.Text.Json;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>
/// <c>==</c> и <c>!=</c> (ADR-0005 п. 2.6): строгое сравнение без приведения типов. Строки - ordinal, регистр значим; числа - как
/// <c>decimal</c> (<c>1</c> равно <c>1.0</c>); булево; <c>null</c> - только явный JSON <c>null</c>. Отсутствующее поле, другой тип и
/// число вне <c>decimal</c> дают ложь для <c>==</c>. <c>!=</c> - строгое отрицание <c>==</c>: отсутствующее поле «не равно» любому значению.
/// </summary>
internal sealed class EqualityOperator(string key, bool negate) : IRuleOperator
{
    public string Key => key;

    public string TraceName => key;

    public IPredicate? Compile(JsonElement operands, string path, List<string> errors)
    {
        if (!OperandParser.TryParseBinary(operands, path, key, errors, out var field, out var literal, out _))
        {
            return null;
        }

        var describe = $"{field!.Text} {key}";
        switch (literal.ValueKind)
        {
            case JsonValueKind.String:
                return new Predicate(field, literal.ValueKind, literal.GetString(), 0m, negate, describe);
            case JsonValueKind.Number:
                if (!OperandParser.TryGetDecimalLiteral(literal, $"{path}.{key}", errors, out var number)) return null;
                return new Predicate(field, JsonValueKind.Number, null, number, negate, describe);
            default:
                // true, false, null
                return new Predicate(field, literal.ValueKind, null, 0m, negate, describe);
        }
    }

    private sealed class Predicate(FieldPath field, JsonValueKind kind, string? text, decimal number, bool negate, string describe) : IPredicate
    {
        public bool Evaluate(JsonElement body)
        {
            var equal = field.TryGet(body, out var value) && Matches(value);
            return negate ? !equal : equal;
        }

        public string Describe() => describe;

        private bool Matches(JsonElement value) => kind switch
        {
            JsonValueKind.String => value.ValueKind == JsonValueKind.String && value.ValueEquals(text),
            JsonValueKind.Number => value.ValueKind == JsonValueKind.Number && value.TryGetDecimal(out var d) && d == number,
            _ => value.ValueKind == kind,
        };
    }
}

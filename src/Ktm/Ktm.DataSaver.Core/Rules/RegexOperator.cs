using System.Text.Json;
using System.Text.RegularExpressions;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>Маркер листа <c>regex</c>: по нему компилятор считает узлы для лимитов правила и набора.</summary>
internal interface IRegexPredicate : IPredicate
{
}

/// <summary>
/// <c>{"regex": [{"var": "путь"}, "шаблон"]}</c> (ADR-0005 п. 2.7): частичное совпадение (<c>IsMatch</c>), движок без возвратов
/// (<c>NonBacktracking</c>, время линейно по длине ввода), <c>CultureInvariant</c>; регистр - через <c>(?i)</c> в шаблоне.
/// Поле не строка или отсутствует - ложь, без приведений. Шаблон компилируется один раз при загрузке правила; ошибка шаблона -
/// ошибка правила (текст без шаблона и без <see cref="Exception.Message"/>). Превышение предела совпадения -
/// <see cref="RuleEvaluationAbortedException"/>, а не «ложь».
/// </summary>
internal sealed class RegexOperator(RegexPolicy policy, Func<string, TimeSpan, Regex>? factory = null) : IRuleOperator
{
    public const string OperatorKey = "regex";

    private readonly Func<string, TimeSpan, Regex> _factory = factory ?? DefaultFactory;

    public string Key => OperatorKey;

    public string TraceName => OperatorKey;

    /// <summary>Рабочая фабрика: единственное место, где задаются параметры движка.</summary>
    internal static Regex DefaultFactory(string pattern, TimeSpan timeout) =>
        new(pattern, RegexOptions.NonBacktracking | RegexOptions.CultureInvariant, timeout);

    public IPredicate? Compile(JsonElement operands, string path, List<string> errors)
    {
        if (operands.ValueKind != JsonValueKind.Array)
        {
            errors.Add(RuleErrors.Format(path, "операнды оператора \"regex\" должны быть массивом"));
            return null;
        }

        var count = operands.GetArrayLength();
        if (count != 2)
        {
            errors.Add(RuleErrors.Format(path, $"оператор \"regex\" принимает ровно два операнда, получено {count}"));
            return null;
        }

        var fieldPath = $"{path}.regex[0]";
        var patternPath = $"{path}.regex[1]";
        var errorsBefore = errors.Count;

        FieldPath? field = null;
        var first = OperandParser.Classify(operands[0], fieldPath, errors, out var parsedField);
        if (first == OperandKind.Literal)
        {
            errors.Add(RuleErrors.Format(fieldPath, "первым операндом должен быть {\"var\": \"путь\"}, шаблон идёт вторым"));
        }
        else if (first == OperandKind.Var)
        {
            field = parsedField;
        }

        var pattern = ReadPattern(operands[1], patternPath, errors);
        if (errors.Count != errorsBefore || field is null || pattern is null)
        {
            return null;
        }

        var regex = Build(pattern, patternPath, errors);
        return regex is null ? null : new Predicate(field, regex, policy.MatchTimeout, $"{field.Text} {OperatorKey}");
    }

    private static string? ReadPattern(JsonElement operand, string path, List<string> errors)
    {
        switch (operand.ValueKind)
        {
            case JsonValueKind.String:
                var pattern = operand.GetString()!;
                if (pattern.Length == 0)
                {
                    errors.Add(RuleErrors.Format(path, "шаблон не может быть пустым"));
                    return null;
                }

                if (pattern.Length > RegexPolicy.MaxPatternLength)
                {
                    errors.Add(RuleErrors.Format(path, $"длина шаблона превышает {RegexPolicy.MaxPatternLength} символов"));
                    return null;
                }

                return pattern;
            case JsonValueKind.Object:
                errors.Add(RuleErrors.Format(path, "шаблон должен быть строковым литералом: var и вложенные операторы не поддерживаются"));
                return null;
            default:
                errors.Add(RuleErrors.Format(path, "шаблон должен быть строкой"));
                return null;
        }
    }

    private Regex? Build(string pattern, string path, List<string> errors)
    {
        try
        {
            return _factory(pattern, policy.MatchTimeout);
        }
        catch (RegexParseException ex)
        {
            // Только код и позиция: Message и сам шаблон в текст не входят.
            errors.Add(RuleErrors.Format(path, $"недопустимый шаблон регулярного выражения: {ex.Error}, позиция {ex.Offset}"));
        }
        catch (NotSupportedException)
        {
            errors.Add(RuleErrors.Format(path, "шаблон не поддерживается движком без возвратов (просмотр вперёд/назад, обратные ссылки, атомарные группы, условные выражения, якорь G) либо автомат шаблона слишком велик"));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            errors.Add(RuleErrors.Format(path, "шаблон регулярного выражения не удалось скомпилировать"));
        }

        return null;
    }

    private sealed class Predicate(FieldPath field, Regex regex, TimeSpan timeout, string describe) : IRegexPredicate
    {
        public bool Evaluate(JsonElement body)
        {
            if (!field.TryGet(body, out var value) || value.ValueKind != JsonValueKind.String)
            {
                return false;
            }

            try
            {
                return regex.IsMatch(value.GetString()!);
            }
            catch (RegexMatchTimeoutException ex)
            {
                // Таймаут - не «ложь»: иначе правило молча перестало бы работать на длинном вводе. Решает стадия.
                throw new RuleEvaluationAbortedException(RuleEvaluationAbortedException.RegexTimeout, ex.MatchTimeout == default ? timeout : ex.MatchTimeout);
            }
        }

        public string Describe() => describe;
    }
}

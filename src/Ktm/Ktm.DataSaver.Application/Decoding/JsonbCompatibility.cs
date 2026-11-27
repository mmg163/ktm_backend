using System.Text.Json;

namespace Ktm.DataSaver.Application.Decoding;

/// <summary>
/// Проверки тела, которое PostgreSQL <c>jsonb</c> отвергает целиком (ошибка класса 22 валит всю пачку и блокирует партицию):
/// символ U+0000, непарные суррогаты (в значениях и именах свойств), числа вне диапазона <c>numeric</c>.
/// Возвращаемые пояснения не содержат значений из тела.
/// Пределы numeric проверены вручную на PostgreSQL 16: до 131072 цифр целой части и до 16383 цифр дробной.
/// </summary>
internal static class JsonbCompatibility
{
    private const long MaxIntegerDigits = 131072;
    private const long MaxScale = 16383;
    private const long MaxExponent = 1_073_741_823;
    private const long ExponentClamp = 1_000_000_000_000;

    /// <summary>Возвращает пояснение первой найденной проблемы или <c>null</c>, если тело совместимо с jsonb.</summary>
    public static string? FindProblem(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                return CheckText(() => element.GetString(), "a string value");
            case JsonValueKind.Number:
                return CheckNumber(element.GetRawText());
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    if (FindProblem(item) is { } problem)
                    {
                        return problem;
                    }
                }

                return null;
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (CheckText(() => property.Name, "a property name") is { } nameProblem)
                    {
                        return nameProblem;
                    }

                    if (FindProblem(property.Value) is { } valueProblem)
                    {
                        return valueProblem;
                    }
                }

                return null;
            default:
                return null;
        }
    }

    private static string? CheckText(Func<string?> read, string where)
    {
        string? text;
        try
        {
            text = read();
        }
        catch (InvalidOperationException)
        {
            // System.Text.Json не может декодировать непарный суррогат из \uXXXX-экранирования.
            return $"Message contains an unpaired UTF-16 surrogate in {where}.";
        }

        if (text is null)
        {
            return null;
        }

        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (c == '\0')
            {
                return $"Message contains the U+0000 character in {where}.";
            }

            if (char.IsHighSurrogate(c))
            {
                if (i + 1 < text.Length && char.IsLowSurrogate(text[i + 1]))
                {
                    i++;
                    continue;
                }

                return $"Message contains an unpaired UTF-16 surrogate in {where}.";
            }

            if (char.IsLowSurrogate(c))
            {
                return $"Message contains an unpaired UTF-16 surrogate in {where}.";
            }
        }

        return null;
    }

    /// <summary>Разбор сырого JSON-числа: <c>-?int(.frac)?([eE][+-]?exp)?</c> (форма уже проверена парсером).</summary>
    private static string? CheckNumber(string raw)
    {
        // Единый расчёт для любых чисел: без экспоненты она равна 0 (длинная дробная часть тоже может выйти за предел).
        var e = raw.IndexOfAny(['e', 'E']);
        var mantissa = e < 0 ? raw : raw[..e];
        var exponent = e < 0 ? 0 : ParseExponent(raw.AsSpan(e + 1));

        var dot = mantissa.IndexOf('.');
        var integerPart = (dot < 0 ? mantissa : mantissa[..dot]).TrimStart('-');
        var fraction = dot < 0 ? string.Empty : mantissa[(dot + 1)..];

        var digits = integerPart + fraction;
        var leadingZeros = digits.Length - digits.TrimStart('0').Length;

        // Ноль не освобождается от проверки: значащих цифр нет (integerDigits = 0), но масштаб и экспонента
        // проверяются, как у любого числа (PostgreSQL 16 отвергает, например, 0e2000000000 и 0e-16384).
        var integerDigits = leadingZeros == digits.Length ? 0 : integerPart.Length - leadingZeros + exponent;
        var scale = fraction.Length - exponent;
        return Math.Abs(exponent) > MaxExponent || integerDigits > MaxIntegerDigits || scale > MaxScale
            ? "Message contains a number outside the PostgreSQL numeric range."
            : null;
    }

    private static long ParseExponent(ReadOnlySpan<char> text)
    {
        var negative = false;
        if (text.Length > 0 && (text[0] == '+' || text[0] == '-'))
        {
            negative = text[0] == '-';
            text = text[1..];
        }

        long value = 0;
        foreach (var c in text)
        {
            value = Math.Min(value * 10 + (c - '0'), ExponentClamp);
        }

        return negative ? -value : value;
    }
}

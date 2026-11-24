namespace Ktm.DataSaver.Core.Rules;

/// <summary>Разбор даты RFC 3339 с обязательной зоной (<c>Z</c> или <c>±hh:mm</c>); результат сравнивается как момент времени.</summary>
internal static class Rfc3339
{
    public static bool TryParse(ReadOnlySpan<char> s, out DateTimeOffset result)
    {
        result = default;
        // YYYY-MM-DDThh:mm:ss + [.f+] + (Z | +hh:mm)
        if (s.Length < 20) return false;
        if (s[4] != '-' || s[7] != '-' || (s[10] != 'T' && s[10] != 't') || s[13] != ':' || s[16] != ':') return false;
        if (!Digits(s, 0, 4, out var year) || !Digits(s, 5, 2, out var month) || !Digits(s, 8, 2, out var day)
            || !Digits(s, 11, 2, out var hour) || !Digits(s, 14, 2, out var minute) || !Digits(s, 17, 2, out var second))
        {
            return false;
        }

        if (year < 1 || month is < 1 or > 12 || day < 1 || day > DateTime.DaysInMonth(year, month)
            || hour > 23 || minute > 59 || second > 59)
        {
            return false;
        }

        var pos = 19;
        long fractionTicks = 0;
        if (s[pos] == '.')
        {
            pos++;
            var start = pos;
            while (pos < s.Length && s[pos] is >= '0' and <= '9')
            {
                if (pos - start < 7) fractionTicks = (fractionTicks * 10) + (s[pos] - '0');
                pos++;
            }

            var digits = pos - start;
            if (digits == 0) return false;
            for (var i = Math.Min(digits, 7); i < 7; i++) fractionTicks *= 10;
        }

        TimeSpan offset;
        if (pos == s.Length - 1 && (s[pos] == 'Z' || s[pos] == 'z'))
        {
            offset = TimeSpan.Zero;
        }
        else if (pos == s.Length - 6 && (s[pos] == '+' || s[pos] == '-') && s[pos + 3] == ':'
                 && Digits(s, pos + 1, 2, out var offsetHour) && Digits(s, pos + 4, 2, out var offsetMinute)
                 && offsetHour <= 23 && offsetMinute <= 59)
        {
            offset = new TimeSpan(offsetHour, offsetMinute, 0);
            if (s[pos] == '-') offset = offset.Negate();
        }
        else
        {
            return false;
        }

        try
        {
            var local = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Unspecified).AddTicks(fractionTicks);
            result = new DateTimeOffset(local, offset);
            return true;
        }
        catch (ArgumentException)
        {
            // Смещение вне допустимых +-14:00 или момент вне диапазона DateTimeOffset: не дата для целей сравнения.
            return false;
        }
    }

    private static bool Digits(ReadOnlySpan<char> s, int start, int count, out int value)
    {
        value = 0;
        for (var i = start; i < start + count; i++)
        {
            if (s[i] is < '0' or > '9') return false;
            value = (value * 10) + (s[i] - '0');
        }

        return true;
    }
}

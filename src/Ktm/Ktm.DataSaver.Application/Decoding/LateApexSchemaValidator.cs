using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using LateApexEarlySpeed.Json.Schema;
using LateApexEarlySpeed.Json.Schema.Common;
using LateApexEarlySpeed.Json.Schema.Keywords;

namespace Ktm.DataSaver.Application.Decoding;

/// <summary>
/// Реализация <see cref="ISchemaValidator"/> на LateApexEarlySpeed.Json.Schema (BSD-3-Clause, draft 2020-12);
/// единственное место, знающее о библиотеке. Экземпляр используется как singleton: валидатор схемы потокобезопасен.
/// </summary>
public sealed partial class LateApexSchemaValidator : ISchemaValidator
{
    // Проверка формата (в т.ч. date-time) в библиотеке выполняется как assertion (ADR-0003 п. 2.2).
    // На успешном пути - дешёвый FailFast; подробный List строится только при ошибке.
    private static readonly JsonSchemaOptions FlagOptions = new() { OutputFormat = OutputFormat.FailFast };
    private static readonly JsonSchemaOptions ListOptions = new() { OutputFormat = OutputFormat.List };

    private readonly JsonValidator _validator;

    public LateApexSchemaValidator(string schemaJson)
    {
        // Встроенный date-time библиотеки отвергает дробную часть длиннее 7 цифр, а RFC 3339 допускает любую
        // (адаптеры могут слать наносекунды; декодер сам усекает до микросекунд) - подменяем формат своим.
        var options = new JsonValidatorOptions();
        options.FormatRegistry.AddFormat("date-time", () => new Rfc3339DateTimeFormat());
        _validator = new JsonValidator(schemaJson, options);
    }

    /// <summary>date-time по RFC 3339: полная дата и время, дробная часть любой длины, смещение Z или ±hh:mm.</summary>
    private sealed partial class Rfc3339DateTimeFormat : FormatValidator
    {
        [GeneratedRegex(@"^([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9]|60)(\.[0-9]+)?([Zz]|[+-]([01][0-9]|2[0-3]):[0-5][0-9])\z", RegexOptions.CultureInvariant)]
        private static partial Regex Pattern();

        public override bool Validate(string content)
        {
            var m = Pattern().Match(content);
            if (!m.Success)
            {
                return false;
            }

            if (!TryInt(m.Groups[1].Value, out var year) || !TryInt(m.Groups[2].Value, out var month) || !TryInt(m.Groups[3].Value, out var day))
            {
                return false;
            }

            return year >= 1 && month is >= 1 and <= 12 && day >= 1 && day <= DateTime.DaysInMonth(year, month);
        }

        private static bool TryInt(string text, out int value) =>
            int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value);
    }

    public SchemaValidationResult Validate(JsonElement instance)
    {
        if (_validator.Validate(instance, FlagOptions).IsValid)
        {
            return SchemaValidationResult.Valid;
        }

        var result = _validator.Validate(instance, ListOptions);
        // Только расположение и ключевое слово схемы; текст ошибки библиотеки и значения полей
        // из тела (возможные ПДн) не копируем.
        var parts = (result.ValidationErrors ?? []).Select(e => $"{e.InstanceLocation}: {e.Keyword}");

        return new SchemaValidationResult(false, !parts.Any() ? "Message does not match the schema." : string.Join("; ", parts.Distinct()));
    }
}

using System.Text.Json;
using LateApexEarlySpeed.Json.Schema;
using System.Text.RegularExpressions;
using LateApexEarlySpeed.Json.Schema.Common;
using LateApexEarlySpeed.Json.Schema.Keywords;

namespace Ktm.DataSaver.Core.Decoding;

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
        [GeneratedRegex(@"^(\d{4})-(\d{2})-(\d{2})[Tt]([01]\d|2[0-3]):([0-5]\d):([0-5]\d|60)(\.\d+)?([Zz]|[+-]([01]\d|2[0-3]):[0-5]\d)$", RegexOptions.CultureInvariant)]
        private static partial Regex Pattern();

        public override bool Validate(string content)
        {
            var m = Pattern().Match(content);
            if (!m.Success)
            {
                return false;
            }

            var year = int.Parse(m.Groups[1].Value);
            var month = int.Parse(m.Groups[2].Value);
            var day = int.Parse(m.Groups[3].Value);
            return year >= 1 && month is >= 1 and <= 12 && day >= 1 && day <= DateTime.DaysInMonth(year, month);
        }
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

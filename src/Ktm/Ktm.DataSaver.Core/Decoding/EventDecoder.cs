using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ktm.DataSaver.Core.Model;
using Json.Schema;

namespace Ktm.DataSaver.Core.Decoding;

/// <summary>
/// Разбирает тело сообщения в <see cref="ParsedEvent"/>: JSON, версия формата, схема
/// <c>docs/api/kafka-event.schema.json</c> (встроенный ресурс), дополнительные проверки Data Saver.
/// Любое нарушение возвращается как <see cref="DecodeResult"/> с причиной из <see cref="RejectReasons"/>, а не исключением.
/// </summary>
public sealed partial class EventDecoder
{
    /// <summary>Предел размера тела сообщения (схема БД, п. 7.5; ADR-0002 G-5).</summary>
    public const int MaxBodyBytes = 64 * 1024;

    private const string SchemaResourceName = "Ktm.DataSaver.Core.Schemas.kafka-event.schema.json";
    private const int MaxDetailLength = 500;

    private static readonly EvaluationOptions EvaluationOptions = new()
    {
        // format: date-time по умолчанию - только аннотация; включаем проверку (ADR-0003 п. 2.2).
        RequireFormatValidation = true,
        OutputFormat = OutputFormat.List,
    };

    private readonly Dictionary<int, JsonSchema> _schemasByVersion;

    public EventDecoder()
    {
        _schemasByVersion = new Dictionary<int, JsonSchema> { [1] = LoadEmbeddedSchema() };
    }

    public DecodeResult Decode(ReadOnlyMemory<byte> body)
    {
        if (body.Length > MaxBodyBytes)
        {
            return Fail(RejectReasons.InvalidSchema, $"Message body is {body.Length} bytes, limit is {MaxBodyBytes}.");
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException ex)
        {
            return Fail(RejectReasons.MalformedJson, ex.Message);
        }

        using (document)
        {
            return DecodeDocument(document.RootElement);
        }
    }

    private DecodeResult DecodeDocument(JsonElement root)
    {
        if (root.ValueKind != JsonValueKind.Object)
        {
            return Fail(RejectReasons.UnknownSchemaVersion, "Top-level JSON value is not an object, schemaVersion is absent.");
        }

        if (!TryReadVersion(root, out var version, out var versionProblem))
        {
            return Fail(RejectReasons.UnknownSchemaVersion, versionProblem);
        }

        if (!_schemasByVersion.TryGetValue(version, out var schema))
        {
            return Fail(RejectReasons.UnknownSchemaVersion, $"schemaVersion {version} is not supported.");
        }

        var evaluation = schema.Evaluate(root, EvaluationOptions);
        if (!evaluation.IsValid)
        {
            return Fail(RejectReasons.InvalidSchema, DescribeErrors(evaluation));
        }

        if (ContainsNullChar(root))
        {
            return Fail(RejectReasons.InvalidSchema, "Message contains the U+0000 character.");
        }

        var source = root.GetProperty("source").GetString()!;
        var sourceEventId = root.GetProperty("sourceEventId").GetString()!;

        if (!TryParseUtcMicros(root.GetProperty("occurredAt").GetString()!, out var occurredAt))
        {
            return Fail(RejectReasons.InvalidSchema, "occurredAt is not a representable UTC timestamp.");
        }

        CorrectionRef? correction = null;
        if (root.TryGetProperty("correction", out var correctionElement))
        {
            if (!TryParseUtcMicros(correctionElement.GetProperty("correctsOccurredAt").GetString()!, out var correctsAt))
            {
                return Fail(RejectReasons.InvalidSchema, "correction.correctsOccurredAt is not a representable UTC timestamp.");
            }

            var correctsId = correctionElement.GetProperty("correctsSourceEventId").GetString()!;
            var reason = correctionElement.TryGetProperty("reason", out var r) ? r.GetString() : null;

            // Источник исходного события равен источнику самого сообщения, поэтому сравниваем остальные части ключа.
            if (correctsId == sourceEventId && correctsAt == occurredAt)
            {
                return Fail(RejectReasons.InvalidSchema, "Correction references its own key (source, sourceEventId, occurredAt).");
            }

            correction = new CorrectionRef(correctsId, correctsAt, reason);
        }

        var deviceId = root.TryGetProperty("deviceId", out var d) && d.ValueKind == JsonValueKind.String
            ? d.GetString()
            : null;

        return DecodeResult.Success(new ParsedEvent(
            version,
            source,
            sourceEventId,
            occurredAt,
            root.GetProperty("eventType").GetString()!,
            deviceId,
            root.GetProperty("externalPersonId").GetString()!,
            root.GetProperty("payload").Clone(),
            correction));
    }

    /// <summary>Версия - целое число JSON; всё остальное (нет поля, строка, дробь) - неизвестная версия.</summary>
    private static bool TryReadVersion(JsonElement root, out int version, out string problem)
    {
        version = 0;
        if (!root.TryGetProperty("schemaVersion", out var element))
        {
            problem = "schemaVersion is absent.";
            return false;
        }

        if (element.ValueKind != JsonValueKind.Number)
        {
            problem = $"schemaVersion is {element.ValueKind}, expected an integer number.";
            return false;
        }

        if (!element.TryGetDecimal(out var number) || number != decimal.Truncate(number) || number is < 0 or > int.MaxValue)
        {
            problem = "schemaVersion is not an integer in the supported range.";
            return false;
        }

        version = (int)number;
        problem = string.Empty;
        return true;
    }

    /// <summary>
    /// Разбор RFC 3339 UTC с детерминированным усечением (не округлением) дробной части до микросекунд:
    /// повтор события с более точным временем даёт тот же ключ идемпотентности.
    /// </summary>
    internal static bool TryParseUtcMicros(string value, out DateTimeOffset result)
    {
        result = default;
        var match = UtcTimestampRegex().Match(value);
        if (!match.Success)
        {
            return false;
        }

        if (!DateTime.TryParseExact(
                match.Groups["base"].Value.Replace('t', 'T'),
                "yyyy-MM-dd'T'HH:mm:ss",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var seconds))
        {
            return false;
        }

        var micros = 0;
        if (match.Groups["frac"].Success)
        {
            var digits = match.Groups["frac"].Value;
            digits = digits.Length >= 6 ? digits[..6] : digits.PadRight(6, '0');
            micros = int.Parse(digits, CultureInfo.InvariantCulture);
        }

        result = new DateTimeOffset(DateTime.SpecifyKind(seconds, DateTimeKind.Utc)).AddTicks(micros * 10L);
        return true;
    }

    private static bool ContainsNullChar(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                return element.GetString()!.Contains('\0');
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    if (ContainsNullChar(item))
                    {
                        return true;
                    }
                }

                return false;
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (property.Name.Contains('\0') || ContainsNullChar(property.Value))
                    {
                        return true;
                    }
                }

                return false;
            default:
                return false;
        }
    }

    private static string DescribeErrors(EvaluationResults evaluation)
    {
        // Только расположение и текст ошибки схемы; значения полей из тела (возможные ПДн) в пояснение не копируем.
        var parts = new List<string>();
        foreach (var detail in (evaluation.Details ?? []).Prepend(evaluation))
        {
            if (detail.Errors is null)
            {
                continue;
            }

            foreach (var error in detail.Errors)
            {
                parts.Add($"{detail.InstanceLocation}: {error.Key} - {error.Value}");
            }
        }

        var text = parts.Count == 0 ? "Message does not match the schema." : string.Join("; ", parts.Distinct());
        return Truncate(text);
    }

    private static DecodeResult Fail(string reason, string detail) => DecodeResult.Failure(reason, Truncate(detail));

    private static string Truncate(string text) => text.Length <= MaxDetailLength ? text : text[..MaxDetailLength];

    private static JsonSchema LoadEmbeddedSchema()
    {
        using var stream = typeof(EventDecoder).Assembly.GetManifestResourceStream(SchemaResourceName)
                           ?? throw new InvalidOperationException($"Embedded resource '{SchemaResourceName}' not found.");
        using var reader = new StreamReader(stream);
        // Собственный реестр: глобальный отвергает повторную регистрацию схемы с тем же $id (второй экземпляр декодера).
        return JsonSchema.FromText(reader.ReadToEnd(), new BuildOptions { SchemaRegistry = new SchemaRegistry() });
    }

    [GeneratedRegex(@"^(?<base>\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2})(?:\.(?<frac>\d+))?Z$")]
    private static partial Regex UtcTimestampRegex();
}

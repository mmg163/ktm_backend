using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Decoding;

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

    private static readonly JsonDocumentOptions ParseOptions = new()
    {
        // Дубликаты имён свойств отвергаем: парсеры расходятся (jsonb берёт последнее), схема и запись увидели бы разное.
        AllowDuplicateProperties = false,
    };

    private readonly Dictionary<int, ISchemaValidator> _validatorsByVersion;

    public EventDecoder() : this(new Dictionary<int, ISchemaValidator> { [1] = LoadEmbeddedSchema() })
    {
    }

    /// <summary>Набор поддерживаемых версий формата и их валидаторов схемы.</summary>
    public EventDecoder(IReadOnlyDictionary<int, ISchemaValidator> validatorsByVersion)
    {
        _validatorsByVersion = new Dictionary<int, ISchemaValidator>(validatorsByVersion);
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
            // UTF-8 BOM в начале тела не принимается (malformed_json): адаптеры обязаны слать JSON без BOM.
            document = JsonDocument.Parse(body, ParseOptions);
        }
        catch (InvalidOperationException)
        {
            // Проверка дубликатов декодирует имена свойств; непарный суррогат в имени бросает InvalidOperationException.
            return Fail(RejectReasons.InvalidSchema, "Message contains an unpaired UTF-16 surrogate in a property name.");
        }
        catch (JsonException ex)
        {
            var position = ex.LineNumber is { } line ? $" at line {line + 1}, position {ex.BytePositionInLine + 1}" : string.Empty;
            return Fail(RejectReasons.MalformedJson, $"Body is not valid JSON (or has duplicate property names){position}.");
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

        if (!_validatorsByVersion.TryGetValue(version, out var validator))
        {
            return Fail(RejectReasons.UnknownSchemaVersion, $"schemaVersion {version} is not supported.");
        }

        // До схемы: библиотека схемы не обязана переживать непарные суррогаты, а jsonb их всё равно отвергает.
        if (JsonbCompatibility.FindProblem(root) is { } jsonbProblem)
        {
            return Fail(RejectReasons.InvalidSchema, jsonbProblem);
        }

        var schemaResult = validator.Validate(root);
        if (!schemaResult.IsValid)
        {
            return Fail(RejectReasons.InvalidSchema, schemaResult.Errors);
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
            correction,
            root.Clone()));
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

    private static DecodeResult Fail(string reason, string detail) => DecodeResult.Failure(reason, Truncate(detail));

    private static string Truncate(string text)
    {
        if (text.Length <= MaxDetailLength)
        {
            return text;
        }

        // Не режем суррогатную пару посередине.
        var length = char.IsHighSurrogate(text[MaxDetailLength - 1]) ? MaxDetailLength - 1 : MaxDetailLength;
        return text[..length];
    }

    private static LateApexSchemaValidator LoadEmbeddedSchema()
    {
        using var stream = typeof(EventDecoder).Assembly.GetManifestResourceStream(SchemaResourceName)
                           ?? throw new InvalidOperationException($"Embedded resource '{SchemaResourceName}' not found.");
        using var reader = new StreamReader(stream);
        return new LateApexSchemaValidator(reader.ReadToEnd());
    }

    [GeneratedRegex(@"^(?<base>[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.(?<frac>[0-9]+))?Z\z")]
    private static partial Regex UtcTimestampRegex();
}

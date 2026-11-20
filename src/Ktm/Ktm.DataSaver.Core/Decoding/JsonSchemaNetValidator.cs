using System.Text.Json;
using Json.Schema;

namespace Ktm.DataSaver.Core.Decoding;

/// <summary>Реализация <see cref="ISchemaValidator"/> на JsonSchema.Net; единственное место, знающее о библиотеке.</summary>
public sealed class JsonSchemaNetValidator : ISchemaValidator
{
    // format: date-time по умолчанию - только аннотация; включаем проверку (ADR-0003 п. 2.2).
    // На успешном пути - дешёвый Flag; подробный List строится только при ошибке.
    private static readonly EvaluationOptions FlagOptions = new() { RequireFormatValidation = true, OutputFormat = OutputFormat.Flag };
    private static readonly EvaluationOptions ListOptions = new() { RequireFormatValidation = true, OutputFormat = OutputFormat.List };

    private readonly JsonSchema _schema;

    public JsonSchemaNetValidator(string schemaJson)
    {
        // Собственный реестр: глобальный отвергает повторную регистрацию схемы с тем же $id.
        _schema = JsonSchema.FromText(schemaJson, new BuildOptions { SchemaRegistry = new SchemaRegistry() });
    }

    public SchemaValidationResult Validate(JsonElement instance)
    {
        if (_schema.Evaluate(instance, FlagOptions).IsValid)
        {
            return SchemaValidationResult.Valid;
        }

        var evaluation = _schema.Evaluate(instance, ListOptions);
        // Только расположение и текст ошибки схемы; значения полей из тела (возможные ПДн) не копируем.
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

        return new SchemaValidationResult(false, parts.Count == 0 ? "Message does not match the schema." : string.Join("; ", parts.Distinct()));
    }
}

using System.Text.Json;

namespace Ktm.DataSaver.Core.Decoding;

/// <summary>Результат проверки по JSON Schema; <c>Errors</c> - краткое пояснение без значений из тела (только при невалидности).</summary>
public sealed record SchemaValidationResult(bool IsValid, string Errors)
{
    public static SchemaValidationResult Valid { get; } = new(true, string.Empty);
}

/// <summary>
/// Граница с библиотекой JSON Schema: остальной код Core не знает, какая библиотека используется.
/// Реализация обязана быть потокобезопасной.
/// </summary>
public interface ISchemaValidator
{
    SchemaValidationResult Validate(JsonElement instance);
}

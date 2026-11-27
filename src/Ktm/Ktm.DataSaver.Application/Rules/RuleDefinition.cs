using System.Text.Json;

namespace Ktm.DataSaver.Application.Rules;

/// <summary>
/// Определение правила отбраковки в терминах модели, без имён колонок БД (ADR-0005 п. 2.1).
/// <c>Source = null</c> - правило для всех источников: значение <c>'*'</c> в таблице источник правил (адаптер БД) отображает в <c>null</c>;
/// строка <c>"*"</c> в <see cref="Source"/> недопустима.
/// </summary>
public sealed record RuleDefinition(
    string Id,
    string Name,
    string? Description,
    string? Source,
    bool Enabled,
    int Priority,
    int Version,
    string ReasonCode,
    JsonElement Condition)
{
    /// <summary>Единый список источников событий (ADR-0005 п. 2.1); <c>null</c> в <see cref="Source"/> означает «все».</summary>
    public static IReadOnlyList<string> KnownSources { get; } = ["skud", "cv", "1c-do"];
}

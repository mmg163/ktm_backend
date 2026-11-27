namespace Ktm.DataSaver.Application.Pipeline;

/// <summary>Вид отбраковки. <see cref="Rule"/> - отбраковка движком правил (ADR-0005).</summary>
public enum RejectionKind
{
    Format,
    Validation,
    Rule,
}

/// <summary>
/// Описание отбраковки. <c>TargetTopic = null</c> - топик по умолчанию (<c>ktm.events.rejected</c>),
/// его подставляет публикатор, а не стадия.
/// </summary>
public sealed record RejectionInfo(
    RejectionKind Kind,
    string Reason,
    string Detail,
    string? RuleId = null,
    string? TargetTopic = null,
    string? RuleName = null,
    int? RuleVersion = null);

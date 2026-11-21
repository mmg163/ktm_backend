namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>Вид отбраковки. <see cref="Rule"/> зарезервирован под будущий движок правил (ADR-0003 п. 2.4).</summary>
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
    string? TargetTopic = null);

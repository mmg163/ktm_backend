using System.Text;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Pipeline;

namespace Ktm.DataSaver.Application.Rules;

/// <summary>
/// Стадия отбраковки правилами (ADR-0005 п. 2.8, 2.9): берёт текущий снимок один раз, вычисляет правила источника события
/// и общие по порядку, на первом сработавшем отбраковывает. Не обращается к БД; исключения вычислителя не перехватывает
/// (ADR-0003 п. 2.1), кроме прерывания по пределу совпадения regex: оно отбраковывает сообщение системным кодом
/// <see cref="RejectReasons.RuleEvalTimeout"/>, остальные правила не вычисляются (ADR-0005 п. 2.7).
/// </summary>
public sealed class RuleRejectionStage(IRuleSnapshotProvider snapshots) : IIngestionStage
{
    public const int MaxDetailLength = 1024;

    public string Stage => PipelineStages.Rules;

    public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
    {
        var snapshot = snapshots.Current;
        foreach (var rule in snapshot.RulesFor(parsed.Source))
        {
            bool fired;
            try
            {
                fired = rule.Evaluate(parsed.Body);
            }
            catch (RuleEvaluationAbortedException ex) when (ex.Kind == RuleEvaluationAbortedException.RegexTimeout)
            {
                return IngestionVerdict.Reject(TimeoutRejection(rule.Definition, ex.Limit));
            }

            if (!fired)
            {
                continue;
            }

            var definition = rule.Definition;
            return IngestionVerdict.Reject(new RejectionInfo(
                RejectionKind.Rule,
                definition.ReasonCode,
                BuildDetail(rule, parsed),
                RuleId: definition.Id,
                RuleName: definition.Name,
                RuleVersion: definition.Version));
        }

        return IngestionVerdict.Accept;
    }

    /// <summary>«Сработало правило «name» (id, vN): путь операция, ...»: пути и операции из правила, значения сообщения не включаются.</summary>
    private static string BuildDetail(CompiledRule rule, ParsedEvent parsed)
    {
        var definition = rule.Definition;
        var text = new StringBuilder("Сработало правило «")
            .Append(definition.Name).Append("» (").Append(definition.Id).Append(", v").Append(definition.Version).Append(')');
        IReadOnlyList<string> leaves;
        try
        {
            leaves = rule.TraceFired(parsed.Body);
        }
        catch (RuleEvaluationAbortedException)
        {
            // Правило уже сработало: отбраковка сохраняется с его reason_code, перечень листьев опускается.
            leaves = [];
        }

        if (leaves.Count > 0)
        {
            text.Append(": ").AppendJoin(", ", leaves);
        }

        return Truncate(text.ToString(), MaxDetailLength);
    }

    /// <summary>Без значений поля, шаблона и совпавшего фрагмента: только правило и предел из исключения.</summary>
    private static RejectionInfo TimeoutRejection(RuleDefinition definition, TimeSpan limit)
    {
        var detail = $"Вычисление правила «{definition.Name}» ({definition.Id}, v{definition.Version}) прервано: " +
                     $"превышен предел совпадения регулярного выражения ({(long)Math.Round(limit.TotalMilliseconds)} мс)";
        return new RejectionInfo(
            RejectionKind.Rule,
            RejectReasons.RuleEvalTimeout,
            Truncate(detail, MaxDetailLength),
            RuleId: definition.Id,
            RuleName: definition.Name,
            RuleVersion: definition.Version);
    }

    private static string Truncate(string value, int max)
    {
        if (value.Length <= max)
        {
            return value;
        }

        var end = max;
        if (char.IsHighSurrogate(value[end - 1]))
        {
            end--;
        }

        return value[..end];
    }
}

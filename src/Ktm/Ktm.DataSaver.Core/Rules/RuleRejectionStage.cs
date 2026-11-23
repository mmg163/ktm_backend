using System.Text;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;

namespace Ktm.DataSaver.Core.Rules;

/// <summary>
/// Стадия отбраковки правилами (ADR-0005 п. 2.8, 2.9): берёт текущий снимок один раз, вычисляет правила источника события
/// и общие по порядку, на первом сработавшем отбраковывает. Не обращается к БД; исключения не перехватывает (ADR-0003 п. 2.1).
/// </summary>
public sealed class RuleRejectionStage(IRuleSnapshotProvider snapshots) : IIngestionStage
{
    public const int MaxDetailLength = 1024;

    public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
    {
        var snapshot = snapshots.Current;
        foreach (var rule in snapshot.RulesFor(parsed.Source))
        {
            if (!rule.Evaluate(parsed.Body))
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
        var leaves = rule.TraceFired(parsed.Body);
        if (leaves.Count > 0)
        {
            text.Append(": ").AppendJoin(", ", leaves);
        }

        return Truncate(text.ToString(), MaxDetailLength);
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

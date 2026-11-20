using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>Итог обработки одного сообщения: <c>Event</c> заполнен только при <c>Verdict.IsAccepted</c>.</summary>
public sealed record PipelineResult(ConsumedMessage Message, ParsedEvent? Event, IngestionVerdict Verdict);

/// <summary>
/// Формат (встроенная стадия), затем стадии <see cref="IIngestionStage"/> строго в порядке регистрации;
/// первая отбраковка прекращает цепочку. Исключения стадий не перехватываются.
/// </summary>
public sealed class IngestionPipeline(FormatStage formatStage, IEnumerable<IIngestionStage> stages)
{
    private readonly IIngestionStage[] _stages = stages.ToArray();

    public PipelineResult Process(ConsumedMessage message)
    {
        var format = formatStage.Run(message);
        if (format.Event is not { } parsed)
        {
            return new PipelineResult(message, null, format.Verdict);
        }

        foreach (var stage in _stages)
        {
            var verdict = stage.Evaluate(message, parsed);
            if (!verdict.IsAccepted)
            {
                return new PipelineResult(message, null, verdict);
            }
        }

        return new PipelineResult(message, parsed, IngestionVerdict.Accept);
    }
}

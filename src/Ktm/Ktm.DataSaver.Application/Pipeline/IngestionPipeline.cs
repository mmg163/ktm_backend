using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Resilience;

namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>
/// Итог обработки одного сообщения: <c>Event</c> заполнен только при <c>Verdict.IsAccepted</c> и отсутствии <c>DeadLetter</c>.
/// <c>Origin</c> - происхождение отбракованного стадией (после разбора формата) события, для лога; null для принятых
/// и для сообщений, не прошедших разбор формата. <c>DeadLetter</c> - причина ухода в dlq: сообщение признано ядовитым
/// (неожиданное исключение стадии или записи, бюджет попыток исчерпан); вердикт при этом не отбраковка и не запись.
/// </summary>
public sealed record PipelineResult(
    ConsumedMessage Message,
    ParsedEvent? Event,
    IngestionVerdict Verdict,
    EventOrigin? Origin = null,
    DeadLetterInfo? DeadLetter = null)
{
    /// <summary>Сообщение признано ядовитым: не пишется, не отбраковывается, публикуется в dlq.</summary>
    public static PipelineResult Poison(ConsumedMessage message, DeadLetterInfo info, EventOrigin? origin) =>
        new(message, null, IngestionVerdict.Accept, origin, info);
}

/// <summary>
/// Формат (встроенная стадия), затем стадии <see cref="IIngestionStage"/> строго в порядке регистрации;
/// первая отбраковка прекращает цепочку. Неожиданное исключение стадии оборачивается в <see cref="PipelineStageException"/>
/// с именем стадии и покидает конвейер: это не отбраковка (решения о данных нет), его обрабатывает цикл приёма.
/// </summary>
public sealed class IngestionPipeline(FormatStage formatStage, IEnumerable<IIngestionStage> stages)
{
    private readonly IIngestionStage[] _stages = stages.ToArray();

    public PipelineResult Process(ConsumedMessage message)
    {
        FormatStageResult format;
        try
        {
            format = formatStage.Run(message);
        }
        catch (Exception ex)
        {
            throw new PipelineStageException(PipelineStages.Parse, ex);
        }

        if (format.Event is not { } parsed)
        {
            return new PipelineResult(message, null, format.Verdict);
        }

        foreach (var stage in _stages)
        {
            IngestionVerdict verdict;
            try
            {
                verdict = stage.Evaluate(message, parsed);
            }
            catch (Exception ex)
            {
                throw new PipelineStageException(stage.Stage, ex, new EventOrigin(parsed.Source, parsed.SourceEventId));
            }

            if (!verdict.IsAccepted)
            {
                return new PipelineResult(message, null, verdict, new EventOrigin(parsed.Source, parsed.SourceEventId));
            }
        }

        return new PipelineResult(message, parsed, IngestionVerdict.Accept);
    }
}

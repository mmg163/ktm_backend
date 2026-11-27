using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Pipeline;

/// <summary>
/// Стадия конвейера после разбора формата: чистая функция «сообщение -> вердикт» (ADR-0003 п. 2.1).
/// Стадия не пишет в БД и не публикует, не меняет тело, не обращается к БД журнала на каждое сообщение.
/// Исключение стадии не превращается в отбраковку: конвейер оборачивает его в <see cref="PipelineStageException"/>
/// с именем стадии, а цикл классифицирует и повторяет сообщение, пока бюджет попыток не исчерпан (ADR-0006).
/// </summary>
public interface IIngestionStage
{
    /// <summary>Имя стадии для <c>ktm-dlq-stage</c>: одно из значений <see cref="PipelineStages"/>.</summary>
    string Stage { get; }

    IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed);
}

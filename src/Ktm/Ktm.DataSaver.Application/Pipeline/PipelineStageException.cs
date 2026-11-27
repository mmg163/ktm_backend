using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>
/// Неожиданное исключение стадии конвейера (разбор, C5, правила, маппинг) с указанием стадии (ADR-0006 п. 2.2).
/// Штатные результаты (не JSON, схема, отказ C5 или правила) исключениями не становятся. Текст исходного исключения
/// сюда не копируется и не логируется: он может нести фрагмент тела сообщения.
/// </summary>
public sealed class PipelineStageException(string stage, Exception inner, EventOrigin? origin = null)
    : Exception($"Pipeline stage '{stage}' failed: {inner.GetType().Name}", inner)
{
    /// <summary>Одно из значений <see cref="PipelineStages"/>.</summary>
    public string Stage { get; } = stage;

    /// <summary>Происхождение события, если формат уже разобран.</summary>
    public EventOrigin? Origin { get; } = origin;
}

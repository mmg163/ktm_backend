namespace Ktm.DataSaver.Application.Pipeline;

/// <summary>Значения заголовка <c>ktm-dlq-stage</c> (ADR-0006 п. 2.4): стадия обработки, на которой возникло исключение.</summary>
public static class PipelineStages
{
    /// <summary>Разбор тела и проверка схемы (<see cref="FormatStage"/>).</summary>
    public const string Parse = "parse";

    /// <summary>Проверка C5 (<see cref="EventValidationStage"/>).</summary>
    public const string Validation = "validation";

    /// <summary>Движок правил отбраковки.</summary>
    public const string Rules = "rules";

    /// <summary>Запись в БД.</summary>
    public const string Write = "write";
}

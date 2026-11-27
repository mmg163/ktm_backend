using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Pipeline;

/// <summary>Стадия-адаптер порта <see cref="IEventValidator"/> (C5). Исключения порта не перехватывает.</summary>
public sealed class EventValidationStage(IEventValidator validator) : IIngestionStage
{
    public string Stage => PipelineStages.Validation;

    public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
    {
        var result = validator.Validate(parsed);
        return result.IsAccepted
            ? IngestionVerdict.Accept
            : IngestionVerdict.Reject(new RejectionInfo(RejectionKind.Validation, result.Reason!, result.Detail ?? string.Empty));
    }
}

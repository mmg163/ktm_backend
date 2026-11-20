using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>Итог стадии формата: <c>Event</c> заполнен тогда и только тогда, когда вердикт - Accept.</summary>
public sealed record FormatStageResult(ParsedEvent? Event, IngestionVerdict Verdict);

/// <summary>
/// Встроенная обязательная стадия формата (ADR-0003 п. 2.2): разбирает тело и проверяет схему.
/// Выполняется первой и в список <see cref="IIngestionStage"/> не входит, так как производит <see cref="ParsedEvent"/>,
/// который получают остальные стадии.
/// </summary>
public sealed class FormatStage(EventDecoder decoder)
{
    public FormatStageResult Run(ConsumedMessage message)
    {
        var result = decoder.Decode(message.Body);
        return result.Event is { } parsed
            ? new FormatStageResult(parsed, IngestionVerdict.Accept)
            : new FormatStageResult(null, IngestionVerdict.Reject(new RejectionInfo(RejectionKind.Format, result.Reason!, result.Detail!)));
    }
}

using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Core.Pipeline;

/// <summary>
/// Встроенная обязательная стадия формата (ADR-0003 п. 2.2): разбирает тело и проверяет схему.
/// Выполняется первой и в список <see cref="IIngestionStage"/> не входит, так как производит <see cref="ParsedEvent"/>,
/// который получают остальные стадии.
/// </summary>
public sealed class FormatStage(EventDecoder decoder)
{
    public (ParsedEvent? Event, IngestionVerdict Verdict) Run(ConsumedMessage message)
    {
        var result = decoder.Decode(message.Body);
        return result.IsSuccess
            ? (result.Event, IngestionVerdict.Accept)
            : (null, IngestionVerdict.Reject(new RejectionInfo(RejectionKind.Format, result.Reason!, result.Detail!)));
    }
}

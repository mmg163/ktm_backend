using System.Text.Json;

namespace Ktm.DataSaver.Core.Model;

/// <summary>Событие формата Kafka v1 после разбора и проверки. <c>OccurredAt</c> - UTC, усечено до микросекунд.</summary>
public sealed record ParsedEvent(
    int SchemaVersion,
    string Source,
    string SourceEventId,
    DateTimeOffset OccurredAt,
    string EventType,
    string? DeviceId,
    string ExternalPersonId,
    JsonElement Payload,
    CorrectionRef? Correction)
{
    public bool IsCorrection => Correction is not null;

    public EventKey Key => new(Source, SourceEventId, OccurredAt);
}

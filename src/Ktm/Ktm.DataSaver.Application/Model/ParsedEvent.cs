using System.Text.Json;

namespace Ktm.DataSaver.Core.Model;

/// <summary>
/// Событие формата Kafka v1 после разбора и проверки. <c>OccurredAt</c> - UTC, усечено до микросекунд.
/// <c>Payload</c> - вложенный объект <c>payload</c>; <c>Body</c> - всё тело сообщения как получено (конверт, <c>correction</c>,
/// неизвестные поля): именно оно пишется в <c>events.payload</c> (ADR-0002 D-3).
/// </summary>
public sealed record ParsedEvent(
    int SchemaVersion,
    string Source,
    string SourceEventId,
    DateTimeOffset OccurredAt,
    string EventType,
    string? DeviceId,
    string ExternalPersonId,
    JsonElement Payload,
    CorrectionRef? Correction,
    JsonElement Body)
{
    public bool IsCorrection => Correction is not null;

    public EventKey Key => new(Source, SourceEventId, OccurredAt);
}

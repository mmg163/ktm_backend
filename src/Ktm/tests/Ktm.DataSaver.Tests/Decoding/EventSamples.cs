using System.Text;
using System.Text.Json.Nodes;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Tests.Decoding;

/// <summary>Примеры из docs/api/kafka-event-format.md п. 5 и помощники для построения невалидных вариантов.</summary>
internal static class EventSamples
{
    public const string Skud = """
        {"schemaVersion":1,"source":"skud","sourceEventId":"48213377","occurredAt":"2026-11-19T04:58:12Z",
         "eventType":"access_granted","deviceId":"turnstile-01-entry","externalPersonId":"card:0012345678",
         "payload":{"direction":"in","zone":"main-gate","orionEventCode":28}}
        """;

    public const string CvPresence = """
        {"schemaVersion":1,"source":"cv","sourceEventId":"cv-cam12-20261119-0001873","occurredAt":"2026-11-19T05:12:40Z",
         "eventType":"presence_detected","deviceId":"cam-12","externalPersonId":"tab:5678",
         "payload":{"workplaceId":"wp-cnc-04","confidence":0.93,"frameQuality":"good","evidenceUrl":"s3://cv-evidence/cam-12/2026-11-19/0001873"}}
        """;

    public const string CvAbsence = """
        {"schemaVersion":1,"source":"cv","sourceEventId":"cv-cam12-20261119-0001911","occurredAt":"2026-11-19T06:47:05Z",
         "eventType":"absence_detected","deviceId":"cam-12","externalPersonId":"tab:5678",
         "payload":{"workplaceId":"wp-cnc-04","confidence":0.88,"absentSinceUtc":"2026-11-19T06:42:05Z","evidenceUrl":"s3://cv-evidence/cam-12/2026-11-19/0001911"}}
        """;

    public const string OneCDo = """
        {"schemaVersion":1,"source":"1c-do","sourceEventId":"task-000451-status-3","occurredAt":"2026-11-18T15:00:00Z",
         "eventType":"task_overdue","deviceId":null,"externalPersonId":"tab:5678",
         "payload":{"taskId":"000451","status":"overdue","dueAt":"2026-11-18T15:00:00Z"}}
        """;

    public const string SkudCorrection = """
        {"schemaVersion":1,"source":"skud","sourceEventId":"48213377-corr-1","occurredAt":"2026-11-19T04:58:12Z",
         "eventType":"access_granted","deviceId":"turnstile-01-entry","externalPersonId":"card:0012345679",
         "payload":{"direction":"in","zone":"main-gate","orionEventCode":28},
         "correction":{"correctsSourceEventId":"48213377","correctsOccurredAt":"2026-11-19T04:58:12Z","reason":"Исправлен владелец карты"}}
        """;

    public static TheoryData<string> AllValid => new() { Skud, CvPresence, CvAbsence, OneCDo, SkudCorrection };

    public static byte[] Bytes(string json) => Encoding.UTF8.GetBytes(json);

    /// <summary>Копия валидного события СКУД с применённой правкой верхнего уровня.</summary>
    public static byte[] SkudWith(Action<JsonObject> mutate)
    {
        var node = JsonNode.Parse(Skud)!.AsObject();
        mutate(node);
        return Encoding.UTF8.GetBytes(node.ToJsonString());
    }

    public static ConsumedMessage Message(byte[] body, int partition = 0, long offset = 0) =>
        new("ktm.events.raw", partition, offset, ReadOnlyMemory<byte>.Empty, body, DateTimeOffset.UnixEpoch);
}

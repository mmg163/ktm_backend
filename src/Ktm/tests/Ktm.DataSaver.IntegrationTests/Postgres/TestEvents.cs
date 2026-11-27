using System.Text.Json;
using System.Text.Json.Nodes;
using Ktm.DataSaver.Application.Ingestion;
using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

internal static class TestEvents
{
    /// <summary>Уникальный источник на тест (до 32 символов): тесты делят одну БД и не пересекаются по ключам.</summary>
    public static string NewSource() => "t" + Guid.NewGuid().ToString("N")[..20];

    public static DateTimeOffset At(int offsetSeconds = 0) =>
        new DateTimeOffset(2026, 3, 10, 8, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

    public static ParsedEvent Event(string source, string id, DateTimeOffset occurredAt, string? deviceId = "dev-1", int n = 0)
    {
        var person = "person-" + (n % 7);
        var body = new JsonObject
        {
            ["schemaVersion"] = 1,
            ["source"] = source,
            ["sourceEventId"] = id,
            ["occurredAt"] = occurredAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'"),
            ["eventType"] = "access.pass",
            ["deviceId"] = deviceId,
            ["externalPersonId"] = person,
            ["payload"] = new JsonObject { ["n"] = n, ["text"] = "тест" },
            ["futureField"] = new JsonObject { ["x"] = new JsonArray(1, 2) },
        };
        return new ParsedEvent(1, source, id, occurredAt, "access.pass", deviceId, person,
            ToElement(body["payload"]!), Correction: null, ToElement(body));
    }

    public static ParsedEvent Correction(string source, string id, DateTimeOffset occurredAt, string originalId,
        DateTimeOffset originalOccurredAt, string? reason = "ошибочный проход")
    {
        var original = Event(source, id, occurredAt);
        var body = JsonNode.Parse(original.Body.GetRawText())!.AsObject();
        var correction = new JsonObject
        {
            ["correctsSourceEventId"] = originalId,
            ["correctsOccurredAt"] = originalOccurredAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'"),
        };
        if (reason is not null) correction["reason"] = reason;
        body["correction"] = correction;
        return original with { Correction = new CorrectionRef(originalId, originalOccurredAt, reason), Body = ToElement(body) };
    }

    private static JsonElement ToElement(JsonNode node)
    {
        using var document = JsonDocument.Parse(node.ToJsonString());
        return document.RootElement.Clone();
    }
}

internal sealed class FixedTrustProvider(short defaultTrust, params (string Source, short Trust)[] overrides) : ISourceTrustProvider
{
    public short GetTrust(string source) => overrides.FirstOrDefault(o => o.Source == source) is { Source: not null } o ? o.Trust : defaultTrust;
}

using System.Text.Json;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.IntegrationTests.Postgres;

internal static class TestEvents
{
    /// <summary>Уникальный источник на тест (до 32 символов): тесты делят одну БД и не пересекаются по ключам.</summary>
    public static string NewSource() => "t" + Guid.NewGuid().ToString("N")[..20];

    public static DateTimeOffset At(int offsetSeconds = 0) =>
        new DateTimeOffset(2026, 3, 10, 8, 0, 0, TimeSpan.Zero).AddSeconds(offsetSeconds);

    public static ParsedEvent Event(string source, string id, DateTimeOffset occurredAt, string? deviceId = "dev-1", int n = 0) =>
        new(1, source, id, occurredAt, "access.pass", deviceId, "person-" + (n % 7),
            JsonDocument.Parse($$"""{"n":{{n}},"text":"тест"}""").RootElement.Clone(), Correction: null);

    public static ParsedEvent Correction(string source, string id, DateTimeOffset occurredAt, string originalId,
        DateTimeOffset originalOccurredAt, string? reason = "ошибочный проход") =>
        Event(source, id, occurredAt) with { Correction = new CorrectionRef(originalId, originalOccurredAt, reason) };
}

internal sealed class FixedTrustProvider(short defaultTrust, params (string Source, short Trust)[] overrides) : ISourceTrustProvider
{
    public short GetTrust(string source) => overrides.FirstOrDefault(o => o.Source == source) is { Source: not null } o ? o.Trust : defaultTrust;
}

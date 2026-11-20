using System.Text.Json.Nodes;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Tests.Decoding;

public class EventDecoderTests
{
    private readonly EventDecoder _decoder = new();

    [Theory]
    [MemberData(nameof(EventSamples.AllValid), MemberType = typeof(EventSamples))]
    public void Decode_AllSamplesFromFormatDocument_AreValid(string json)
    {
        var result = _decoder.Decode(EventSamples.Bytes(json));

        Assert.True(result.IsSuccess, result.Detail);
        Assert.Equal(TimeSpan.Zero, result.Event!.OccurredAt.Offset);
    }

    [Fact]
    public void Decode_Skud_MapsFields()
    {
        var parsed = _decoder.Decode(EventSamples.Bytes(EventSamples.Skud)).Event!;

        Assert.Equal(1, parsed.SchemaVersion);
        Assert.Equal("skud", parsed.Source);
        Assert.Equal("48213377", parsed.SourceEventId);
        Assert.Equal(new DateTimeOffset(2026, 11, 19, 4, 58, 12, TimeSpan.Zero), parsed.OccurredAt);
        Assert.Equal("access_granted", parsed.EventType);
        Assert.Equal("turnstile-01-entry", parsed.DeviceId);
        Assert.Equal("card:0012345678", parsed.ExternalPersonId);
        Assert.Equal(28, parsed.Payload.GetProperty("orionEventCode").GetInt32());
        Assert.False(parsed.IsCorrection);
    }

    [Fact]
    public void Decode_OneCDoWithNullDevice_HasNoDeviceId()
    {
        var parsed = _decoder.Decode(EventSamples.Bytes(EventSamples.OneCDo)).Event!;

        Assert.Null(parsed.DeviceId);
    }

    [Fact]
    public void Decode_Correction_ExposesReferenceToOriginal()
    {
        var parsed = _decoder.Decode(EventSamples.Bytes(EventSamples.SkudCorrection)).Event!;

        Assert.True(parsed.IsCorrection);
        Assert.Equal("48213377", parsed.Correction!.CorrectsSourceEventId);
        Assert.Equal(new DateTimeOffset(2026, 11, 19, 4, 58, 12, TimeSpan.Zero), parsed.Correction.CorrectsOccurredAt);
        Assert.Equal("Исправлен владелец карты", parsed.Correction.Reason);
        Assert.Equal("48213377-corr-1", parsed.Key.SourceEventId);
    }

    public static TheoryData<string, byte[], string> InvalidCases() => new()
    {
        { "not json", EventSamples.Bytes("{ this is not json"), RejectReasons.MalformedJson },
        { "empty body", [], RejectReasons.MalformedJson },
        { "invalid utf-8", [0x7B, 0xFF, 0xFE, 0x7D], RejectReasons.MalformedJson },
        { "array at top level", EventSamples.Bytes("[]"), RejectReasons.UnknownSchemaVersion },
        { "schemaVersion 2", EventSamples.SkudWith(o => o["schemaVersion"] = 2), RejectReasons.UnknownSchemaVersion },
        { "schemaVersion string", EventSamples.SkudWith(o => o["schemaVersion"] = "1"), RejectReasons.UnknownSchemaVersion },
        { "schemaVersion absent", EventSamples.SkudWith(o => o.Remove("schemaVersion")), RejectReasons.UnknownSchemaVersion },
        { "occurredAt with offset", EventSamples.SkudWith(o => o["occurredAt"] = "2026-11-19T07:58:12+03:00"), RejectReasons.InvalidSchema },
        { "occurredAt not a date", EventSamples.SkudWith(o => o["occurredAt"] = "2026-13-45T04:58:12Z"), RejectReasons.InvalidSchema },
        { "deviceId absent for skud", EventSamples.SkudWith(o => o.Remove("deviceId")), RejectReasons.InvalidSchema },
        { "eventType uppercase", EventSamples.SkudWith(o => o["eventType"] = "Access_Granted"), RejectReasons.InvalidSchema },
        { "sourceEventId 129 chars", EventSamples.SkudWith(o => o["sourceEventId"] = new string('x', 129)), RejectReasons.InvalidSchema },
        { "unknown source", EventSamples.SkudWith(o => o["source"] = "lidar"), RejectReasons.InvalidSchema },
        { "payload not object", EventSamples.SkudWith(o => o["payload"] = "x"), RejectReasons.InvalidSchema },
    };

    [Theory]
    [MemberData(nameof(InvalidCases))]
    public void Decode_InvalidMessage_ReturnsReasonAndDetail(string caseName, byte[] body, string expectedReason)
    {
        var result = _decoder.Decode(body);

        Assert.False(result.IsSuccess, caseName);
        Assert.Equal(expectedReason, result.Reason);
        Assert.False(string.IsNullOrWhiteSpace(result.Detail), caseName);
    }

    [Fact]
    public void Decode_BodyOver64KiB_IsInvalidSchemaWithExplanation()
    {
        var body = EventSamples.SkudWith(o => o["payload"] = new JsonObject { ["blob"] = new string('a', EventDecoder.MaxBodyBytes) });

        var result = _decoder.Decode(body);

        Assert.Equal(RejectReasons.InvalidSchema, result.Reason);
        Assert.Contains("limit", result.Detail);
    }

    [Fact]
    public void Decode_NullCharInPayloadValueOrKey_IsInvalidSchema()
    {
        var inValue = EventSamples.SkudWith(o => o["payload"] = new JsonObject { ["zone"] = "gate\0A" });
        var inKey = EventSamples.SkudWith(o => o["payload"] = new JsonObject { ["zo\0ne"] = "gate" });
        var inNestedArray = EventSamples.SkudWith(o => o["payload"] = new JsonObject { ["tags"] = new JsonArray("ok", "b\0ad") });

        foreach (var body in new[] { inValue, inKey, inNestedArray })
        {
            var result = _decoder.Decode(body);

            Assert.Equal(RejectReasons.InvalidSchema, result.Reason);
            Assert.Contains("U+0000", result.Detail);
        }
    }

    [Fact]
    public void Decode_EscapedBackslashBeforeU0000_IsNotNullChar()
    {
        // Текст \\u0000 в JSON - это литерал "\u0000" из шести символов, а не символ U+0000.
        var body = EventSamples.Bytes(EventSamples.Skud.Replace("main-gate", "a\\\\u0000b"));

        Assert.True(_decoder.Decode(body).IsSuccess);
    }

    [Fact]
    public void Decode_CorrectionReferencingOwnKey_IsInvalidSchema()
    {
        var body = EventSamples.SkudWith(o => o["correction"] = new JsonObject
        {
            ["correctsSourceEventId"] = "48213377",
            ["correctsOccurredAt"] = "2026-11-19T04:58:12.0000009Z", // после усечения совпадает с occurredAt
        });

        var result = _decoder.Decode(body);

        Assert.Equal(RejectReasons.InvalidSchema, result.Reason);
        Assert.Contains("own key", result.Detail);
    }

    [Fact]
    public void Decode_CorrectionOfSameIdAtDifferentTime_IsAccepted()
    {
        var body = EventSamples.SkudWith(o => o["correction"] = new JsonObject
        {
            ["correctsSourceEventId"] = "48213377",
            ["correctsOccurredAt"] = "2026-11-19T04:58:11Z",
        });

        Assert.True(_decoder.Decode(body).IsSuccess);
    }

    [Fact]
    public void Decode_SubMicrosecondTails_TruncateToSameValueAndEqualKeys()
    {
        var a = _decoder.Decode(EventSamples.SkudWith(o => o["occurredAt"] = "2026-11-19T04:58:12.1234567Z")).Event!;
        var b = _decoder.Decode(EventSamples.SkudWith(o => o["occurredAt"] = "2026-11-19T04:58:12.1234569Z")).Event!;
        var nanos = _decoder.Decode(EventSamples.SkudWith(o => o["occurredAt"] = "2026-11-19T04:58:12.123456999Z")).Event!;

        Assert.Equal(a.OccurredAt, b.OccurredAt);
        Assert.Equal(a.OccurredAt, nanos.OccurredAt);
        Assert.Equal(a.Key, b.Key);
        Assert.Equal(new DateTimeOffset(2026, 11, 19, 4, 58, 12, TimeSpan.Zero).AddTicks(1234560), a.OccurredAt);
    }

    [Fact]
    public void Decode_ShortFraction_IsPaddedNotShifted()
    {
        var parsed = _decoder.Decode(EventSamples.SkudWith(o => o["occurredAt"] = "2026-11-19T04:58:12.5Z")).Event!;

        Assert.Equal(new DateTimeOffset(2026, 11, 19, 4, 58, 12, 500, TimeSpan.Zero), parsed.OccurredAt);
    }
}

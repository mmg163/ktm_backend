using System.Text;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Model;

namespace Ktm.DataSaver.Tests.Decoding;

/// <summary>
/// Тела, которые PostgreSQL jsonb отвергает целиком (ошибка класса 22 валит всю пачку), должны отбраковываться на разборе.
/// Поведение jsonb подтверждено вручную на PostgreSQL 16.
/// </summary>
public class EventDecoderJsonbCompatibilityTests
{
    private const string OriginalPayload = "\"payload\":{\"direction\":\"in\",\"zone\":\"main-gate\",\"orionEventCode\":28}";

    private readonly EventDecoder _decoder = new();

    private static byte[] WithPayload(string payloadJson)
    {
        Assert.Contains(OriginalPayload, EventSamples.Skud);
        return Encoding.UTF8.GetBytes(EventSamples.Skud.Replace(OriginalPayload, "\"payload\":" + payloadJson));
    }

    public static TheoryData<string, string> UnpairedSurrogates() => new()
    {
        { "high in value", """{"zone":"a\uD800b"}""" },
        { "low alone in value", """{"zone":"\uDC00"}""" },
        { "high at end of value", """{"zone":"abc\uD800"}""" },
        { "reversed pair", """{"zone":"\uDE00\uD83D"}""" },
        { "in property name", """{"zo\uDC00ne":"gate"}""" },
        { "in nested array", """{"tags":["ok",["deep",{"k":["\uD800"]}]]}""" },
        { "in nested property name", """{"a":{"b":{"\uD800":1}}}""" },
    };

    [Theory]
    [MemberData(nameof(UnpairedSurrogates))]
    public void Decode_UnpairedSurrogate_IsInvalidSchemaWithoutEchoingValue(string caseName, string payload)
    {
        var result = _decoder.Decode(WithPayload(payload));

        Assert.Equal(RejectReasons.InvalidSchema, result.Reason);
        Assert.Contains("surrogate", result.Detail);
        Assert.DoesNotContain("D800", result.Detail!, StringComparison.OrdinalIgnoreCase);
        Assert.False(result.IsSuccess, caseName);
    }

    [Fact]
    public void Decode_ValidSurrogatePair_IsAccepted()
    {
        var escapedPair = _decoder.Decode(WithPayload("""{"zone":"gate 😀","tags":["😀"],"😀":1}"""));
        var rawEmoji = _decoder.Decode(WithPayload("{\"zone\":\"gate 😀\",\"😀\":1}"));

        Assert.True(escapedPair.IsSuccess, escapedPair.Detail);
        Assert.True(rawEmoji.IsSuccess, rawEmoji.Detail);
    }

    [Theory]
    [InlineData("1e1000000000")]
    [InlineData("1e131072")]
    [InlineData("-1E+400000")]
    [InlineData("0.5e200000")]
    [InlineData("1e-16384")]
    [InlineData("1e-1000000000")]
    [InlineData("1e99999999999999999999999")]
    [InlineData("0e2000000000")]
    [InlineData("0.0e-2000000000")]
    [InlineData("0e-16384")]
    [InlineData("0.0E-1000000000")]
    public void Decode_NumberOutsideNumericRange_IsInvalidSchema(string number)
    {
        var result = _decoder.Decode(WithPayload($$"""{"values":[1,{"n":{{number}}}]}"""));

        Assert.Equal(RejectReasons.InvalidSchema, result.Reason);
        Assert.Contains("numeric range", result.Detail);
        Assert.DoesNotContain(number, result.Detail!);
    }

    [Theory]
    [InlineData("1e131071")]
    [InlineData("1e-16383")]
    [InlineData("1.5e300")]
    [InlineData("-2.5E-10")]
    [InlineData("0e1073741823")]
    [InlineData("0e-16383")]
    [InlineData("123456789012345678901234567890")]
    public void Decode_NumberInsideNumericRange_IsAccepted(string number)
    {
        var result = _decoder.Decode(WithPayload($$"""{"n":{{number}}}"""));

        Assert.True(result.IsSuccess, result.Detail);
    }

    [Fact]
    public void Decode_FractionWithoutExponent_IsLimitedBy16383Digits()
    {
        static string Fraction(int digits) => "0." + new string('0', digits - 1) + "1";

        var over = _decoder.Decode(WithPayload($$"""{"n":{{Fraction(16384)}}}"""));
        var edge = _decoder.Decode(WithPayload($$"""{"n":{{Fraction(16383)}}}"""));
        var zeroOver = _decoder.Decode(WithPayload($$"""{"n":0.{{new string('0', 16384)}}}"""));
        var zeroEdge = _decoder.Decode(WithPayload($$"""{"n":0.{{new string('0', 16383)}}}"""));

        Assert.Equal(RejectReasons.InvalidSchema, over.Reason);
        Assert.Contains("numeric range", over.Detail);
        Assert.True(edge.IsSuccess, edge.Detail);
        Assert.Equal(RejectReasons.InvalidSchema, zeroOver.Reason);
        Assert.True(zeroEdge.IsSuccess, zeroEdge.Detail);
    }

    [Fact]
    public void Decode_BodyOfExactly64KiB_IsAccepted_AndOneByteMoreIsRejected()
    {
        byte[] BodyWithBlob(int blobLength) => WithPayload($$"""{"blob":"{{new string('a', blobLength)}}"}""");
        var baseLength = BodyWithBlob(0).Length;

        var exact = BodyWithBlob(EventDecoder.MaxBodyBytes - baseLength);
        var over = BodyWithBlob(EventDecoder.MaxBodyBytes - baseLength + 1);

        Assert.Equal(EventDecoder.MaxBodyBytes, exact.Length);
        Assert.True(_decoder.Decode(exact).IsSuccess);
        Assert.Equal(RejectReasons.InvalidSchema, _decoder.Decode(over).Reason);
    }

    [Fact]
    public void Decode_OneCDoWithNonNullDeviceId_IsAccepted()
    {
        var body = EventSamples.Bytes(EventSamples.OneCDo.Replace("\"deviceId\":null", "\"deviceId\":\"terminal-7\""));

        var result = _decoder.Decode(body);

        Assert.True(result.IsSuccess, result.Detail);
        Assert.Equal("terminal-7", result.Event!.DeviceId);
    }

    [Fact]
    public void Decode_DuplicatePropertyNames_AreRejectedAsMalformedJson()
    {
        // jsonb оставляет последнее значение, схема проверила бы другое: неоднозначное тело не принимаем.
        var body = EventSamples.Bytes(EventSamples.Skud.Replace("\"eventType\":\"access_granted\"", "\"eventType\":\"access_granted\",\"eventType\":\"access_denied\""));

        Assert.Equal(RejectReasons.MalformedJson, _decoder.Decode(body).Reason);
    }

    [Fact]
    public void Decode_Utf8Bom_IsRejectedAsMalformedJson()
    {
        byte[] body = [0xEF, 0xBB, 0xBF, .. EventSamples.Bytes(EventSamples.Skud)];

        Assert.Equal(RejectReasons.MalformedJson, _decoder.Decode(body).Reason);
    }

    [Fact]
    public void Decode_DeterministicMutationsOfValidBodies_NeverThrow()
    {
        var random = new Random(20261120);
        byte[][] samples =
        [
            EventSamples.Bytes(EventSamples.Skud),
            EventSamples.Bytes(EventSamples.CvPresence),
            EventSamples.Bytes(EventSamples.CvAbsence),
            EventSamples.Bytes(EventSamples.OneCDo),
            EventSamples.Bytes(EventSamples.SkudCorrection),
            Encoding.UTF8.GetBytes(EventSamples.Skud.Replace("main-gate", "шлюз 😀 \\uD83D\\uDE00")),
        ];
        byte[] interesting = [0x00, 0x22, 0x5C, 0x7B, 0x7D, 0x5B, 0x5D, 0x2C, 0x3A, 0xC3, 0xED, 0xFF, 0x65, 0x2E, 0x2D];

        var decoded = 0;
        foreach (var sample in samples)
        {
            // Все префиксы (в том числе обрыв посреди многобайтного символа и экранирования).
            for (var length = 0; length < sample.Length; length++)
            {
                _decoder.Decode(sample.AsMemory(0, length));
                decoded++;
            }

            // Случайные замены, вставки и удаления байтов.
            for (var i = 0; i < 300; i++)
            {
                var mutated = sample.ToList();
                var position = random.Next(mutated.Count);
                switch (random.Next(3))
                {
                    case 0: mutated[position] = interesting[random.Next(interesting.Length)]; break;
                    case 1: mutated.Insert(position, interesting[random.Next(interesting.Length)]); break;
                    default: mutated.RemoveAt(position); break;
                }

                var result = _decoder.Decode(mutated.ToArray());
                Assert.True(result.IsSuccess || (result.Reason is not null && result.Detail is not null));
                decoded++;
            }
        }

        Assert.True(decoded > 3000);
    }
}

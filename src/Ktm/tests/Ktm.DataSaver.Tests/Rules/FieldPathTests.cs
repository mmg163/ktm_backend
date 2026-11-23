using Ktm.DataSaver.Core.Rules;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class FieldPathTests
{
    private static FieldPath Parse(string text)
    {
        Assert.True(FieldPath.TryParse(text, out var path, out var error), error);
        return path!;
    }

    private static readonly System.Text.Json.JsonElement Message = Json("""
        {"externalPersonId":"tab:1","payload":{"confidence":0.5,"items":[{"code":"a"},{"code":"b"}],"map":{"1":"obj-key"},"flag":null,"text":"x"}}
        """);

    [Theory]
    [InlineData("externalPersonId", "\"tab:1\"")]
    [InlineData("payload.confidence", "0.5")]
    [InlineData("payload.items.1.code", "\"b\"")]
    [InlineData("payload.items.0.code", "\"a\"")]
    [InlineData("payload.map.1", "\"obj-key\"")]
    [InlineData("payload.flag", "null")]
    public void TryGet_FindsValue_ArrayIndexAndNumericObjectKey(string path, string expectedRaw)
    {
        Assert.True(Parse(path).TryGet(Message, out var value));
        Assert.Equal(expectedRaw, value.GetRawText());
    }

    [Theory]
    [InlineData("payload.missing")]
    [InlineData("payload.text.length")]
    [InlineData("payload.items.2.code")]
    [InlineData("payload.items.code")]
    [InlineData("payload.confidence.x")]
    [InlineData("deviceId")]
    public void TryGet_ReturnsFalse_ForMissingPathThroughNonObjectIndexOutOfRangeOrOtherCase(string path)
    {
        Assert.False(Parse(path).TryGet(Message, out _));
    }

    [Theory]
    [InlineData("")]
    [InlineData("a..b")]
    [InlineData("payload.")]
    [InlineData(".payload")]
    [InlineData("payload[0]")]
    [InlineData("payload.*")]
    [InlineData("unknownField")]
    [InlineData("unknownField.payload")]
    [InlineData("PAYLOAD")]
    public void TryParse_RejectsInvalidPaths(string text)
    {
        Assert.False(FieldPath.TryParse(text, out var path, out var error));
        Assert.Null(path);
        Assert.False(string.IsNullOrEmpty(error));
    }

    [Fact]
    public void TryParse_AcceptsEveryEnvelopeFieldAsFirstSegment()
    {
        foreach (var field in FieldPath.EnvelopeFields) Assert.True(FieldPath.TryParse(field, out _, out _));
    }
}

using Ktm.DataSaver.Core.Reprocessing;

namespace Ktm.DataSaver.Tests.Reprocessing;

/// <summary>Валидация запроса повторной обработки (ADR-0006 п. 2.6, 2.7): каждая ошибка называет своё поле.</summary>
public class ReprocessRequestValidatorTests
{
    private static readonly DateTimeOffset T1 = new(2026, 11, 20, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset T2 = T1.AddHours(1);

    public static TheoryData<ReprocessRequest, string[]> Cases => new()
    {
        { new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 200), [] },
        { new ReprocessRequest(ReprocessSource.Rejected, T1, T2, "rule_x", 1), [] },
        { new ReprocessRequest(ReprocessSource.Raw, T1, T2, null, 400), [] },
        { new ReprocessRequest((ReprocessSource)42, null, null, null, 200), ["source"] },
        { new ReprocessRequest(ReprocessSource.Dlq, T2, T1, null, 200), ["from"] },
        { new ReprocessRequest(ReprocessSource.Dlq, T1, T1, null, 200), ["from"] },
        { new ReprocessRequest(ReprocessSource.Dlq, T1.ToOffset(TimeSpan.FromHours(3)), T2, null, 200), ["from"] },
        { new ReprocessRequest(ReprocessSource.Dlq, T1, T2.ToOffset(TimeSpan.FromHours(3)), null, 200), ["to"] },
        { new ReprocessRequest(ReprocessSource.Raw, null, T2, null, 200), ["from"] },
        { new ReprocessRequest(ReprocessSource.Raw, T1, null, null, 200), ["to"] },
        { new ReprocessRequest(ReprocessSource.Raw, null, null, null, 200), ["from", "to"] },
        { new ReprocessRequest(ReprocessSource.Raw, T1, T2, "rule_x", 200), ["reason"] },
        { new ReprocessRequest(ReprocessSource.Dlq, null, null, "  ", 200), ["reason"] },
        { new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 0), ["maxRate"] },
        { new ReprocessRequest(ReprocessSource.Dlq, null, null, null, 401), ["maxRate"] },
        { new ReprocessRequest(ReprocessSource.Raw, null, T1, "x", 500), ["from", "reason", "maxRate"] },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Validate_ReportsEveryInvalidFieldByName(ReprocessRequest request, string[] expectedFields)
    {
        var errors = ReprocessRequestValidator.Validate(request, maxRateLimit: 400);

        Assert.Equal(expectedFields.Order(), errors.Select(e => e.Field).Order());
        Assert.All(errors, e => Assert.False(string.IsNullOrWhiteSpace(e.Message)));
    }

    [Theory]
    [InlineData("malformed_json")]
    [InlineData("write_value_error")]
    public void Reason_ShortCodesAreAccepted_AlsoAtTheLengthLimit(string reason)
    {
        var request = new ReprocessRequest(ReprocessSource.Dlq, null, null, reason, 200);

        Assert.Empty(ReprocessRequestValidator.Validate(request, 400));
        Assert.Empty(ReprocessRequestValidator.Validate(request with { Reason = new string('a', 64) }, 400));
    }

    [Theory]
    [InlineData("SECRET-LONG-VALUE-0123456789012345678901234567890123456789012345678901234567890")]
    [InlineData("a\nb")]
    [InlineData("a\rb")]
    [InlineData("a\tb")]
    [InlineData("a\0b")]
    [InlineData("a\u001bb")]
    [InlineData("a\u2028b")]
    [InlineData("a\u2029b")]
    public void Reason_TooLongOrWithBreakingCharacters_IsRejected_WithoutEchoingTheValue(string reason)
    {
        var request = new ReprocessRequest(ReprocessSource.Dlq, T1, T2, reason, 200);

        var error = Assert.Single(ReprocessRequestValidator.Validate(request, 400));

        Assert.Equal("reason", error.Field);
        Assert.DoesNotContain(reason, error.Message);
    }

    [Fact]
    public void Reason_ForRaw_GivesOnlyTheNotAllowedError()
    {
        var request = new ReprocessRequest(ReprocessSource.Raw, T1, T2, "a\nb", 200);

        var error = Assert.Single(ReprocessRequestValidator.Validate(request, 400));

        Assert.Equal(("reason", "reason is not allowed for source raw."), (error.Field, error.Message));
    }

    [Theory]
    [InlineData("2026-11-20T00:00:00Z", true)]
    [InlineData("2026-11-20T00:00:00.123Z", true)]
    [InlineData("2026-11-20T00:00:00+00:00", false)]
    [InlineData("2026-11-20T03:00:00+03:00", false)]
    [InlineData("2026-11-20T00:00:00", false)]
    [InlineData("yesterday", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void TryParseUtc_AcceptsOnlyRfc3339WithZ(string? value, bool expected)
    {
        var parsed = ReprocessRequestValidator.TryParseUtc(value, out var result);

        Assert.Equal(expected, parsed);
        if (expected)
        {
            Assert.Equal(TimeSpan.Zero, result.Offset);
        }
    }
}

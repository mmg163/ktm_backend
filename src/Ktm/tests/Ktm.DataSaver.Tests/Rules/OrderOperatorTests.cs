using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class OrderOperatorTests
{
    // Поле 0.3 против порога 0.3: граница включается только в <= и >=.
    [Theory]
    [InlineData("<", "0.3", "0.3", false)]
    [InlineData("<", "0.3", "0.29", true)]
    [InlineData("<=", "0.3", "0.3", true)]
    [InlineData("<=", "0.3", "0.31", false)]
    [InlineData(">", "0.3", "0.3", false)]
    [InlineData(">", "0.3", "0.31", true)]
    [InlineData(">=", "0.3", "0.3", true)]
    [InlineData(">=", "0.3", "0.29", false)]
    [InlineData(">=", "1", "1.0", true)] // decimal
    public void Numbers_CompareAsDecimal_BoundaryInclusivePerOperator(string op, string literal, string field, bool expected)
    {
        Assert.Equal(expected, Eval(Cmp(op, "payload.v", literal), Body($$"""{"v":{{field}}}""")));
    }

    [Theory]
    [InlineData("<", "\"2020-01-01T00:00:00Z\"", "\"2019-12-31T23:59:59Z\"", true)]
    [InlineData("<", "\"2020-01-01T00:00:00Z\"", "\"2020-01-01T00:00:00Z\"", false)]
    [InlineData("<=", "\"2020-01-01T00:00:00Z\"", "\"2020-01-01T00:00:00Z\"", true)]
    // тот же момент в другой зоне: 01:00+01:00 = 00:00Z
    [InlineData("<=", "\"2020-01-01T00:00:00Z\"", "\"2020-01-01T01:00:00+01:00\"", true)]
    [InlineData("<", "\"2020-01-01T00:00:00Z\"", "\"2020-01-01T01:00:00+01:00\"", false)]
    [InlineData(">", "\"2020-01-01T00:00:00+03:00\"", "\"2019-12-31T21:00:01Z\"", true)]
    [InlineData(">=", "\"2020-01-01T00:00:00Z\"", "\"2020-01-01T00:00:00.5Z\"", true)]
    public void Dates_CompareByInstant_AcrossZoneOffsets(string op, string literal, string field, bool expected)
    {
        Assert.Equal(expected, Eval(Cmp(op, "payload.dueAt", literal), Body($$"""{"dueAt":{{field}}}""")));
    }

    [Fact]
    public void EnvelopeOccurredAt_IsComparedAsDate()
    {
        // Body(): occurredAt = 2026-01-01T00:00:00Z
        Assert.False(Eval(Cmp("<", "occurredAt", "\"2020-01-01T00:00:00Z\""), Body()));
        Assert.True(Eval(Cmp(">", "occurredAt", "\"2020-01-01T00:00:00Z\""), Body()));
    }

    [Fact]
    public void LiteralOnTheLeft_ReadsMirrored()
    {
        Assert.True(Eval("""{"<":[5,{"var":"payload.v"}]}""", Body("""{"v":7}""")));
        Assert.False(Eval("""{"<":[5,{"var":"payload.v"}]}""", Body("""{"v":5}""")));
        Assert.True(Eval("""{"<=":[5,{"var":"payload.v"}]}""", Body("""{"v":5}""")));
        Assert.True(Eval("""{">":[5,{"var":"payload.v"}]}""", Body("""{"v":4}""")));
        Assert.True(Eval("""{">=":[5,{"var":"payload.v"}]}""", Body("""{"v":5}""")));
    }

    [Theory]
    [InlineData("0.3", "\"0.3\"")] // строка-число не приводится к числу
    [InlineData("0.3", "true")]
    [InlineData("0.3", "null")]
    [InlineData("0.3", "[0.1]")]
    [InlineData("0.3", "1e400")] // вне decimal
    [InlineData("\"2020-01-01T00:00:00Z\"", "5")] // число против даты
    [InlineData("\"2020-01-01T00:00:00Z\"", "\"not a date\"")]
    [InlineData("\"2020-01-01T00:00:00Z\"", "\"2019-01-01\"")] // дата в поле без зоны
    [InlineData("\"2020-01-01T00:00:00Z\"", "null")]
    public void WrongTypeNullOrUnparsableField_GivesFalseForAllOrderOperators(string literal, string field)
    {
        foreach (var op in new[] { "<", "<=", ">", ">=" })
        {
            Assert.False(Eval(Cmp(op, "payload.v", literal), Body($$"""{"v":{{field}}}""")), op);
        }
    }

    [Fact]
    public void MissingField_GivesFalseForAllOrderOperators()
    {
        foreach (var op in new[] { "<", "<=", ">", ">=" })
        {
            Assert.False(Eval(Cmp(op, "payload.v", "1"), Body()), op);
        }
    }

    [Theory]
    [InlineData("\"2026-01-01\"")] // без зоны
    [InlineData("\"2026-01-01T00:00:00\"")] // без зоны
    [InlineData("\"yesterday\"")]
    [InlineData("true")]
    [InlineData("null")]
    [InlineData("1e400")]
    public void InvalidOrderLiteralInRule_IsRuleError(string literal)
    {
        Assert.Contains("condition.<[1]", Assert.Single(Errors(Cmp("<", "payload.v", literal))));
    }

    // --- Диапазон {"<=": [нижняя, var, верхняя]}: обе границы включительно ---

    private const string Range = """{"<=":[0.2,{"var":"payload.v"},0.8]}""";

    [Theory]
    [InlineData("0.2", true)] // равно нижней границе
    [InlineData("0.8", true)] // равно верхней границе
    [InlineData("0.5", true)]
    [InlineData("0.1999999999", false)]
    [InlineData("0.8000000001", false)]
    [InlineData("\"0.5\"", false)]
    [InlineData("null", false)]
    [InlineData("true", false)]
    public void NumberRange_IncludesBothBounds(string field, bool expected)
    {
        Assert.Equal(expected, Eval(Range, Body($$"""{"v":{{field}}}""")));
    }

    [Fact]
    public void Range_MissingField_IsFalse_AndDegenerateRangeMatchesOnlyThatValue()
    {
        Assert.False(Eval(Range, Body()));
        const string point = """{"<=":[5,{"var":"payload.v"},5]}""";
        Assert.True(Eval(point, Body("""{"v":5}""")));
        Assert.False(Eval(point, Body("""{"v":4.99}""")));
        Assert.False(Eval(point, Body("""{"v":5.01}""")));
    }

    [Theory]
    [InlineData("\"2026-01-01T00:00:00Z\"", true)]
    [InlineData("\"2026-01-02T00:00:00Z\"", true)]
    [InlineData("\"2026-01-01T12:00:00Z\"", true)]
    [InlineData("\"2025-12-31T23:59:59Z\"", false)]
    [InlineData("\"2026-01-02T00:00:01Z\"", false)]
    [InlineData("\"2026-01-01T03:00:00+03:00\"", true)] // = нижняя граница в другой зоне
    [InlineData("\"2026-01-02T03:00:00+03:00\"", true)] // = верхняя граница в другой зоне
    [InlineData("\"2026-01-01T02:59:59+03:00\"", false)]
    [InlineData("5", false)]
    public void DateRange_IncludesBothBounds_ByInstant(string field, bool expected)
    {
        const string range = """{"<=":["2026-01-01T00:00:00Z",{"var":"payload.at"},"2026-01-02T00:00:00Z"]}""";
        Assert.Equal(expected, Eval(range, Body($$"""{"at":{{field}}}""")));
    }

    [Theory]
    [InlineData("""{"<=":[0.8,{"var":"payload.v"},0.2]}""", "больше верхней")]
    [InlineData("""{"<=":[{"var":"payload.v"},0.2,0.8]}""", "посередине")]
    [InlineData("""{"<=":[0.2,0.5,{"var":"payload.v"}]}""", "посередине")]
    [InlineData("""{"<=":[0.2,{"var":"payload.v"},"2026-01-01T00:00:00Z"]}""", "одного вида")]
    [InlineData("""{"<=":["2026-01-01",{"var":"payload.v"},"2026-01-02T00:00:00Z"]}""", "RFC 3339")]
    [InlineData("""{"<=":[0.2,{"var":"payload.v"},{"var":"payload.w"}]}""", "посередине")]
    [InlineData("""{"<":[0.2,{"var":"payload.v"},0.8]}""", "только для оператора \"<=\"")]
    [InlineData("""{">":[0.2,{"var":"payload.v"},0.8]}""", "только для оператора \"<=\"")]
    [InlineData("""{">=":[0.2,{"var":"payload.v"},0.8]}""", "только для оператора \"<=\"")]
    [InlineData("""{"<=":[0.2,{"var":"payload.v"},0.8,1]}""", "ровно два")]
    public void InvalidRange_IsRuleError(string condition, string expectedFragment)
    {
        Assert.Contains(expectedFragment, Assert.Single(Errors(condition)));
    }

    [Fact]
    public void Range_IsEquivalentToAndOfGreaterOrEqualAndLessOrEqual_OnValueSet()
    {
        const string and = """{"and":[{">=":[{"var":"payload.v"},0.2]},{"<=":[{"var":"payload.v"},0.8]}]}""";
        foreach (var value in new[] { "0", "0.19", "0.2", "0.5", "0.8", "0.81", "1", "\"0.5\"", "null", "true", "[]" })
        {
            var body = Body($$"""{"v":{{value}}}""");
            Assert.Equal(Eval(and, body), Eval(Range, body));
        }

        Assert.Equal(Eval(and, Body()), Eval(Range, Body()));
    }
}

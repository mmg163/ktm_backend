using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class EqualityOperatorTests
{
    [Theory]
    [InlineData("\"draft\"", "\"draft\"", true)]
    [InlineData("\"draft\"", "\"Draft\"", false)] // регистр значим
    [InlineData("\"draft\"", "5", false)]
    [InlineData("5", "5", true)]
    [InlineData("5", "5.0", true)] // decimal: 1 = 1.0
    [InlineData("5", "5.0000000000000000001", false)]
    [InlineData("5", "\"5\"", false)] // строка не равна числу
    [InlineData("\"5\"", "5", false)]
    [InlineData("true", "true", true)]
    [InlineData("true", "false", false)]
    [InlineData("true", "\"true\"", false)]
    [InlineData("false", "false", true)]
    [InlineData("null", "null", true)]
    [InlineData("null", "\"\"", false)]
    [InlineData("null", "0", false)]
    [InlineData("5", "1e400", false)] // число вне decimal несравнимо
    [InlineData("5", "null", false)]
    public void Equals_IsStrict_AndNotEqualsIsItsExactNegation(string literal, string fieldValue, bool expected)
    {
        var body = Body($$"""{"v":{{fieldValue}}}""");
        Assert.Equal(expected, Eval(Cmp("==", "payload.v", literal), body));
        Assert.Equal(!expected, Eval(Cmp("!=", "payload.v", literal), body));
        Assert.Equal(expected, Eval($$$"""{"!":{"!=":[{"var":"payload.v"},{{{literal}}}]}}""", body));
    }

    [Fact]
    public void MissingField_EqualsIsFalse_NotEqualsIsTrue_AndNullLiteralDoesNotMatchAbsence()
    {
        var body = Body();
        Assert.False(Eval(Cmp("==", "payload.v", "\"a\""), body));
        Assert.True(Eval(Cmp("!=", "payload.v", "\"a\""), body));
        Assert.False(Eval(Cmp("==", "payload.v", "null"), body));
        Assert.True(Eval(Cmp("!=", "payload.v", "null"), body));
    }

    [Fact]
    public void NotEquals_PairMatchesNegatedEquals_OnEveryFieldShape()
    {
        foreach (var raw in new[] { "\"a\"", "1", "true", "null", "{}", "[1]", "1e999" })
        {
            var body = Body($$"""{"v":{{raw}}}""");
            Assert.Equal(
                !Eval(Cmp("==", "payload.v", "\"a\""), body),
                Eval(Cmp("!=", "payload.v", "\"a\""), body));
        }

        var missing = Body();
        Assert.Equal(!Eval(Cmp("==", "payload.v", "1"), missing), Eval(Cmp("!=", "payload.v", "1"), missing));
    }

    [Fact]
    public void LiteralOnTheLeft_IsEquivalentToLiteralOnTheRight()
    {
        var body = Body("""{"v":"x"}""");
        Assert.True(Eval("""{"==":["x",{"var":"payload.v"}]}""", body));
        Assert.False(Eval("""{"==":["y",{"var":"payload.v"}]}""", body));
        Assert.True(Eval("""{"!=":["y",{"var":"payload.v"}]}""", body));
    }

    [Fact]
    public void ArrayAndObjectFieldValues_NeverEqualScalarLiterals()
    {
        Assert.False(Eval(Cmp("==", "payload.v", "1"), Body("""{"v":[1]}""")));
        Assert.False(Eval(Cmp("==", "payload.v", "null"), Body("""{"v":{}}""")));
    }

    [Theory]
    [InlineData("""{"==":["a","b"]}""", "литералы")]
    [InlineData("""{"==":[{"var":"payload.a"},{"var":"payload.b"}]}""", "два поля")]
    [InlineData("""{"==":[{"var":"payload.a"},"x","y"]}""", "ровно два")]
    [InlineData("""{"==":[{"var":"payload.a"}]}""", "ровно два")]
    [InlineData("""{"==":[{"var":["payload.a",0]},"x"]}""", "по умолчанию")]
    [InlineData("""{"==":[{"var":"payload.a"},[1,2]]}""", "массив")]
    [InlineData("""{"==":"x"}""", "массивом")]
    [InlineData("""{"==":[{"var":"payload.a"},1e400]}""", "decimal")]
    [InlineData("""{"!=":[{"var":"payload.a"},{"+":[1,2]}]}""", "операнд должен быть")]
    public void InvalidOperandShape_IsRuleError(string condition, string expectedFragment)
    {
        var error = Assert.Single(Errors(condition));
        Assert.StartsWith("condition", error);
        Assert.Contains(expectedFragment.Replace("литералы", "литерал").Replace("два поля", "var"), error);
    }
}

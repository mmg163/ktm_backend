using System.Text.Json;
using Ktm.DataSaver.Core.Rules;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class ConditionCompilerTests
{
    [Theory]
    [InlineData("""[]""", "condition: узел условия должен быть объектом")]
    [InlineData("\"x\"", "condition: узел условия должен быть объектом")]
    [InlineData("""{}""", "condition: узел должен содержать ровно один оператор, найдено 0")]
    [InlineData("""{"and":[{"==":[{"var":"source"},"cv"]}],"or":[]}""", "condition: узел должен содержать ровно один оператор, найдено 2")]
    [InlineData("""{"in":[{"var":"payload.a"},["x"]]}""", "condition: оператор \"in\" не поддерживается")]
    [InlineData("""{"missing":"payload.a"}""", "condition: оператор \"missing\" не поддерживается")]
    [InlineData("""{"regex":[{"var":"payload.a"},"^x"]}""", "condition: оператор \"regex\" не поддерживается")]
    [InlineData("""{"if":[true,1,2]}""", "condition: оператор \"if\" не поддерживается")]
    [InlineData("""{"all":[{"==":[{"var":"source"},"cv"]}]}""", "condition: оператор \"all\" не поддерживается")]
    [InlineData("""{"and":[{"==":[{"var":"source"},"cv"]},{"in":[1,2]}]}""", "condition.and[1]: оператор \"in\" не поддерживается")]
    [InlineData("""{"or":[{"and":[{"!":{"+":[1,2]}}]}]}""", "condition.or[0].and[0].!: оператор \"+\" не поддерживается")]
    [InlineData("""{"and":{"==":[{"var":"source"},"cv"]}}""", "condition: операнды \"and\" должны быть массивом узлов")]
    [InlineData("""{"!":[{"==":[{"var":"source"},"cv"]},{"==":[{"var":"source"},"skud"]}]}""", "condition: массив оператора \"!\" должен содержать ровно один узел")]
    [InlineData("""{"==":[{"var":"payload.a"}]}""", "condition: оператор \"==\" принимает ровно два операнда, получено 1")]
    [InlineData("""{"==":[{"var":"unknownField"},1]}""", "condition.==[0]: неизвестное поле \"unknownField\"")]
    [InlineData("""{"==":[{"var":"payload..a"},1]}""", "condition.==[0]: пустой сегмент в пути \"payload..a\"")]
    [InlineData("""{"==":[{"var":""},1]}""", "condition.==[0]: путь не может быть пустым")]
    [InlineData("""{"==":[{"var":5},1]}""", "condition.==[0]: путь var должен быть строкой")]
    [InlineData("""{"<":[{"var":"payload.a"},"2026-01-01"]}""", "condition.<[1]: строка не является датой RFC 3339 с обязательной зоной (Z или ±hh:mm)")]
    public void RuleError_HasPathAndReason(string condition, string expected)
    {
        Assert.StartsWith(expected, Assert.Single(Errors(condition)));
    }

    [Fact]
    public void Errors_AreCollectedFromAllBranches_AndNothingThrows()
    {
        var errors = Errors("""{"and":[{"in":[1]},{"==":[1,2]},{"x":1}]}""");
        Assert.Equal(3, errors.Count);
        Assert.StartsWith("condition.and[0]:", errors[0]);
        Assert.StartsWith("condition.and[1]:", errors[1]);
        Assert.StartsWith("condition.and[2]:", errors[2]);
        Assert.DoesNotContain(errors, e => !e.Contains(": "));
    }

    [Theory]
    [InlineData("""{"==":[{"var":"source"},"cv"]}""")]
    [InlineData("""{"!=":[{"var":"source"},"cv"]}""")]
    [InlineData("""{"<":[{"var":"payload.a"},1]}""")]
    [InlineData("""{"<=":[{"var":"payload.a"},1]}""")]
    [InlineData("""{"<=":[0,{"var":"payload.a"},1]}""")]
    [InlineData("""{">":[{"var":"payload.a"},1]}""")]
    [InlineData("""{">=":[{"var":"payload.a"},1]}""")]
    [InlineData("""{"and":[{"==":[{"var":"source"},"cv"]}]}""")]
    [InlineData("""{"or":[{"==":[{"var":"source"},"cv"]}]}""")]
    [InlineData("""{"!":{"==":[{"var":"source"},"cv"]}}""")]
    [InlineData("""{"!":[{"==":[{"var":"source"},"cv"]}]}""")]
    public void ValidCondition_CompilesForEveryOperator(string condition)
    {
        Assert.NotNull(Compile(condition));
    }

    [Theory]
    [InlineData("skud")]
    [InlineData("cv")]
    [InlineData("1c-do")]
    [InlineData(null)]
    public void KnownSources_AreAccepted_EvenForDisabledRule(string? source)
    {
        // Принудительная компиляция: выключенное правило компилируется так же.
        Assert.True(new ConditionCompiler().Compile(Rule("r-1", source, enabled: false)).Succeeded);
    }

    [Fact]
    public void UnknownSource_AndReservedReasonCode_AreRuleErrors()
    {
        var unknown = new ConditionCompiler().Compile(Rule("r-1", "erp"));
        Assert.Equal("source: неизвестный источник \"erp\" (допустимы: skud, cv, 1c-do, *)", Assert.Single(unknown.Errors));
        Assert.Null(unknown.Rule);

        var reserved = new ConditionCompiler().Compile(Rule("r-1", "cv", reason: "invalid_schema"));
        Assert.StartsWith("reason_code:", Assert.Single(reserved.Errors));
    }

    [Fact]
    public void NonObjectCondition_IsRuleError_NotException()
    {
        var rule = Rule("r-1", "cv") with { Condition = default };
        var result = new ConditionCompiler().Compile(rule);
        Assert.False(result.Succeeded);
        Assert.Contains("condition:", Assert.Single(result.Errors));
    }

    [Fact]
    public void Errors_NeverContainMessageValues_OnlyRuleText()
    {
        // При загрузке сообщений нет; тексты строятся из правила, а вычисление не бросает на любых данных.
        var rule = Compile("""{"or":[{"==":[{"var":"payload.a"},"x"]},{"<":[{"var":"payload.b"},1]}]}""");
        foreach (var raw in new[] { "null", "[]", "{}", "\"s\"", "1e999", "true" })
        {
            var ex = Record.Exception(() => rule.Evaluate(Body($$"""{"a":{{raw}},"b":{{raw}}}""")));
            Assert.Null(ex);
        }

        Assert.False(rule.Evaluate(JsonDocument.Parse("[]").RootElement));
    }

    [Theory]
    [InlineData("")]
    [InlineData("bad id")]
    [InlineData("правило")]
    [InlineData("id\n")]
    public void InvalidRuleId_IsRuleError(string id)
    {
        var result = new ConditionCompiler().Compile(Rule(id));
        Assert.Null(result.Rule);
        Assert.StartsWith("rule_id:", Assert.Single(result.Errors));
    }

    [Fact]
    public void RuleId_AtMaxLengthWithAllAllowedSymbols_IsAccepted_AndOverMaxIsRejected()
    {
        Assert.True(new ConditionCompiler().Compile(Rule("A.z_0:9-" + new string('x', 120))).Succeeded);
        Assert.False(new ConditionCompiler().Compile(Rule(new string('x', 129))).Succeeded);
    }

    [Theory]
    [InlineData("Upper")]
    [InlineData("1abc")]
    [InlineData("_abc")]
    [InlineData("a-b")]
    [InlineData("")]
    [InlineData("ok\n")]
    public void InvalidReasonCodeFormat_IsRuleError(string reason)
    {
        var result = new ConditionCompiler().Compile(Rule("r-1", reason: reason));
        Assert.Null(result.Rule);
        Assert.StartsWith("reason_code:", Assert.Single(result.Errors));
    }

    [Fact]
    public void ReasonCode_Boundaries_64CharsAccepted_65Rejected()
    {
        Assert.True(new ConditionCompiler().Compile(Rule("r-1", reason: "a" + new string('9', 63))).Succeeded);
        Assert.False(new ConditionCompiler().Compile(Rule("r-1", reason: "a" + new string('9', 64))).Succeeded);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void EmptyName_IsRuleError(string name)
    {
        var result = new ConditionCompiler().Compile(Rule("r-1", name: name));
        Assert.Null(result.Rule);
        Assert.StartsWith("name:", Assert.Single(result.Errors));
    }

    [Fact]
    public void Name_Boundaries_200CharsAccepted_201Rejected()
    {
        Assert.True(new ConditionCompiler().Compile(Rule("r-1", name: new string('н', 200))).Succeeded);
        var tooLong = new ConditionCompiler().Compile(Rule("r-1", name: new string('н', 201)));
        Assert.StartsWith("name:", Assert.Single(tooLong.Errors));
    }

    [Fact]
    public void StarSource_IsRuleError_BecauseDbStarIsMappedToNullByTheAdapter()
    {
        var result = new ConditionCompiler().Compile(Rule("r-1", "*"));
        Assert.StartsWith("source:", Assert.Single(result.Errors));
    }
}

using System.Text.Json;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class LogicalNodesTests
{
    private const string Low = """{"<":[{"var":"payload.confidence"},0.5]}""";
    private const string IsCv = """{"==":[{"var":"source"},"cv"]}""";

    private static string And(params string[] nodes) => $$"""{"and":[{{string.Join(",", nodes)}}]}""";

    private static string Or(params string[] nodes) => $$"""{"or":[{{string.Join(",", nodes)}}]}""";

    [Fact]
    public void And_IsTrueOnlyWhenAllOperandsAreTrue()
    {
        var cv = Body("""{"confidence":0.1}""", "cv");
        Assert.True(Eval(And(Low, IsCv), cv));
        Assert.False(Eval(And(Low, IsCv), Body("""{"confidence":0.1}""")));
        Assert.False(Eval(And(Low, IsCv), Body("""{"confidence":0.9}""", "cv")));
    }

    [Fact]
    public void Or_IsTrueWhenAtLeastOneOperandIsTrue_AndFalseWhenAllAreFalse()
    {
        Assert.True(Eval(Or(Low, IsCv), Body("""{"confidence":0.9}""", "cv")));
        Assert.True(Eval(Or(Low, IsCv), Body("""{"confidence":0.1}""")));
        Assert.False(Eval(Or(Low, IsCv), Body("""{"confidence":0.9}""")));
    }

    [Theory]
    [InlineData("""{"and":[]}""")]
    [InlineData("""{"or":[]}""")]
    public void EmptyAndOrArray_IsRejected(string condition)
    {
        Assert.Contains("не может быть пустым", Assert.Single(Errors(condition)));
    }

    [Fact]
    public void Not_InvertsResult_InBothForms_AndTreatsMissingFieldAsFalseLeaf()
    {
        var present = Body("""{"confidence":0.9}""");
        var missing = Body();
        var plain = $$"""{"!":{{Low}}}""";
        var array = $$"""{"!":[{{Low}}]}""";

        Assert.True(Eval(plain, present));
        Assert.True(Eval(array, present));
        Assert.False(Eval(plain, Body("""{"confidence":0.1}""")));
        Assert.False(Eval(array, Body("""{"confidence":0.1}""")));

        // Отсутствующее поле: x < 0.5 ложно, а !(x >= 0.5) истинно (ADR-0005 п. 2.4).
        Assert.False(Eval(Low, missing));
        Assert.True(Eval("""{"!":{">=":[{"var":"payload.confidence"},0.5]}}""", missing));
    }

    [Fact]
    public void And_ShortCircuitsAtFirstFalse_NoDependenceOnLaterOperands()
    {
        // Результат булев и не зависит от порядка: ложный операнд решает всё.
        Assert.False(Eval(And(IsCv, Low), Body("""{"confidence":0.1}""")));
        Assert.True(Eval(Or(Low, IsCv), Body("""{"confidence":0.1}""")));
    }

    [Fact]
    public void ThreeLevelNesting_EvaluatesAsBooleanExpression()
    {
        // or(low, and(cv, !(payload.confidence > 0.99)))
        var condition = Or(Low, And(IsCv, """{"!":{">":[{"var":"payload.confidence"},0.99]}}"""));
        Assert.True(Eval(condition, Body("""{"confidence":0.7}""", "cv")));
        Assert.False(Eval(condition, Body("""{"confidence":1}""", "cv")));
        Assert.False(Eval(condition, Body("""{"confidence":0.7}""")));
        Assert.True(Eval(condition, Body("""{"confidence":0.2}""")));
    }

    [Fact]
    public void Depth8IsAccepted_Depth9IsRejected()
    {
        string Nest(int depth) => depth == 1 ? Low : $$"""{"!":{{Nest(depth - 1)}}}""";

        Compile(Nest(8));
        Assert.Contains("глубина", Assert.Single(Errors(Nest(9))));
    }

    [Fact]
    public void Nodes64AreAccepted_65AreRejected()
    {
        // and + 63 листьев = 64 узла; 64 листа = 65 узлов.
        string Flat(int leaves) => And(Enumerable.Repeat(IsCv, leaves).ToArray());

        Compile(Flat(63));
        Assert.Contains("узлов", Assert.Single(Errors(Flat(64))));
    }

    [Fact]
    public void TraceFired_ListsLeavesBehindTheMatch_WithoutMessageValues()
    {
        var rule = Compile(Or(Low, IsCv));
        var body = Body("""{"confidence":0.1}""", "cv");
        var fired = new List<string>();
        rule.CollectFired(body, fired);
        Assert.Equal(["payload.confidence <", "source =="], fired);

        var negated = new List<string>();
        Compile($$"""{"!":{{Low}}}""").CollectFired(Body("""{"confidence":0.9}"""), negated);
        Assert.Equal(["!payload.confidence <"], negated);
    }

    private static List<string> Trace(string condition, JsonElement body)
    {
        var node = Compile(condition);
        Assert.True(node.Evaluate(body));
        var sink = new List<string>();
        node.CollectFired(body, sink);
        return sink;
    }

    [Fact]
    public void Trace_UnderNot_ListsOnlyLeavesThatWereFalse()
    {
        const string Zone = """{"==":[{"var":"payload.zone"},"a"]}""";

        // !(and(low, zone)): истинно из-за zone (low истинен и в причину не входит).
        Assert.Equal(["!payload.zone =="], Trace($$"""{"!":{{And(Low, Zone)}}}""", Body("""{"confidence":0.1,"zone":"b"}""")));

        // !(or(low, zone)): ложны оба операнда.
        Assert.Equal(
            ["!payload.confidence <", "!payload.zone =="],
            Trace($$"""{"!":{{Or(Low, Zone)}}}""", Body("""{"confidence":0.9,"zone":"b"}""")));

        // or(!(and(low, zone)), cv): сработал только cv, лист под ложным NOT не попадает в трассу.
        Assert.Equal(["source =="], Trace(Or($$"""{"!":{{And(Low, Zone)}}}""", IsCv), Body("""{"confidence":0.1,"zone":"a"}""", "cv")));

        // !(!(low)): двойное отрицание, лист сработавшего вложенного узла.
        Assert.Equal(["!!payload.confidence <"], Trace("{\"!\":{\"!\":" + Low + "}}", Body("""{"confidence":0.1}""")));
    }
}

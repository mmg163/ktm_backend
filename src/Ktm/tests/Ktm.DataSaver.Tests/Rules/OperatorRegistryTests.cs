using System.Text.Json;
using Ktm.DataSaver.Application.Rules;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class OperatorRegistryTests
{
    /// <summary>Тестовый оператор «пусто»: <c>{"empty": {"var": "путь"}}</c> - поля нет, null либо пустая строка.</summary>
    private sealed class EmptyOperator : IRuleOperator
    {
        public string Key => "empty";

        public string TraceName => "empty";

        public IPredicate? Compile(JsonElement operands, string path, List<string> errors)
        {
            var kind = OperandParser.Classify(operands, path, errors, out var field);
            return kind == OperandKind.Var ? new Predicate(field!) : null;
        }

        private sealed class Predicate(FieldPath field) : IPredicate
        {
            public bool Evaluate(JsonElement body) =>
                !field.TryGet(body, out var v) || v.ValueKind == JsonValueKind.Null
                || (v.ValueKind == JsonValueKind.String && v.GetString()!.Length == 0);

            public string Describe() => field.Text + " empty";
        }
    }

    [Fact]
    public void NewOperator_WorksInsideAndOrNot_WithoutChangingNodesOrCompiler()
    {
        var registry = OperatorRegistry.CreateDefault();
        const string condition = """{"or":[{"empty":{"var":"externalPersonId"}},{"and":[{"!":{"empty":{"var":"payload.a"}}},{"==":[{"var":"source"},"cv"]}]}]}""";

        // До регистрации: оператор неизвестен.
        Assert.Contains("оператор \"empty\" не поддерживается", Errors(condition, registry)[0]);

        registry.Register(new EmptyOperator());
        var rule = Compile(condition, registry);

        Assert.True(rule.Evaluate(Json("""{"externalPersonId":""}""")));
        Assert.False(rule.Evaluate(Json("""{"externalPersonId":"tab:1","source":"cv"}""")));
        Assert.True(rule.Evaluate(Json("""{"externalPersonId":"tab:1","source":"cv","payload":{"a":"x"}}""")));
        Assert.False(rule.Evaluate(Json("""{"externalPersonId":"tab:1","source":"skud","payload":{"a":"x"}}""")));

        var fired = new List<string>();
        rule.CollectFired(Json("""{"externalPersonId":""}"""), fired);
        Assert.Equal(["externalPersonId empty"], fired);
    }

    [Fact]
    public void Register_RejectsDuplicateKeys_AndKeysOfLogicalNodes()
    {
        var registry = OperatorRegistry.CreateDefault();
        registry.Register(new EmptyOperator());
        Assert.Throws<ArgumentException>(() => registry.Register(new EmptyOperator()));
        Assert.Throws<ArgumentException>(() => registry.Register(new Named("and")));
        Assert.Throws<ArgumentException>(() => registry.Register(new Named("==")));
    }

    [Theory]
    [InlineData("==")]
    [InlineData("!=")]
    [InlineData("<")]
    [InlineData("<=")]
    [InlineData(">")]
    [InlineData(">=")]
    [InlineData("regex")]
    public void DefaultRegistry_HasTheFirstIterationOperatorsAndRegex(string key)
    {
        Assert.True(OperatorRegistry.CreateDefault().TryGet(key, out _));
    }

    [Theory]
    [InlineData("in")]
    [InlineData("missing")]
    [InlineData("iregex")]
    [InlineData("if")]
    [InlineData("+")]
    [InlineData("and")]
    public void DefaultRegistry_DoesNotKnowDeferredOperators(string key)
    {
        Assert.False(OperatorRegistry.CreateDefault().TryGet(key, out _));
    }

    private sealed class Named(string key) : IRuleOperator
    {
        public string Key => key;

        public string TraceName => key;

        public IPredicate? Compile(JsonElement operands, string path, List<string> errors) => null;
    }
}

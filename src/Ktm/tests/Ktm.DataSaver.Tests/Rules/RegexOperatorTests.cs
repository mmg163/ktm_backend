using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Model;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.Tests.Decoding;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

/// <summary>Оператор <c>regex</c> (ADR-0005 п. 2.7, tasks 9.5): вычисление, загрузка, лимиты, безопасность и таймаут совпадения.</summary>
public class RegexOperatorTests
{
    private const string ValueMark = "SECRETVALUE";
    private const string PatternMark = "SECRETPATTERN";

    /// <summary>Лист <c>regex</c>; шаблон сериализуется JSON-ом, чтобы обратные косые и кавычки были корректны.</summary>
    private static string Rx(string path, string pattern) =>
        $$"""{"regex":[{"var":"{{path}}"},{{JsonSerializer.Serialize(pattern)}}]}""";

    private static JsonElement PayloadS(string sJson) => Body($$"""{"s":{{sJson}}}""");

    private static string Str(string value) => JsonSerializer.Serialize(value);

    // --- вычисление ---

    [Theory]
    [InlineData("TEST", "MY-TEST-1", true)]
    [InlineData("^TEST-", "MY-TEST-1", false)]
    [InlineData("^TEST-\\d+\\z", "TEST-42", true)]
    [InlineData("^TEST-\\d+\\z", "MY-TEST-42", false)]
    [InlineData("^TEST-\\d+\\z", "TEST-42x", false)]
    [InlineData("^TEST-42$", "TEST-42\n", true)]
    [InlineData("^TEST-42\\z", "TEST-42\n", false)]
    [InlineData("test", "TEST", false)]
    [InlineData("(?i)test", "TEST", true)]
    [InlineData("(?i)^тест-\\d+$", "ТЕСТ-1", true)]
    [InlineData("^Тест", "Тест-1", true)]
    [InlineData("a.b", "a\nb", false)]
    [InlineData("(?s)a.b", "a\nb", true)]
    [InlineData("^$", "", true)]
    [InlineData("^x", "", false)]
    public void Evaluate_StringField_PartialMatchAnchorsFlagsAndUnicode(string pattern, string value, bool expected)
    {
        Assert.Equal(expected, Eval(Rx("payload.s", pattern), PayloadS(Str(value))));
    }

    [Fact]
    public void Evaluate_CaseInsensitive_DoesNotDependOnTheProcessCulture_TurkishAndInvariantAgree()
    {
        // Под tr-TR без CultureInvariant: i<->İ и I<->ı, а i<->I не равны. Инвариантно: i<->I равны, İ и ı не равны ни i, ни I.
        (string Pattern, string Value, bool Expected)[] cases =
        [
            ("(?i)^i$", "I", true),
            ("(?i)^I$", "i", true),
            ("(?i)^i$", "İ", false),
            ("(?i)^ı$", "I", false),
            ("(?i)^title$", "TITLE", true),
        ];

        // Культура потока локальна для него, тест синхронный: на другие тесты не влияет; восстановление - в finally.
        var savedCulture = CultureInfo.CurrentCulture;
        var savedUiCulture = CultureInfo.CurrentUICulture;
        try
        {
            foreach (var name in new[] { "tr-TR", "en-US", "az-Latn-AZ" })
            {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
                CultureInfo.CurrentUICulture = CultureInfo.GetCultureInfo(name);

                foreach (var (pattern, value, expected) in cases)
                {
                    Assert.True(
                        expected == Eval(Rx("payload.s", pattern), PayloadS(Str(value))),
                        $"culture {name}: {pattern} vs {value}, expected {expected}");
                }
            }
        }
        finally
        {
            CultureInfo.CurrentCulture = savedCulture;
            CultureInfo.CurrentUICulture = savedUiCulture;
        }
    }

    [Theory]
    [InlineData("42")]
    [InlineData("null")]
    [InlineData("true")]
    [InlineData("false")]
    [InlineData("""{"a":"x"}""")]
    [InlineData("""["x"]""")]
    public void Evaluate_NonStringField_IsFalseWithoutCoercion_EvenForPatternThatMatchesAnything(string value)
    {
        Assert.False(Eval(Rx("payload.s", "^"), PayloadS(value)));
        Assert.False(Eval(Rx("payload.s", "4"), PayloadS(value)));
    }

    [Fact]
    public void Evaluate_MissingField_IsFalse_AndThePathCanGoThroughPayloadAndArrayIndexes()
    {
        Assert.False(Eval(Rx("payload.nope", "^"), Body("{}")));
        Assert.True(Eval(Rx("payload.items.1.code", "^B-"), Body("""{"items":[{"code":"A-1"},{"code":"B-2"}]}""")));
        Assert.False(Eval(Rx("payload.items.0.code", "^B-"), Body("""{"items":[{"code":"A-1"},{"code":"B-2"}]}""")));
    }

    [Fact]
    public void Evaluate_InsideLogicalNodes_NotOnMissingFieldIsTrue_AndCaretIdiomMeansPresentAndString()
    {
        var present = PayloadS(Str("TEST-1"));
        var missing = Body("{}");
        var number = PayloadS("7");
        var presentAndNotProd = And(Rx("payload.s", "^"), Not(Rx("payload.s", "^PROD-")));

        // !regex на отсутствующем поле истинно (ловушка), идиома ^ - «поле есть и строка»
        Assert.True(Eval(Not(Rx("payload.s", "^TEST-")), missing));
        Assert.True(Eval(presentAndNotProd, present));
        Assert.False(Eval(presentAndNotProd, missing));
        Assert.False(Eval(presentAndNotProd, number));
        Assert.True(Eval(Or(Rx("payload.s", "^PROD-"), And("""{"==":[{"var":"source"},"skud"]}""", Rx("payload.s", "-1$"))), present));
        Assert.False(Eval(Or(Rx("payload.s", "^PROD-"), Not(Or(Rx("payload.s", "TEST")))), present));
    }

    private static string Not(string node) => "{\"!\":" + node + "}";

    private static string And(params string[] nodes) => "{\"and\":[" + string.Join(",", nodes) + "]}";

    private static string Or(params string[] nodes) => "{\"or\":[" + string.Join(",", nodes) + "]}";

    [Fact]
    public void Trace_ListsPathAndOperationOnly_NeverTheFieldValueOrThePattern()
    {
        var body = PayloadS(Str(ValueMark));
        var plain = Compile(Rx("payload.s", $"^{PatternMark}|{ValueMark}"));
        var negated = Compile(Not(Rx("payload.s", $"^{PatternMark}")));

        var fired = new List<string>();
        plain.CollectFired(body, fired);
        negated.CollectFired(body, fired);

        Assert.Equal(["payload.s regex", "!payload.s regex"], fired);
        Assert.DoesNotContain(fired, t => t.Contains(ValueMark, StringComparison.Ordinal) || t.Contains(PatternMark, StringComparison.Ordinal));
    }

    // --- загрузка ---

    [Theory]
    [InlineData("""{"regex":"^a"}""", "condition: операнды оператора \"regex\" должны быть массивом")]
    [InlineData("""{"regex":[{"var":"deviceId"}]}""", "condition: оператор \"regex\" принимает ровно два операнда, получено 1")]
    [InlineData("""{"regex":[{"var":"deviceId"},"a","b"]}""", "condition: оператор \"regex\" принимает ровно два операнда, получено 3")]
    [InlineData("""{"regex":["^a",{"var":"deviceId"}]}""", "condition.regex[0]: первым операндом должен быть")]
    [InlineData("""{"regex":["^a","^b"]}""", "condition.regex[0]: первым операндом должен быть")]
    [InlineData("""{"regex":[{"var":"deviceId"},{"var":"payload.a"}]}""", "condition.regex[1]: шаблон должен быть строковым литералом")]
    [InlineData("""{"regex":[{"var":"deviceId"},5]}""", "condition.regex[1]: шаблон должен быть строкой")]
    [InlineData("""{"regex":[{"var":"deviceId"},null]}""", "condition.regex[1]: шаблон должен быть строкой")]
    [InlineData("""{"regex":[{"var":"deviceId"},true]}""", "condition.regex[1]: шаблон должен быть строкой")]
    [InlineData("""{"regex":[{"var":"deviceId"},["^a"]]}""", "condition.regex[1]: шаблон должен быть строкой")]
    [InlineData("""{"regex":[{"var":"deviceId"},""]}""", "condition.regex[1]: шаблон не может быть пустым")]
    [InlineData("""{"regex":[{"var":"unknownField"},"^a"]}""", "condition.regex[0]: неизвестное поле")]
    [InlineData("""{"regex":[{"var":["deviceId",1]},"^a"]}""", "condition.regex[0]: var со значением по умолчанию")]
    public void Operands_EachShapeError_HasItsOwnPathAndReason(string condition, string expected)
    {
        Assert.Contains(Errors(condition), e => e.StartsWith(expected, StringComparison.Ordinal));
    }

    [Fact]
    public void PatternLength_512IsAccepted_513IsRejected()
    {
        Compile(Rx("deviceId", new string('a', RegexPolicy.MaxPatternLength)));

        var error = Assert.Single(Errors(Rx("deviceId", new string('a', RegexPolicy.MaxPatternLength + 1))));

        Assert.Equal("condition.regex[1]: длина шаблона превышает 512 символов", error);
    }

    [Theory]
    [InlineData($"({PatternMark}")]
    [InlineData($"[z-a{PatternMark}]")]
    [InlineData($"(?={PatternMark})")]
    [InlineData($"(?<={PatternMark})b")]
    [InlineData($"({PatternMark})\\1")]
    [InlineData($"(?>{PatternMark}+)")]
    [InlineData($"(?({PatternMark})b|c)")]
    [InlineData($"\\G{PatternMark}")]
    [InlineData("(a{1000}){1000}")]
    public void InvalidOrUnsupportedPattern_IsRuleError_WithoutThrowing_PatternOrExceptionMessage(string pattern)
    {
        var condition = Rx("deviceId", pattern);
        var thrown = Assert.ThrowsAny<Exception>(() => new Regex(pattern, RegexOptions.NonBacktracking | RegexOptions.CultureInvariant));

        var result = new ConditionCompiler().Compile(Json(condition));

        Assert.False(result.Succeeded);
        var error = Assert.Single(result.Errors);
        Assert.StartsWith("condition.regex[1]: ", error);
        Assert.DoesNotContain(PatternMark, error);
        Assert.DoesNotContain(thrown.Message, error);
        Assert.DoesNotContain(pattern, error);
    }

    [Fact]
    public void RegexNodesInRule_EightAreAccepted_NinthIsRejectedOnce_AndTheCountIsKept()
    {
        string Or(int n) => "{\"or\":[" + string.Join(",", Enumerable.Range(0, n).Select(i => Rx("deviceId", $"^{i}"))) + "]}";
        var compiler = new ConditionCompiler();

        var eight = compiler.Compile(Rule("r-8", condition: Or(8)));
        var nine = compiler.Compile(Rule("r-9", condition: Or(10)));

        Assert.True(eight.Succeeded);
        Assert.Equal(8, eight.Rule!.RegexNodeCount);
        Assert.Equal("condition.or[8]: число узлов regex в правиле превышает 8", Assert.Single(nine.Errors));
    }

    [Theory]
    [InlineData("malformed_json")]
    [InlineData("unknown_schema_version")]
    [InlineData("invalid_schema")]
    [InlineData("rule_eval_timeout")]
    public void SystemReasonCodes_AreReserved_ForRules(string reason)
    {
        var result = new ConditionCompiler().Compile(Rule("r-1", reason: reason));

        var error = Assert.Single(result.Errors);
        Assert.StartsWith("reason_code: код причины зарезервирован", error);
    }

    // --- набор ---

    private static RuleDefinition RegexRule(string id, int nodes, int priority = 100, bool enabled = true, string pattern = "^a") =>
        Rule(id, priority: priority, enabled: enabled,
            condition: nodes == 0
                ? AnyCondition
                : "{\"or\":[" + string.Join(",", Enumerable.Range(0, nodes).Select(_ => Rx("deviceId", pattern))) + "]}");

    [Fact]
    public void Builder_MaxRegexNodes_RejectsTheRuleOnWhichItIsExhausted_AndKeepsOthersInPriorityThenIdOrder()
    {
        var builder = new RuleSnapshotBuilder(new ConditionCompiler(), maxRegexNodes: 4);

        var snapshot = builder.Build("rev", [
            RegexRule("b-two", 2, priority: 10),
            RegexRule("a-three", 3, priority: 10),
            RegexRule("c-one", 1, priority: 20),
            RegexRule("d-plain", 0, priority: 30),
            RegexRule("e-one", 1, priority: 40),
        ], []);

        // a-three (3) помещается, b-two (3+2 > 4) нет, c-one (3+1 = 4) помещается, e-one (4+1) нет; без regex - без ограничений
        Assert.Equal(["a-three", "c-one", "d-plain"], snapshot.Rules.Select(r => r.Definition.Id));
        Assert.Equal(["b-two", "e-one"], snapshot.Errors.Select(e => e.RuleId));
        Assert.All(snapshot.Errors, e => Assert.Contains("MaxRegexNodes=4", e.Reason));
    }

    [Fact]
    public void Builder_DisabledRuleWithBrokenPattern_IsNotCompiledNorCounted_AndBrokenEnabledOneDoesNotSpendTheBudget()
    {
        var builder = new RuleSnapshotBuilder(new ConditionCompiler(), maxRegexNodes: 2);

        var snapshot = builder.Build("rev", [
            RegexRule("disabled-bad", 1, priority: 1, enabled: false, pattern: "("),
            RegexRule("enabled-bad", 1, priority: 2, pattern: "("),
            RegexRule("good", 2, priority: 3),
        ], []);

        Assert.Equal(["good"], snapshot.Rules.Select(r => r.Definition.Id));
        var error = Assert.Single(snapshot.Errors);
        Assert.Equal("enabled-bad", error.RuleId);
        Assert.StartsWith("condition.or[0].regex[1]: ", error.Reason);
    }

    // --- безопасность и таймаут ---

    [Fact]
    public void CatastrophicPattern_OnTheDefaultEngine_FinishesFastAndIsNotAMatch()
    {
        var body = PayloadS(Str(new string('a', 40) + "b"));
        var plain = Compile(Rx("payload.s", "^(a+)+$"));
        var negated = Compile(Not(Rx("payload.s", "^(a+)+$")));

        var watch = Stopwatch.StartNew();
        var results = (plain.Evaluate(body), negated.Evaluate(body));
        watch.Stop();

        Assert.Equal((false, true), results);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(1), $"took {watch.Elapsed}");
    }

    private static readonly TimeSpan SeamTimeout = TimeSpan.FromMilliseconds(20);

    /// <summary>Откатный движок и 20 мс: шов, на котором таймаут воспроизводится детерминированно (NonBacktracking по таймауту не падает).</summary>
    private static OperatorRegistry BacktrackingRegistry() => OperatorRegistry.CreateDefault(
        new RegexPolicy(SeamTimeout), (pattern, timeout) => new Regex(pattern, RegexOptions.CultureInvariant, timeout));

    private static readonly string Catastrophic = ValueMark + new string('A', 32) + "!";

    private const string CatastrophicPattern = $"^(\\w+)+$(?#{PatternMark})";

    [Fact]
    public void RegexTimeout_AbortsEvaluation_InsteadOfReturningFalse_WithTheLimitFromTheEngine()
    {
        var node = Compile(Rx("payload.s", CatastrophicPattern), BacktrackingRegistry());

        var ex = Assert.Throws<RuleEvaluationAbortedException>(() => node.Evaluate(PayloadS(Str(Catastrophic))));

        Assert.Equal(RuleEvaluationAbortedException.RegexTimeout, ex.Kind);
        Assert.Equal(SeamTimeout, ex.Limit);
        Assert.DoesNotContain(ValueMark, ex.Message);
    }

    private static RuleSnapshotHolder HolderWith(OperatorRegistry registry, params RuleDefinition[] rules)
    {
        var holder = new RuleSnapshotHolder();
        holder.Replace(new RuleSnapshotBuilder(new ConditionCompiler(registry)).Build("rev", rules, []), DateTimeOffset.UnixEpoch);
        return holder;
    }

    private static byte[] SkudBytes(string s) =>
        EventSamples.SkudWith(node => node["payload"]!.AsObject()["s"] = s);

    private static IngestionVerdict Run(RuleSnapshotHolder holder, byte[] bytes)
    {
        var parsed = new EventDecoder().Decode(bytes).Event!;
        return new RuleRejectionStage(holder).Evaluate(EventSamples.Message(bytes), parsed);
    }

    private static string Timed(string name) => $"Вычисление правила «{name}» ";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Stage_RegexTimeout_RejectsWithSystemCode_ForPlainAndNegatedForm_WithoutValuesInDetail(bool negated)
    {
        var leaf = Rx("payload.s", CatastrophicPattern);
        var condition = negated ? Not(leaf) : leaf;
        var holder = HolderWith(BacktrackingRegistry(), Rule("rx-rule", condition: condition, name: "Проверка s", version: 7, reason: "admin_reason"));

        var rejection = Run(holder, SkudBytes(Catastrophic)).Rejection!;

        Assert.Equal(RejectionKind.Rule, rejection.Kind);
        Assert.Equal(RejectReasons.RuleEvalTimeout, rejection.Reason);
        Assert.Equal(("rx-rule", "Проверка s", 7), (rejection.RuleId, rejection.RuleName, rejection.RuleVersion));
        Assert.Equal(
            "Вычисление правила «Проверка s» (rx-rule, v7) прервано: превышен предел совпадения регулярного выражения (20 мс)",
            rejection.Detail);
        Assert.DoesNotContain(ValueMark, rejection.Detail);
        Assert.DoesNotContain(PatternMark, rejection.Detail);
    }

    private sealed class CountingOperator : IRuleOperator
    {
        public int Calls { get; private set; }

        public string Key => "count";

        public string TraceName => "count";

        public IPredicate? Compile(JsonElement operands, string path, List<string> errors) => new Predicate(this);

        private sealed class Predicate(CountingOperator owner) : IPredicate
        {
            public bool Evaluate(JsonElement body)
            {
                owner.Calls++;
                return false;
            }

            public string Describe() => "count";
        }
    }

    [Fact]
    public void Stage_AfterTheTimeoutNoFurtherRuleIsEvaluated_AndTheIdentityIsThatOfTheTimedOutRule()
    {
        var counter = new CountingOperator();
        var registry = BacktrackingRegistry();
        registry.Register(counter);
        var holder = HolderWith(
            registry,
            Rule("before", priority: 1, condition: """{"count":1}"""),
            Rule("slow", priority: 2, condition: Rx("payload.s", CatastrophicPattern)),
            Rule("after", priority: 3, condition: """{"count":1}"""));

        var rejection = Run(holder, SkudBytes(Catastrophic)).Rejection!;

        Assert.Equal(("slow", RejectReasons.RuleEvalTimeout), (rejection.RuleId, rejection.Reason));
        Assert.Equal(1, counter.Calls);
    }

    [Fact]
    public void Stage_EarlierRuleThatFiresNormally_WinsOverALaterRuleThatWouldTimeOut()
    {
        var holder = HolderWith(
            BacktrackingRegistry(),
            Rule("first", priority: 1, reason: "first_reason", condition: """{"==":[{"var":"payload.zone"},"main-gate"]}"""),
            Rule("slow", priority: 2, condition: Rx("payload.s", CatastrophicPattern)));

        var rejection = Run(holder, SkudBytes(Catastrophic)).Rejection!;

        Assert.Equal(("first", "first_reason"), (rejection.RuleId, rejection.Reason));
    }

    [Fact]
    public void Stage_RegexThatMatchesWithoutTimeout_RejectsWithTheRulesOwnReasonAndTheLeafInDetail()
    {
        var holder = HolderWith(OperatorRegistry.CreateDefault(), Rule("device", condition: Rx("deviceId", "^turnstile-"), reason: "test_device", name: "Тест"));

        var rejection = Run(holder, EventSamples.Bytes(EventSamples.Skud)).Rejection!;

        Assert.Equal("test_device", rejection.Reason);
        Assert.Equal("Сработало правило «Тест» (device, v1): deviceId regex", rejection.Detail);
    }

    private sealed class ThrowingAfterFirstOperator(Exception failure) : IRuleOperator
    {
        private readonly Exception _failure = failure;
        private int _calls;

        public string Key => "flaky";

        public string TraceName => "flaky";

        public IPredicate? Compile(JsonElement operands, string path, List<string> errors) => new Predicate(this);

        private sealed class Predicate(ThrowingAfterFirstOperator owner) : IPredicate
        {
            public bool Evaluate(JsonElement body) => ++owner._calls == 1 ? true : throw owner._failure;

            public string Describe() => "flaky";
        }
    }

    [Fact]
    public void Stage_TimeoutOnTheTracePass_KeepsTheRulesRejection_WithoutLeavesInDetail()
    {
        var registry = OperatorRegistry.CreateDefault();
        registry.Register(new ThrowingAfterFirstOperator(new RuleEvaluationAbortedException(RuleEvaluationAbortedException.RegexTimeout, SeamTimeout)));
        // or: Evaluate вычисляет лист (истина), трассировка вычисляет его повторно и получает отказ
        var holder = HolderWith(registry, Rule("traced", condition: """{"or":[{"flaky":1}]}""", reason: "admin_reason"));

        var rejection = Run(holder, EventSamples.Bytes(EventSamples.Skud)).Rejection!;

        Assert.Equal(("admin_reason", "traced"), (rejection.Reason, rejection.RuleId));
        Assert.Equal("Сработало правило «Название» (traced, v1)", rejection.Detail);
    }

    [Fact]
    public void Stage_OtherAbortKindsAndOrdinaryExceptions_StillPropagate()
    {
        var other = OperatorRegistry.CreateDefault();
        other.Register(new ThrowingAfterFirstOperator(new InvalidOperationException("defect")));
        var abort = OperatorRegistry.CreateDefault();
        abort.Register(new ThrowingAfterFirstOperator(new RuleEvaluationAbortedException("input_too_long", SeamTimeout)));

        // первое вычисление «flaky» истинно: чтобы исключение вышло из Evaluate, лист вычисляется дважды в одном правиле
        const string twice = """{"and":[{"flaky":1},{"flaky":2}]}""";
        Assert.Throws<InvalidOperationException>(() => Run(HolderWith(other, Rule("r", condition: twice)), EventSamples.Bytes(EventSamples.Skud)));
        Assert.Throws<RuleEvaluationAbortedException>(() => Run(HolderWith(abort, Rule("r", condition: twice)), EventSamples.Bytes(EventSamples.Skud)));
    }
}

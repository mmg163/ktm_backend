using System.Text.Json;
using Ktm.DataSaver.Application.Decoding;
using Ktm.DataSaver.Application.Pipeline;
using Ktm.DataSaver.Application.Rules;
using Ktm.DataSaver.Tests.Decoding;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class RuleRejectionStageTests
{
    private const string SkudZone = """{"==":[{"var":"payload.zone"},"main-gate"]}""";

    internal static RuleSnapshotHolder HolderWith(params RuleDefinition[] rules)
    {
        var holder = new RuleSnapshotHolder();
        holder.Replace(new RuleSnapshotBuilder(new ConditionCompiler()).Build("rev", rules, []), DateTimeOffset.UnixEpoch);
        return holder;
    }

    private static IngestionVerdict Run(RuleSnapshotHolder holder, string json) => RunBytes(holder, EventSamples.Bytes(json));

    private static IngestionVerdict RunBytes(RuleSnapshotHolder holder, byte[] bytes)
    {
        var message = EventSamples.Message(bytes);
        var parsed = new EventDecoder().Decode(bytes).Event!;
        return new RuleRejectionStage(holder).Evaluate(message, parsed);
    }

    [Fact]
    public void OwnSourceRule_Rejects_WithRuleInfoAndDetailWithoutMessageValues()
    {
        var holder = HolderWith(Rule("skud-zone", condition: SkudZone, version: 3, name: "Запретная зона", reason: "zone_blocked"));

        var rejection = Run(holder, EventSamples.Skud).Rejection!;

        Assert.Equal(RejectionKind.Rule, rejection.Kind);
        Assert.Equal("zone_blocked", rejection.Reason);
        Assert.Equal("skud-zone", rejection.RuleId);
        Assert.Equal("Запретная зона", rejection.RuleName);
        Assert.Equal(3, rejection.RuleVersion);
        Assert.Null(rejection.TargetTopic);
        // шаблон ADR п. 2.9: только путь и операция; ни значение из правила ("main-gate"), ни из сообщения
        Assert.Equal("Сработало правило «Запретная зона» (skud-zone, v3): payload.zone ==", rejection.Detail);
    }

    [Fact]
    public void Detail_NeverContainsFieldValuesFromTheMessage()
    {
        var holder = HolderWith(Rule("cv-low", source: "cv", condition: """{"<":[{"var":"payload.confidence"},0.95]}"""));

        var detail = Run(holder, EventSamples.CvPresence).Rejection!.Detail;

        Assert.Contains("payload.confidence", detail);
        Assert.DoesNotContain("0.93", detail);
        Assert.DoesNotContain("tab:5678", detail);
        Assert.DoesNotContain("cam-12", detail);
    }

    [Fact]
    public void OtherSourceRule_DoesNotFire_ButCommonRuleFiresForAnySource()
    {
        var cvOnly = HolderWith(Rule("cv-zone", source: "cv", condition: SkudZone));
        Assert.True(Run(cvOnly, EventSamples.Skud).IsAccepted);

        var common = HolderWith(Rule("any-zone", source: null, condition: """{"==":[{"var":"schemaVersion"},1]}"""));
        Assert.Equal("any-zone", Run(common, EventSamples.Skud).Rejection!.RuleId);
        Assert.Equal("any-zone", Run(common, EventSamples.OneCDo).Rejection!.RuleId);
    }

    [Fact]
    public void TwoMatchingRules_FirstInPriorityOrderWins_AndDisabledRuleNeverFires()
    {
        var holder = HolderWith(
            Rule("later", priority: 20, condition: SkudZone),
            Rule("first", source: null, priority: 5, condition: SkudZone),
            Rule("disabled", priority: 1, condition: SkudZone, enabled: false));

        Assert.Equal("first", Run(holder, EventSamples.Skud).Rejection!.RuleId);
    }

    [Fact]
    public void CorrectionMessageIsEvaluatedToo()
    {
        var holder = HolderWith(Rule("skud-zone", condition: SkudZone));

        var verdict = Run(holder, EventSamples.SkudCorrection);

        Assert.Equal("skud-zone", verdict.Rejection!.RuleId);
    }

    [Fact]
    public void NoRuleFires_OrSnapshotIsEmpty_Accepts()
    {
        var notMatching = HolderWith(Rule("other-zone", condition: """{"==":[{"var":"payload.zone"},"warehouse"]}"""));

        Assert.True(Run(notMatching, EventSamples.Skud).IsAccepted);
        Assert.True(Run(new RuleSnapshotHolder(), EventSamples.Skud).IsAccepted);
        Assert.True(Run(HolderWith(), EventSamples.Skud).IsAccepted);
    }

    [Fact]
    public void LongDetail_IsCutTo1024()
    {
        // Название 200 символов и десять ложных листьев под отрицанием с длинными путями: текст заведомо длиннее предела.
        var leaves = Enumerable.Range(0, 10).Select(i => "{\"==\":[{\"var\":\"payload.f" + i + new string('p', 100) + "\"},\"x\"]}");
        var condition = "{\"!\":{\"or\":[" + string.Join(",", leaves) + "]}}";
        var holder = HolderWith(Rule("long", condition: condition, name: new string('a', 200)));

        var detail = Run(holder, EventSamples.Skud).Rejection!.Detail;

        Assert.Equal(RuleRejectionStage.MaxDetailLength, detail.Length);
    }

    private sealed class ThrowingOperator : IRuleOperator
    {
        public string Key => "boom";

        public string TraceName => "boom";

        public IPredicate? Compile(JsonElement operands, string path, List<string> errors) => new ThrowingPredicate();

        private sealed class ThrowingPredicate : IPredicate
        {
            public bool Evaluate(JsonElement body) => throw new InvalidOperationException("evaluator defect");

            public string Describe() => "boom";
        }
    }

    [Fact]
    public void EvaluatorException_IsNotTurnedIntoRejection()
    {
        var registry = OperatorRegistry.CreateDefault();
        registry.Register(new ThrowingOperator());
        var holder = new RuleSnapshotHolder();
        holder.Replace(new RuleSnapshotBuilder(new ConditionCompiler(registry)).Build("rev", [Rule("boom", condition: """{"boom":1}""")], []), DateTimeOffset.UnixEpoch);

        Assert.Throws<InvalidOperationException>(() => Run(holder, EventSamples.Skud));
    }

    private sealed class SwappingProvider(params RuleSnapshot[] snapshots) : IRuleSnapshotProvider
    {
        public int Reads { get; private set; }

        public RuleSnapshot Current => snapshots[Math.Min(Reads++, snapshots.Length - 1)];
    }

    [Fact]
    public void Stage_ReadsSnapshotOncePerMessage_EvenIfProviderSwapsItBetweenReads()
    {
        var builder = new RuleSnapshotBuilder(new ConditionCompiler());
        var first = builder.Build("rev-1", [Rule("old-rule", condition: SkudZone)], []);
        var second = builder.Build("rev-2", [Rule("new-rule", condition: SkudZone)], []);
        var provider = new SwappingProvider(first, second);
        var bytes = EventSamples.Bytes(EventSamples.Skud);

        var verdict = new RuleRejectionStage(provider).Evaluate(EventSamples.Message(bytes), new EventDecoder().Decode(bytes).Event!);

        Assert.Equal("old-rule", verdict.Rejection!.RuleId);
        Assert.Equal(1, provider.Reads);
    }
}

using Ktm.DataSaver.Application.Rules;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

public class RuleSnapshotBuilderTests
{
    private static RuleSnapshotBuilder Builder(int maxRules = RuleSnapshotBuilder.DefaultMaxRules) => new(new ConditionCompiler(), maxRules);

    private static string[] Ids(ReadOnlySpan<CompiledRule> rules) => rules.ToArray().Select(r => r.Definition.Id).ToArray();

    [Fact]
    public void Build_OrdersByPriorityThenOrdinalId_AndEmbedsCommonRulesIntoEverySource()
    {
        var snapshot = Builder().Build("rev", [
            Rule("b-same", priority: 10),
            Rule("a-same", priority: 10),
            Rule("common-late", source: null, priority: 50),
            Rule("cv-first", source: "cv", priority: 1),
            Rule("skud-last", priority: 200),
            Rule("B-upper", priority: 10),
        ], []);

        // ordinal: заглавная буква раньше строчной
        Assert.Equal(["B-upper", "a-same", "b-same", "common-late", "skud-last"], Ids(snapshot.RulesFor("skud")));
        Assert.Equal(["cv-first", "common-late"], Ids(snapshot.RulesFor("cv")));
        Assert.Equal(["common-late"], Ids(snapshot.RulesFor("1c-do")));
        Assert.Equal(["common-late"], Ids(snapshot.RulesFor("unknown-source")));
        Assert.Equal(6, snapshot.Rules.Count);
        Assert.Equal("rev", snapshot.Revision);
        Assert.Empty(snapshot.Errors);
    }

    [Fact]
    public void Build_DropsRulesBeyondMaxRules_WithErrorForEachDroppedOne()
    {
        var rules = Enumerable.Range(0, 501).Select(i => Rule($"r{i:000}", priority: i)).ToList();

        var snapshot = Builder().Build("rev", rules, []);

        Assert.Equal(500, snapshot.Rules.Count);
        var error = Assert.Single(snapshot.Errors);
        Assert.Equal("r500", error.RuleId);
        Assert.Contains("MaxRules=500", error.Reason);
    }

    [Fact]
    public void Build_BrokenRulesAndSourceProblemsGoToErrors_WithoutAffectingTheOthers()
    {
        var snapshot = Builder().Build(
            "rev",
            [
                Rule("good-1", priority: 1),
                Rule("bad-operator", condition: """{"in":[{"var":"eventType"},["t"]]}""", version: 4),
                Rule("bad-source", source: "xyz"),
                Rule("good-2", priority: 2),
                Rule("disabled-bad", condition: """{"nope":1}""", enabled: false),
            ],
            [new RuleProblem("row-7", "condition", "не JSON-объект")]);

        Assert.Equal(["good-1", "good-2"], snapshot.Rules.Select(r => r.Definition.Id));
        Assert.Equal(3, snapshot.Errors.Count);
        Assert.Contains(snapshot.Errors, e => e.RuleId == "bad-operator" && e.RuleVersion == 4 && e.Reason.Contains("не поддерживается"));
        Assert.Contains(snapshot.Errors, e => e.RuleId == "bad-source" && e.Reason.Contains("source"));
        Assert.Contains(snapshot.Errors, e => e.RuleId == "row-7" && e.RuleVersion is null && e.Reason.Contains("не JSON-объект"));
    }

    [Fact]
    public void Build_EmptySetAndOnlyDisabledRules_YieldNoRulesAndNoErrors()
    {
        var empty = Builder().Build("e", [], []);
        var disabled = Builder().Build("d", [Rule("off-1", enabled: false), Rule("off-2", source: null, enabled: false)], []);

        Assert.Empty(empty.Rules);
        Assert.Empty(empty.Errors);
        Assert.Empty(disabled.Rules);
        Assert.Empty(disabled.Errors);
        Assert.Empty(disabled.RulesFor("skud").ToArray());
        // выключенные видны в диффе (журнал изменений), но не применяются
        Assert.Equal(["off-1", "off-2"], disabled.Changes.Added.Select(r => r.Id));
    }

    [Fact]
    public void Build_Diff_ReportsAddedChangedRemoved_AndChangeWithoutVersionIncrease()
    {
        var builder = Builder();
        var first = builder.Build("r1", [Rule("keep"), Rule("bump"), Rule("silent"), Rule("gone")], []);
        Assert.Equal(["bump", "gone", "keep", "silent"], first.Changes.Added.Select(r => r.Id));

        var second = builder.Build(
            "r2",
            [
                Rule("keep"),
                Rule("bump", condition: """{"==":[{"var":"eventType"},"other"]}""", version: 2),
                Rule("silent", condition: """{"==":[{"var":"eventType"},"other"]}"""),
                Rule("new"),
            ],
            [],
            first);

        Assert.Equal([new RuleRef("new", 1)], second.Changes.Added);
        Assert.Equal([new RuleRef("bump", 2), new RuleRef("silent", 1)], second.Changes.Changed);
        Assert.Equal([new RuleRef("gone", 1)], second.Changes.Removed);
        Assert.Equal([new RuleRef("silent", 1)], second.Changes.ChangedWithoutVersionIncrease);

        var third = builder.Build("r3", [Rule("keep"), Rule("new")], [], builder.Build("r3", [Rule("keep"), Rule("new")], []));
        Assert.Empty(third.Changes.Added);
        Assert.Empty(third.Changes.Changed);
        Assert.Empty(third.Changes.Removed);
    }

    [Fact]
    public void Build_DuplicateRuleId_KeepsFirstAndReportsError()
    {
        var snapshot = Builder().Build("rev", [Rule("dup", reason: "first_reason"), Rule("dup", reason: "second_reason", version: 2), Rule("other")], []);

        Assert.Equal(["dup", "other"], snapshot.Rules.Select(r => r.Definition.Id));
        Assert.Equal("first_reason", snapshot.Rules[0].Definition.ReasonCode);
        var error = Assert.Single(snapshot.Errors);
        Assert.Equal("dup", error.RuleId);
        Assert.Contains("повторяется", error.Reason);
    }
}

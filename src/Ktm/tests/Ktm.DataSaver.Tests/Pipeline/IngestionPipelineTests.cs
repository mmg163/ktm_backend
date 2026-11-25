using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Rules;
using Ktm.DataSaver.Tests.Decoding;
using Ktm.DataSaver.Tests.Rules;

namespace Ktm.DataSaver.Tests.Pipeline;

public class IngestionPipelineTests
{
    private static readonly ConsumedMessage ValidMessage = EventSamples.Message(EventSamples.Bytes(EventSamples.Skud), 2, 41);

    private sealed class RecordingStage(string name, List<string> calls, IngestionVerdict? verdict = null) : IIngestionStage
    {
        public string Stage => PipelineStages.Validation;

        public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
        {
            calls.Add(name);
            return verdict ?? IngestionVerdict.Accept;
        }
    }

    private sealed class ThrowingStage : IIngestionStage
    {
        public string Stage => PipelineStages.Rules;

        public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed) =>
            throw new InvalidOperationException("stage defect");
    }

    private sealed class RejectEventTypeValidator(string eventType) : IEventValidator
    {
        public ValidationResult Validate(ParsedEvent parsed) =>
            parsed.EventType == eventType ? ValidationResult.Reject("unsupported_event_type", "test rule") : ValidationResult.Accept;
    }

    private sealed class ThrowingValidator : IEventValidator
    {
        public ValidationResult Validate(ParsedEvent parsed) => throw new TimeoutException("C5 backend is down");
    }

    private static IngestionPipeline Pipeline(params IIngestionStage[] stages) => new(new FormatStage(new EventDecoder()), stages);

    [Fact]
    public void Process_RunsStagesInRegistrationOrder_AndAcceptsWithParsedEvent()
    {
        var calls = new List<string>();

        var result = Pipeline(new RecordingStage("first", calls), new RecordingStage("second", calls), new RecordingStage("third", calls))
            .Process(ValidMessage);

        Assert.Equal(["first", "second", "third"], calls);
        Assert.True(result.Verdict.IsAccepted);
        Assert.Equal("48213377", result.Event!.SourceEventId);
        Assert.Same(ValidMessage, result.Message);
        Assert.Null(result.Origin);
    }

    [Fact]
    public void Process_FirstRejectionStopsChain_AndKeepsRuleInfo()
    {
        var calls = new List<string>();
        var rejection = new RejectionInfo(RejectionKind.Rule, "rule_matched", "every third", RuleId: "r-7", TargetTopic: "ktm.events.rule-rejected");

        var result = Pipeline(
                new RecordingStage("first", calls),
                new RecordingStage("rejecting", calls, IngestionVerdict.Reject(rejection)),
                new RecordingStage("never", calls))
            .Process(ValidMessage);

        Assert.Equal(["first", "rejecting"], calls);
        Assert.False(result.Verdict.IsAccepted);
        Assert.Null(result.Event);
        Assert.Equal(rejection, result.Verdict.Rejection);
        // Событие разобрано: происхождение берётся из конверта, а семантика Event («принято») не меняется.
        Assert.Equal(new EventOrigin("skud", "48213377"), result.Origin);
    }

    [Fact]
    public void Process_FormatFailure_RejectsWithFormatKindWithoutCallingStages()
    {
        var calls = new List<string>();
        var message = EventSamples.Message(EventSamples.SkudWith(o => o["schemaVersion"] = 2), 0, 5);

        var result = Pipeline(new RecordingStage("any", calls)).Process(message);

        Assert.Empty(calls);
        Assert.Equal(RejectionKind.Format, result.Verdict.Rejection!.Kind);
        Assert.Equal(RejectReasons.UnknownSchemaVersion, result.Verdict.Rejection.Reason);
        Assert.Null(result.Verdict.Rejection.TargetTopic);
        Assert.Same(message, result.Message);
        Assert.Null(result.Origin);
    }

    [Fact]
    public void Process_AcceptAllValidator_AcceptsAnyEventType()
    {
        var message = EventSamples.Message(EventSamples.SkudWith(o => o["eventType"] = "never_heard_of_it"));

        var result = Pipeline(new EventValidationStage(new AcceptAllEventValidator())).Process(message);

        Assert.True(result.Verdict.IsAccepted);
    }

    [Fact]
    public void Process_ValidatorRejection_BecomesValidationKindRejection()
    {
        var result = Pipeline(new EventValidationStage(new RejectEventTypeValidator("access_granted"))).Process(ValidMessage);

        var rejection = result.Verdict.Rejection!;
        Assert.Equal(RejectionKind.Validation, rejection.Kind);
        Assert.Equal("unsupported_event_type", rejection.Reason);
        Assert.Equal("test rule", rejection.Detail);
        Assert.Null(result.Event);
    }

    private const string MatchAnyEvent = """{"==":[{"var":"source"},"skud"]}""";

    [Fact]
    public void Process_RuleStageRunsAfterFormatAndC5_SoRulesNeverSeeFormatOrC5Rejections()
    {
        var holder = RuleRejectionStageTests.HolderWith(RuleTestSupport.Rule("match-all", source: null, condition: """{"!=":[{"var":"eventType"},"x"]}"""));
        var ruleStage = new RuleRejectionStage(holder);

        var format = Pipeline(new EventValidationStage(new AcceptAllEventValidator()), ruleStage)
            .Process(EventSamples.Message(EventSamples.SkudWith(o => o["schemaVersion"] = 2), 0, 5));
        var c5 = Pipeline(new EventValidationStage(new RejectEventTypeValidator("access_granted")), ruleStage).Process(ValidMessage);

        Assert.Equal(RejectionKind.Format, format.Verdict.Rejection!.Kind);
        Assert.Equal(RejectReasons.UnknownSchemaVersion, format.Verdict.Rejection.Reason);
        Assert.Null(format.Verdict.Rejection.RuleId);
        Assert.Equal(RejectionKind.Validation, c5.Verdict.Rejection!.Kind);
        Assert.Null(c5.Verdict.Rejection.RuleId);
    }

    [Fact]
    public void Process_RuleStage_RejectsMatchingEventWithOrigin_AndPassesNonMatchingOnToLaterStages()
    {
        var calls = new List<string>();
        var holder = RuleRejectionStageTests.HolderWith(RuleTestSupport.Rule("skud-rule", condition: MatchAnyEvent, reason: "skud_junk"));
        var ruleStage = new RuleRejectionStage(holder);
        var cv = EventSamples.Message(EventSamples.Bytes(EventSamples.CvPresence), 1, 9);

        var rejected = Pipeline(new EventValidationStage(new AcceptAllEventValidator()), ruleStage, new RecordingStage("after", calls))
            .Process(ValidMessage);
        var accepted = Pipeline(new EventValidationStage(new AcceptAllEventValidator()), ruleStage, new RecordingStage("after", calls))
            .Process(cv);

        Assert.Equal(RejectionKind.Rule, rejected.Verdict.Rejection!.Kind);
        Assert.Equal("skud_junk", rejected.Verdict.Rejection.Reason);
        Assert.Equal("skud-rule", rejected.Verdict.Rejection.RuleId);
        Assert.Equal(new EventOrigin("skud", "48213377"), rejected.Origin);
        Assert.Null(rejected.Event);
        Assert.True(accepted.Verdict.IsAccepted);
        Assert.Equal(["after"], calls);
    }

    private sealed class ThrowingSchemaValidator : ISchemaValidator
    {
        public SchemaValidationResult Validate(System.Text.Json.JsonElement instance) => throw new InvalidOperationException("schema defect");
    }

    [Fact]
    public void Process_ExceptionOfAnyStage_IsWrappedWithItsStageAndOrigin_AndIsNotARejection()
    {
        var validator = Assert.Throws<PipelineStageException>(
            () => Pipeline(new EventValidationStage(new ThrowingValidator())).Process(ValidMessage));
        var rules = Assert.Throws<PipelineStageException>(() => Pipeline(new ThrowingStage()).Process(ValidMessage));
        var parse = Assert.Throws<PipelineStageException>(() =>
            new IngestionPipeline(new FormatStage(new EventDecoder(new Dictionary<int, ISchemaValidator> { [1] = new ThrowingSchemaValidator() })), [])
                .Process(ValidMessage));

        Assert.Equal((PipelineStages.Validation, typeof(TimeoutException)), (validator.Stage, validator.InnerException!.GetType()));
        Assert.Equal((PipelineStages.Rules, typeof(InvalidOperationException)), (rules.Stage, rules.InnerException!.GetType()));
        Assert.Equal(PipelineStages.Parse, parse.Stage);
        Assert.Equal(new EventOrigin("skud", "48213377"), validator.Origin);
        Assert.Null(parse.Origin);
        // Текст исходного исключения в обёртку не копируется.
        Assert.DoesNotContain("C5 backend is down", validator.Message);
    }

    [Fact]
    public async Task BlockingWriteFailureHandler_AlwaysRetries_AndLosesNothing()
    {
        var batch = new[] { ValidMessage };
        var context = new WriteFailureContext(batch, new InvalidOperationException("23505"), Attempt: 3);

        var decision = await new BlockingWriteFailureHandler().HandleAsync(context, CancellationToken.None);

        Assert.Equal(WriteFailureKind.Retry, decision.Kind);
        Assert.Empty(decision.DeadLetterItems);
    }
}

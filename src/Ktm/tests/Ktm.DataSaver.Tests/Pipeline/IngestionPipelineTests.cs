using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Tests.Decoding;

namespace Ktm.DataSaver.Tests.Pipeline;

public class IngestionPipelineTests
{
    private static readonly ConsumedMessage ValidMessage = EventSamples.Message(EventSamples.Bytes(EventSamples.Skud), 2, 41);

    private sealed class RecordingStage(string name, List<string> calls, IngestionVerdict? verdict = null) : IIngestionStage
    {
        public IngestionVerdict Evaluate(ConsumedMessage message, ParsedEvent parsed)
        {
            calls.Add(name);
            return verdict ?? IngestionVerdict.Accept;
        }
    }

    private sealed class ThrowingStage : IIngestionStage
    {
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

    [Fact]
    public void Process_PortOrStageException_IsNotARejection()
    {
        var validatorFailure = Assert.Throws<TimeoutException>(
            () => Pipeline(new EventValidationStage(new ThrowingValidator())).Process(ValidMessage));
        var stageFailure = Assert.Throws<InvalidOperationException>(
            () => Pipeline(new ThrowingStage()).Process(ValidMessage));

        Assert.Equal("C5 backend is down", validatorFailure.Message);
        Assert.Equal("stage defect", stageFailure.Message);
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

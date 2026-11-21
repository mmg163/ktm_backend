using System.Text;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Infrastructure.Kafka;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaRejectionMessageTests
{
    private static RejectedMessage Rejected(RejectionKind kind, string? ruleId, string? targetTopic = null) => new(
        new ConsumedMessage("ktm.events.raw", 3, 42, Encoding.UTF8.GetBytes("skud:7"), Encoding.UTF8.GetBytes("{}"), DateTimeOffset.UtcNow),
        new RejectionInfo(kind, "rule_matched", "detail", ruleId, targetTopic));

    private static string? RuleHeader(RejectedMessage r) =>
        KafkaRejectionPublisher.ToMessage(r).Headers.TryGetLastBytes("ktm-reject-rule-id", out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

    [Fact]
    public void RuleId_goes_to_header_as_utf8_for_any_kind_and_topic_and_is_not_duplicated_in_detail()
    {
        var message = KafkaRejectionPublisher.ToMessage(Rejected(RejectionKind.Rule, "  r-7 ", "ktm.events.rule-rejected"));

        Assert.Equal(6, message.Headers.Count);
        Assert.Equal("r-7", Encoding.UTF8.GetString(message.Headers.GetLastBytes("ktm-reject-rule-id")));
        Assert.Equal("detail", Encoding.UTF8.GetString(message.Headers.GetLastBytes("ktm-reject-detail")));
        Assert.Equal("r-7", RuleHeader(Rejected(RejectionKind.Validation, "r-7")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Missing_or_blank_RuleId_gives_no_header(string? ruleId)
    {
        var message = KafkaRejectionPublisher.ToMessage(Rejected(RejectionKind.Format, ruleId));

        Assert.Equal(5, message.Headers.Count);
        Assert.False(message.Headers.TryGetLastBytes("ktm-reject-rule-id", out _));
    }

    [Fact]
    public void Long_RuleId_is_truncated_to_128_chars_without_splitting_a_surrogate_pair()
    {
        Assert.Equal(128, RuleHeader(Rejected(RejectionKind.Rule, new string('a', 200)))!.Length);

        // Эмодзи (2 UTF-16 единицы) начинается на позиции 127: пара не должна разорваться.
        var onBoundary = new string('a', 127) + "😀" + "tail";
        var truncated = KafkaRejectionPublisher.NormalizeRuleId(onBoundary)!;
        Assert.Equal(new string('a', 127), truncated);
        Assert.Equal(truncated, RuleHeader(Rejected(RejectionKind.Rule, onBoundary)));

        // Эмодзи целиком влезает (позиции 126-127).
        var fits = new string('a', 126) + "😀" + "tail";
        var kept = KafkaRejectionPublisher.NormalizeRuleId(fits)!;
        Assert.Equal(128, kept.Length);
        Assert.EndsWith("😀", kept, StringComparison.Ordinal);
    }
}

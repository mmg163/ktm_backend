using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Infrastructure.Kafka;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaRuleRejectionHeadersTests
{
    private static readonly byte[] Body = Encoding.UTF8.GetBytes("""{"k":"v"}""");

    private static RejectedMessage Rejected(RejectionInfo info) => new(
        new ConsumedMessage("ktm.events.raw", 3, 42, Encoding.UTF8.GetBytes("skud:7"), Body, DateTimeOffset.UtcNow), info);

    private static string? Header(Message<byte[], byte[]> message, string name) =>
        message.Headers.TryGetLastBytes(name, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

    [Fact]
    public void Rule_rejection_carries_all_rule_headers_and_original_key_and_body()
    {
        var info = new RejectionInfo(RejectionKind.Rule, "test_data", "Сработало правило", "skud-test-event", RuleName: "Тестовое событие СКУД", RuleVersion: 3);
        var rejected = Rejected(info);
        var message = KafkaRejectionPublisher.ToMessage(rejected);

        Assert.Equal(9, message.Headers.Count);
        Assert.EndsWith("Z", Header(message, "ktm-source-timestamp"));
        Assert.Equal("test_data", Header(message, "ktm-reject-reason"));
        Assert.Equal("skud-test-event", Header(message, "ktm-reject-rule-id"));
        Assert.Equal("Тестовое событие СКУД", Header(message, "ktm-reject-rule-name"));
        Assert.Equal("3", Header(message, "ktm-reject-rule-version"));
        Assert.Equal("Сработало правило", Header(message, "ktm-reject-detail"));
        Assert.Equal("ktm.events.raw", Header(message, "ktm-source-topic"));
        Assert.Equal(Body, message.Value);
        Assert.Equal("skud:7", Encoding.UTF8.GetString(message.Key));
    }

    [Fact]
    public void Rule_name_is_trimmed_and_missing_name_or_version_gives_no_header()
    {
        var trimmed = KafkaRejectionPublisher.ToMessage(Rejected(new RejectionInfo(RejectionKind.Rule, "r", "d", "id", RuleName: "  Имя  ", RuleVersion: 1)));
        Assert.Equal("Имя", Header(trimmed, "ktm-reject-rule-name"));

        foreach (var name in new string?[] { null, "", "   " })
        {
            var message = KafkaRejectionPublisher.ToMessage(Rejected(new RejectionInfo(RejectionKind.Rule, "r", "d", "id", RuleName: name)));
            Assert.Null(Header(message, "ktm-reject-rule-name"));
            Assert.Null(Header(message, "ktm-reject-rule-version"));
        }
    }

    [Fact]
    public void Rule_name_and_detail_are_cut_exactly_at_the_limit()
    {
        var info = new RejectionInfo(RejectionKind.Rule, "r", new string('d', 1024) + "x", "id", RuleName: new string('n', 256) + "x", RuleVersion: 1);
        var message = KafkaRejectionPublisher.ToMessage(Rejected(info));

        Assert.Equal(256, Header(message, "ktm-reject-rule-name")!.Length);
        Assert.Equal(1024, Header(message, "ktm-reject-detail")!.Length);
    }

    [Fact]
    public void Long_rule_name_and_detail_are_truncated_without_splitting_a_surrogate_pair()
    {
        // Эмодзи занимает позиции 255-256 (0-based), граница 256 символов приходится на его середину.
        var name = new string('я', 255) + "😀" + "tail";
        var detail = new string('d', 1023) + "😀" + "tail";
        var info = new RejectionInfo(RejectionKind.Rule, "r", detail, "id", RuleName: name, RuleVersion: 1);

        var message = KafkaRejectionPublisher.ToMessage(Rejected(info));
        var headerName = Header(message, "ktm-reject-rule-name")!;
        var headerDetail = Header(message, "ktm-reject-detail")!;

        Assert.Equal(new string('я', 255), headerName);
        Assert.Equal(new string('d', 1023), headerDetail);
    }

    [Theory]
    [InlineData(RejectionKind.Format)]
    [InlineData(RejectionKind.Validation)]
    public void Non_rule_rejection_gets_no_rule_name_or_version_headers_and_keeps_full_detail(RejectionKind kind)
    {
        var detail = new string('x', 2000);
        var info = new RejectionInfo(kind, "invalid_schema", detail, RuleName: "n", RuleVersion: 1);
        var message = KafkaRejectionPublisher.ToMessage(Rejected(info));

        Assert.Equal(6, message.Headers.Count);
        Assert.False(message.Headers.TryGetLastBytes("ktm-reject-rule-name", out _));
        Assert.False(message.Headers.TryGetLastBytes("ktm-reject-rule-version", out _));
        Assert.Equal(detail, Header(message, "ktm-reject-detail"));
    }

    [Fact]
    public void Regex_timeout_rejection_carries_the_system_reason_and_the_headers_of_the_rule_that_timed_out()
    {
        var info = new RejectionInfo(
            RejectionKind.Rule, RejectReasons.RuleEvalTimeout,
            "Вычисление правила «Медленное» (skud-slow, v2) прервано: превышен предел совпадения регулярного выражения (200 мс)",
            "skud-slow", RuleName: "Медленное", RuleVersion: 2);

        var message = KafkaRejectionPublisher.ToMessage(Rejected(info));

        Assert.Equal("rule_eval_timeout", Header(message, "ktm-reject-reason"));
        Assert.Equal("skud-slow", Header(message, "ktm-reject-rule-id"));
        Assert.Equal("Медленное", Header(message, "ktm-reject-rule-name"));
        Assert.Equal("2", Header(message, "ktm-reject-rule-version"));
        Assert.Contains("(200 мс)", Header(message, "ktm-reject-detail"));
        Assert.Equal(Body, message.Value);
    }
}

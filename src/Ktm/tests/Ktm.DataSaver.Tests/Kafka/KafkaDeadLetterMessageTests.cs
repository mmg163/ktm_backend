using System.Text;
using Confluent.Kafka;
using Ktm.DataSaver.Core.Ingestion;
using Ktm.DataSaver.Core.Model;
using Ktm.DataSaver.Core.Pipeline;
using Ktm.DataSaver.Core.Resilience;
using Ktm.DataSaver.Infrastructure.Kafka;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaDeadLetterMessageTests
{
    private static readonly byte[] Body = Encoding.UTF8.GetBytes("""{"externalPersonId":"Иванов"}""");
    private static readonly DateTimeOffset SourceTime = new(2026, 11, 24, 9, 30, 15, 123, TimeSpan.FromHours(3));
    private static readonly DateTimeOffset FailedAt = new(2026, 11, 24, 7, 0, 0, TimeSpan.Zero);

    private static DeadLetteredMessage Item(DeadLetterInfo info, byte[]? key = null) =>
        new(new ConsumedMessage("ktm.events.raw", 3, 42, key ?? Encoding.UTF8.GetBytes("skud:7"), Body, SourceTime), null, info);

    private static string? Header(Message<byte[], byte[]> message, string name) =>
        message.Headers.TryGetLastBytes(name, out var bytes) ? Encoding.UTF8.GetString(bytes) : null;

    [Fact]
    public void WriteError_CarriesAllHeaders_OriginalKeyAndBody_AndNothingFromTheBody()
    {
        var info = new DeadLetterInfo(DeadLetterReasons.WriteDataError, "write", "Npgsql.PostgresException", "23514", "ck_poison", "events", 4, FailedAt);

        var message = KafkaDeadLetterPublisher.ToMessage(Item(info));

        Assert.Equal(11, message.Headers.Count);
        Assert.Equal(
            ("write_data_error", "write", "Npgsql.PostgresException", info.Attempts.ToString(System.Globalization.CultureInfo.InvariantCulture), "2026-11-24T07:00:00.000Z", "23514"),
            (Header(message, "ktm-dlq-reason"), Header(message, "ktm-dlq-stage"), Header(message, "ktm-dlq-exception-type"),
                Header(message, "ktm-dlq-attempts"), Header(message, "ktm-dlq-failed-at"), Header(message, "ktm-dlq-sqlstate")));
        Assert.Equal(
            ("ktm.events.raw", "3", "42", "2026-11-24T06:30:15.123Z"),
            (Header(message, "ktm-source-topic"), Header(message, "ktm-source-partition"), Header(message, "ktm-source-offset"), Header(message, "ktm-source-timestamp")));
        Assert.Contains("ck_poison", Header(message, "ktm-dlq-detail"));
        Assert.False(message.Headers.TryGetLastBytes("ktm-reject-reason", out _));
        Assert.Equal(Body, message.Value);
        Assert.Equal("skud:7", Encoding.UTF8.GetString(message.Key));
        Assert.All(message.Headers, h => Assert.DoesNotContain("Иванов", Encoding.UTF8.GetString(h.GetValueBytes())));
    }

    [Theory]
    [InlineData(PipelineStages.Parse)]
    [InlineData(PipelineStages.Validation)]
    [InlineData(PipelineStages.Rules)]
    public void StageFailure_CarriesStageAndExceptionTypeWithoutSqlState(string stage)
    {
        var info = new DeadLetterInfo(DeadLetterReasons.UnexpectedError, stage, typeof(InvalidOperationException).FullName!, null, null, null, 3, FailedAt);

        var message = KafkaDeadLetterPublisher.ToMessage(Item(info));

        Assert.Equal(10, message.Headers.Count);
        Assert.Equal(("unexpected_error", stage, "System.InvalidOperationException"),
            (Header(message, "ktm-dlq-reason"), Header(message, "ktm-dlq-stage"), Header(message, "ktm-dlq-exception-type")));
        Assert.False(message.Headers.TryGetLastBytes("ktm-dlq-sqlstate", out _));
    }

    [Fact]
    public void EmptyKeyMeansNoKey_AndLongDetailIsTruncated()
    {
        var info = new DeadLetterInfo("r", "write", "X", null, null, new string('t', 3000), 3, FailedAt);

        var message = KafkaDeadLetterPublisher.ToMessage(Item(info, key: []));

        Assert.Null(message.Key);
        Assert.Equal(1024, Header(message, "ktm-dlq-detail")!.Length);
    }
}

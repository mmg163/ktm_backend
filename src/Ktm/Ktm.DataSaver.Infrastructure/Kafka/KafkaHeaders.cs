namespace Ktm.DataSaver.Infrastructure.Kafka;

/// <summary>
/// Единый источник имён заголовков Kafka (kafka-event-format.md, ADR-0006 п. 2.4): публикаторы <c>rejected</c>/<c>dlq</c> и
/// источник повторной обработки используют одни и те же константы, поэтому имя не может разойтись между записью и чтением.
/// Значения - формат на проводе, менять их нельзя.
/// </summary>
internal static class KafkaHeaders
{
    public const string SourceTopic = "ktm-source-topic";
    public const string SourcePartition = "ktm-source-partition";
    public const string SourceOffset = "ktm-source-offset";
    public const string SourceTimestamp = "ktm-source-timestamp";

    public const string RejectReason = "ktm-reject-reason";
    public const string RejectDetail = "ktm-reject-detail";
    public const string RejectRuleId = "ktm-reject-rule-id";
    public const string RejectRuleName = "ktm-reject-rule-name";
    public const string RejectRuleVersion = "ktm-reject-rule-version";

    public const string DlqReason = "ktm-dlq-reason";
    public const string DlqStage = "ktm-dlq-stage";
    public const string DlqExceptionType = "ktm-dlq-exception-type";
    public const string DlqDetail = "ktm-dlq-detail";
    public const string DlqAttempts = "ktm-dlq-attempts";
    public const string DlqFailedAt = "ktm-dlq-failed-at";
    public const string DlqSqlState = "ktm-dlq-sqlstate";
}

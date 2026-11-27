namespace Ktm.DataSaver.Application.Model;

/// <summary>Значения заголовка <c>ktm-reject-reason</c> по kafka-event-format.md п. 3.</summary>
public static class RejectReasons
{
    public const string MalformedJson = "malformed_json";
    public const string UnknownSchemaVersion = "unknown_schema_version";
    public const string InvalidSchema = "invalid_schema";

    /// <summary>Системный код: вычисление правила прервано по пределу совпадения regex (ADR-0005 п. 2.7). Правилу администратора недоступен.</summary>
    public const string RuleEvalTimeout = "rule_eval_timeout";
}

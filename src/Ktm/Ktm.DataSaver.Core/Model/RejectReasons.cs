namespace Ktm.DataSaver.Core.Model;

/// <summary>Значения заголовка <c>ktm-reject-reason</c> по kafka-event-format.md п. 3.</summary>
public static class RejectReasons
{
    public const string MalformedJson = "malformed_json";
    public const string UnknownSchemaVersion = "unknown_schema_version";
    public const string InvalidSchema = "invalid_schema";
}

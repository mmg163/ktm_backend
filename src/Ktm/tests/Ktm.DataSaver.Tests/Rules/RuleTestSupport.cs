using System.Text.Json;
using Ktm.DataSaver.Core.Rules;

namespace Ktm.DataSaver.Tests.Rules;

internal static class RuleTestSupport
{
    public const string AnyCondition = """{"==":[{"var":"eventType"},"t"]}""";

    public static RuleDefinition Rule(
        string id,
        string? source = "skud",
        string condition = AnyCondition,
        bool enabled = true,
        int priority = 100,
        int version = 1,
        string name = "Название",
        string reason = "test_reason") =>
        new(id, name, null, source, enabled, priority, version, reason, Json(condition));

    public static JsonElement Json(string json) => JsonDocument.Parse(json).RootElement.Clone();

    /// <summary>Компилирует условие; тест падает, если компилятор вернул ошибки.</summary>
    public static IConditionNode Compile(string condition, OperatorRegistry? registry = null)
    {
        var result = new ConditionCompiler(registry ?? OperatorRegistry.CreateDefault()).Compile(Json(condition));
        Assert.True(result.Succeeded, string.Join("; ", result.Errors));
        return result.Root!;
    }

    public static IReadOnlyList<string> Errors(string condition, OperatorRegistry? registry = null)
    {
        var result = new ConditionCompiler(registry ?? OperatorRegistry.CreateDefault()).Compile(Json(condition));
        Assert.False(result.Succeeded);
        return result.Errors;
    }

    /// <summary>Тело сообщения с заданным payload (JSON-объект) и источником skud.</summary>
    public static JsonElement Body(string payload = "{}", string source = "skud") =>
        Json($$"""{"schemaVersion":1,"source":"{{source}}","eventType":"t","occurredAt":"2026-01-01T00:00:00Z","payload":{{payload}}}""");

    public static bool Eval(string condition, JsonElement body) => Compile(condition).Evaluate(body);

    public static string Cmp(string op, string path, string literal) => $$"""{"{{op}}":[{"var":"{{path}}"},{{literal}}]}""";
}

using System.Text.RegularExpressions;
using Ktm.DataSaver.Core.Decoding;
using Ktm.DataSaver.Core.Rules;
using static Ktm.DataSaver.Tests.Rules.RuleTestSupport;

namespace Ktm.DataSaver.Tests.Rules;

/// <summary>
/// Задачи 2.8 и 2.9: схема условия (docs/api/rejection-condition.schema.json) и компилятор описывают одну грамматику;
/// расхождения допустимы только по семантике, которую схема выразить не может, и перечислены явно.
/// </summary>
public class ConditionSchemaParityTests
{
    private static readonly ISchemaValidator Schema = LoadSchema();

    private static ISchemaValidator LoadSchema()
    {
        using var stream = typeof(ConditionSchemaParityTests).Assembly
            .GetManifestResourceStream("Ktm.DataSaver.Tests.Schemas.rejection-condition.schema.json")!;
        using var reader = new StreamReader(stream);
        return new LateApexSchemaValidator(reader.ReadToEnd());
    }

    private static bool SchemaAccepts(string condition) => Schema.Validate(Json(condition)).IsValid;

    private static bool CompilerAccepts(string condition) => new ConditionCompiler().Compile(Json(condition)).Succeeded;

    private static string NestedNot(int depth) => depth == 1
        ? """{"==":[{"var":"source"},"cv"]}"""
        : $$"""{"!":{{NestedNot(depth - 1)}}}""";

    private static string FlatAnd(int leaves) =>
        $$"""{"and":[{{string.Join(",", Enumerable.Repeat("""{"==":[{"var":"source"},"cv"]}""", leaves))}}]}""";

    // Образцы, по которым схема и компилятор обязаны совпасть.
    public static TheoryData<string, bool> Agreed => new()
    {
        { """{"==":[{"var":"eventType"},"test_event"]}""", true },
        { """{"!=":[{"var":"payload.status"},"draft"]}""", true },
        { """{"==":["draft",{"var":"payload.status"}]}""", true },
        { """{"==":[{"var":"payload.x"},null]}""", true },
        { """{"==":[{"var":"payload.isTest"},true]}""", true },
        { """{"<":[{"var":"occurredAt"},"2020-01-01T00:00:00Z"]}""", true },
        { """{"<=":[{"var":"payload.c"},0.3]}""", true },
        { """{">":[5,{"var":"payload.c"}]}""", true },
        { """{">=":[{"var":"payload.items.1.code"},1]}""", true },
        { """{"<=":[0,{"var":"payload.confidence"},0.3]}""", true },
        { """{"and":[{"==":[{"var":"source"},"cv"]},{"or":[{"<":[{"var":"payload.c"},1]},{"!":{"==":[{"var":"deviceId"},null]}}]}]}""", true },
        { """{"!":[{"==":[{"var":"source"},"cv"]}]}""", true },
        { """{"==":["a","b"]}""", false },
        { """{"==":[{"var":"payload.a"},{"var":"payload.b"}]}""", false },
        { """{"==":[{"var":["payload.a",0]},"x"]}""", false },
        { """{"in":[{"var":"payload.a"},["x"]]}""", false },
        { """{"missing":"payload.a"}""", false },
        { """{"and":[]}""", false },
        { """{"or":[]}""", false },
        { """{"<":[0,{"var":"payload.c"},1]}""", false },
        { """{">":[0,{"var":"payload.c"},1]}""", false },
        { """{">=":[0,{"var":"payload.c"},1]}""", false },
        { """{"==":[{"var":""},1]}""", false },
        { """{"==":[{"var":"payload..a"},1]}""", false },
        { """{"==":[{"var":"unknownField"},1]}""", false },
        { """{"all":[{"==":[{"var":"source"},"cv"]}]}""", false },
        { """{"any":[{"==":[{"var":"source"},"cv"]}]}""", false },
        { """{}""", false },
        { """{"and":[{"==":[{"var":"source"},"cv"]}],"or":[{"==":[{"var":"source"},"cv"]}]}""", false },
        { """{"!":[{"==":[{"var":"source"},"cv"]},{"==":[{"var":"source"},"cv"]}]}""", false },
        { """{"<":[{"var":"payload.c"},true]}""", false },
        { """{"<":[{"var":"payload.c"},null]}""", false },
        { """{"==":[{"var":"payload.c"},[1]]}""", false },
        { """[]""", false },
    };

    [Theory]
    [MemberData(nameof(Agreed))]
    public void SchemaAndCompiler_AgreeOnSample(string condition, bool valid)
    {
        Assert.Equal(valid, SchemaAccepts(condition));
        Assert.Equal(valid, CompilerAccepts(condition));
    }

    /// <summary>Явный список расхождений: схема допускает, компилятор отвергает (семантика вне выразительности схемы).</summary>
    public static TheoryData<string, string> SchemaOnlyAccepts => new()
    {
        { "дата без зоны", """{"<":[{"var":"payload.d"},"2026-01-01"]}""" },
        { "строка не дата", """{"<":[{"var":"payload.d"},"yesterday"]}""" },
        { "нижняя граница больше верхней", """{"<=":[0.8,{"var":"payload.c"},0.2]}""" },
        { "границы разного вида", """{"<=":[0.2,{"var":"payload.c"},"2026-01-01T00:00:00Z"]}""" },
        { "граница-дата без зоны", """{"<=":["2026-01-01",{"var":"payload.d"},"2026-01-02T00:00:00Z"]}""" },
        { "глубина 9", NestedNot(9) },
        { "65 узлов", FlatAnd(64) },
        { "число вне decimal", """{"<":[{"var":"payload.c"},1e400]}""" },
        { "строка длиннее 1024", $$"""{"==":[{"var":"payload.c"},"{{new string('a', 1025)}}"]}""" },
    };

    [Theory]
    [MemberData(nameof(SchemaOnlyAccepts))]
    public void DocumentedDivergence_SchemaAcceptsButCompilerRejects(string reason, string condition)
    {
        Assert.True(SchemaAccepts(condition), reason);
        Assert.False(CompilerAccepts(condition), reason);
    }

    [Fact]
    public void LimitsBoundaries_AreAcceptedByBoth()
    {
        Assert.True(SchemaAccepts(NestedNot(8)) && CompilerAccepts(NestedNot(8)));
        Assert.True(SchemaAccepts(FlatAnd(63)) && CompilerAccepts(FlatAnd(63)));
    }

    // --- 2.9: примеры V003 ---

    private static readonly Regex ExampleRow = new(
        @"\('(?<id>[^']+)',[\s\S]*?'(?<src>skud|cv|1c-do|\*)',\s*'(?<cond>\{[^\n]*\})'::jsonb,\s*'(?<reason>[^']+)',\s*(?<enabled>true|false)",
        RegexOptions.CultureInvariant);

    private static string RepositoryFile(string relative)
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate)) return candidate;
        }

        throw new FileNotFoundException(relative);
    }

    [Fact]
    public void ShippedExamples_AreDisabled_CompileWithoutErrors_AndPassTheSchema()
    {
        var sql = File.ReadAllText(RepositoryFile("db/migrations/V003__rejection_rules_examples.sql"));
        var rows = ExampleRow.Matches(sql);

        Assert.Equal(
            ["1c-do-draft-document", "common-test-flag", "cv-confidence-out-of-range", "cv-low-confidence", "skud-stale-event", "skud-test-event"],
            rows.Select(m => m.Groups["id"].Value).Order(StringComparer.Ordinal));

        var compiler = new ConditionCompiler();
        foreach (Match row in rows)
        {
            var id = row.Groups["id"].Value;
            var condition = row.Groups["cond"].Value;
            var source = row.Groups["src"].Value;

            Assert.Equal("false", row.Groups["enabled"].Value);
            var rule = new RuleDefinition(id, id, null, source == "*" ? null : source, false, 100, 1, row.Groups["reason"].Value, Json(condition));
            var result = compiler.Compile(rule);
            Assert.True(result.Succeeded, $"{id}: {string.Join("; ", result.Errors)}");
            Assert.True(SchemaAccepts(condition), id);
        }
    }
}

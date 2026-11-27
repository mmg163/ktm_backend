using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using Ktm.HostedServices.Admin;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Ktm.DataSaver.Tests.Admin;

/// <summary>Каркас admin-области (tasks 5.1, 5.3, 5.7): реестр разделов, OpenAPI, Swagger, изоляция от /health.</summary>
public sealed class AdminAreaTests
{
    private sealed class PingSection : IAdminSection
    {
        public string Id => "ping";

        public string Title => "Ping";

        public void MapEndpoints(RouteGroupBuilder group) =>
            group.MapGet("/status", () => TypedResults.Ok(new PingStatus("up"))).WithSummary("Ping status").Produces<PingStatus>();
    }

    public sealed record PingStatus(string State);

    private static readonly Dictionary<string, string?> SwaggerOff = new() { ["SwaggerEnabled"] = "false" };

    [Fact]
    public async Task Sections_ListsReprocessing_AndNewSectionAppearsInSectionsRoutesAndOpenApiWithoutFrameworkChanges()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages(), configure: s => s.AddAdminSection<PingSection>());

        var sections = await host.GetJsonAsync("/admin/v1/sections");
        var ping = await host.GetJsonAsync("/admin/v1/ping/status");
        var document = await host.GetJsonAsync("/admin/openapi/v1.json");

        Assert.Equal(["reprocessing", "ping"], sections.EnumerateArray().Select(s => s.GetProperty("id").GetString()!).ToArray());
        Assert.Equal("/admin/v1/reprocessing", sections[0].GetProperty("apiBase").GetString());
        Assert.Equal("up", ping.GetProperty("state").GetString());
        Assert.Equal("ping", document.GetProperty("paths").GetProperty("/admin/v1/ping/status").GetProperty("get").GetProperty("tags")[0].GetString());
    }

    [Fact]
    public async Task DuplicateSectionId_FailsAtMapping()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages(), configure: s =>
            {
                s.AddAdminSection<PingSection>();
                s.AddAdminSection<PingSectionTwin>();
            });
        });

        Assert.Contains("'ping'", ex.Message, StringComparison.Ordinal);
    }

    private sealed class PingSectionTwin : IAdminSection
    {
        public string Id => "ping";

        public string Title => "Ping 2";

        public void MapEndpoints(RouteGroupBuilder group)
        {
        }
    }

    [Fact]
    public async Task OpenApiDocument_DescribesReprocessingAndProblemDetails_NotHealth_AndIsServedWithSwaggerUi()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());

        var document = await host.GetJsonAsync("/admin/openapi/v1.json");
        var paths = document.GetProperty("paths").EnumerateObject().Select(p => p.Name).ToArray();
        var text = document.GetRawText();
        using var ui = await host.Client.GetAsync("/admin/swagger/index.html");
        var uiBody = await ui.Content.ReadAsStringAsync();

        Assert.All(paths, p => Assert.StartsWith("/admin/v1/", p, StringComparison.Ordinal));
        Assert.Contains("/admin/v1/sections", paths);
        Assert.Contains("/admin/v1/reprocessing/jobs", paths);
        Assert.Contains("/admin/v1/reprocessing/jobs/{id}", paths);
        Assert.Contains("/admin/v1/reprocessing/jobs/{id}/report", paths);
        Assert.Contains("/admin/v1/reprocessing/jobs/{id}/cancel", paths);
        Assert.DoesNotContain("/health", text, StringComparison.Ordinal);
        Assert.True(document.GetProperty("components").GetProperty("schemas").TryGetProperty("JobResponse", out _));
        Assert.Contains("ProblemDetails", text, StringComparison.Ordinal);
        var start = document.GetProperty("paths").GetProperty("/admin/v1/reprocessing/jobs").GetProperty("post");
        Assert.Equal(["202", "400", "409", "415", "500", "503"], start.GetProperty("responses").EnumerateObject().Select(r => r.Name).Order().ToArray());
        Assert.Equal("reprocessing", start.GetProperty("tags")[0].GetString());
        Assert.Equal(HttpStatusCode.OK, ui.StatusCode);
        Assert.Contains("Data Saver admin API", uiBody, StringComparison.Ordinal);
        Assert.Contains("/admin/openapi/v1.json", await host.Client.GetStringAsync("/admin/swagger/index.js"), StringComparison.Ordinal);
    }

    [Fact]
    public async Task SwaggerDisabled_DocumentAndUiAre404_ButApiStillWorks()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages(), SwaggerOff);

        using var document = await host.Client.GetAsync("/admin/openapi/v1.json");
        using var ui = await host.Client.GetAsync("/admin/swagger/index.html");
        using var sections = await host.Client.GetAsync("/admin/v1/sections");

        Assert.Equal(HttpStatusCode.NotFound, document.StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, ui.StatusCode);
        Assert.Equal(HttpStatusCode.OK, sections.StatusCode);
    }

    [Fact]
    public async Task CommittedSnapshot_MatchesGeneratedDocument()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());
        using var response = await host.Client.GetAsync("/admin/openapi/v1.json");
        var generated = Normalize(await response.Content.ReadAsStringAsync());
        var path = SnapshotPath();

        // Обновление снимка осознанно: KTM_UPDATE_SNAPSHOTS=1 dotnet test --filter CommittedSnapshot (затем снимок смотрят в ревью).
        if (Environment.GetEnvironmentVariable("KTM_UPDATE_SNAPSHOTS") == "1")
        {
            await File.WriteAllTextAsync(path, generated);
        }

        Assert.True(File.Exists(path), "docs/api/data-saver-admin.openapi.json is missing");
        Assert.Equal(Normalize(await File.ReadAllTextAsync(path)), generated);
    }

    [Fact]
    public async Task AdminRoutes_DoNotAffectHealth_AndUnknownAdminPathIs404()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());

        using var health = await host.Client.GetAsync("/health/live");
        using var unknown = await host.Client.GetAsync("/admin/v1/unknown");

        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        Assert.Equal("alive", await health.Content.ReadAsStringAsync());
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
    }

    private static string Normalize(string json) =>
        JsonNode.Parse(json)!.ToJsonString(new JsonSerializerOptions { WriteIndented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }).ReplaceLineEndings("\n") + "\n";

    private static string SnapshotPath()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !Directory.Exists(Path.Combine(directory.FullName, "docs", "api")))
        {
            directory = directory.Parent;
        }

        Assert.NotNull(directory);
        return Path.Combine(directory.FullName, "docs", "api", "data-saver-admin.openapi.json");
    }
}

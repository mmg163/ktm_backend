using System.Net;
using Ktm.HostedServices.Admin;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Ktm.DataSaver.Tests.Admin;

/// <summary>Отдача админки /admin/ui/ (tasks 5.5): ресурсы, защитные заголовки, отсутствие внешних ссылок. Без Docker: Kestrel на свободном порту.</summary>
public sealed class AdminUiEndpointsTests : IAsyncLifetime
{
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        _app = builder.Build();
        _app.MapAdminUi();
        _app.MapGet("/health/live", () => "alive");
        await _app.StartAsync();
        var address = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        _client = new HttpClient { BaseAddress = new Uri(address) };
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Theory]
    [InlineData("/admin/ui/", "text/html")]
    [InlineData("/admin/ui/app.js", "text/javascript")]
    [InlineData("/admin/ui/api.js", "text/javascript")]
    [InlineData("/admin/ui/sections/index.js", "text/javascript")]
    [InlineData("/admin/ui/sections/reprocessing.js", "text/javascript")]
    [InlineData("/admin/ui/app.css", "text/css")]
    public async Task Resource_Served_WithSecurityHeaders(string path, string contentType)
    {
        var response = await _client.GetAsync(path);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(contentType, response.Content.Headers.ContentType!.MediaType);
        Assert.Equal("default-src 'self'; frame-ancestors 'none'; base-uri 'none'", response.Headers.GetValues("Content-Security-Policy").Single());
        Assert.Equal("nosniff", response.Headers.GetValues("X-Content-Type-Options").Single());
        Assert.Equal("no-referrer", response.Headers.GetValues("Referrer-Policy").Single());
        Assert.Contains("no-store", response.Headers.CacheControl!.ToString());
    }

    [Fact]
    public async Task AllResources_HaveNoExternalHosts_NoStorage_NoInnerHtml()
    {
        var all = new List<string>();
        foreach (var path in new[] { "/", "/app.js", "/api.js", "/sections/index.js", "/sections/reprocessing.js", "/app.css" })
        {
            all.Add(await _client.GetStringAsync("/admin/ui" + path));
        }

        var text = string.Join('\n', all);
        Assert.DoesNotContain("http://", text);
        Assert.DoesNotContain("https://", text);
        Assert.DoesNotContain("innerHTML", text);
        Assert.DoesNotContain("localStorage.", text);
        Assert.DoesNotContain("sessionStorage.", text);
        Assert.DoesNotContain("document.cookie", text);
        Assert.DoesNotContain("<script>", all[0]);
    }

    [Theory]
    [InlineData("/admin/ui/missing.js")]
    [InlineData("/admin/ui/sections/missing.js")]
    [InlineData("/admin/ui/../health/live")]
    [InlineData("/admin/ui/Ktm.DataSaver.dll")]
    public async Task UnknownPath_Returns404(string path)
    {
        var response = await _client.GetAsync(path);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task WithoutTrailingSlash_RedirectsAndHealthUntouched()
    {
        using var noRedirect = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false }) { BaseAddress = _client.BaseAddress };

        var redirect = await noRedirect.GetAsync("/admin/ui");
        var health = await _client.GetAsync("/health/live");

        Assert.Equal("/admin/ui/", redirect.Headers.Location!.OriginalString);
        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        Assert.False(health.Headers.Contains("Content-Security-Policy"));
    }
}

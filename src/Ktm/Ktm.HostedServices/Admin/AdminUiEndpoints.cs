using System.Reflection;
using Microsoft.AspNetCore.StaticFiles;

namespace Ktm.DataSaver.Admin;

/// <summary>
/// Отдача статической админки <c>/admin/ui/</c> из встроенных ресурсов сборки (design Р-8): без сборки фронтенда,
/// без внешних зависимостей, без доступа к файловой системе. Допустимы только ресурсы из белого списка, поэтому
/// обход каталогов невозможен по построению.
/// </summary>
public static class AdminUiEndpoints
{
    public const string RoutePrefix = "/admin/ui";
    private const string ResourcePrefix = "Ktm.DataSaver.Admin.Ui/";

    private static readonly FileExtensionContentTypeProvider ContentTypes = new()
    {
        Mappings = { [".js"] = "text/javascript", [".css"] = "text/css", [".html"] = "text/html" },
    };

    private static readonly Lazy<IReadOnlyDictionary<string, byte[]>> Files = new(LoadFiles);

    public static IEndpointRouteBuilder MapAdminUi(this IEndpointRouteBuilder endpoints)
    {
        // Без завершающего слэша относительные ссылки страницы (app.js, api.js) указали бы на /admin/.
        endpoints.MapGet(RoutePrefix + "/{**path}", (string? path, HttpContext context) =>
        {
            if (string.IsNullOrEmpty(path) && !context.Request.Path.Value!.EndsWith('/'))
            {
                return Results.Redirect(RoutePrefix + "/");
            }

            ApplySecurityHeaders(context.Response);
            var name = string.IsNullOrEmpty(path) ? "index.html" : path;
            if (!Files.Value.TryGetValue(name, out var content) || !ContentTypes.TryGetContentType(name, out var contentType))
            {
                return Results.NotFound();
            }

            return Results.Bytes(content, contentType + "; charset=utf-8");
        }).ExcludeFromDescription();
        return endpoints;
    }

    private static void ApplySecurityHeaders(HttpResponse response)
    {
        var headers = response.Headers;
        headers.ContentSecurityPolicy = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'";
        headers.XContentTypeOptions = "nosniff";
        headers["Referrer-Policy"] = "no-referrer";
        headers.CacheControl = "no-store";
    }

    private static Dictionary<string, byte[]> LoadFiles()
    {
        var assembly = typeof(AdminUiEndpoints).Assembly;
        var files = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach (var resource in assembly.GetManifestResourceNames())
        {
            if (!resource.StartsWith(ResourcePrefix, StringComparison.Ordinal))
            {
                continue;
            }

            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var memory = new MemoryStream();
            stream.CopyTo(memory);
            files[resource[ResourcePrefix.Length..].Replace('\\', '/')] = memory.ToArray();
        }

        return files;
    }
}

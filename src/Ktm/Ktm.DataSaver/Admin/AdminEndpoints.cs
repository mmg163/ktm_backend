using System.Text.Json.Serialization;
using Ktm.DataSaver.Configuration;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;

namespace Ktm.DataSaver.Admin;

/// <summary>
/// Admin-область Data Saver на порту health (ADR-0006 п. 2.6, design Р-8): группа <c>/admin/v1</c>, реестр разделов
/// <see cref="IAdminSection"/>, <c>GET /admin/v1/sections</c>, единый обработчик ошибок (<c>application/problem+json</c>, 5xx без текста
/// исключения), документ OpenAPI и Swagger UI по флагу. Аутентификации нет (решение лида, R-8): защита сетевая.
/// </summary>
public static class AdminEndpoints
{
    public const string V1Prefix = "/admin/v1";
    public const string OpenApiDocumentName = "v1";
    public const string OpenApiPath = "/admin/openapi/v1.json";
    public const string SwaggerPrefix = "admin/swagger";

    /// <summary>Службы admin-области: опции, аудит, JSON (enum строками), генерация OpenAPI. Разделы добавляются <see cref="AdminSectionRegistration.AddAdminSection{TSection}"/>.</summary>
    public static IServiceCollection AddAdmin(this IServiceCollection services, IConfiguration adminSection)
    {
        services.AddSingleton<IValidateOptions<AdminOptions>>(new OptionsValidator<AdminOptions>(_ => []));
        services.AddOptions<AdminOptions>().Bind(adminSection).ValidateOnStart();
        services.AddSingleton<AdminAudit>();
        services.AddSingleton<AdminErrorFilter>();

        // Перечисления - строками (state, outcome, kind); общий для приложения параметр не затрагивает health (там только строки).
        services.ConfigureHttpJsonOptions(options => options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
        services.AddOpenApi(OpenApiDocumentName, options =>
        {
            // Только /admin/v1: health и прочие маршруты в документ не попадают.
            options.ShouldInclude = description =>
                description.RelativePath?.StartsWith("admin/v1/", StringComparison.Ordinal) == true;
            options.AddDocumentTransformer((document, _, _) =>
            {
                document.Info.Title = "Data Saver admin API";
                document.Info.Version = "v1";
                document.Info.Description = "Administrative area of Data Saver (ADR-0006): reprocessing of dlq, rejected and raw messages. " +
                                            "No authentication: reachable only from the internal network, never publish it outside.";
                // Относительный сервер: документ не зависит от хоста запроса и совпадает со снимком в docs/api.
                document.Servers = [new OpenApiServer { Url = "/" }];
                return Task.CompletedTask;
            });
        });
        return services;
    }

    public static IEndpointRouteBuilder MapAdmin(this IEndpointRouteBuilder endpoints)
    {
        var sections = endpoints.ServiceProvider.GetServices<IAdminSection>().ToArray();
        var duplicate = sections.GroupBy(s => s.Id, StringComparer.Ordinal).FirstOrDefault(g => g.Count() > 1);
        if (duplicate is not null)
        {
            throw new InvalidOperationException($"Admin section id '{duplicate.Key}' is registered more than once.");
        }

        var v1 = endpoints.MapGroup(V1Prefix)
            .AddEndpointFilter<AdminErrorFilter>()
            .ProducesProblem(StatusCodes.Status500InternalServerError);

        var info = sections.Select(s => new AdminSectionInfo(s.Id, s.Title, $"{V1Prefix}/{s.Id}")).ToArray();
        v1.MapGet("/sections", () => TypedResults.Ok(info))
            .WithName("ListAdminSections")
            .WithTags("sections")
            .WithSummary("List admin sections")
            .WithDescription("Sections of the admin area; the admin UI builds its menu from this list.")
            .Produces<AdminSectionInfo[]>();

        foreach (var section in sections)
        {
            section.MapEndpoints(v1.MapGroup("/" + section.Id).WithTags(section.Id));
        }

        if (endpoints.ServiceProvider.GetRequiredService<IOptions<AdminOptions>>().Value.SwaggerEnabled)
        {
            MapSwagger(endpoints);
        }

        return endpoints;
    }

    // Swagger UI - middleware: встраиваем его в ветку конвейера на маршруте, чтобы MapAdmin не требовал IApplicationBuilder.
    private static void MapSwagger(IEndpointRouteBuilder endpoints)
    {
        endpoints.MapOpenApi("/admin/openapi/{documentName}.json");

        var pipeline = endpoints.CreateApplicationBuilder();
        pipeline.UseSwaggerUI(ui =>
        {
            ui.RoutePrefix = SwaggerPrefix;
            ui.DocumentTitle = "Data Saver admin API";
            ui.SwaggerEndpoint(OpenApiPath, "Data Saver admin v1");
        });
        endpoints.Map("/admin/swagger/{**path}", pipeline.Build()).ExcludeFromDescription();
    }
}

/// <summary>Ошибка обработчика admin-маршрута -> <c>application/problem+json</c> 500 без текста исключения; в лог идёт только имя типа.</summary>
public sealed class AdminErrorFilter(ILogger<AdminErrorFilter> logger) : IEndpointFilter
{
    public async ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        try
        {
            return await next(context);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !context.HttpContext.RequestAborted.IsCancellationRequested)
        {
            logger.LogError("Admin request failed: {ErrorType}", ex.GetType().Name);
            return TypedResults.Problem(statusCode: StatusCodes.Status500InternalServerError, title: "Internal server error");
        }
    }
}

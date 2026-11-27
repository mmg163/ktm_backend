namespace Ktm.HostedServices.Health;

/// <summary>Тело ответа health-маршрутов; имена JSON status/reason (camelCase по умолчанию Results.Json).</summary>
public sealed record HealthResponse(string Status, string? Reason);

public static class HealthEndpoints
{
    /// <summary>Два маршрута без аутентификации (внутренняя сеть, D-10); тело - только статус и фиксированный код причины.</summary>
    public static IEndpointRouteBuilder MapDataSaverHealth(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapGet("/health/live", (HealthEvaluator health) => ToResult(health.Live()));
        endpoints.MapGet("/health/ready", async (HealthEvaluator health, CancellationToken ct) =>
            ToResult(await health.ReadyAsync(ct)));
        return endpoints;
    }

    private static IResult ToResult(HealthResult result) =>
        Results.Json(
            new HealthResponse(result.Status, result.Reason),
            statusCode: result.IsHealthy ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
}

namespace Ktm.DataSaver.Health;

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
            new { status = result.Status, reason = result.Reason },
            statusCode: result.IsHealthy ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
}

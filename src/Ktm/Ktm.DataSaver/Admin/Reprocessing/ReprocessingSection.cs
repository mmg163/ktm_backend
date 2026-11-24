using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Ktm.DataSaver.Configuration;
using Ktm.DataSaver.Core.Reprocessing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.OpenApi;
using Microsoft.Extensions.Options;
using Microsoft.OpenApi;

namespace Ktm.DataSaver.Admin.Reprocessing;

/// <summary>
/// Раздел <c>reprocessing</c>: тонкий слой над <see cref="ReprocessJobManager"/> (ADR-0006 п. 2.6) - разбор запроса, вызов сервиса,
/// отображение результата в HTTP. Логики повторной обработки здесь нет.
/// </summary>
public sealed class ReprocessingSection : IAdminSection
{
    public const string SectionId = "reprocessing";
    public const int MaxReasonLength = 200;
    public const int DefaultReportLimit = 100;
    public const int MaxReportLimit = 1000;
    private const string JobsPath = AdminEndpoints.V1Prefix + "/" + SectionId + "/jobs";

    private static readonly Dictionary<string, ReprocessSource> Sources = new(StringComparer.OrdinalIgnoreCase)
    {
        ["dlq"] = ReprocessSource.Dlq,
        ["rejected"] = ReprocessSource.Rejected,
        ["raw"] = ReprocessSource.Raw,
    };

    public string Id => SectionId;

    public string Title => "Повторная обработка";

    public void MapEndpoints(RouteGroupBuilder group)
    {
        group.MapPost("/jobs", StartAsync)
            .WithName("StartReprocessingJob")
            .WithSummary("Start a reprocessing job")
            .WithDescription("Starts an asynchronous job that re-runs messages of dlq, rejected or raw (for a period) through the Data Saver pipeline. " +
                             "One job at a time. Request body: application/json (StartJobRequest).")
            .Accepts<StartJobRequest>("application/json")
            .Produces<JobResponse>(StatusCodes.Status202Accepted)
            .ProducesValidationProblem()
            .ProducesProblem(StatusCodes.Status409Conflict)
            .Produces(StatusCodes.Status415UnsupportedMediaType)
            .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

        group.MapGet("/jobs", List)
            .WithName("ListReprocessingJobs")
            .WithSummary("List the latest jobs")
            .WithDescription("The latest 10 jobs, newest first. State is kept in memory: a service restart loses it.")
            .Produces<JobListResponse>();

        group.MapGet("/jobs/{id}", Get)
            .WithName("GetReprocessingJob")
            .WithSummary("Get job status and progress")
            .Produces<JobResponse>()
            .ProducesProblem(StatusCodes.Status404NotFound);

        group.MapGet("/jobs/{id}/report", GetReport)
            .WithName("GetReprocessingJobReport")
            .WithSummary("Get the report of a finished job")
            .WithDescription("Coordinates, stage and reason code of messages that were rejected again or failed again; no message bodies. " +
                             "Query: limit 1..1000 (default 100), offset >= 0.")
            .Produces<ReportPageResponse>()
            .ProducesValidationProblem()
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict)
            // Параметры разбираются вручную (ошибка значения - ProblemDetails с полем, а не пустой 400), поэтому описываются здесь.
            .AddOpenApiOperationTransformer((operation, _, _) =>
            {
                operation.Parameters ??= [];
                operation.Parameters.Add(new OpenApiParameter
                {
                    Name = "limit",
                    In = ParameterLocation.Query,
                    Required = false,
                    Description = "Page size, 1..1000 (default 100).",
                    Schema = new OpenApiSchema { Type = JsonSchemaType.Integer, Minimum = "1", Maximum = "1000", Default = JsonValue.Create(DefaultReportLimit) },
                });
                operation.Parameters.Add(new OpenApiParameter
                {
                    Name = "offset",
                    In = ParameterLocation.Query,
                    Required = false,
                    Description = "Number of report entries to skip (default 0).",
                    Schema = new OpenApiSchema { Type = JsonSchemaType.Integer, Minimum = "0", Default = JsonValue.Create(0) },
                });
                return Task.CompletedTask;
            });

        group.MapPost("/jobs/{id}/cancel", Cancel)
            .WithName("CancelReprocessingJob")
            .WithSummary("Cancel an active job")
            .WithDescription("Cooperative: the job stops after the current batch, achieved progress is kept.")
            .Produces<JobResponse>(StatusCodes.Status202Accepted)
            .ProducesProblem(StatusCodes.Status404NotFound)
            .ProducesProblem(StatusCodes.Status409Conflict);
    }

    private static async Task<IResult> StartAsync(
        HttpContext context,
        ReprocessJobManager manager,
        IOptions<ReprocessOptions> options,
        AdminAudit audit,
        CancellationToken cancellationToken)
    {
        if (!context.Request.HasJsonContentType())
        {
            return TypedResults.Problem(statusCode: StatusCodes.Status415UnsupportedMediaType, title: "Content-Type must be application/json");
        }

        StartJobRequest? body;
        try
        {
            body = await context.Request.ReadFromJsonAsync<StartJobRequest>(cancellationToken);
        }
        catch (JsonException ex)
        {
            // Текст исключения не отдаётся: он может содержать часть тела. Из пути берётся только имя поля.
            var field = FieldFromPath(ex.Path);
            return Invalid(context, audit, [new ReprocessValidationError(field, field == "body" ? "Request body is not valid JSON." : $"{field} has an invalid JSON value.")]);
        }

        var settings = options.Value;
        var (request, errors) = Parse(body ?? new StartJobRequest(), settings);
        if (errors.Count > 0)
        {
            return Invalid(context, audit, errors);
        }

        var result = await manager.StartAsync(request!, cancellationToken);
        audit.JobStartRequested(context, request!, result.Status, result.Job?.Id);
        switch (result.Status)
        {
            case StartStatus.Started:
                return TypedResults.Accepted($"{JobsPath}/{result.Job!.Id}", JobResponse.From(result.Job));
            case StartStatus.Conflict:
                return TypedResults.Problem(
                    statusCode: StatusCodes.Status409Conflict,
                    title: "A reprocessing job is already active",
                    detail: "Only one job can run at a time; wait for it to finish or cancel it.",
                    extensions: new Dictionary<string, object?> { ["activeJobId"] = result.ActiveJobId });
            case StartStatus.Invalid:
                return ToValidationProblem(result.Errors);
            default:
                return TypedResults.Problem(
                    statusCode: StatusCodes.Status503ServiceUnavailable,
                    title: "Source is unavailable",
                    detail: "The message source (Kafka) did not respond; the job was not created. Retry later.");
        }
    }

    private static Ok<JobListResponse> List(ReprocessJobManager manager) =>
        TypedResults.Ok(new JobListResponse(manager.List().Select(JobResponse.From).ToArray()));

    private static Results<Ok<JobResponse>, ProblemHttpResult> Get(string id, ReprocessJobManager manager) =>
        Guid.TryParse(id, out var jobId) && manager.Get(jobId) is { } job
            ? TypedResults.Ok(JobResponse.From(job))
            : NotFound();

    private static Results<Ok<ReportPageResponse>, ProblemHttpResult, ValidationProblem> GetReport(
        string id, HttpContext context, ReprocessJobManager manager)
    {
        var errors = new List<ReprocessValidationError>();
        var limit = ParseQuery(context, "limit", DefaultReportLimit, 1, MaxReportLimit, errors);
        var offset = ParseQuery(context, "offset", 0, 0, int.MaxValue, errors);
        if (errors.Count > 0)
        {
            return ToValidationProblem(errors);
        }

        if (!Guid.TryParse(id, out var jobId))
        {
            return NotFound();
        }

        var report = manager.GetReport(jobId, limit, offset);
        return report.Status switch
        {
            ReportStatus.Ok => TypedResults.Ok(new ReportPageResponse(
                report.Page!.Entries.Select(ReportEntryResponse.From).ToArray(),
                report.Page.Total, report.Page.Limit, report.Page.Offset, report.Page.Truncated)),
            ReportStatus.NotFinished => TypedResults.Problem(
                statusCode: StatusCodes.Status409Conflict,
                title: "The job is not finished",
                detail: "The report is available after the job finishes."),
            _ => NotFound(),
        };
    }

    private static Results<Accepted<JobResponse>, ProblemHttpResult> Cancel(
        string id, HttpContext context, ReprocessJobManager manager, AdminAudit audit)
    {
        if (!Guid.TryParse(id, out var jobId))
        {
            return NotFound();
        }

        var status = manager.Cancel(jobId);
        audit.JobCancelRequested(context, jobId, status);
        return status switch
        {
            CancelStatus.Accepted => TypedResults.Accepted($"{JobsPath}/{jobId}", JobResponse.From(manager.Get(jobId)!)),
            CancelStatus.AlreadyFinished => TypedResults.Problem(
                statusCode: StatusCodes.Status409Conflict,
                title: "The job is already finished",
                detail: "A finished job cannot be cancelled."),
            _ => NotFound(),
        };
    }

    private static ProblemHttpResult NotFound() =>
        TypedResults.Problem(statusCode: StatusCodes.Status404NotFound, title: "Job not found", detail: "There is no such job; the job history is kept in memory and is lost on restart.");

    private static ValidationProblem ToValidationProblem(IEnumerable<ReprocessValidationError> errors) =>
        TypedResults.ValidationProblem(errors
            .GroupBy(e => e.Field, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(e => e.Message).Distinct(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal));

    private static IResult Invalid(HttpContext context, AdminAudit audit, IReadOnlyList<ReprocessValidationError> errors)
    {
        audit.JobStartRejected(context, errors.Select(e => e.Field).Distinct(StringComparer.Ordinal));
        return ToValidationProblem(errors);
    }

    /// <summary>
    /// Разбор тела: строки -> типизированный запрос. Ошибки разбора и ошибки значений ядра собираются вместе (по полям); для поля,
    /// которое не разобрано, ошибки ядра не добавляются (они были бы следствием, а не причиной).
    /// </summary>
    internal static (ReprocessRequest? Request, IReadOnlyList<ReprocessValidationError> Errors) Parse(StartJobRequest body, ReprocessOptions options)
    {
        var errors = new List<ReprocessValidationError>();

        var source = (ReprocessSource)(-1);
        if (string.IsNullOrWhiteSpace(body.Source))
        {
            errors.Add(new ReprocessValidationError("source", "source is required. Allowed: dlq, rejected, raw."));
        }
        else if (Sources.TryGetValue(body.Source.Trim(), out var parsed))
        {
            source = parsed;
        }
        else
        {
            errors.Add(new ReprocessValidationError("source", "Unknown source. Allowed: dlq, rejected, raw."));
        }

        var from = ParseTime(body.From, "from", errors);
        var to = ParseTime(body.To, "to", errors);
        if (body.Reason is { Length: > MaxReasonLength })
        {
            errors.Add(new ReprocessValidationError("reason", $"reason must not exceed {MaxReasonLength.ToString(CultureInfo.InvariantCulture)} characters."));
        }

        var request = new ReprocessRequest(source, from, to, body.Reason, body.MaxRate ?? options.MaxRatePerSecond);
        var explicitFields = errors.Select(e => e.Field).ToHashSet(StringComparer.Ordinal);
        errors.AddRange(ReprocessRequestValidator.Validate(request, options.MaxRateLimit).Where(e => !explicitFields.Contains(e.Field)));
        return errors.Count == 0 ? (request, errors) : (null, errors);
    }

    private static DateTimeOffset? ParseTime(string? value, string field, List<ReprocessValidationError> errors)
    {
        if (value is null)
        {
            return null;
        }

        if (ReprocessRequestValidator.TryParseUtc(value, out var parsed))
        {
            return parsed;
        }

        errors.Add(new ReprocessValidationError(field, $"{field} must be a UTC time (RFC 3339 with the suffix Z)."));
        return null;
    }

    private static int ParseQuery(HttpContext context, string name, int defaultValue, int min, int max, List<ReprocessValidationError> errors)
    {
        var raw = context.Request.Query[name].ToString();
        if (raw.Length == 0)
        {
            return defaultValue;
        }

        if (int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var value) && value >= min && value <= max)
        {
            return value;
        }

        errors.Add(new ReprocessValidationError(name, $"{name} must be an integer in range {min.ToString(CultureInfo.InvariantCulture)}..{(max == int.MaxValue ? "2147483647" : max.ToString(CultureInfo.InvariantCulture))}."));
        return defaultValue;
    }

    // "$.maxRate" -> "maxRate"; всё, что не похоже на имя поля, - "body".
    private static string FieldFromPath(string? path)
    {
        if (path is { Length: > 2 } && path.StartsWith("$.", StringComparison.Ordinal))
        {
            var name = path[2..];
            if (name.Length <= 32 && name.All(char.IsAsciiLetterOrDigit))
            {
                return name;
            }
        }

        return "body";
    }
}

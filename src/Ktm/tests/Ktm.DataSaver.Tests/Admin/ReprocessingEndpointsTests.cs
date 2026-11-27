using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Ktm.DataSaver.Application.Reprocessing;
using Ktm.DataSaver.Tests.Reprocessing;
using Ktm.HostedServices.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Testing;
using Microsoft.Extensions.DependencyInjection;
using static Ktm.DataSaver.Tests.Reprocessing.ReprocessFixture;

namespace Ktm.DataSaver.Tests.Admin;

/// <summary>Раздел reprocessing (tasks 5.2): статусы и тела по спеку, ProblemDetails, пагинация отчёта, аудит без тел.</summary>
public sealed class ReprocessingEndpointsTests
{
    [Theory]
    [InlineData("ktm.events.raw.skud")]
    [InlineData("ktm.events.raw.cv")]
    [InlineData("ktm.events.raw.1c-do")]
    public async Task RawTopic_IsPassedToReplayAndReturnedInJob(string topic)
    {
        ReprocessRequest? observed = null;
        await using var host = await AdminTestHost.StartAsync(r => { observed = r; return ReplayOpenResult.Opened(new FakeReader([])); });
        using var response = await host.Client.PostAsJsonAsync(Jobs, new { source = "raw", topic, from = "2026-01-01T00:00:00Z", to = "2026-01-02T00:00:00Z" });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(topic, observed!.Topic);
        Assert.Equal(observed.Topic, (await Json.OfAsync(response)).GetProperty("parameters").GetProperty("topic").GetString());
    }

    [Theory]
    [InlineData("dlq", "ktm.events.raw.cv")]
    [InlineData("raw", "other.topic")]
    [InlineData("raw", "^ktm.events.raw.*")]
    [InlineData("raw", "ktm.events.raw.other")]
    public async Task InvalidTopic_IsRejected(string source, string topic)
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());
        using var response = await host.Client.PostAsJsonAsync(Jobs, new { source, topic, from = "2026-01-01T00:00:00Z", to = "2026-01-02T00:00:00Z" });
        Assert.True((await ErrorsAsync(response)).TryGetProperty("topic", out _));
    }

    [Fact]
    public async Task RawWithoutTopic_RequiresSelectionForRegex_ButAcceptsConcreteConfiguration()
    {
        const string body = """{"source":"raw","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z"}""";
        await using var regex = await AdminTestHost.StartAsync(AdminTestHost.Messages(), configure: s => s.AddSingleton(
            Microsoft.Extensions.Options.Options.Create(new Ktm.DataSaver.Infrastructure.Kafka.KafkaOptions { Topic = "^ktm.events.raw.*" })));
        using var missing = await regex.PostJobAsync(body);
        Assert.True((await ErrorsAsync(missing)).TryGetProperty("topic", out _));
        await using var concrete = await AdminTestHost.StartAsync(AdminTestHost.Messages());
        using var compatible = await concrete.PostJobAsync(body);
        Assert.Equal(HttpStatusCode.Accepted, compatible.StatusCode);
    }

    private const string Jobs = "/admin/v1/reprocessing/jobs";

    private static async Task AssertProblemAsync(HttpResponseMessage response, HttpStatusCode status)
    {
        Assert.Equal(status, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType!.MediaType);
        var body = await Json.OfAsync(response);
        Assert.Equal((int)status, body.GetProperty("status").GetInt32());
    }

    private static async Task<JsonElement> ErrorsAsync(HttpResponseMessage response)
    {
        await AssertProblemAsync(response, HttpStatusCode.BadRequest);
        return (await Json.OfAsync(response)).GetProperty("errors");
    }

    [Fact]
    public async Task Start_Returns202WithLocation_DoesNotBlock_SecondStartIs409WithActiveJobId_ThenJobCompletesWithReport()
    {
        var (open, release) = AdminTestHost.BlockedSource();
        await using var host = await AdminTestHost.StartAsync(open);

        using var first = await host.PostJobAsync("""{"source":"dlq","maxRate":100}""");
        var created = await Json.OfAsync(first);
        var id = created.GetProperty("id").GetGuid();

        Assert.Equal(HttpStatusCode.Accepted, first.StatusCode);
        Assert.Equal($"/admin/v1/reprocessing/jobs/{id}", first.Headers.Location!.OriginalString);
        Assert.Equal("Running", created.GetProperty("state").GetString());
        Assert.Equal("dlq", created.GetProperty("parameters").GetProperty("source").GetString());
        Assert.Equal(100, created.GetProperty("parameters").GetProperty("maxRate").GetInt32());

        // Задача активна, 202 не ждал завершения: статус - Running, отчёт - 409, второй запуск - 409 с activeJobId.
        Assert.Equal("Running", (await host.GetJsonAsync($"{Jobs}/{id}")).GetProperty("state").GetString());
        using (var report = await host.Client.GetAsync($"{Jobs}/{id}/report"))
        {
            await AssertProblemAsync(report, HttpStatusCode.Conflict);
        }

        using var second = await host.PostJobAsync("""{"source":"rejected"}""");
        await AssertProblemAsync(second, HttpStatusCode.Conflict);
        Assert.Equal(id, (await Json.OfAsync(second)).GetProperty("activeJobId").GetGuid());
        Assert.Equal(1, host.Source.Opened);

        release.SetResult();
        await host.WaitFinishedAsync(id);

        var done = await host.GetJsonAsync($"{Jobs}/{id}");
        Assert.Equal("Completed", done.GetProperty("state").GetString());
        Assert.Equal("Succeeded", done.GetProperty("outcome").GetString());
        Assert.Equal((3, 3, 0), (done.GetProperty("read").GetInt32(), done.GetProperty("written").GetInt32(), done.GetProperty("duplicates").GetInt32()));
        Assert.True(done.TryGetProperty("finishedAt", out var finished) && finished.GetDateTimeOffset().Offset == TimeSpan.Zero);

        var list = await host.GetJsonAsync(Jobs);
        Assert.Equal(id, list.GetProperty("items")[0].GetProperty("id").GetGuid());
    }

    [Fact]
    public async Task Report_IsPaged_AndRejectsOutOfRangeLimitAndOffset_Returns404ForUnknownJob()
    {
        // 5 нечитаемых сообщений (не JSON) -> 5 записей отчёта rejectedAgain.
        var garbage = Enumerable.Range(0, 5).Select(i => Garbage(i)).ToArray();
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages(garbage));
        var id = await host.StartJobAsync();
        await host.WaitFinishedAsync(id);

        var page = await host.GetJsonAsync($"{Jobs}/{id}/report?limit=2&offset=3");
        Assert.Equal((5, 2, 3), (page.GetProperty("total").GetInt32(), page.GetProperty("limit").GetInt32(), page.GetProperty("offset").GetInt32()));
        Assert.Equal(2, page.GetProperty("items").GetArrayLength());
        Assert.Equal("RejectedAgain", page.GetProperty("items")[0].GetProperty("kind").GetString());
        Assert.Equal(100, (await host.GetJsonAsync($"{Jobs}/{id}/report")).GetProperty("limit").GetInt32());

        foreach (var query in new[] { "limit=0", "limit=1001", "limit=abc", "offset=-1", "offset=x" })
        {
            using var response = await host.Client.GetAsync($"{Jobs}/{id}/report?{query}");
            var errors = await ErrorsAsync(response);
            Assert.True(errors.TryGetProperty(query.Split('=')[0], out _), query);
        }

        foreach (var path in new[] { $"{Jobs}/{Guid.NewGuid()}", $"{Jobs}/{Guid.NewGuid()}/report", $"{Jobs}/not-a-guid" })
        {
            using var response = await host.Client.GetAsync(path);
            await AssertProblemAsync(response, HttpStatusCode.NotFound);
        }
    }

    [Theory]
    [InlineData("""{"source":"nope"}""", "source")]
    [InlineData("""{}""", "source")]
    [InlineData("""{"source":"dlq","from":"2026-01-01T00:00:00+03:00"}""", "from")]
    [InlineData("""{"source":"dlq","to":"2026-01-01T00:00:00"}""", "to")]
    [InlineData("""{"source":"dlq","from":"2026-01-02T00:00:00Z","to":"2026-01-01T00:00:00Z"}""", "from")]
    [InlineData("""{"source":"raw"}""", "from")]
    [InlineData("""{"source":"raw"}""", "to")]
    [InlineData("""{"source":"raw","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","reason":"x"}""", "reason")]
    [InlineData("""{"source":"dlq","reason":"   "}""", "reason")]
    [InlineData("""{"source":"dlq","maxRate":0}""", "maxRate")]
    [InlineData("""{"source":"dlq","maxRate":401}""", "maxRate")]
    [InlineData("""{"source":"dlq","maxRate":"fast"}""", "maxRate")]
    [InlineData("""{not json""", "body")]
    public async Task Start_InvalidArguments_Returns400ByField_AndDoesNotOpenSource(string json, string field)
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());

        using var response = await host.PostJobAsync(json);

        Assert.True((await ErrorsAsync(response)).TryGetProperty(field, out _), field);
        Assert.Equal(0, host.Source.Opened);
        Assert.Empty(host.Manager.List());
    }

    [Fact]
    public async Task Start_TooLongReasonAndNonJsonContentType_AreRejected()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());

        using var tooLong = await host.PostJobAsync($$"""{"source":"dlq","reason":"{{new string('a', 201)}}"}""");
        using var text = await host.Client.PostAsync(Jobs, new StringContent("source=dlq", Encoding.UTF8, "text/plain"));

        Assert.True((await ErrorsAsync(tooLong)).TryGetProperty("reason", out _));
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, text.StatusCode);
    }

    [Fact]
    public async Task Start_SourceRejectsPeriod_Returns400WithFieldFromSource_SourceDown_Returns503_NoJobCreated()
    {
        var down = false;
        await using var host = await AdminTestHost.StartAsync(_ => down
            ? throw new ReplaySourceException("broker")
            : ReplayOpenResult.Invalid([new ReprocessValidationError("from", "from is earlier than the earliest available message (2026-01-01T00:00:00Z).")]));

        using var tooEarly = await host.PostJobAsync("""{"source":"raw","from":"2020-01-01T00:00:00Z","to":"2020-01-02T00:00:00Z"}""");
        down = true;
        using var unavailable = await host.PostJobAsync("""{"source":"dlq"}""");

        Assert.True((await ErrorsAsync(tooEarly)).TryGetProperty("from", out _));
        await AssertProblemAsync(unavailable, HttpStatusCode.ServiceUnavailable);
        Assert.Empty(host.Manager.List());

        // Слот освобождён: после отказа источника можно запускать снова.
        down = false;
        using var retry = await host.PostJobAsync("""{"source":"raw","from":"2020-01-01T00:00:00Z","to":"2020-01-02T00:00:00Z"}""");
        Assert.Equal(HttpStatusCode.BadRequest, retry.StatusCode);
    }

    [Fact]
    public async Task Cancel_Active_Returns202_Finished_Returns409_Unknown_Returns404()
    {
        var (open, release) = AdminTestHost.BlockedSource(messages: 6);
        await using var host = await AdminTestHost.StartAsync(open);
        var id = await host.StartJobAsync();

        using var accepted = await host.Client.PostAsync($"{Jobs}/{id}/cancel", null);
        Assert.Equal(HttpStatusCode.Accepted, accepted.StatusCode);

        // Отмена кооперативная (после текущей пачки): читатель заблокирован до первой пачки, поэтому отпускаем его после запроса.
        release.SetResult();
        await host.WaitFinishedAsync(id);
        var cancelled = await host.GetJsonAsync($"{Jobs}/{id}");
        Assert.Equal("Cancelled", cancelled.GetProperty("state").GetString());
        Assert.Equal("Cancelled", cancelled.GetProperty("outcome").GetString());
        Assert.True(cancelled.GetProperty("read").GetInt32() < 6);

        using var again = await host.Client.PostAsync($"{Jobs}/{id}/cancel", null);
        using var unknown = await host.Client.PostAsync($"{Jobs}/{Guid.NewGuid()}/cancel", null);
        await AssertProblemAsync(again, HttpStatusCode.Conflict);
        await AssertProblemAsync(unknown, HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task UnexpectedServerError_Returns500Problem_WithoutExceptionText()
    {
        await using var host = await AdminTestHost.StartAsync(_ => throw new InvalidOperationException("SECRET-EXCEPTION-TEXT"));

        using var response = await host.PostJobAsync("""{"source":"dlq"}""");

        await AssertProblemAsync(response, HttpStatusCode.InternalServerError);
        Assert.DoesNotContain("SECRET-EXCEPTION-TEXT", await response.Content.ReadAsStringAsync());
        Assert.DoesNotContain(host.Logs.GetSnapshot(), r => r.Message.Contains("SECRET-EXCEPTION-TEXT", StringComparison.Ordinal)
                                                          || r.Exception is not null);
        Assert.Contains(host.Logs.GetSnapshot(), r => r.Level == LogLevel.Error && r.Message.Contains(nameof(InvalidOperationException), StringComparison.Ordinal));
    }

    [Fact]
    public async Task Audit_StartAndCancel_WriteStructuredRecordsWithJobIdAndCaller_WithoutBodies_RequestedByIsSanitized()
    {
        var (open, release) = AdminTestHost.BlockedSource();
        await using var host = await AdminTestHost.StartAsync(open);
        // Лишнее поле тела - маркер данных: в аудит попадают только параметры задачи, не тело запроса.
        var noisyHeader = "ivanov" + new string('x', 100);
        Assert.Equal("ivanovx", AdminAudit.SanitizeRequestedBy("ivanov\u0007x"));
        using var started = await host.PostJobAsync("""{"source":"dlq","reason":"write_data_error","note":"BODY-MARKER"}""", noisyHeader);
        var id = (await Json.OfAsync(started)).GetProperty("id").GetGuid();
        using var cancel = await host.Client.PostAsync($"{Jobs}/{id}/cancel", null);
        release.SetResult();
        await host.WaitFinishedAsync(id);

        var audit = host.Logs.GetSnapshot().Where(r => r.Message.StartsWith("Admin audit:", StringComparison.Ordinal)).ToArray();
        var start = Assert.Single(audit, r => r.Message.Contains("job start", StringComparison.Ordinal));
        var cancelled = Assert.Single(audit, r => r.Message.Contains("job cancel", StringComparison.Ordinal));
        Assert.Equal(id.ToString(), State(start, "JobId"));
        Assert.Equal("127.0.0.1", State(start, "RemoteIp"));
        Assert.Equal("write_data_error", State(start, "Reason"));
        Assert.Equal("Dlq", State(start, "Source"));
        Assert.False(string.IsNullOrEmpty(State(start, "AtUtc")));
        var requestedBy = State(start, "RequestedBy")!;
        Assert.Equal(64, requestedBy.Length);
        Assert.StartsWith("ivanovxxxx", requestedBy, StringComparison.Ordinal);
        Assert.DoesNotContain(requestedBy, char.IsControl);
        Assert.Equal(id.ToString(), State(cancelled, "JobId"));
        Assert.DoesNotContain(host.Logs.GetSnapshot(), r => r.Message.Contains("BODY-MARKER", StringComparison.Ordinal));
    }

    private static string? State(FakeLogRecord record, string key) => record.StructuredState!.First(p => p.Key == key).Value;

    [Fact]
    public async Task Audit_RejectedStart_IsRecordedWithFieldsOnly()
    {
        await using var host = await AdminTestHost.StartAsync(AdminTestHost.Messages());

        using var response = await host.PostJobAsync("""{"source":"raw","reason":"PARAM-MARKER"}""");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var record = Assert.Single(host.Logs.GetSnapshot(), r => r.Message.StartsWith("Admin audit:", StringComparison.Ordinal));
        Assert.Contains("Result=Invalid", record.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("PARAM-MARKER", record.Message, StringComparison.Ordinal);
    }
}

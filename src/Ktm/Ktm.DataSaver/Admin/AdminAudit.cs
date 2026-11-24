using Ktm.DataSaver.Core.Reprocessing;

namespace Ktm.DataSaver.Admin;

/// <summary>
/// Аудит вызовов admin-API структурной записью в лог (ADR-0006 В-6, R-8): идентификатор задачи, время UTC, параметры,
/// адрес клиента и необязательное имя из <c>X-Requested-By</c>. Тел запросов и ответов, значений сообщений и текстов
/// исключений в записях нет. Имя из заголовка не доверенное: оно только метка для разбора и не даёт прав.
/// Итог задачи (счётчики) пишет <c>ReprocessJobManager</c> с тем же <c>JobId</c>.
/// </summary>
public sealed class AdminAudit(ILogger<AdminAudit> logger, TimeProvider time)
{
    public const string RequestedByHeader = "X-Requested-By";
    public const int RequestedByMaxLength = 64;

    /// <summary>Печатные ASCII-символы (0x20..0x7E), не больше <see cref="RequestedByMaxLength"/>; пусто - <c>null</c>. Управляющие символы убираются (подделка записей лога).</summary>
    public static string? SanitizeRequestedBy(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return null;
        }

        var buffer = new char[Math.Min(value.Length, RequestedByMaxLength)];
        var length = 0;
        foreach (var c in value)
        {
            if (c is < ' ' or > '~')
            {
                continue;
            }

            buffer[length++] = c;
            if (length == buffer.Length)
            {
                break;
            }
        }

        var result = new string(buffer, 0, length).Trim();
        return result.Length == 0 ? null : result;
    }

    public void JobStartRequested(HttpContext context, ReprocessRequest request, StartStatus status, Guid? jobId)
    {
        var (remoteIp, requestedBy) = Caller(context);
        logger.LogInformation(
            "Admin audit: reprocessing job start Result={Result} JobId={JobId} AtUtc={AtUtc} RemoteIp={RemoteIp} RequestedBy={RequestedBy} Source={Source} From={From} To={To} Reason={Reason} MaxRate={MaxRate}",
            status, jobId, time.GetUtcNow(), remoteIp, requestedBy, request.Source, request.From, request.To, request.Reason, request.MaxRate);
    }

    /// <summary>Запрос запуска, отвергнутый ещё до обращения к менеджеру (неверные аргументы): параметры не разобраны, пишется только факт вызова.</summary>
    public void JobStartRejected(HttpContext context, IEnumerable<string> fields)
    {
        var (remoteIp, requestedBy) = Caller(context);
        logger.LogInformation(
            "Admin audit: reprocessing job start Result=Invalid JobId={JobId} AtUtc={AtUtc} RemoteIp={RemoteIp} RequestedBy={RequestedBy} Fields={Fields}",
            null, time.GetUtcNow(), remoteIp, requestedBy, string.Join(',', fields));
    }

    public void JobCancelRequested(HttpContext context, Guid jobId, CancelStatus status)
    {
        var (remoteIp, requestedBy) = Caller(context);
        logger.LogInformation(
            "Admin audit: reprocessing job cancel Result={Result} JobId={JobId} AtUtc={AtUtc} RemoteIp={RemoteIp} RequestedBy={RequestedBy}",
            status, jobId, time.GetUtcNow(), remoteIp, requestedBy);
    }

    private static (string? RemoteIp, string? RequestedBy) Caller(HttpContext context) =>
        (context.Connection.RemoteIpAddress?.ToString(),
            SanitizeRequestedBy(context.Request.Headers[RequestedByHeader].ToString()));
}

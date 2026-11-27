using System.Globalization;

namespace Ktm.DataSaver.Application.Reprocessing;

/// <summary>Откуда читаются сообщения повторной обработки (ADR-0006 п. 2.6).</summary>
public enum ReprocessSource
{
    /// <summary><c>ktm.events.dlq</c>: сообщения, которые не удалось обработать.</summary>
    Dlq,

    /// <summary><c>ktm.events.rejected</c>: отбракованные сообщения.</summary>
    Rejected,

    /// <summary>Входной топик: перечитывание за период.</summary>
    Raw,
}

/// <summary>
/// Запрос повторной обработки. <c>From</c>/<c>To</c> - период по Kafka-timestamp сообщения, полуинтервал <c>[From, To)</c>, UTC;
/// <c>Reason</c> - фильтр по коду причины (только <c>dlq</c>/<c>rejected</c>); <c>MaxRate</c> - сообщений в секунду.
/// </summary>
public sealed record ReprocessRequest(ReprocessSource Source, DateTimeOffset? From, DateTimeOffset? To, string? Reason, int MaxRate, string? Topic = null);

/// <summary>Имена входных топиков источников.</summary>
public static class RawTopicName
{
    public static bool IsValid(string? name) => name is "ktm.events.raw.skud" or "ktm.events.raw.cv" or "ktm.events.raw.1c-do";
}

/// <summary>Ошибка аргумента запроса: имя поля в camelCase (как в теле запроса) и понятное описание, без данных сообщений.</summary>
public sealed record ReprocessValidationError(string Field, string Message);

/// <summary>Пределы и параметры исполнения задач повторной обработки (опции сервиса приводятся к ним в composition root).</summary>
/// <param name="MaxRateLimit">Верхний предел <c>maxRate</c>.</param>
/// <param name="MaxUnavailable">Сколько ждать восстановления БД, прежде чем завершить задачу ошибкой <c>db_unavailable</c>.</param>
/// <param name="MaxReportEntries">Потолок числа записей отчёта (сверх него - признак усечения).</param>
/// <param name="BatchSize">Размер пачки (тот же <c>Batch__MaxSize</c>, что у рабочего цикла).</param>
/// <param name="MaxAttempts">Бюджет попыток по ошибке, привязанной к сообщению (тот же <c>Dlq__MaxAttempts</c>).</param>
public sealed record ReprocessSettings(int MaxRateLimit, TimeSpan MaxUnavailable, int MaxReportEntries, int BatchSize, int MaxAttempts)
{
    public const int DefaultMaxReportEntries = 100_000;
}

/// <summary>Проверка аргументов запроса (ADR-0006 п. 2.6, 2.7): все ошибки сразу, по полям; ничего не читает и не пишет.</summary>
public static class ReprocessRequestValidator
{
    /// <summary>Предел длины <c>reason</c>: коды причин проекта короткие латинские (правила - до 64 символов).</summary>
    public const int MaxReasonLength = 64;

    private static bool HasLineBreakingCategory(char c) =>
        char.GetUnicodeCategory(c) is UnicodeCategory.Control or UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator;

    public static IReadOnlyList<ReprocessValidationError> Validate(ReprocessRequest request, int maxRateLimit)
    {
        ArgumentNullException.ThrowIfNull(request);

        var errors = new List<ReprocessValidationError>();
        if (request.Topic is not null && (request.Source != ReprocessSource.Raw || !RawTopicName.IsValid(request.Topic)))
        {
            errors.Add(new ReprocessValidationError("topic", "topic must be a raw source topic and is allowed only for source raw."));
        }
        if (!Enum.IsDefined(request.Source))
        {
            errors.Add(new ReprocessValidationError("source", "Unknown source. Allowed: dlq, rejected, raw."));
        }

        if (request.From is { } from && from.Offset != TimeSpan.Zero)
        {
            errors.Add(new ReprocessValidationError("from", "from must be a UTC time (RFC 3339 with the suffix Z)."));
        }

        if (request.To is { } to && to.Offset != TimeSpan.Zero)
        {
            errors.Add(new ReprocessValidationError("to", "to must be a UTC time (RFC 3339 with the suffix Z)."));
        }

        if (request.From is { } start && request.To is { } end && start >= end)
        {
            errors.Add(new ReprocessValidationError("from", "from must be earlier than to."));
        }

        if (request.Source == ReprocessSource.Raw)
        {
            if (request.From is null)
            {
                errors.Add(new ReprocessValidationError("from", "from is required for source raw."));
            }

            if (request.To is null)
            {
                errors.Add(new ReprocessValidationError("to", "to is required for source raw."));
            }

            if (request.Reason is not null)
            {
                errors.Add(new ReprocessValidationError("reason", "reason is not allowed for source raw."));
            }
        }
        else if (request.Reason is not null)
        {
            if (string.IsNullOrWhiteSpace(request.Reason))
            {
                errors.Add(new ReprocessValidationError("reason", "reason must not be blank."));
            }
            else if (request.Reason.Length > MaxReasonLength)
            {
                errors.Add(new ReprocessValidationError("reason", $"reason must not be longer than {MaxReasonLength.ToString(CultureInfo.InvariantCulture)} characters."));
            }
            else if (request.Reason.Any(HasLineBreakingCategory))
            {
                // Значение в тексте ошибки не повторяется: оно уходит в лог запуска, и многострочная запись его бы исказила.
                errors.Add(new ReprocessValidationError("reason", "reason must not contain control characters."));
            }
        }

        if (request.MaxRate < 1 || request.MaxRate > maxRateLimit)
        {
            errors.Add(new ReprocessValidationError("maxRate", $"maxRate must be between 1 and {maxRateLimit.ToString(CultureInfo.InvariantCulture)}."));
        }

        return errors;
    }

    /// <summary>
    /// Разбор времени запроса: только RFC 3339 в UTC с суффиксом <c>Z</c> (смещение вида <c>+03:00</c> и время без зоны отвергаются).
    /// Для admin-слоя, который получает время строкой.
    /// </summary>
    public static bool TryParseUtc(string? value, out DateTimeOffset result)
    {
        result = default;
        if (string.IsNullOrEmpty(value) || value[^1] != 'Z')
        {
            return false;
        }

        if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
        {
            return false;
        }

        result = parsed;
        return true;
    }
}

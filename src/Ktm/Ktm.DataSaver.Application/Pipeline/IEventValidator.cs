using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Pipeline;

/// <summary>Результат проверки события портом C5: принять либо отбраковать с причиной.</summary>
public sealed record ValidationResult
{
    private ValidationResult() { }

    public static ValidationResult Accept { get; } = new();

    public string? Reason { get; private init; }
    public string? Detail { get; private init; }

    public bool IsAccepted => Reason is null;

    public static ValidationResult Reject(string reason, string detail)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new ValidationResult { Reason = reason, Detail = detail };
    }
}

/// <summary>Порт C5 (проверка событий). Исключение порта - сбой пачки, а не отбраковка.</summary>
public interface IEventValidator
{
    ValidationResult Validate(ParsedEvent parsed);
}

/// <summary>Заглушка до появления C5: принимает любое событие, по <c>eventType</c> не отбраковывает.</summary>
public sealed class AcceptAllEventValidator : IEventValidator
{
    public ValidationResult Validate(ParsedEvent parsed) => ValidationResult.Accept;
}

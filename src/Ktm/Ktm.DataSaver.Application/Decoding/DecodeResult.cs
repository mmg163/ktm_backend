using Ktm.DataSaver.Application.Model;

namespace Ktm.DataSaver.Application.Decoding;

/// <summary>Результат разбора: либо событие, либо причина отбраковки формата (<see cref="RejectReasons"/>) с пояснением.</summary>
public sealed record DecodeResult
{
    private DecodeResult() { }

    public ParsedEvent? Event { get; private init; }
    public string? Reason { get; private init; }
    public string? Detail { get; private init; }

    public bool IsSuccess => Event is not null;

    public static DecodeResult Success(ParsedEvent parsed) => new() { Event = parsed };

    public static DecodeResult Failure(string reason, string detail) => new() { Reason = reason, Detail = detail };
}

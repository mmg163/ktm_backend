namespace Ktm.DataSaver.Core.Model;

/// <summary>Ссылка корректирующего сообщения на исходное событие (источник берётся из самого сообщения).</summary>
public sealed record CorrectionRef(string CorrectsSourceEventId, DateTimeOffset CorrectsOccurredAt, string? Reason);

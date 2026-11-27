namespace Ktm.DataSaver.Core.Model;

/// <summary>Ключ идемпотентности события: источник + ID события + время (docs/05, п. 5.1).</summary>
public readonly record struct EventKey(string Source, string SourceEventId, DateTimeOffset OccurredAt);

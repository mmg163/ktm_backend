namespace Ktm.DataSaver.Application.Model;

/// <summary>Происхождение события из конверта, прошедшего разбор формата: для структурных полей лога отбраковки.</summary>
public sealed record EventOrigin(string Source, string SourceEventId);

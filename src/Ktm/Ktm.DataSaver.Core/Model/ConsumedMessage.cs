namespace Ktm.DataSaver.Core.Model;

/// <summary>Сообщение, прочитанное из топика: тело и ключ - ровно как получены, без интерпретации.</summary>
public sealed record ConsumedMessage(
    string Topic,
    int Partition,
    long Offset,
    ReadOnlyMemory<byte> Key,
    ReadOnlyMemory<byte> Body,
    DateTimeOffset Timestamp);

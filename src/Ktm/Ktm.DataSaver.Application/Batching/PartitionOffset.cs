namespace Ktm.DataSaver.Core.Batching;

/// <summary>Позиция сообщения в Kafka: топик, партиция, offset.</summary>
public readonly record struct PartitionOffset(string Topic, int Partition, long Offset);

/// <summary>Партиция топика.</summary>
public readonly record struct TopicPartitionKey(string Topic, int Partition);

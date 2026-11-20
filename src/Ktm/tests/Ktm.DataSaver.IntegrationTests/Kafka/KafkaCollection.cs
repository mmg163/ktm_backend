namespace Ktm.DataSaver.IntegrationTests.Kafka;

[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaFixture>
{
    public const string Name = "kafka";
}

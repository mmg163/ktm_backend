using Confluent.Kafka;
using Ktm.DataSaver.Infrastructure.Kafka;

namespace Ktm.DataSaver.Tests.Kafka;

public sealed class KafkaConfigParsingTests
{
    [Theory]
    [InlineData("PLAINTEXT", SecurityProtocol.Plaintext)]
    [InlineData("ssl", SecurityProtocol.Ssl)]
    [InlineData("SASL_PLAINTEXT", SecurityProtocol.SaslPlaintext)]
    [InlineData("Sasl_Ssl", SecurityProtocol.SaslSsl)]
    public void Documented_security_protocol_names_are_mapped(string name, SecurityProtocol expected) =>
        Assert.Equal(expected, KafkaConfigExtensions.ParseSecurityProtocol(name));

    [Theory]
    [InlineData("PLAIN", SaslMechanism.Plain)]
    [InlineData("scram-sha-256", SaslMechanism.ScramSha256)]
    [InlineData("SCRAM-SHA-512", SaslMechanism.ScramSha512)]
    [InlineData("GSSAPI", SaslMechanism.Gssapi)]
    [InlineData("OAUTHBEARER", SaslMechanism.OAuthBearer)]
    public void Documented_sasl_mechanism_names_are_mapped(string name, SaslMechanism expected) =>
        Assert.Equal(expected, KafkaConfigExtensions.ParseSaslMechanism(name));

    [Fact]
    public void Unknown_values_fail_with_setting_name_and_allowed_list()
    {
        var protocol = Assert.Throws<ArgumentException>(() => KafkaConfigExtensions.ParseSecurityProtocol("SaslSsl"));
        var mechanism = Assert.Throws<ArgumentException>(() => KafkaConfigExtensions.ParseSaslMechanism("MD5"));

        Assert.Contains("Kafka__SecurityProtocol", protocol.Message);
        Assert.Contains("SASL_SSL", protocol.Message);
        Assert.Contains("Kafka__SaslMechanism", mechanism.Message);
        Assert.Contains("SCRAM-SHA-256", mechanism.Message);
    }
}

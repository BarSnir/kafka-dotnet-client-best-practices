using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Shared;

public sealed record AppConfig(string BootstrapServers, string KafkaKey, string KafkaSecret, string SchemaRegistryUrl, string SchemaRegistryKey, string SchemaRegistrySecret)
{
    public static AppConfig FromEnvironment() => new(
        Required("KAFKA_BOOTSTRAP_SERVERS"), Required("KAFKA_API_KEY"), Required("KAFKA_API_SECRET"),
        Required("SCHEMA_REGISTRY_URL"), Required("SCHEMA_REGISTRY_API_KEY"), Required("SCHEMA_REGISTRY_API_SECRET"));

    public ClientConfig Kafka(string clientId) => new()
    {
        BootstrapServers = BootstrapServers,
        SecurityProtocol = SecurityProtocol.SaslSsl,
        SaslMechanism = SaslMechanism.Plain,
        SaslUsername = KafkaKey,
        SaslPassword = KafkaSecret,
        ClientId = $"{Environment.GetEnvironmentVariable("KAFKA_CLIENT_ID") ?? clientId}-{Environment.MachineName}",
        StatisticsIntervalMs = 10000,
        ApiVersionRequestTimeoutMs = 30000
    };

    public SchemaRegistryConfig SchemaRegistry() => new()
    {
        Url = SchemaRegistryUrl,
        BasicAuthUserInfo = $"{SchemaRegistryKey}:{SchemaRegistrySecret}",
        BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo
    };

    private static string Required(string name) => Environment.GetEnvironmentVariable(name)
        ?? throw new InvalidOperationException($"Missing required environment variable {name}.");
}

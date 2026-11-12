using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Shared;

internal static class ConsumerFactory
{
    public static IConsumer<string, GenericRecord> Create(AppConfig config, ISchemaRegistryClient schemaRegistry, Action<string> publish, Action<Error> handleError)
    {
        var consumerConfig = new ConsumerConfig(config.Kafka("orders-consumer"))
        {
            GroupId = "orders-demo",
            GroupProtocol = GroupProtocol.Consumer,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false
        };
        return new ConsumerBuilder<string, GenericRecord>(consumerConfig)
            .SetValueDeserializer(new BlockingDeserializer<GenericRecord>(new AvroDeserializer<GenericRecord>(schemaRegistry, new AvroDeserializerConfig
            {
                SubjectNameStrategy = SubjectNameStrategy.Topic
            })))
            .SetStatisticsHandler((_, statistics) => publish(statistics))
            .SetErrorHandler((_, error) => handleError(error))
            .Build();
    }
}

sealed class BlockingDeserializer<T>(IAsyncDeserializer<T> inner) : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        inner.DeserializeAsync(data.ToArray(), isNull, context).GetAwaiter().GetResult();
}

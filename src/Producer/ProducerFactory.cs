using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Shared;

internal static class ProducerFactory
{
    public static IProducer<string, GenericRecord> Create(AppConfig config, ISchemaRegistryClient schemaRegistry, Action<string> publish, Action<Error> handleError)
    {
        var producerConfig = new ProducerConfig(config.Kafka("orders-producer"))
        {
            StatisticsIntervalMs = 1000,
            EnableIdempotence = true,
            Acks = Acks.All,
            MessageTimeoutMs = 30000
        };
        return new ProducerBuilder<string, GenericRecord>(producerConfig)
            .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                SubjectNameStrategy = SubjectNameStrategy.Topic
            }))
            .SetStatisticsHandler((_, statistics) => publish(statistics))
            .SetErrorHandler((_, error) => handleError(error))
            .Build();
    }
}

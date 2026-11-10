using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Shared;
using System.Runtime.InteropServices;

var config = AppConfig.FromEnvironment();
var printEvery = int.TryParse(Environment.GetEnvironmentVariable("PRINT_EVERY"), out var printValue) ? Math.Max(1, printValue) : 1;
long processed = 0;
using var stop = new CancellationTokenSource();
var fatalError = false;
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; stop.Cancel(); };
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => stop.Cancel());

using var metrics = new KafkaMetrics("consumer");
using var schemaRegistry = new CachedSchemaRegistryClient(config.SchemaRegistry());
var consumerConfig = new ConsumerConfig(config.Kafka("orders-consumer"))
{
    GroupId = "orders-demo",
    GroupProtocol = GroupProtocol.Consumer,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = true,
    EnableAutoOffsetStore = false
};
using var consumer = new ConsumerBuilder<string, GenericRecord>(consumerConfig)
    .SetValueDeserializer(new BlockingDeserializer<GenericRecord>(new AvroDeserializer<GenericRecord>(schemaRegistry, new AvroDeserializerConfig
    {
        SubjectNameStrategy = SubjectNameStrategy.Topic
    })))
    .SetStatisticsHandler((_, statistics) => metrics.Publish(statistics))
    .SetErrorHandler((_, error) =>
    {
        Console.Error.WriteLine(error.IsFatal ? $"fatal Kafka error: {error.Reason}" : $"recoverable Kafka notification: {error.Reason}");
        if (error.IsFatal) { fatalError = true; stop.Cancel(); }
    })
    .Build();

consumer.Subscribe("orders");
var failed = false;
try
{
    while (!stop.IsCancellationRequested)
    {
        ConsumeResult<string, GenericRecord> result;
        try { result = consumer.Consume(stop.Token); }
        catch (ConsumeException exception)
        {
            var record = exception.ConsumerRecord;
            if (record is null && !exception.Error.IsFatal)
            {
                Console.Error.WriteLine($"recoverable consume error: {exception.Error.Reason}");
                continue;
            }
            Console.Error.WriteLine($"consume failure at {record?.TopicPartitionOffset}: {exception.Error.Reason}");
            failed = true;
            break;
        }

        try
        {
            var order = result.Message.Value;
            if (Interlocked.Increment(ref processed) % printEvery == 0)
                Console.WriteLine($"orderId={order["orderId"]} customerId={order["customerId"]} amountCents={order["amountCents"]} payloadBytes={((byte[])order["payload"]).Length} partition={result.Partition.Value} offset={result.Offset.Value}");
            consumer.StoreOffset(result);
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"processing failure at {result.TopicPartitionOffset}: {exception.Message}");
            failed = true;
            break;
        }
    }
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
finally { consumer.Close(); }
if (failed || fatalError) Environment.ExitCode = 1;

sealed class BlockingDeserializer<T>(IAsyncDeserializer<T> inner) : IDeserializer<T>
{
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
        inner.DeserializeAsync(data.ToArray(), isNull, context).GetAwaiter().GetResult();
}

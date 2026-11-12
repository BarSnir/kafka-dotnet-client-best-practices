using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Shared;
using System.Runtime.InteropServices;

var config = AppConfig.FromEnvironment();
var workload = WorkloadOptions.Load();
long processed = 0;
using var stop = new CancellationTokenSource();
var fatalError = false;
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; stop.Cancel(); };
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, signal => { signal.Cancel = true; stop.Cancel(); });

using var metrics = new KafkaMetrics("consumer");
using var schemaRegistry = new CachedSchemaRegistryClient(config.SchemaRegistry());
using var consumer = ConsumerFactory.Create(config, schemaRegistry, metrics.Publish, error =>
{
    Console.Error.WriteLine($"Kafka notification code={error.Code} fatal={error.IsFatal}: {error.Reason}");
    if (error.IsFatal) { fatalError = true; stop.Cancel(); }
});

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
            if (record is null && !exception.Error.IsFatal && !KafkaFailures.RequiresIntervention(exception.Error.Code))
            {
                Console.Error.WriteLine($"recoverable consume error: {KafkaFailures.Describe(exception)}");
                continue;
            }
            Console.Error.WriteLine($"consume failure at {record?.TopicPartitionOffset}: {KafkaFailures.Describe(exception)}");
            failed = true;
            break;
        }

        try
        {
            var order = result.Message.Value;
            if (Interlocked.Increment(ref processed) % workload.PrintEvery == 0)
                Console.WriteLine($"orderId={order["orderId"]} customerId={order["customerId"]} amountCents={order["amountCents"]} payloadBytes={(order.TryGetValue("payload", out var payload) && payload is byte[] bytes ? bytes.Length : 0)} partition={result.Partition.Value} offset={result.Offset.Value}");
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

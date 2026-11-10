using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Shared;
using System.Runtime.InteropServices;

var config = AppConfig.FromEnvironment();
var eventsPerSecond = int.TryParse(Environment.GetEnvironmentVariable("EVENTS_PER_SECOND"), out var value) ? Math.Max(1, value) : 10;
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; stop.Cancel(); };
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ => stop.Cancel());

using var metrics = new KafkaMetrics("producer");
using var schemaRegistry = new CachedSchemaRegistryClient(config.SchemaRegistry());
var producerConfig = new ProducerConfig(config.Kafka("orders-producer"))
{
    EnableIdempotence = true,
    Acks = Acks.All,
    MessageTimeoutMs = 30000
};
using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry, new AvroSerializerConfig
    {
        AutoRegisterSchemas = false,
        SubjectNameStrategy = SubjectNameStrategy.Topic
    }))
    .SetStatisticsHandler((_, statistics) => metrics.Publish(statistics))
    .SetErrorHandler((_, error) =>
    {
        Console.Error.WriteLine(error.IsFatal ? $"fatal Kafka error: {error.Reason}" : $"recoverable Kafka notification: {error.Reason}");
        if (error.IsFatal) stop.Cancel();
    })
    .Build();

var outstanding = new SemaphoreSlim(100);
var deliveries = new List<Task>();
try
{
    while (!stop.IsCancellationRequested)
    {
        await outstanding.WaitAsync(stop.Token);
        var order = OrderFactory.Create();
        var orderId = (string)order["orderId"];
        var customerId = (string)order["customerId"];
        var delivery = producer.ProduceAsync("orders", new Message<string, GenericRecord> { Key = customerId, Value = order })
            .ContinueWith(task =>
            {
                outstanding.Release();
                if (task.IsFaulted)
                    Console.Error.WriteLine($"final delivery failure for order {orderId}: {task.Exception?.GetBaseException().Message}");
                else
                    Console.WriteLine($"delivered orderId={orderId} partition={task.Result.Partition.Value} offset={task.Result.Offset.Value}");
            }, TaskScheduler.Default);
        deliveries.Add(delivery);
        if (deliveries.Count > 1000) deliveries.RemoveAll(task => task.IsCompleted);
        await Task.Delay(TimeSpan.FromSeconds(1d / eventsPerSecond), stop.Token);
    }
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
finally
{
    var drain = Task.WhenAll(deliveries);
    await Task.WhenAny(drain, Task.Delay(TimeSpan.FromSeconds(30)));
    producer.Flush(TimeSpan.FromSeconds(5));
}
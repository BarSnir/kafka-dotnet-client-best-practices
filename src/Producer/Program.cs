using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Shared;
using System.Runtime.InteropServices;

var config = AppConfig.FromEnvironment();
var eventsPerSecond = int.TryParse(Environment.GetEnvironmentVariable("EVENTS_PER_SECOND"), out var value) ? Math.Max(1, value) : 10;
using var stop = new CancellationTokenSource();
var fatalError = false;
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
        if (error.IsFatal) { fatalError = true; stop.Cancel(); }
    })
    .Build();

using var outstanding = new SemaphoreSlim(100);
long succeeded = 0;
long failed = 0;
try
{
    while (!stop.IsCancellationRequested)
    {
        await outstanding.WaitAsync(stop.Token);
        var order = OrderFactory.Create();
        var orderId = (string)order["orderId"];
        var customerId = (string)order["customerId"];
        Task<DeliveryResult<string, GenericRecord>> send;
        try { send = producer.ProduceAsync("orders", new Message<string, GenericRecord> { Key = customerId, Value = order }); }
        catch
        {
            outstanding.Release();
            throw;
        }
        _ = send
            .ContinueWith(task =>
            {
                try
                {
                    if (task.IsFaulted || task.IsCanceled)
                    {
                        Interlocked.Increment(ref failed);
                        Console.Error.WriteLine($"final delivery failure for order {orderId}: {task.Exception?.GetBaseException().Message ?? "cancelled"}");
                    }
                    else
                    {
                        Interlocked.Increment(ref succeeded);
                        Console.WriteLine($"delivered orderId={orderId} partition={task.Result.Partition.Value} offset={task.Result.Offset.Value}");
                    }
                }
                finally { outstanding.Release(); }
            }, TaskScheduler.Default);
        await Task.Delay(TimeSpan.FromSeconds(1d / eventsPerSecond), stop.Token);
    }
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
finally
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var acquired = 0;
    try
    {
        for (; acquired < 100; acquired++) await outstanding.WaitAsync(timeout.Token);
    }
    catch (OperationCanceledException) when (timeout.IsCancellationRequested) { }
    var pending = 100 - acquired - outstanding.CurrentCount;
    Console.WriteLine($"delivery summary: succeeded={Interlocked.Read(ref succeeded)} failed={Interlocked.Read(ref failed)} pending={pending}");
    if (pending > 0 || Interlocked.Read(ref failed) > 0 || fatalError) Environment.ExitCode = 1;
}

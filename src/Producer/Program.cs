using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Shared;
using System.Runtime.InteropServices;

var config = AppConfig.FromEnvironment();
var workload = WorkloadOptions.Load();
using var stop = new CancellationTokenSource();
var fatalError = false;
Console.CancelKeyPress += (_, eventArgs) => { eventArgs.Cancel = true; stop.Cancel(); };
using var sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, signal => { signal.Cancel = true; stop.Cancel(); });

using var metrics = new KafkaMetrics("producer");
using var schemaRegistry = new CachedSchemaRegistryClient(config.SchemaRegistry());
using var producer = ProducerFactory.Create(config, schemaRegistry, metrics.Publish, error =>
{
    Console.Error.WriteLine($"Kafka notification code={error.Code} fatal={error.IsFatal}: {error.Reason}");
    if (error.IsFatal) { fatalError = true; stop.Cancel(); }
});

const int maxOutstanding = 100;
var deliveries = new List<Task>(maxOutstanding);
long succeeded = 0;
long failed = 0;

async Task DeliverAsync(GenericRecord order)
{
    var id = (string)order["orderId"];
    try
    {
        var message = new Message<string, GenericRecord> { Key = (string)order["customerId"], Value = order };
        var result = await producer.ProduceAsync("orders", message);
        if (Interlocked.Increment(ref succeeded) % workload.PrintEvery == 0)
            Console.WriteLine($"delivered orderId={id} partition={result.Partition.Value} offset={result.Offset.Value}");
    }
    catch (ProduceException<string, GenericRecord> ex)
    {
        Interlocked.Increment(ref failed);
        Console.Error.WriteLine($"orderId={id} status={ex.DeliveryResult?.Status} {KafkaFailures.Describe(ex)}");
        if (ex.Error.IsFatal || KafkaFailures.RequiresIntervention(ex.Error.Code)) stop.Cancel();
    }
}

var interval = TimeSpan.FromSeconds(1d / workload.EventsPerSecond);
var clock = System.Diagnostics.Stopwatch.StartNew();
var nextSend = TimeSpan.Zero;
try
{
    while (!stop.IsCancellationRequested)
    {
        if (deliveries.Count == maxOutstanding)
        {
            await Task.WhenAny(deliveries).WaitAsync(stop.Token);
            foreach (var completed in deliveries.Where(task => task.IsCompleted).ToArray())
            {
                await completed;
                deliveries.Remove(completed);
            }
        }
        deliveries.Add(DeliverAsync(OrderFactory.Create(workload.PayloadBytes, workload.HotPartition)));
        nextSend += interval;
        var delay = nextSend - clock.Elapsed;
        if (delay > TimeSpan.Zero) await Task.Delay(delay, stop.Token);
        else nextSend = clock.Elapsed;
    }
}
catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
finally
{
    try { await Task.WhenAll(deliveries).WaitAsync(TimeSpan.FromSeconds(30)); }
    catch (TimeoutException) { Environment.ExitCode = 1; }
    var pending = deliveries.Count(task => !task.IsCompleted);
    Console.WriteLine($"delivery summary: succeeded={succeeded} failed={failed} pending={pending}");
    if (pending > 0 || failed > 0 || fatalError) Environment.ExitCode = 1;
}

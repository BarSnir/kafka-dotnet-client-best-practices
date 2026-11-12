using System.Diagnostics;
using Avro.Generic;
using Confluent.Kafka;
using Shared;

internal sealed class OrderPublisher(
    IProducer<string, GenericRecord> producer,
    string topic,
    OrderFactory orders,
    WorkloadOptions workload,
    ProducerShutdown shutdown)
{
    private const int MaxOutstanding = 100;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);
    private readonly List<Task> deliveries = new(MaxOutstanding);
    private long succeeded;
    private long failed;

    public async Task<int> RunAsync()
    {
        bool drained;
        try
        {
            await PublishOrdersAsync(shutdown.Token);
        }
        catch (OperationCanceledException) when (shutdown.Token.IsCancellationRequested)
        {
            // Stop accepting orders and finish deliveries already in progress.
        }
        finally
        {
            drained = await DrainAsync();
        }

        return drained && failed == 0 && !shutdown.HasFailed ? 0 : 1;
    }

    private async Task PublishOrdersAsync(CancellationToken token)
    {
        var interval = TimeSpan.FromSeconds(1d / workload.EventsPerSecond);
        var clock = Stopwatch.StartNew();
        var nextSend = TimeSpan.Zero;

        while (!token.IsCancellationRequested)
        {
            if (deliveries.Count == MaxOutstanding) await WaitForCapacityAsync(token);
            var order = orders.Create(workload.PayloadBytes, workload.HotPartition);
            deliveries.Add(DeliverAsync(order));

            nextSend += interval;
            var delay = nextSend - clock.Elapsed;
            if (delay > TimeSpan.Zero) await Task.Delay(delay, token);
            else nextSend = clock.Elapsed;
        }
    }

    private async Task WaitForCapacityAsync(CancellationToken token)
    {
        await Task.WhenAny(deliveries).WaitAsync(token);
        var finished = deliveries.Where(task => task.IsCompleted).ToArray();
        await Task.WhenAll(finished);
        foreach (var task in finished) deliveries.Remove(task);
        token.ThrowIfCancellationRequested();
    }

    private async Task DeliverAsync(Message<string, GenericRecord> order)
    {
        var id = (string)order.Value["orderId"];
        try
        {
            var result = await producer.ProduceAsync(topic, order);
            if (Interlocked.Increment(ref succeeded) % workload.PrintEvery == 0)
                Console.WriteLine($"delivered orderId={id} partition={result.Partition.Value} offset={result.Offset.Value}");
        }
        catch (ProduceException<string, GenericRecord> error)
        {
            Interlocked.Increment(ref failed);
            Console.Error.WriteLine($"orderId={id} status={error.DeliveryResult?.Status} {KafkaFailures.Describe(error)}");
            if (error.Error.IsFatal || KafkaFailures.RequiresIntervention(error.Error.Code))
                shutdown.StopWithFailure();
        }
    }

    private async Task<bool> DrainAsync()
    {
        try
        {
            await Task.WhenAll(deliveries).WaitAsync(DrainTimeout);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
        finally
        {
            var pending = deliveries.Count(task => !task.IsCompleted);
            Console.WriteLine($"delivery summary: succeeded={Interlocked.Read(ref succeeded)} failed={Interlocked.Read(ref failed)} pending={pending}");
        }
    }
}

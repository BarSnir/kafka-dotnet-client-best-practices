using System.Diagnostics;
using Avro.Generic;
using Confluent.Kafka;
using Shared;

internal sealed class OrderPublisher(
    IProducer<string, GenericRecord> producer,
    string topic,
    OrderFactory orders,
    WorkloadOptions workload,
    ProducerShutdown shutdown,
    KafkaMetrics metrics)
{
    private const int MaxOutstanding = 100;
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromSeconds(30);
    private readonly List<Task> deliveries = new(MaxOutstanding);
    private readonly object throughputGate = new();
    private readonly Stopwatch throughputClock = Stopwatch.StartNew();
    private long succeeded;
    private long failed;
    private long deliveredSinceReport;

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
        var targets = GetTargets();
        var secondsPerRecord = workload.TargetMbps > 0
            ? workload.PayloadBytes * 8d / (workload.TargetMbps * 1_000_000d)
            : 1d / workload.EventsPerSecond;
        var interval = TimeSpan.FromSeconds(secondsPerRecord);
        var targetIndex = 0;
        var clock = Stopwatch.StartNew();
        var nextSend = TimeSpan.Zero;

        while (!token.IsCancellationRequested)
        {
            if (deliveries.Count == MaxOutstanding) await WaitForCapacityAsync(token);
            var order = orders.Create(workload.PayloadBytes, workload.HotPartition);
            deliveries.Add(DeliverAsync(targets[targetIndex], order));
            targetIndex = (targetIndex + 1) % targets.Length;

            nextSend += interval;
            var delay = nextSend - clock.Elapsed;
            if (delay > TimeSpan.Zero) await Task.Delay(delay, token);
            else nextSend = clock.Elapsed;
        }
    }

    private TopicPartition[] GetTargets()
    {
        if (workload.TargetMbps == 0) return [new TopicPartition(topic, Partition.Any)];
        if (workload.PayloadBytes == 0) throw new ArgumentException("Per-partition pacing requires a positive PAYLOAD_BYTES.");

        using var admin = new DependentAdminClientBuilder(producer.Handle).Build();
        var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(30)).Topics.Single(t => t.Topic == topic);
        if (metadata.Error.IsError) throw new KafkaException(metadata.Error);
        if (metadata.Partitions.Count == 0) throw new InvalidOperationException($"No partitions found for {topic}.");
        return metadata.Partitions.Select(p => new TopicPartition(topic, new Partition(p.PartitionId))).ToArray();
    }

    private async Task WaitForCapacityAsync(CancellationToken token)
    {
        await Task.WhenAny(deliveries).WaitAsync(token);
        var finished = deliveries.Where(task => task.IsCompleted).ToArray();
        await Task.WhenAll(finished);
        foreach (var task in finished) deliveries.Remove(task);
        token.ThrowIfCancellationRequested();
    }

    private async Task DeliverAsync(TopicPartition target, Message<string, GenericRecord> order)
    {
        var id = (string)order.Value["orderId"];
        try
        {
            var result = await producer.ProduceAsync(target, order);
            if (workload.PayloadBytes > 0)
            {
                metrics.RecordDeliveredPayload(result.Partition.Value, workload.PayloadBytes);
                ReportThroughput(workload.PayloadBytes);
            }
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

    private void ReportThroughput(int bytes)
    {
        lock (throughputGate)
        {
            deliveredSinceReport += bytes;
            if (throughputClock.Elapsed < TimeSpan.FromSeconds(1)) return;
            var megabytesPerSecond = deliveredSinceReport / throughputClock.Elapsed.TotalSeconds / 1_000_000d;
            Console.WriteLine(FormattableString.Invariant($"payload_MB/s={megabytesPerSecond:F4}"));
            deliveredSinceReport = 0;
            throughputClock.Restart();
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

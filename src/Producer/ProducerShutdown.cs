using System.Runtime.InteropServices;
using Confluent.Kafka;

internal sealed class ProducerShutdown : IDisposable
{
    private readonly CancellationTokenSource stop = new();
    private readonly PosixSignalRegistration sigterm;
    private int failure;

    public ProducerShutdown()
    {
        sigterm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, signal =>
        {
            signal.Cancel = true;
            stop.Cancel();
        });
        Console.CancelKeyPress += OnCancelKeyPress;
    }

    public CancellationToken Token => stop.Token;
    public bool HasFailed => Volatile.Read(ref failure) != 0;

    public void OnKafkaError(Error error)
    {
        Console.Error.WriteLine($"Kafka notification code={error.Code} fatal={error.IsFatal}: {error.Reason}");
        if (error.IsFatal) StopWithFailure();
    }

    public void StopWithFailure()
    {
        Interlocked.Exchange(ref failure, 1);
        stop.Cancel();
    }

    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs args)
    {
        args.Cancel = true;
        stop.Cancel();
    }

    public void Dispose()
    {
        Console.CancelKeyPress -= OnCancelKeyPress;
        sigterm.Dispose();
        stop.Dispose();
    }
}

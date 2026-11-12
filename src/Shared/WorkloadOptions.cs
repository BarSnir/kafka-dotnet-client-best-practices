namespace Shared;

public sealed record WorkloadOptions(int EventsPerSecond, int PayloadBytes, bool HotPartition, int PrintEvery, int TargetMbps = 0)
{
    public static WorkloadOptions Load()
    {
        var rawHot = Environment.GetEnvironmentVariable("HOT_PARTITION");
        var hot = false;
        if (!string.IsNullOrWhiteSpace(rawHot) && !bool.TryParse(rawHot, out hot))
            throw new ArgumentException("HOT_PARTITION must be true or false.");

        return new WorkloadOptions(
            ReadInt("EVENTS_PER_SECOND", 10, 1, 1000),
            ReadInt("PAYLOAD_BYTES", 0, 0, 524288),
            hot, ReadInt("PRINT_EVERY", 1, 1, int.MaxValue),
            ReadInt("TARGET_MBPS", 0, 0, 1000));
    }

    private static int ReadInt(string name, int fallback, int min, int max)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw)) return fallback;
        if (int.TryParse(raw, out var value) && value >= min && value <= max) return value;
        throw new ArgumentException($"{name} must be between {min} and {max}.");
    }
}

namespace Shared;

public sealed record WorkloadOptions(int EventsPerSecond, int PayloadBytes, bool HotPartition, int PrintEvery)
{
    public static WorkloadOptions Load()
    {
        var defaults = (Environment.GetEnvironmentVariable("LOAD_PROFILE") ?? "standard") switch
        {
            "standard" => new WorkloadOptions(10, 0, false, 1),
            "high-throughput" => new WorkloadOptions(100, 131072, true, 100),
            "throttle" => new WorkloadOptions(200, 131072, true, 100),
            var name => throw new ArgumentException($"Unknown LOAD_PROFILE '{name}'.")
        };
        var rawHot = Environment.GetEnvironmentVariable("HOT_PARTITION");
        var hot = defaults.HotPartition;
        if (!string.IsNullOrWhiteSpace(rawHot) && !bool.TryParse(rawHot, out hot))
            throw new ArgumentException("HOT_PARTITION must be true or false.");

        return new WorkloadOptions(
            ReadInt("EVENTS_PER_SECOND", defaults.EventsPerSecond, 1, 1000),
            ReadInt("PAYLOAD_BYTES", defaults.PayloadBytes, 0, 524288),
            hot, ReadInt("PRINT_EVERY", defaults.PrintEvery, 1, int.MaxValue));
    }

    private static int ReadInt(string name, int fallback, int min, int max)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrWhiteSpace(raw)) return fallback;
        if (int.TryParse(raw, out var value) && value >= min && value <= max) return value;
        throw new ArgumentException($"{name} must be between {min} and {max}.");
    }
}

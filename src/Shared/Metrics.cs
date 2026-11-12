using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;

namespace Shared;

public sealed class KafkaMetrics : IDisposable
{
    private readonly string _role;
    private readonly string _instance = Environment.MachineName;
    private readonly object _gate = new();
    private string? _latest;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _parser;
    private readonly HttpListener _listener = new();
    private readonly Task _server;
    private Snapshot _snapshot = new();

    public KafkaMetrics(string role)
    {
        _role = role;
        _listener.Prefixes.Add("http://+:8080/");
        _listener.Start();
        _parser = Task.Run(ParseLoop);
        _server = Task.Run(ServerLoop);
    }

    public void Publish(string json)
    {
        lock (_gate) _latest = json;
    }

    private async Task ParseLoop()
    {
        var throughputReported = false;
        while (!_stop.IsCancellationRequested)
        {
            string? json;
            lock (_gate) { json = _latest; _latest = null; }
            if (json is not null)
            {
                try
                {
                    var current = Snapshot.Parse(json, _role);
                    if (_role == "producer" && !throughputReported)
                        throughputReported = current.TryLogStartupThroughput(_snapshot);
                    _snapshot = current;
                }
                catch (JsonException ex) { Console.Error.WriteLine($"statistics parse error: {ex.Message}"); }
            }
            try { await Task.Delay(250, _stop.Token); } catch (OperationCanceledException) { }
        }
    }

    private async Task ServerLoop()
    {
        while (!_stop.IsCancellationRequested)
        {
            try
            {
                var context = await _listener.GetContextAsync();
                var body = Encoding.UTF8.GetBytes(_snapshot.Render(_role, _instance));
                context.Response.ContentType = "text/plain; version=0.0.4";
                context.Response.ContentLength64 = body.Length;
                await context.Response.OutputStream.WriteAsync(body);
                context.Response.Close();
            }
            catch (HttpListenerException) when (_stop.IsCancellationRequested) { }
        }
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        try { Task.WaitAll([_parser, _server], TimeSpan.FromSeconds(2)); } catch (AggregateException) { }
        _listener.Close();
        _stop.Dispose();
    }

    private sealed class Snapshot
    {
        private double _timestampUs;

        public bool TryLogStartupThroughput(Snapshot previous)
        {
            // The first window with deliveries can include a startup backlog.
            if (previous._timestampUs <= 0 || previous._values["txmsgs"] == 0) return false;
            var elapsedUs = _timestampUs - previous._timestampUs;
            var bytes = _values["txmsg_bytes"] - previous._values["txmsg_bytes"];
            if (elapsedUs <= 0 || bytes <= 0) return false;

            var megabytesPerSecond = bytes / elapsedUs; // bytes/µs equals decimal MB/s
            Console.WriteLine(FormattableString.Invariant($"payload_MB/s={megabytesPerSecond:F4}"));
            return true;
        }

        private string _clientId = "";
        private readonly Dictionary<string, double> _values = [];
        private readonly Dictionary<string, double> _brokerRetries = [];
        private readonly Dictionary<string, double> _brokerRtt = [];
        private readonly Dictionary<string, double> _brokerThrottle = [];
        private readonly Dictionary<string, double> _partitionBytes = [];
        private readonly Dictionary<string, double> _lags = [];

        public static Snapshot Parse(string json, string role)
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            var snapshot = new Snapshot { _timestampUs = Number(root, "ts") };
            snapshot._values["tx_bytes"] = Number(root, "tx_bytes");
            if (root.TryGetProperty("client_id", out var id)) snapshot._clientId = id.GetString() ?? "";
            snapshot._values["txmsgs"] = Number(root, "txmsgs");
            snapshot._values["txmsg_bytes"] = Number(root, "txmsg_bytes");
            snapshot._values["msg_cnt"] = Number(root, "msg_cnt");
            snapshot._values["rxmsgs"] = Number(root, "rxmsgs");
            snapshot._values["rxmsg_bytes"] = Number(root, "rxmsg_bytes");
            if (root.TryGetProperty("cgrp", out var cgrp))
            {
                snapshot._values["rebalance_cnt"] = Number(cgrp, "rebalance_cnt");
                snapshot._values["assignment_size"] = Number(cgrp, "assignment_size");
            }
            if (root.TryGetProperty("brokers", out var brokers))
                foreach (var broker in brokers.EnumerateObject())
                {
                    snapshot._brokerRetries[broker.Name] = Number(broker.Value, "txretries");
                    if (broker.Value.TryGetProperty("rtt", out var rtt) && Number(rtt, "cnt") > 0)
                        snapshot._brokerRtt[broker.Name] = Number(rtt, "p95") / 1000d;
                    if (broker.Value.TryGetProperty("throttle", out var throttle))
                        snapshot._brokerThrottle[broker.Name] = Number(throttle, "cnt") > 0 ? Number(throttle, "avg") : 0;
                }
            if (root.TryGetProperty("topics", out var topics) && topics.TryGetProperty("orders", out var orders) && orders.TryGetProperty("partitions", out var partitions))
                foreach (var partition in partitions.EnumerateObject())
                {
                    if (partition.Name == "-1") continue;
                    if (role == "producer")
                    {
                        snapshot._partitionBytes[partition.Name] = Number(partition.Value, "txbytes");
                        continue;
                    }
                    var lag = Number(partition.Value, "consumer_lag");
                    // Numbered, desired partitions are the consumer's current assignment; -1 is topic-wide.
                    if (partition.Name != "-1" && lag >= 0 && partition.Value.TryGetProperty("fetch_state", out var fetchState) && fetchState.GetString() == "active")
                        snapshot._lags[partition.Name] = lag;
                }
            return snapshot;
        }

        public string Render(string role, string instance)
        {
            var labels = $"client_id=\"{Escape(_clientId)}\",role=\"{role}\",instance=\"{Escape(instance)}\"";
            var builder = new StringBuilder();
            Add(builder, "kafka_client_txmsgs_total", _values.GetValueOrDefault("txmsgs"), labels);
            Add(builder, "kafka_client_txmsg_bytes_total", _values.GetValueOrDefault("txmsg_bytes"), labels);
            Add(builder, "kafka_client_msg_cnt", _values.GetValueOrDefault("msg_cnt"), labels);
            Add(builder, "kafka_client_rxmsgs_total", _values.GetValueOrDefault("rxmsgs"), labels);
            Add(builder, "kafka_client_rxmsg_bytes_total", _values.GetValueOrDefault("rxmsg_bytes"), labels);
            Add(builder, "kafka_client_rebalance_cnt_total", _values.GetValueOrDefault("rebalance_cnt"), labels);
            Add(builder, "kafka_client_assignment_size", _values.GetValueOrDefault("assignment_size"), labels);
            foreach (var item in _brokerRetries) Add(builder, "kafka_client_broker_txretries_total", item.Value, $"{labels},broker=\"{Escape(item.Key)}\"");
            foreach (var item in _brokerRtt) Add(builder, "kafka_client_broker_rtt_p95_ms", item.Value, $"{labels},broker=\"{Escape(item.Key)}\"");
            foreach (var item in _brokerThrottle) Add(builder, "kafka_client_broker_throttle_avg_ms", item.Value, $"{labels},broker=\"{Escape(item.Key)}\"");
            foreach (var item in _partitionBytes) Add(builder, "kafka_client_partition_tx_bytes_total", item.Value, $"{labels},topic=\"orders\",partition=\"{item.Key}\"");
            foreach (var item in _lags) Add(builder, "kafka_client_consumer_lag", item.Value, $"{labels},topic=\"orders\",partition=\"{item.Key}\"");
            return builder.ToString();
        }

        private static double Number(JsonElement element, string property) => element.TryGetProperty(property, out var value) && value.TryGetDouble(out var number) ? number : 0;
        private static string Escape(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");
        private static void Add(StringBuilder builder, string name, double value, string labels) => builder.Append(name).Append('{').Append(labels).Append("} ").Append(value.ToString(CultureInfo.InvariantCulture)).Append('\n');
    }
}

using Avro;
using Confluent.SchemaRegistry;
using Shared;

const string topic = "orders";
var config = AppConfig.FromEnvironment();
var workload = WorkloadOptions.Load();

using var shutdown = new ProducerShutdown();
using var metrics = new KafkaMetrics("producer");
using var registry = new CachedSchemaRegistryClient(config.SchemaRegistry());
var orders = await LoadOrdersAsync(registry, topic + "-value");
using var producer = ProducerFactory.Create(config, registry, metrics.Publish, shutdown.OnKafkaError);

var publisher = new OrderPublisher(producer, topic, orders, workload, shutdown, metrics);
return await publisher.RunAsync();

static async Task<OrderFactory> LoadOrdersAsync(ISchemaRegistryClient registry, string subject)
{
    var registered = await registry.GetLatestSchemaAsync(subject);
    var schema = (RecordSchema)Avro.Schema.Parse(registered.SchemaString);
    Console.WriteLine($"Using {subject} version {registered.Version} (id {registered.Id})");
    return new OrderFactory(schema);
}

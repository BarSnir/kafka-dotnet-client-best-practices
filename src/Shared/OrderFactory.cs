using Avro;
using Avro.Generic;
using Confluent.Kafka;

namespace Shared;

public sealed class OrderFactory(RecordSchema schema)
{
    private readonly bool hasPayload = schema.Fields.Any(field => field.Name == "payload");
    private static readonly string[] Customers = ["customer-a", "customer-b", "customer-c"];
    private static int _nextCustomer;

    public Message<string, GenericRecord> Create(int payloadBytes = 0, bool hotPartition = false)
    {
        var customer = hotPartition ? Customers[0] : Customers[Interlocked.Increment(ref _nextCustomer) % Customers.Length];
        var order = new GenericRecord(schema);
        order.Add("orderId", Guid.NewGuid().ToString("N"));
        order.Add("customerId", customer);
        order.Add("amountCents", Random.Shared.Next(100, 100000));
        var payload = new byte[payloadBytes];
        Random.Shared.NextBytes(payload);
        var message = new Message<string, GenericRecord> { Key = customer, Value = order };
        if (hasPayload) order.Add("payload", payload);
        else if (payload.Length > 0) message.Headers = new Headers { { "demo-payload", payload } };
        return message;
    }
}

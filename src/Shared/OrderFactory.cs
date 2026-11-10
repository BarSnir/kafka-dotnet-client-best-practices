using Avro;
using Avro.Generic;

namespace Shared;

public static class OrderFactory
{
    private static readonly RecordSchema Schema = (RecordSchema)Avro.Schema.Parse(File.ReadAllText("/app/schemas/order.avsc"));
    private static readonly string[] Customers = ["customer-a", "customer-b", "customer-c"];
    private static int _nextCustomer;

    public static GenericRecord Create(int payloadBytes = 0, bool hotPartition = false)
    {
        var customer = hotPartition ? Customers[0] : Customers[Interlocked.Increment(ref _nextCustomer) % Customers.Length];
        var order = new GenericRecord(Schema);
        order.Add("orderId", Guid.NewGuid().ToString("N"));
        order.Add("customerId", customer);
        order.Add("amountCents", Random.Shared.Next(100, 100000));
        var payload = new byte[payloadBytes];
        Random.Shared.NextBytes(payload);
        order.Add("payload", payload);
        return order;
    }
}

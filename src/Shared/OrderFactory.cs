using Avro;
using Avro.Generic;

namespace Shared;

public static class OrderFactory
{
    private static readonly RecordSchema Schema = (RecordSchema)Avro.Schema.Parse(File.ReadAllText("/app/schemas/order.avsc"));
    private static readonly string[] Customers = ["customer-a", "customer-b", "customer-c"];
    private static int _nextCustomer;

    public static GenericRecord Create()
    {
        var customer = Customers[Interlocked.Increment(ref _nextCustomer) % Customers.Length];
        var order = new GenericRecord(Schema);
        order.Add("orderId", Guid.NewGuid().ToString("N"));
        order.Add("customerId", customer);
        order.Add("amountCents", Random.Shared.Next(100, 100000));
        return order;
    }
}
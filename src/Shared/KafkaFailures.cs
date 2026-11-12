using Confluent.Kafka;
using Confluent.SchemaRegistry;

namespace Shared;

public static class KafkaFailures
{
    public static bool RequiresIntervention(ErrorCode code) => code is
        ErrorCode.TopicAuthorizationFailed or ErrorCode.GroupAuthorizationFailed or
        ErrorCode.Local_ValueSerialization or ErrorCode.Local_ValueDeserialization or ErrorCode.MsgSizeTooLarge;

    public static string Describe(KafkaException exception)
    {
        var code = exception.Error.Code;
        var category = code switch
        {
            ErrorCode.Local_MsgTimedOut => "delivery deadline exceeded",
            ErrorCode.Local_QueueFull => "producer queue full",
            ErrorCode.MsgSizeTooLarge => "message exceeds broker limit",
            ErrorCode.TopicAuthorizationFailed => "topic authorization failed",
            ErrorCode.GroupAuthorizationFailed => "consumer group authorization failed",
            ErrorCode.Local_ValueDeserialization => "value deserialization failed",
            ErrorCode.Local_ValueSerialization => "Avro value serialization failed",
            _ => "Kafka operation failed"
        };
        for (var cause = exception.InnerException; cause != null; cause = cause.InnerException)
        {
            if (cause is SchemaRegistryException registry)
                return $"{category}: Schema Registry HTTP {(int)registry.Status}, registry code {registry.ErrorCode}: {registry.Message}";
        }
        return $"{category} ({code}): {exception.InnerException?.Message ?? exception.Error.Reason}";
    }
}

using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>
/// Projects the envelope's contract fields onto native Azure Service Bus message fields —
/// <c>Subject</c> = URN, <c>CorrelationId</c> = <c>trace_id</c>, <c>MessageId</c> =
/// <c>meta.id</c>, plus the <c>bq-</c> <c>ApplicationProperties</c> — so a consumer can route
/// and correlate without decoding the body. The body stays authoritative (Contract §4.2–§4.3).
/// </summary>
internal static class AsbProperties
{
    public static ServiceBusMessage ToMessage(Envelope envelope, TimeSpan? delay = null)
    {
        var message = new ServiceBusMessage(BinaryData.FromString(EnvelopeCodec.Encode(envelope)))
        {
            ContentType = "application/json",
            Subject = envelope.Job,
            CorrelationId = envelope.TraceId,
        };

        var meta = envelope.Meta;
        if (meta is not null)
        {
            if (!string.IsNullOrEmpty(meta.Id))
            {
                message.MessageId = meta.Id;
            }

            message.ApplicationProperties["bq-schema-version"] = meta.SchemaVersion;
            if (!string.IsNullOrEmpty(meta.Lang))
            {
                message.ApplicationProperties["bq-source-lang"] = meta.Lang;
            }

            message.ApplicationProperties["bq-created-at"] = meta.CreatedAt;
        }

        if (delay is { } window && window > TimeSpan.Zero)
        {
            message.ScheduledEnqueueTime = DateTimeOffset.UtcNow + window;
            message.ApplicationProperties["bq-delay"] = (long)window.TotalMilliseconds;
        }

        return message;
    }
}

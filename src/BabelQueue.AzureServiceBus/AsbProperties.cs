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
    /// <summary>
    /// The application properties the transport owns (the §4 <c>bq-</c> projection, the optional <c>bq-attempts</c> mirror and <c>bq-delay</c>). They are
    /// never accepted from, nor surfaced as, out-of-band transport headers (parity with the Go reference).
    /// </summary>
    private static readonly HashSet<string> Contract = new(StringComparer.Ordinal)
    {
        "bq-schema-version", "bq-source-lang", "bq-created-at", "bq-attempts", "bq-delay",
    };

    /// <summary>True when <paramref name="key"/> is a contract property the transport owns.</summary>
    public static bool IsContract(string key) => Contract.Contains(key);

    public static ServiceBusMessage ToMessage(Envelope envelope, TimeSpan? delay = null, IReadOnlyDictionary<string, string>? headers = null)
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

        Merge(message, headers);

        return message;
    }

    /// <summary>
    /// Overlays the out-of-band string <paramref name="headers"/> (e.g. a W3C <c>traceparent</c>, ADR-0028) onto the
    /// message's <c>ApplicationProperties</c>, beside the <c>bq-</c> contract projection. An existing property is
    /// never overwritten, a contract property name (<see cref="IsContract"/>, including <c>bq-delay</c>) is always
    /// dropped, and empty keys/values are skipped. Keys are applied in
    /// ordinal order so the result is deterministic. A <c>null</c>/empty map is a no-op.
    /// </summary>
    public static void Merge(ServiceBusMessage message, IReadOnlyDictionary<string, string>? headers)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (headers is null || headers.Count == 0)
        {
            return;
        }

        foreach (var key in headers.Keys.OrderBy(static k => k, StringComparer.Ordinal))
        {
            var value = headers[key];
            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value) || IsContract(key) || message.ApplicationProperties.ContainsKey(key))
            {
                continue;
            }

            message.ApplicationProperties[key] = value;
        }
    }
}

/// <summary>
/// Out-of-band transport-header carrier for Azure Service Bus (ADR-0028). A W3C <c>traceparent</c> (and
/// <c>tracestate</c>) rides as a string <c>ApplicationProperty</c> <b>beside</b> the frozen envelope — never inside
/// it — next to the §4 <c>bq-</c> projection. This is the consume-side half: it surfaces a received message's
/// application properties as a flat map a handler hands to <c>Telemetry.Wrap(handler, headers)</c>, so the consumer
/// span becomes a true child of the producer span. With no <c>traceparent</c> the core falls back to the v0.1
/// <c>trace_id</c> mapping.
/// </summary>
public static class AsbTraceHeaders
{
    /// <summary>
    /// The string-valued application properties of a received message as a flat map (empty when it has none).
    /// Only out-of-band headers (e.g. <c>traceparent</c>) surface: the <c>bq-</c> contract properties the transport
    /// owns are left out, so handing the map back to <c>PublishWithHeadersAsync</c> cannot re-inject them. Non-string
    /// and empty values are skipped.
    /// </summary>
    public static Dictionary<string, string> Extract(ServiceBusReceivedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var headers = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (key, value) in message.ApplicationProperties)
        {
            if (value is string text && text.Length > 0 && !AsbProperties.IsContract(key))
            {
                headers[key] = text;
            }
        }

        return headers;
    }
}

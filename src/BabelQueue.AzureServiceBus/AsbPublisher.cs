using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>
/// Sends canonical-envelope messages to one Service Bus entity (queue or topic) with the §4
/// native projection — <c>Subject</c> = URN, <c>CorrelationId</c> = <c>trace_id</c>,
/// <c>MessageId</c> = <c>meta.id</c>, plus the <c>bq-</c> <c>ApplicationProperties</c> — so a
/// consumer can route and correlate without decoding the body. The envelope is unchanged
/// (<c>schema_version</c> stays 1); Azure Service Bus is purely additive.
/// </summary>
public sealed class AsbPublisher
{
    private readonly ServiceBusSender _sender;

    /// <param name="sender">A sender for the target queue or topic (mockable in tests).</param>
    public AsbPublisher(ServiceBusSender sender)
    {
        ArgumentNullException.ThrowIfNull(sender);
        _sender = sender;
    }

    /// <summary>
    /// Builds the canonical envelope for <c>(urn, data)</c>, sends it with the native
    /// projection, and returns the message id (<c>meta.id</c>). A positive <paramref name="delay"/>
    /// schedules native delayed delivery via <c>ScheduledEnqueueTime</c>.
    /// </summary>
    public async Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        var envelope = EnvelopeCodec.Make(urn, data, _sender.EntityPath, traceId);
        var message = AsbProperties.ToMessage(envelope, delay);
        await _sender.SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
        return envelope.Meta?.Id ?? string.Empty;
    }
}

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
    public Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
        => PublishWithHeadersAsync(urn, data, headers: null, traceId, delay, cancellationToken);

    /// <summary>
    /// The header-aware (ADR-0028) counterpart of
    /// <see cref="PublishAsync(string, IReadOnlyDictionary{string, object?}?, string?, TimeSpan?, CancellationToken)"/>:
    /// in addition to the §4 projection it overlays the out-of-band <paramref name="headers"/> (e.g. a W3C
    /// <c>traceparent</c> from <c>Telemetry.PublishAsync(..., headers, ...)</c>) as string
    /// <c>ApplicationProperties</c>. The headers ride <b>beside</b> the frozen envelope, never inside it, and never
    /// overwrite a <c>bq-</c> contract property. A <c>null</c>/empty header map is identical to <c>PublishAsync</c>.
    /// </summary>
    /// <param name="urn">The message URN to publish.</param>
    /// <param name="data">The message payload, or <c>null</c> for an empty body.</param>
    /// <param name="headers">The out-of-band transport headers to ride beside the envelope.</param>
    /// <param name="traceId">An existing trace id to continue, or <c>null</c> to mint one.</param>
    /// <param name="delay">A positive delay schedules native delayed delivery.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<string> PublishWithHeadersAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data,
        IReadOnlyDictionary<string, string>? headers,
        string? traceId = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        var envelope = EnvelopeCodec.Make(urn, data, _sender.EntityPath, traceId);
        return await PublishEnvelopeAsync(envelope, headers, delay, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Sends an <b>already-built</b> <paramref name="envelope"/> exactly as given (the same §4 projection, header
    /// overlay and scheduled delivery as <see cref="PublishWithHeadersAsync"/>) without rebuilding it:
    /// <c>meta.id</c>, <c>trace_id</c> and every extension field reach the wire unchanged. Use it as the <c>send</c>
    /// callback of <c>Telemetry.PublishAsync(..., headers, env =&gt; publisher.PublishEnvelopeAsync(env, headers), queue)</c>,
    /// so the producer span's <c>messaging.message.id</c> is the <c>meta.id</c> on the wire. Returns <c>meta.id</c>.
    /// </summary>
    /// <param name="envelope">The envelope to send; its <c>meta.queue</c> is the caller's responsibility.</param>
    /// <remarks>
    /// <c>meta.queue</c> is whatever the envelope carries; it is not rewritten to this publisher's target. An envelope
    /// built by <c>Telemetry.PublishAsync</c> carries that call's <c>queue</c> argument (default <c>"default"</c>), so pass
    /// the queue/topic name there, or the wire <c>meta.queue</c> and the span's destination will not name it.
    /// </remarks>
    /// <param name="headers">The out-of-band transport headers to ride beside the envelope.</param>
    /// <param name="delay">A positive delay schedules native delayed delivery.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<string> PublishEnvelopeAsync(
        Envelope envelope,
        IReadOnlyDictionary<string, string>? headers = null,
        TimeSpan? delay = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        var message = AsbProperties.ToMessage(envelope, delay, headers);
        await _sender.SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
        return envelope.Meta?.Id ?? string.Empty;
    }
}

using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>Tuning and hooks for <see cref="AsbConsumer"/>.</summary>
public sealed class AsbConsumerOptions
{
    /// <summary>Max messages per receive (default 10).</summary>
    public int MaxMessages { get; set; } = 10;

    /// <summary>Max time to wait for a batch; <c>null</c> uses the receiver default.</summary>
    public TimeSpan? MaxWaitTime { get; set; }

    /// <summary>
    /// Called for a non-conformant envelope, a schema-version-gated message, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), or a throwing handler. The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, ServiceBusReceivedMessage>? OnError { get; set; }

    /// <summary>
    /// Called instead of erroring when a URN has no handler; the message is then Completed (dropped). It is for an
    /// unknown URN only and never affects a poison body or a schema-version-gated message (ADR-0037); this consumer has
    /// no unknown-URN strategy option and behaves as <c>fail</c> for both.
    /// </summary>
    public Func<Envelope, ServiceBusReceivedMessage, CancellationToken, Task>? OnUnknownUrn { get; set; }

    /// <summary>
    /// A sender bound to the dead-letter queue (by convention <c>&lt;queue&gt;.dlq</c>, an ordinary Service Bus queue,
    /// <c>client.CreateSender("orders.dlq")</c>). Setting it turns BabelQueue dead-lettering on (ADR-0037); leaving it
    /// <c>null</c> (default) leaves poison and gated messages to the broker's native dead-letter sub-queue. With it set a
    /// poison body is written as the ADR-0035 record (<c>reason: "poison"</c>, original bytes preserved) and a
    /// schema-version-gated message as the raw incoming body (no <c>dead_letter</c> block), each on its first
    /// delivery; the original is <c>Complete</c>d only after the write succeeded and <c>Abandon</c>ed when it failed.
    /// </summary>
    public ServiceBusSender? DeadLetterSender { get; set; }

    /// <summary>
    /// Optional dead-letter observer (ADR-0039 section 5.1), called once per dead-letter the SDK itself carried out,
    /// after it is confirmed: a record written through <see cref="DeadLetterSender"/> (a poison body, or a raw
    /// schema-version-gated body with an empty reason), or, with no sender, the explicit native
    /// <c>DeadLetterMessageAsync</c> of a schema-version-gated message (empty <c>DeadLetterQueue</c> and reason).
    /// Not called when the write or the native call fails, when the broker dead-letters on its own
    /// (<c>MaxDeliveryCount</c>), or for a poison body with no sender (which is only abandoned). An observer exception
    /// is caught and logged; it never changes the settle outcome. Needs a <c>BabelQueue.Core</c> that ships
    /// <see cref="IDeadLetterObserver"/>.
    /// </summary>
    public IDeadLetterObserver? DeadLetterObserver { get; set; }

    /// <summary>
    /// The consumer's queue name, written as <c>dead_letter.original_queue</c> of a poison record. Default: the
    /// receiver's entity path, else the <see cref="DeadLetterSender"/> entity path without its <c>.dlq</c> suffix.
    /// </summary>
    public string? Queue { get; set; }
}

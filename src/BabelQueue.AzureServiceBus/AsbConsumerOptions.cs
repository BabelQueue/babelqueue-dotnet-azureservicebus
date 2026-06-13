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
    /// Called for a non-conformant envelope, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), or a throwing handler. The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, ServiceBusReceivedMessage>? OnError { get; set; }

    /// <summary>Called instead of erroring when a URN has no handler; the message is then Completed (dropped).</summary>
    public Func<Envelope, ServiceBusReceivedMessage, CancellationToken, Task>? OnUnknownUrn { get; set; }
}

using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>
/// Receives from a Service Bus entity in <c>PeekLock</c> mode, decodes and validates each
/// message, routes it to the handler registered for its URN, and <c>Complete</c>s it on
/// success. A throwing handler <c>Abandon</c>s the message — the broker redelivers it and
/// increments <c>DeliveryCount</c> (at-least-once). <c>attempts</c> is reconciled to
/// <c>DeliveryCount - 1</c> (broker-authoritative on ASB) for the handler. The loop never
/// stops on a bad message — observe via the option hooks.
/// </summary>
public sealed class AsbConsumer
{
    private readonly ServiceBusReceiver _receiver;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly AsbConsumerOptions _options;

    public AsbConsumer(
        ServiceBusReceiver receiver,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        AsbConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(receiver);
        ArgumentNullException.ThrowIfNull(handlers);
        _receiver = receiver;
        _handlers = handlers;
        _options = options ?? new AsbConsumerOptions();
    }

    /// <summary>Receive one batch, route each message, settle each. Returns the batch size.</summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var messages = await _receiver
            .ReceiveMessagesAsync(_options.MaxMessages, _options.MaxWaitTime, cancellationToken)
            .ConfigureAwait(false);

        foreach (var message in messages)
        {
            await HandleAsync(message, cancellationToken).ConfigureAwait(false);
        }

        return messages.Count;
    }

    /// <summary>Poll until <paramref name="cancellationToken"/> is cancelled.</summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await PollAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        var envelope = Reconcile(
            EnvelopeCodec.Decode(message.Body?.ToString() ?? string.Empty),
            message.DeliveryCount);

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Azure Service Bus."),
                envelope, message);
            await AbandonAsync(message, cancellationToken).ConfigureAwait(false);
            return;
        }

        var urn = EnvelopeCodec.Urn(envelope);
        if (!_handlers.TryGetValue(urn, out var handler))
        {
            if (_options.OnUnknownUrn is not null)
            {
                await _options.OnUnknownUrn(envelope, message, cancellationToken).ConfigureAwait(false);
                await CompleteAsync(message, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, message);
                await AbandonAsync(message, cancellationToken).ConfigureAwait(false);
            }

            return;
        }

        try
        {
            await handler(envelope, message, cancellationToken).ConfigureAwait(false);
            await CompleteAsync(message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            // Abandon releases the lock — the broker redelivers and increments DeliveryCount.
            _options.OnError?.Invoke(error, envelope, message);
            await AbandonAsync(message, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Sets <c>attempts</c> to <c>max(current, DeliveryCount - 1)</c>. <c>DeliveryCount</c> is
    /// 1-based and broker-authoritative on ASB (first delivery = 1 → attempts 0); the max never
    /// lowers a higher body count carried by a message republished from another SDK.
    /// </summary>
    private static Envelope Reconcile(Envelope envelope, int deliveryCount)
    {
        var native = deliveryCount - 1;
        return native > envelope.Attempts ? envelope with { Attempts = native } : envelope;
    }

    private Task CompleteAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        => _receiver.CompleteMessageAsync(message, cancellationToken);

    private Task AbandonAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
        => _receiver.AbandonMessageAsync(message, propertiesToModify: null, cancellationToken);
}

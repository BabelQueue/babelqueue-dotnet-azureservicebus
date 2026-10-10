using System.Globalization;
using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>
/// Receives from a Service Bus entity in <c>PeekLock</c> mode, decodes and validates each
/// message, routes it to the handler registered for its URN, and <c>Complete</c>s it on
/// success. A throwing handler <c>Abandon</c>s the message — the broker redelivers it and
/// increments <c>DeliveryCount</c> (at-least-once). <c>attempts</c> is reconciled to
/// <c>DeliveryCount - 1</c> (broker-authoritative on ASB) for the handler. The loop never
/// stops on a bad message — observe via the option hooks.
/// <para>
/// Per §4.7 the <c>bq-schema-version</c> application property is checked <b>before</b> the body is
/// decoded: when it is present and is not the schema version the core supports
/// (<see cref="EnvelopeCodec.SchemaVersion"/>), the body is never decoded or routed; <c>OnError</c> is
/// notified (with an empty, undecoded envelope) and the message is <b>explicitly dead-lettered</b>
/// (<c>DeadLetterMessageAsync</c>, reason <c>unsupported schema version</c>) — never abandoned, since
/// redelivery cannot make an unknown version supported. A string value is compared exactly as sent, as the
/// canonical decimal string, with no trimming (so <c>"2"</c>, <c>"x"</c>, <c>"01"</c> and <c>" 1"</c> are unknown);
/// producers write an AMQP integer, so any integral type (byte/short/int/long, signed or unsigned) equal to
/// the supported version is accepted too, while every other number or type is rejected. A missing, null or blank
/// (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR) property changes nothing, and a supported
/// version still goes through the post-decode <see cref="EnvelopeCodec.Accepts"/> check.
/// </para>
/// </summary>
public sealed class AsbConsumer
{
    private const string SchemaVersionProperty = "bq-schema-version";
    private const string UnsupportedSchemaVersionReason = "unsupported schema version";

    // The offending value is sender-controlled and Service Bus caps the dead-letter reason/description at
    // 4096 characters (longer values make DeadLetterMessageAsync throw), so only a short prefix is echoed.
    private const int MaxDeclaredVersionChars = 64;

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
        // §4.7: version-gate on the application property before decoding the body.
        if (!IsSupportedSchemaVersion(message, out var declaredVersion))
        {
            // The body is deliberately not decoded, so hand OnError an empty envelope.
            var reason = $"Rejected an Azure Service Bus message: unsupported {SchemaVersionProperty} property "
                + $"'{TruncateDeclaredVersion(declaredVersion)}' (supported: {EnvelopeCodec.SchemaVersion}); body not decoded.";
            _options.OnError?.Invoke(
                new BabelQueueException(reason),
                new Envelope(null, null, null, null, 0, null),
                message);
            await _receiver
                .DeadLetterMessageAsync(message, UnsupportedSchemaVersionReason, reason, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

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

    /// <summary>The first 64 characters of the rejected value, plus <c>...</c> when it was cut.</summary>
    private static string? TruncateDeclaredVersion(string? value)
        => value is not null && value.Length > MaxDeclaredVersionChars
            ? string.Concat(value.AsSpan(0, MaxDeclaredVersionChars), "...")
            : value;

    /// <summary>
    /// The §4.7 gate verdict for the <c>bq-schema-version</c> application property. <c>true</c> when it is absent,
    /// null, blank, the exact canonical string, or an integral AMQP number equal to the supported version.
    /// <paramref name="declared"/> is the offending value rendered for the error message otherwise.
    /// </summary>
    private static bool IsSupportedSchemaVersion(ServiceBusReceivedMessage message, out string? declared)
    {
        declared = null;
        if (message.ApplicationProperties is null
            || !message.ApplicationProperties.TryGetValue(SchemaVersionProperty, out var value)
            || value is null)
        {
            return true;
        }

        var supported = EnvelopeCodec.SchemaVersion;
        switch (value)
        {
            case string text:
                declared = text;
                return IsBlankSchemaVersion(text)
                    || string.Equals(text, supported.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
            case byte or sbyte or short or ushort or int or uint or long or ulong:
                declared = Convert.ToString(value, CultureInfo.InvariantCulture);
                return string.Equals(
                    declared, supported.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
            default:
                declared = Convert.ToString(value, CultureInfo.InvariantCulture) ?? value.GetType().Name;
                return false;
        }
    }

    /// <summary>
    /// The shared cross-SDK "blank" definition for <c>bq-schema-version</c>: the empty string, or a value made up
    /// <b>only</b> of ASCII whitespace (space, <c>\t</c>, <c>\n</c>, U+000B, <c>\f</c>, <c>\r</c>). Anything else
    /// (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike <see cref="string.IsNullOrWhiteSpace"/>.
    /// </summary>
    private static bool IsBlankSchemaVersion(string value)
    {
        foreach (var c in value)
        {
            if (c != ' ' && (c < '\t' || c > '\r'))
            {
                return false;
            }
        }

        return true;
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

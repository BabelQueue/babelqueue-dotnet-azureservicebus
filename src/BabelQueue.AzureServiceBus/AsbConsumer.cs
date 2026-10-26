using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
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
/// notified (with an empty, undecoded envelope) and the message is settled on its <b>first delivery</b> (ADR-0037
/// decision 1) — never abandoned for a retry, since redelivery cannot make an unknown version supported. With
/// <see cref="AsbConsumerOptions.DeadLetterSender"/> set the body is written <b>exactly as received</b> (no
/// <c>dead_letter</c> block) to <c>&lt;queue&gt;.dlq</c> and the message is <c>Complete</c>d after the write (a failed
/// write <c>Abandon</c>s it: unsettled, reported); without a sender it is handed to the native mechanism by an explicit
/// <c>DeadLetterMessageAsync</c> (reason <c>unsupported schema version</c>). This consumer has no unknown-URN strategy
/// option and the <see cref="AsbConsumerOptions.OnUnknownUrn"/> hook never applies to a gated message: it behaves as
/// <c>fail</c>. A string value is compared exactly as sent, as the
/// canonical decimal string, with no trimming (so <c>"2"</c>, <c>"x"</c>, <c>"01"</c> and <c>" 1"</c> are unknown);
/// producers write an AMQP integer, so any integral type (byte/short/int/long, signed or unsigned) equal to
/// the supported version is accepted too, while every other number or type is rejected. A missing, null or blank
/// (empty or ASCII-whitespace-only: space, tab, LF, VT, FF, CR) property changes nothing, and a supported
/// version still goes through the post-decode <see cref="EnvelopeCodec.Accepts"/> check.
/// </para>
/// <para>
/// A poison body (not valid UTF-8, not a JSON object, no valid <c>job</c>/<c>urn</c> identity, or any other non-conformant
/// envelope) is never handed to a handler and never retried by the SDK (ADR-0037 decision 2). With a
/// <see cref="AsbConsumerOptions.DeadLetterSender"/> the ADR-0035 record (<c>reason: "poison"</c>) is written to
/// <c>&lt;queue&gt;.dlq</c> and the message <c>Complete</c>d after the write (a failed write <c>Abandon</c>s it); without a
/// sender it is <c>Abandon</c>ed, so the broker moves it to the native dead-letter sub-queue at <c>MaxDeliveryCount</c>.
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
            // ADR-0037 decision 1: settled on the first delivery, never decoded, never retried. The body is deliberately
            // not decoded, so hand OnError an empty envelope.
            var reason = $"Rejected an Azure Service Bus message: unsupported {SchemaVersionProperty} property "
                + $"'{TruncateDeclaredVersion(declaredVersion)}' (supported: {EnvelopeCodec.SchemaVersion}); body not decoded.";
            var empty = new Envelope(null, null, null, null, 0, null);
            if (_options.DeadLetterSender is null)
            {
                // Dead-lettering off: the native mechanism, explicitly and now (the body stays unchanged).
                _options.OnError?.Invoke(new BabelQueueException(reason), empty, message);
                await _receiver
                    .DeadLetterMessageAsync(message, UnsupportedSchemaVersionReason, reason, cancellationToken)
                    .ConfigureAwait(false);
                return;
            }

            await SettleToDeadLetterAsync(
                message,
                () => new ServiceBusMessage(message.Body ?? BinaryData.FromBytes(Array.Empty<byte>())),
                empty,
                new BabelQueueException(reason),
                cancellationToken).ConfigureAwait(false);
            return;
        }

        // Invalid UTF-8 is poison (a lenient decode would hand a corrupted payload to a handler).
        var bodyBytes = message.Body?.ToArray() ?? Array.Empty<byte>();
        var text = StrictUtf8(bodyBytes);
        var envelope = Reconcile(
            text is null ? new Envelope(null, null, null, null, 0, null) : EnvelopeCodec.Decode(text),
            message.DeliveryCount);

        if (text is null || !EnvelopeCodec.Accepts(envelope))
        {
            // ADR-0037 decision 2: the poison route -- the ADR-0035 record to <queue>.dlq, never a retry by the SDK.
            var cause = new BabelQueueException(text is null
                ? "Rejected an Azure Service Bus message: the body is not valid UTF-8."
                : "Rejected a non-conformant BabelQueue envelope from Azure Service Bus.");
            if (_options.DeadLetterSender is null)
            {
                // Dead-lettering off: native -- Abandon until MaxDeliveryCount moves it to $DeadLetterQueue.
                _options.OnError?.Invoke(cause, envelope, message);
                await AbandonAsync(message, cancellationToken).ConfigureAwait(false);
                return;
            }

            await SettleToDeadLetterAsync(
                message,
                () => new ServiceBusMessage(BinaryData.FromBytes(PoisonBody(
                    bodyBytes,
                    cause.Message,
                    cause.GetType().FullName,
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    QueueName(),
                    envelope.Attempts))) { ContentType = "application/json" },
                envelope,
                cause,
                cancellationToken).ConfigureAwait(false);
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
    /// ADR-0037 decisions 1 and 2 with a dead-letter sender: the record from <paramref name="build"/> is written to
    /// <c>&lt;queue&gt;.dlq</c> and the message <c>Complete</c>d only after the write succeeded. A failed write
    /// <c>Abandon</c>s it (never completed, never retried by the SDK; the rejection repeats, so the write is tried again) and
    /// is reported. The rejection is reported after the settle.
    /// </summary>
    private async Task SettleToDeadLetterAsync(
        ServiceBusReceivedMessage message,
        Func<ServiceBusMessage> build,
        Envelope envelope,
        BabelQueueException rejected,
        CancellationToken cancellationToken)
    {
        try
        {
            await _options.DeadLetterSender!.SendMessageAsync(build(), cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // Whatever the write failure was, the message must stay unsettled for redelivery, not be lost.
        catch (Exception writeError) when (writeError is not OperationCanceledException)
#pragma warning restore CA1031
        {
            await AbandonAsync(message, cancellationToken).ConfigureAwait(false);
            _options.OnError?.Invoke(rejected, envelope, message);
            _options.OnError?.Invoke(
                new BabelQueueException(
                    "Dead-letter write failed; the message was abandoned for redelivery, not completed.", writeError),
                envelope,
                message);
            return;
        }

        await CompleteAsync(message, cancellationToken).ConfigureAwait(false);
        _options.OnError?.Invoke(rejected, envelope, message);
    }

    private static readonly UTF8Encoding StrictUtf8Encoding = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static string? StrictUtf8(byte[] body)
    {
        try
        {
            return StrictUtf8Encoding.GetString(body);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    /// <summary>
    /// The consumer's queue name, for the poison record's <c>dead_letter.original_queue</c>: the explicit
    /// <see cref="AsbConsumerOptions.Queue"/>, else the receiver's entity path, else the dead-letter sender's entity path
    /// without its <c>.dlq</c> suffix.
    /// </summary>
    private string QueueName()
    {
        if (!string.IsNullOrWhiteSpace(_options.Queue))
        {
            return _options.Queue;
        }

        var path = _receiver.EntityPath;
        if (!string.IsNullOrWhiteSpace(path))
        {
            return path;
        }

        var dlq = _options.DeadLetterSender?.EntityPath;
        if (string.IsNullOrWhiteSpace(dlq))
        {
            return "default";
        }

        return dlq.EndsWith(".dlq", StringComparison.Ordinal) && dlq.Length > ".dlq".Length ? dlq[..^".dlq".Length] : dlq;
    }

    private static readonly JsonWriterOptions PoisonWriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    /// <summary>
    /// The poison DLQ record (ADR-0035, <see cref="PoisonRecord"/>): a body that is a strict JSON object without a
    /// top-level <c>dead_letter</c> key is spliced byte for byte; anything else is carried as <c>raw</c> (plain text when
    /// valid UTF-8, otherwise base64 plus <c>raw_encoding: "base64"</c>). The <c>dead_letter</c> block has reason
    /// <c>poison</c>.
    /// </summary>
    internal static byte[] PoisonBody(byte[] body, string? error, string? exception, long failedAt, string originalQueue, int attempts)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, PoisonWriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("reason", "poison");
            if (error is null) { writer.WriteNull("error"); } else { writer.WriteString("error", error); }
            if (exception is null) { writer.WriteNull("exception"); } else { writer.WriteString("exception", exception); }
            writer.WriteNumber("failed_at", failedAt);
            writer.WriteString("original_queue", originalQueue);
            writer.WriteNumber("attempts", attempts);
            writer.WriteString("lang", EnvelopeCodec.SourceLang);
            writer.WriteEndObject();
        }

        return PoisonRecord.Build(body, Encoding.UTF8.GetString(stream.ToArray()));
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

using System.Text;
using Azure.Messaging.ServiceBus;
using BabelQueue;
using BabelQueue.AzureServiceBus;
using Moq;
using Xunit;

namespace BabelQueue.AzureServiceBus.Tests;

/// <summary>
/// Consumer behaviour against a mocked receiver (no broker): attempts = DeliveryCount - 1,
/// Complete on success, Abandon on failure / unmapped URN, and the unknown-URN hooks.
/// </summary>
public sealed class AsbConsumerTests
{
    private const string Urn = "urn:babel:orders:created";

    private static ServiceBusReceivedMessage Received(int deliveryCount)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", null);
        return ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(EnvelopeCodec.Encode(env)),
            subject: Urn,
            deliveryCount: deliveryCount);
    }

    private static Mock<ServiceBusReceiver> ReceiverWith(params ServiceBusReceivedMessage[] messages)
    {
        var receiver = new Mock<ServiceBusReceiver>();
        receiver
            .Setup(r => r.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((IReadOnlyList<ServiceBusReceivedMessage>)messages);
        receiver
            .Setup(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        receiver
            .Setup(r => r.AbandonMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(),
                It.IsAny<IDictionary<string, object>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return receiver;
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(3, 2)]
    public async Task AttemptsIsDeliveryCountMinusOneAndCompletes(int deliveryCount, int expectedAttempts)
    {
        var receiver = ReceiverWith(Received(deliveryCount));
        var seen = -1;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; },
        };

        var count = await new AsbConsumer(receiver.Object, handlers).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(expectedAttempts, seen);
        receiver.Verify(
            r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ThrowingHandlerAbandonsAndReportsOnError()
    {
        var receiver = ReceiverWith(Received(1));
        Exception? reported = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var options = new AsbConsumerOptions { OnError = (e, _, _) => reported = e };

        await new AsbConsumer(receiver.Object, handlers, options).PollAsync();

        Assert.IsType<InvalidOperationException>(reported);
        receiver.Verify(
            r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        receiver.Verify(
            r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task UnknownUrnWithHookCompletes()
    {
        var receiver = ReceiverWith(Received(1));
        var called = false;
        var options = new AsbConsumerOptions
        {
            OnUnknownUrn = (_, _, _) => { called = true; return Task.CompletedTask; },
        };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.True(called);
        receiver.Verify(
            r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task UnknownUrnWithoutHookAbandonsAndReportsOnError()
    {
        var receiver = ReceiverWith(Received(1));
        Exception? reported = null;
        var options = new AsbConsumerOptions { OnError = (e, _, _) => reported = e };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.IsType<UnknownUrnException>(reported);
        receiver.Verify(
            r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task NonConformantEnvelopeAbandonsAndReportsOnError()
    {
        var bad = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{\"trace_id\":\"t\",\"data\":{\"x\":1},\"meta\":{\"id\":\"m\",\"queue\":\"q\",\"lang\":\"dotnet\",\"schema_version\":1,\"created_at\":1},\"attempts\":0}"),
            deliveryCount: 1);
        var receiver = ReceiverWith(bad);
        Exception? reported = null;
        var options = new AsbConsumerOptions { OnError = (e, _, _) => reported = e };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.IsType<BabelQueueException>(reported);
        receiver.Verify(
            r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private static ServiceBusReceivedMessage WithSchemaVersion(string body, object? version)
    {
        var properties = new Dictionary<string, object>();
        if (version is not null)
        {
            properties["bq-schema-version"] = version;
        }

        return ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(body),
            subject: Urn,
            deliveryCount: 1,
            properties: properties);
    }

    private static string ValidBody()
        => EnvelopeCodec.Encode(
            EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 1 }, "orders", null));

    private static Mock<ServiceBusReceiver> WithDeadLetter(Mock<ServiceBusReceiver> receiver)
    {
        receiver
            .Setup(r => r.DeadLetterMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return receiver;
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" \t")]
    [InlineData("1")]
    [InlineData(1)]
    [InlineData(1L)]
    [InlineData((short)1)]
    [InlineData((byte)1)]
    [InlineData(1u)]
    [InlineData(1UL)]
    public async Task SchemaVersionGateLetsAbsentBlankOrSupportedVersionThrough(object? version)
    {
        var receiver = WithDeadLetter(ReceiverWith(WithSchemaVersion(ValidBody(), version)));
        var calls = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        await new AsbConsumer(receiver.Object, handlers).PollAsync();

        Assert.Equal(1, calls);
        receiver.Verify(
            r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
        receiver.Verify(
            r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Theory]
    [InlineData("2")]
    [InlineData("x")]
    [InlineData(" 1")]
    [InlineData(2)]
    [InlineData(2L)]
    [InlineData(0)]
    [InlineData(1.5)]
    [InlineData(true)]
    public async Task UnsupportedSchemaVersionIsDeadLetteredWithoutDecoding(object version)
    {
        // The body is not valid JSON: if the gate decoded it first, Decode would throw instead of
        // the message being dead-lettered — so a clean dead-letter proves the body was never decoded.
        var receiver = WithDeadLetter(ReceiverWith(WithSchemaVersion("{ this is not json", version)));
        Exception? reported = null;
        Envelope? reportedEnvelope = null;
        var calls = 0;
        var options = new AsbConsumerOptions { OnError = (e, env, _) => { reported = e; reportedEnvelope = env; } };
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; },
        };

        var count = await new AsbConsumer(receiver.Object, handlers, options).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(0, calls);
        Assert.IsType<BabelQueueException>(reported);
        Assert.Null(reportedEnvelope!.Job);
        receiver.Verify(
            r => r.DeadLetterMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(),
                "unsupported schema version",
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
        receiver.Verify(
            r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()),
            Times.Never);
        receiver.Verify(
            r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task OversizedSchemaVersionIsTruncatedSoDeadLetterDoesNotThrow()
    {
        // Service Bus rejects a dead-letter reason/description over 4096 characters, so the echoed value
        // is cut to its first 64 characters plus "...".
        var huge = new string('x', 5000);
        var receiver = ReceiverWith(WithSchemaVersion("{ this is not json", huge));
        string? description = null;
        receiver
            .Setup(r => r.DeadLetterMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<CancellationToken>()))
            .Returns<ServiceBusReceivedMessage, string, string, CancellationToken>((_, reason, desc, _) =>
            {
                // Mirror the SDK's own guard (AssertNotTooLong, 4096).
                if (reason.Length > 4096 || desc.Length > 4096)
                {
                    throw new ArgumentOutOfRangeException(nameof(desc));
                }

                description = desc;
                return Task.CompletedTask;
            });
        Exception? reported = null;
        var options = new AsbConsumerOptions { OnError = (e, _, _) => reported = e };

        var count = await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Equal(1, count);
        var expected = new string('x', 64) + "...";
        Assert.NotNull(description);
        Assert.Contains($"'{expected}'", description);
        Assert.DoesNotContain(new string('x', 65), description);
        Assert.DoesNotContain(new string('x', 65), reported!.Message);
        Assert.True(description.Length < 300);
    }

    [Fact]
    public async Task SchemaVersionOfExactlyTheLimitIsNotTruncated()
    {
        var value = new string('x', 64);
        var receiver = WithDeadLetter(ReceiverWith(WithSchemaVersion("{ this is not json", value)));
        Exception? reported = null;
        var options = new AsbConsumerOptions { OnError = (e, _, _) => reported = e };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Contains($"'{value}'", reported!.Message);
        Assert.DoesNotContain("...", reported.Message);
    }

    private static (Mock<ServiceBusSender> Sender, List<ServiceBusMessage> Sent) DlqSender(Exception? failWith = null)
    {
        var sent = new List<ServiceBusMessage>();
        var sender = new Mock<ServiceBusSender>();
        var setup = sender.Setup(x => x.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()));
        if (failWith is not null)
        {
            setup.ThrowsAsync(failWith);
        }
        else
        {
            setup.Callback<ServiceBusMessage, CancellationToken>((m, _) => sent.Add(m)).Returns(Task.CompletedTask);
        }

        return (sender, sent);
    }

    private static ServiceBusReceivedMessage WithBody(byte[] body, object? version = null, int deliveryCount = 1)
    {
        var properties = new Dictionary<string, object>();
        if (version is not null)
        {
            properties["bq-schema-version"] = version;
        }

        return ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromBytes(body), deliveryCount: deliveryCount, properties: properties);
    }

    private static void VerifyOnly(
        Mock<ServiceBusReceiver> receiver, int completes = 0, int abandons = 0, int deadLetters = 0)
    {
        receiver.Verify(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()), Times.Exactly(completes));
        receiver.Verify(r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()), Times.Exactly(abandons));
        receiver.Verify(r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(deadLetters));
    }

    [Fact]
    public async Task VersionGatedMessageIsWrittenRawToTheDlqThenCompletedOnTheFirstDelivery()
    {
        // delivery-count 9, hook set: the gate applies on the first delivery, copies the bytes as received, and the hook is not called.
        var body = Encoding.UTF8.GetBytes("{\n  \"meta\": {\"schema_version\": 2}, \"job\": \"" + Urn + "\"\n}\n");
        var receiver = WithDeadLetter(ReceiverWith(WithBody(body, 2, deliveryCount: 9)));
        var (sender, sent) = DlqSender();
        var hook = 0;
        var calls = 0;
        var options = new AsbConsumerOptions
        {
            DeadLetterSender = sender.Object,
            OnUnknownUrn = (_, _, _) => { hook++; return Task.CompletedTask; },
        };
        var handlers = new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; } };

        await new AsbConsumer(receiver.Object, handlers, options).PollAsync();

        Assert.Equal(0, calls);
        Assert.Equal(0, hook);
        Assert.Equal(body, Assert.Single(sent).Body.ToArray());
        VerifyOnly(receiver, completes: 1);
    }

    [Fact]
    public async Task VersionGatedMessageWhoseDlqWriteFailsIsAbandonedNotCompleted()
    {
        var receiver = WithDeadLetter(ReceiverWith(WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2)));
        var (sender, _) = DlqSender(new InvalidOperationException("dlq down"));
        var reported = new List<Exception>();
        var options = new AsbConsumerOptions { DeadLetterSender = sender.Object, OnError = (e, _, _) => reported.Add(e) };

        var count = await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Equal(1, count);
        VerifyOnly(receiver, abandons: 1);
        Assert.Equal(2, reported.Count);
        Assert.IsType<InvalidOperationException>(reported[1].InnerException);
    }

    [Fact]
    public async Task HookOnlyConsumerDeadLettersAGatedMessageNativelyAndNeverCallsTheHook()
    {
        // OnUnknownUrn alone must not turn the gate into a silent drop (Complete) -- it behaves as fail.
        var receiver = WithDeadLetter(ReceiverWith(WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2)));
        var hook = 0;
        var options = new AsbConsumerOptions { OnUnknownUrn = (_, _, _) => { hook++; return Task.CompletedTask; } };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.Equal(0, hook);
        VerifyOnly(receiver, deadLetters: 1);
    }

    [Fact]
    public async Task PoisonBodyIsWrittenAsAnAdr0035RecordToTheDlqThenCompleted()
    {
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("{\"not\":\"an envelope\"}"), deliveryCount: 4));
        var (sender, sent) = DlqSender();
        Exception? reported = null;
        var calls = 0;
        var options = new AsbConsumerOptions { DeadLetterSender = sender.Object, Queue = "orders", OnError = (e, _, _) => reported = e };
        var handlers = new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => { calls++; return Task.CompletedTask; } };

        await new AsbConsumer(receiver.Object, handlers, options).PollAsync();

        Assert.Equal(0, calls);
        Assert.IsType<BabelQueueException>(reported);
        using var doc = System.Text.Json.JsonDocument.Parse(Assert.Single(sent).Body.ToArray());
        Assert.Equal("an envelope", doc.RootElement.GetProperty("not").GetString());
        var block = doc.RootElement.GetProperty("dead_letter");
        Assert.Equal("poison", block.GetProperty("reason").GetString());
        Assert.Equal("orders", block.GetProperty("original_queue").GetString());
        Assert.Equal(3, block.GetProperty("attempts").GetInt32());
        VerifyOnly(receiver, completes: 1);
    }

    [Fact]
    public async Task HookOnlyConsumerDoesNotDropPoisonSilently()
    {
        // The OnUnknownUrn hook is for an unknown URN only: with the DLQ on a poison record is written, with it off the
        // message is abandoned to the native mechanism -- never Completed (dropped) and the hook is never called.
        var hook = 0;
        foreach (var dlqOn in new[] { true, false })
        {
            var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));
            var (sender, sent) = DlqSender();
            var options = new AsbConsumerOptions
            {
                DeadLetterSender = dlqOn ? sender.Object : null,
                OnUnknownUrn = (_, _, _) => { hook++; return Task.CompletedTask; },
            };

            await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

            Assert.Equal(dlqOn ? 1 : 0, sent.Count);
            if (dlqOn)
            {
                VerifyOnly(receiver, completes: 1);
            }
            else
            {
                VerifyOnly(receiver, abandons: 1);
            }
        }

        Assert.Equal(0, hook);
    }

    [Fact]
    public async Task InvalidUtf8PoisonIsWrappedAsBase64()
    {
        var bytes = new byte[] { 0x7B, 0xFF, 0xFE, 0x7D };
        var receiver = ReceiverWith(WithBody(bytes));
        var (sender, sent) = DlqSender();
        var options = new AsbConsumerOptions { DeadLetterSender = sender.Object };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        using var doc = System.Text.Json.JsonDocument.Parse(Assert.Single(sent).Body.ToArray());
        Assert.Equal(Convert.ToBase64String(bytes), doc.RootElement.GetProperty("raw").GetString());
        Assert.Equal("base64", doc.RootElement.GetProperty("raw_encoding").GetString());
        VerifyOnly(receiver, completes: 1);
    }

    [Fact]
    public async Task PoisonOriginalQueueFallsBackToTheSenderPathWithoutItsDlqSuffix()
    {
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));
        var (sender, sent) = DlqSender();
        sender.SetupGet(x => x.EntityPath).Returns("invoices.dlq");

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions { DeadLetterSender = sender.Object }).PollAsync();

        using var doc = System.Text.Json.JsonDocument.Parse(Assert.Single(sent).Body.ToArray());
        Assert.Equal("invoices", doc.RootElement.GetProperty("dead_letter").GetProperty("original_queue").GetString());
    }

    [Fact]
    public async Task PoisonWhoseDlqWriteFailsIsAbandonedNotCompleted()
    {
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));
        var (sender, _) = DlqSender(new InvalidOperationException("dlq down"));
        var reported = new List<Exception>();
        var options = new AsbConsumerOptions { DeadLetterSender = sender.Object, OnError = (e, _, _) => reported.Add(e) };

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), options).PollAsync();

        VerifyOnly(receiver, abandons: 1);
        Assert.Equal(2, reported.Count);
    }

    [Fact]
    public async Task PoisonWithoutADlqIsLeftToTheNativeMechanismByAbandon()
    {
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>()).PollAsync();

        VerifyOnly(receiver, abandons: 1);
    }

    [Fact]
    public async Task RunAsyncStopsWhenAlreadyCancelled()
    {
        var receiver = ReceiverWith(Received(1));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);

        receiver.Verify(
            r => r.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task HandlerReceivesThePublishedTraceparentThroughTheConsumer()
    {
        const string traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        ServiceBusMessage? published = null;
        var sender = new Mock<ServiceBusSender>();
        sender.SetupGet(s => s.EntityPath).Returns("orders");
        sender.Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
            .Callback<ServiceBusMessage, CancellationToken>((m, _) => published = m)
            .Returns(Task.CompletedTask);
        await new AsbPublisher(sender.Object).PublishWithHeadersAsync(
            Urn, null, new Dictionary<string, string> { ["traceparent"] = traceparent }, "trace-1");

        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: published!.Body,
            subject: published.Subject,
            deliveryCount: 1,
            properties: new Dictionary<string, object>(published.ApplicationProperties));
        IReadOnlyDictionary<string, string>? seen = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            [Urn] = (_, message, _) =>
            {
                seen = AsbTraceHeaders.Extract(message);
                return Task.CompletedTask;
            },
        };

        await new AsbConsumer(ReceiverWith(received).Object, handlers).PollAsync();

        Assert.NotNull(seen);
        Assert.Equal(traceparent, seen!["traceparent"]);
        Assert.DoesNotContain("bq-schema-version", seen.Keys);
    }

    // ---- Dead-letter observer (ADR-0039 section 5.1-5.3) ----

    private sealed class RecordingObserver : IDeadLetterObserver
    {
        public List<DeadLetterEvent> Events { get; } = new();

        public void OnDeadLetter(DeadLetterEvent deadLetterEvent) => Events.Add(deadLetterEvent);
    }

    private sealed class ThrowingObserver : IDeadLetterObserver
    {
        public int Calls { get; private set; }

        public void OnDeadLetter(DeadLetterEvent deadLetterEvent)
        {
            Calls++;
            throw new InvalidOperationException("observer exploded");
        }
    }

    private static Mock<ServiceBusSender> NamedDlqSender(List<ServiceBusMessage> sent, Exception? failWith = null)
    {
        var (sender, _) = DlqSender(failWith);
        sender.SetupGet(x => x.EntityPath).Returns("orders.dlq");
        if (failWith is null)
        {
            sender
                .Setup(x => x.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
                .Callback<ServiceBusMessage, CancellationToken>((m, _) => sent.Add(m))
                .Returns(Task.CompletedTask);
        }

        return sender;
    }

    [Fact]
    public async Task ObserverIsCalledOnceForAPoisonBodyWrittenToTheDlq()
    {
        var observer = new RecordingObserver();
        var sent = new List<ServiceBusMessage>();
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            DeadLetterSender = NamedDlqSender(sent).Object,
            Queue = "orders",
            DeadLetterObserver = observer,
        }).PollAsync();

        Assert.Single(sent);
        Assert.Equal(new DeadLetterEvent("orders", "orders.dlq", "poison"), Assert.Single(observer.Events));
        VerifyOnly(receiver, completes: 1);
    }

    [Fact]
    public async Task ObserverIsCalledOnceForAVersionGatedRawBodyWithAnEmptyReason()
    {
        var observer = new RecordingObserver();
        var sent = new List<ServiceBusMessage>();
        var receiver = WithDeadLetter(ReceiverWith(WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2)));

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            DeadLetterSender = NamedDlqSender(sent).Object,
            Queue = "orders",
            DeadLetterObserver = observer,
        }).PollAsync();

        Assert.Single(sent);
        Assert.Equal(new DeadLetterEvent("orders", "orders.dlq", string.Empty), Assert.Single(observer.Events));
        VerifyOnly(receiver, completes: 1);
    }

    [Fact]
    public async Task ObserverQueueFallsBackToTheSenderPathWithoutItsDlqSuffix()
    {
        var observer = new RecordingObserver();
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            DeadLetterSender = NamedDlqSender(new List<ServiceBusMessage>()).Object,
            DeadLetterObserver = observer,
        }).PollAsync();

        Assert.Equal("orders", Assert.Single(observer.Events).Queue);
    }

    [Fact]
    public async Task ObserverIsCalledOnceForTheNativeDeadLetterOfAGatedMessageWithAnEmptyDeadLetterQueue()
    {
        var observer = new RecordingObserver();
        var receiver = WithDeadLetter(ReceiverWith(WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2)));

        await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            Queue = "orders",
            DeadLetterObserver = observer,
        }).PollAsync();

        VerifyOnly(receiver, deadLetters: 1);
        Assert.Equal(new DeadLetterEvent("orders", string.Empty, string.Empty), Assert.Single(observer.Events));
    }

    [Fact]
    public async Task ObserverIsNotCalledWhenTheNativeDeadLetterCallFails()
    {
        var observer = new RecordingObserver();
        var receiver = ReceiverWith(WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2));
        receiver
            .Setup(r => r.DeadLetterMessageAsync(
                It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("lock lost"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
            {
                DeadLetterObserver = observer,
            }).PollAsync());

        Assert.Empty(observer.Events);
    }

    [Fact]
    public async Task ObserverIsNotCalledWhenTheDeadLetterWriteFails()
    {
        var observer = new RecordingObserver();

        foreach (var message in new[]
                 {
                     WithBody(Encoding.UTF8.GetBytes("junk")),
                     WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2),
                 })
        {
            var receiver = WithDeadLetter(ReceiverWith(message));

            await new AsbConsumer(receiver.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
            {
                DeadLetterSender = NamedDlqSender(new List<ServiceBusMessage>(), new InvalidOperationException("dlq down")).Object,
                DeadLetterObserver = observer,
            }).PollAsync();

            VerifyOnly(receiver, abandons: 1);
        }

        Assert.Empty(observer.Events);
    }

    [Fact]
    public async Task ObserverIsNotCalledForAPoisonBodyWithNoSenderOrForAHandlerFailure()
    {
        var observer = new RecordingObserver();

        var poison = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));
        await new AsbConsumer(poison.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            DeadLetterObserver = observer,
        }).PollAsync();
        VerifyOnly(poison, abandons: 1);

        // A throwing handler is abandoned; the broker owns the MaxDeliveryCount dead-letter, which is never counted.
        var failing = ReceiverWith(Received(10));
        await new AsbConsumer(
            failing.Object,
            new Dictionary<string, BabelHandler> { [Urn] = (_, _, _) => throw new InvalidOperationException("boom") },
            new AsbConsumerOptions { DeadLetterObserver = observer }).PollAsync();
        VerifyOnly(failing, abandons: 1);

        Assert.Empty(observer.Events);
    }

    [Fact]
    public async Task AThrowingObserverDoesNotChangeTheSettleOutcome()
    {
        // Written poison record: still completed.
        var written = new ThrowingObserver();
        var poison = ReceiverWith(WithBody(Encoding.UTF8.GetBytes("junk")));
        var sent = new List<ServiceBusMessage>();
        var count = await new AsbConsumer(poison.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            DeadLetterSender = NamedDlqSender(sent).Object,
            DeadLetterObserver = written,
        }).PollAsync();

        Assert.Equal(1, count);
        Assert.Equal(1, written.Calls);
        Assert.Single(sent);
        VerifyOnly(poison, completes: 1);

        // Native dead-letter: still exactly one native call, nothing completed or abandoned.
        var native = new ThrowingObserver();
        var gated = WithDeadLetter(ReceiverWith(WithBody(Encoding.UTF8.GetBytes(ValidBody()), 2)));
        await new AsbConsumer(gated.Object, new Dictionary<string, BabelHandler>(), new AsbConsumerOptions
        {
            DeadLetterObserver = native,
        }).PollAsync();

        Assert.Equal(1, native.Calls);
        VerifyOnly(gated, deadLetters: 1);
    }
}

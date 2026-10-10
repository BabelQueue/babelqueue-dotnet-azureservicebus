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
}

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

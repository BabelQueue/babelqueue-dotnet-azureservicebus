using Azure.Messaging.ServiceBus;
using BabelQueue.AzureServiceBus;
using Moq;
using Xunit;

namespace BabelQueue.AzureServiceBus.Tests;

/// <summary>The publisher projects the envelope and returns meta.id — against a mocked sender (no broker).</summary>
public sealed class AsbPublisherTests
{
    [Fact]
    public async Task PublishProjectsSubjectAndReturnsMessageId()
    {
        ServiceBusMessage? captured = null;
        var sender = new Mock<ServiceBusSender>();
        sender.SetupGet(s => s.EntityPath).Returns("orders");
        sender.Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
            .Callback<ServiceBusMessage, CancellationToken>((m, _) => captured = m)
            .Returns(Task.CompletedTask);

        var id = await new AsbPublisher(sender.Object)
            .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 });

        Assert.NotNull(captured);
        Assert.Equal("urn:babel:orders:created", captured!.Subject);
        Assert.Equal(id, captured.MessageId);
        Assert.Equal("application/json", captured.ContentType);
        sender.Verify(
            s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task PublishWithDelaySchedulesEnqueueTime()
    {
        ServiceBusMessage? captured = null;
        var sender = new Mock<ServiceBusSender>();
        sender.SetupGet(s => s.EntityPath).Returns("orders");
        sender.Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
            .Callback<ServiceBusMessage, CancellationToken>((m, _) => captured = m)
            .Returns(Task.CompletedTask);

        await new AsbPublisher(sender.Object)
            .PublishAsync("urn:babel:orders:created", delay: TimeSpan.FromSeconds(15));

        Assert.NotNull(captured);
        Assert.True(captured!.ScheduledEnqueueTime > DateTimeOffset.UtcNow);
    }
}

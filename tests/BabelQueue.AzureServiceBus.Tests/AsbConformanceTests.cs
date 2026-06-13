using System.Globalization;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using BabelQueue;
using BabelQueue.AzureServiceBus;
using Moq;
using Xunit;

namespace BabelQueue.AzureServiceBus.Tests;

/// <summary>
/// Azure Service Bus binding conformance against the vendored canonical suite's <c>asb</c>
/// block: the §4 native projection and the <c>attempts = max(body, DeliveryCount - 1)</c>
/// reconciliation. No Azure, no network.
/// </summary>
public sealed class AsbConformanceTests
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static JsonElement Asb()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("asb").Clone();
    }

    [Fact]
    public void PropertyProjectionMatchesGolden()
    {
        var projection = Asb().GetProperty("property_projection");
        var body = File.ReadAllText(Path.Combine(Dir, projection.GetProperty("envelope_file").GetString()!));
        var msg = AsbProperties.ToMessage(EnvelopeCodec.Decode(body));

        var message = projection.GetProperty("message");
        Assert.Equal(message.GetProperty("subject").GetString(), msg.Subject);
        Assert.Equal(message.GetProperty("correlation_id").GetString(), msg.CorrelationId);
        Assert.Equal(message.GetProperty("message_id").GetString(), msg.MessageId);
        Assert.Equal(message.GetProperty("content_type").GetString(), msg.ContentType);

        var want = projection.GetProperty("application_properties");
        Assert.Equal(want.EnumerateObject().Count(), msg.ApplicationProperties.Count);
        foreach (var prop in want.EnumerateObject())
        {
            Assert.True(msg.ApplicationProperties.ContainsKey(prop.Name), prop.Name);
            var got = msg.ApplicationProperties[prop.Name];
            if (prop.Value.ValueKind == JsonValueKind.String)
            {
                Assert.Equal(prop.Value.GetString(), got?.ToString());
            }
            else
            {
                Assert.Equal(prop.Value.GetInt64(), Convert.ToInt64(got, CultureInfo.InvariantCulture));
            }
        }
    }

    [Fact]
    public async Task AttemptsReconciliationMatchesGolden()
    {
        foreach (var testCase in Asb().GetProperty("attempts_reconciliation").GetProperty("cases").EnumerateArray())
        {
            var bodyAttempts = testCase.GetProperty("body_attempts").GetInt32();
            var deliveryCount = testCase.GetProperty("delivery_count").GetInt32();
            var expected = testCase.GetProperty("expected_attempts").GetInt32();

            var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, "orders")
                with { Attempts = bodyAttempts };
            var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                body: BinaryData.FromString(EnvelopeCodec.Encode(env)),
                deliveryCount: deliveryCount);

            var mock = new Mock<ServiceBusReceiver>();
            mock.Setup(r => r.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync((IReadOnlyList<ServiceBusReceivedMessage>)new[] { message });
            mock.Setup(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            mock.Setup(r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var seen = -1;
            var handlers = new Dictionary<string, BabelHandler>
            {
                ["urn:babel:orders:created"] = (e, _, _) => { seen = e.Attempts; return Task.CompletedTask; },
            };
            await new AsbConsumer(mock.Object, handlers).PollAsync();

            Assert.Equal(expected, seen);
        }
    }
}

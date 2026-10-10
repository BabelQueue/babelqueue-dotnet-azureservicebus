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

    // Marker for a present-but-null application property (distinct from an absent one).
    private static readonly object NullProperty = new();

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

    [Fact]
    public async Task SchemaVersionGateMatchesGolden()
    {
        var gate = Asb().GetProperty("schema_version_gate");
        var property = gate.GetProperty("property").GetString()!;
        var body = File.ReadAllText(Path.Combine(Dir, gate.GetProperty("fixture").GetString()!));
        var job = EnvelopeCodec.Decode(body).Job!;

        var cases = gate.GetProperty("cases").EnumerateArray().ToList();
        Assert.NotEmpty(cases);
        foreach (var testCase in cases)
        {
            var expect = testCase.GetProperty("expect").GetString();
            var absent = testCase.TryGetProperty("absent", out var a) && a.GetBoolean();

            if (testCase.TryGetProperty("value_type", out var declaredType) && declaredType.GetString() != "float")
            {
                throw new InvalidOperationException(
                    $"Unknown value_type '{declaredType.GetString()}' (this reader only understands 'float').");
            }

            // JSON numbers carry no width: integral values run at both int32 and int64 width.
            var values = new List<(string Label, object? Value)>();
            if (absent)
            {
                values.Add(("<absent>", null));
            }
            else
            {
                var value = testCase.GetProperty("value");
                switch (value.ValueKind)
                {
                    case JsonValueKind.String:
                        values.Add(($"'{value.GetString()}'", value.GetString()));
                        break;
                    case JsonValueKind.Null:
                        // A present-but-null property: must behave exactly like an absent one.
                        values.Add(("<null>", NullProperty));
                        break;
                    case JsonValueKind.Number when testCase.TryGetProperty("value_type", out var valueType)
                                                   && valueType.GetString() == "float":
                        // An explicit floating-point case (e.g. 1.0): hand over real floats, never integers.
                        values.Add(($"double {value.GetDouble().ToString(CultureInfo.InvariantCulture)}", value.GetDouble()));
                        values.Add(($"float {value.GetSingle().ToString(CultureInfo.InvariantCulture)}", value.GetSingle()));
                        break;
                    case JsonValueKind.Number when value.TryGetInt64(out var integral):
                        values.Add(($"int32 {integral}", (int)integral));
                        values.Add(($"int64 {integral}", integral));
                        break;
                    case JsonValueKind.Number:
                        values.Add(($"double {value.GetDouble().ToString(CultureInfo.InvariantCulture)}", value.GetDouble()));
                        break;
                    default:
                        throw new InvalidOperationException($"Unexpected gate value kind {value.ValueKind}.");
                }
            }

            foreach (var (label, value) in values)
            {
                var properties = new Dictionary<string, object>();
                if (ReferenceEquals(value, NullProperty))
                {
                    properties[property] = null!;
                }
                else if (value is not null)
                {
                    properties[property] = value;
                }

                var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                    body: BinaryData.FromString(body),
                    deliveryCount: 1,
                    properties: properties);

                var mock = new Mock<ServiceBusReceiver>();
                mock.Setup(r => r.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync((IReadOnlyList<ServiceBusReceivedMessage>)new[] { message });
                mock.Setup(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.CompletedTask);
                mock.Setup(r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.CompletedTask);

                var seen = 0;
                var handlers = new Dictionary<string, BabelHandler>
                {
                    [job] = (_, _, _) => { seen++; return Task.CompletedTask; },
                };
                await new AsbConsumer(mock.Object, handlers).PollAsync();

                Assert.True(
                    (expect == "decode" ? 1 : 0) == seen,
                    $"{label}: expected {expect} but handler ran {seen} time(s)");
                mock.Verify(
                    r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()),
                    expect == "decode" ? Times.Never() : Times.Once());
            }
        }
    }
}

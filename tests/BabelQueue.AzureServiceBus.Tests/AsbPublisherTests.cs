using System.Diagnostics;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using BabelQueue;
using BabelQueue.AzureServiceBus;
using BabelQueue.Tracing;
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

    // ADR-0028: a W3C traceparent rides as an application property beside the envelope, never inside it.
    private const string Traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private const string Tracestate = "vendor=opaque,other=1";

    private static Dictionary<string, string> TraceHeaders() => new()
    {
        ["traceparent"] = Traceparent,
        ["tracestate"] = Tracestate,
    };

    private static HashSet<string> TopLevelKeys(string body)
    {
        using var doc = JsonDocument.Parse(body);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in doc.RootElement.EnumerateObject())
        {
            keys.Add(property.Name);
            if (property is { Name: "meta", Value.ValueKind: JsonValueKind.Object })
            {
                foreach (var inner in property.Value.EnumerateObject())
                {
                    keys.Add("meta." + inner.Name);
                }
            }
        }
        return keys;
    }

    private static async Task<Activity?> ConsumeUnderListener(Envelope envelope, IReadOnlyDictionary<string, string> headers, ActivityTraceId expectedTrace)
    {
        Activity? consumerSpan = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                if (activity.Kind == ActivityKind.Consumer && activity.TraceId == expectedTrace)
                {
                    consumerSpan = activity;
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        await Telemetry.Wrap(_ => Task.CompletedTask, headers)(envelope);
        return consumerSpan;
    }

    private const string Urn = "urn:babel:orders:created";

    private static (Mock<ServiceBusSender> Sender, List<ServiceBusMessage> Sent) SenderOn(string entity)
    {
        var sent = new List<ServiceBusMessage>();
        var sender = new Mock<ServiceBusSender>();
        sender.SetupGet(s => s.EntityPath).Returns(entity);
        sender.Setup(s => s.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()))
            .Callback<ServiceBusMessage, CancellationToken>((m, _) => sent.Add(m))
            .Returns(Task.CompletedTask);
        return (sender, sent);
    }

    /// <summary>The message a consumer receives for a sent one: same body, subject and application properties.</summary>
    private static ServiceBusReceivedMessage Received(ServiceBusMessage sent) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: sent.Body,
            subject: sent.Subject,
            deliveryCount: 1,
            properties: new Dictionary<string, object>(sent.ApplicationProperties));

    [Fact]
    public async Task PublishWithHeadersCarriesTraceparentAndLeavesTheBodyAnEnvelope()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new AsbPublisher(sender.Object);

        await publisher.PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "trace-1");
        await publisher.PublishWithHeadersAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, TraceHeaders(), "trace-1");

        var traced = sent[1];
        var received = Received(traced);

        // The received message carries the header verbatim.
        Assert.Equal(Traceparent, received.ApplicationProperties["traceparent"]);
        Assert.Equal(Tracestate, received.ApplicationProperties["tracestate"]);
        Assert.Equal(Traceparent, AsbTraceHeaders.Extract(received)["traceparent"]);

        // The body is the same envelope shape and never mentions the trace header.
        var body = received.Body.ToString();
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
        Assert.DoesNotContain(Traceparent, body, StringComparison.Ordinal);
        Assert.Equal(TopLevelKeys(sent[0].Body.ToString()), TopLevelKeys(body));
        Assert.Equal(Urn, EnvelopeCodec.Decode(body).Job);

        // The contract projection is untouched.
        Assert.Equal(Urn, traced.Subject);
        Assert.Equal("trace-1", traced.CorrelationId);
        Assert.Equal(1, traced.ApplicationProperties["bq-schema-version"]);
    }

    [Fact]
    public async Task PublishWithoutHeadersAddsNoTraceProperties()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new AsbPublisher(sender.Object);

        await publisher.PublishAsync(Urn);
        await publisher.PublishWithHeadersAsync(Urn, null, new Dictionary<string, string>());
        await publisher.PublishWithHeadersAsync(Urn, null, null);

        foreach (var message in sent)
        {
            Assert.False(message.ApplicationProperties.ContainsKey("traceparent"));
            Assert.False(message.ApplicationProperties.ContainsKey("tracestate"));
            Assert.Equal(sent[0].ApplicationProperties.Count, message.ApplicationProperties.Count);
        }
    }

    [Fact]
    public async Task PublishWithHeadersNeverClobbersAContractProperty()
    {
        var (sender, sent) = SenderOn("orders");

        await new AsbPublisher(sender.Object).PublishWithHeadersAsync(
            Urn,
            null,
            new Dictionary<string, string> { ["bq-schema-version"] = "9", ["bq-delay"] = "1", [""] = "x" },
            delay: TimeSpan.FromSeconds(15));

        var properties = sent[0].ApplicationProperties;
        Assert.Equal(1, properties["bq-schema-version"]);
        Assert.Equal(15000L, properties["bq-delay"]);
    }

    [Fact]
    public async Task ConsumerSpanIsATrueChildOfTheProducerSpanViaTheReceivedProperties()
    {
        var (sender, sent) = SenderOn("orders");
        await new AsbPublisher(sender.Object).PublishWithHeadersAsync(Urn, null, TraceHeaders(), "trace-1");
        var received = Received(sent[0]);
        var envelope = EnvelopeCodec.Decode(received.Body.ToString());

        var span = await ConsumeUnderListener(
            envelope, AsbTraceHeaders.Extract(received), ActivityTraceId.CreateFromString(Traceparent.AsSpan(3, 32)));

        Assert.NotNull(span);
        Assert.Equal("b7ad6b7169203331", span!.ParentSpanId.ToHexString());
    }

    [Fact]
    public void ExtractSkipsNonStringAndEmptyValues()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{}"),
            properties: new Dictionary<string, object>
            {
                ["bq-schema-version"] = 1,
                ["empty"] = string.Empty,
                ["traceparent"] = Traceparent,
            });

        Assert.Equal(Traceparent, Assert.Single(AsbTraceHeaders.Extract(received)).Value);
        Assert.Empty(AsbTraceHeaders.Extract(ServiceBusModelFactory.ServiceBusReceivedMessage(body: BinaryData.FromString("{}"))));
    }

    [Fact]
    public async Task PublishWithHeadersDropsEveryContractPropertyIncludingBqDelay()
    {
        // Parity with the Go reference: contract properties are owned by the transport and never accepted from
        // the carrier, even bq-delay on a publish that has no delay.
        var (sender, sent) = SenderOn("orders");
        var carrier = new Dictionary<string, string> { ["traceparent"] = Traceparent };
        foreach (var key in new[] { "bq-schema-version", "bq-source-lang", "bq-created-at", "bq-attempts", "bq-delay" })
        {
            carrier[key] = "evil";
        }

        await new AsbPublisher(sender.Object).PublishWithHeadersAsync(Urn, null, carrier, "trace-1");

        var properties = sent[0].ApplicationProperties;
        Assert.False(properties.ContainsKey("bq-delay"));
        Assert.False(properties.ContainsKey("bq-attempts"));
        Assert.Equal(1, properties["bq-schema-version"]);
        Assert.IsType<long>(properties["bq-created-at"]);
        Assert.NotEqual("evil", properties["bq-source-lang"]);
        Assert.Equal(Traceparent, properties["traceparent"]);
    }

    [Fact]
    public void ExtractLeavesOutTheContractProperties()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("{}"),
            properties: new Dictionary<string, object>
            {
                ["bq-source-lang"] = "dotnet",
                ["bq-attempts"] = "9",
                ["bq-delay"] = "30000",
                ["traceparent"] = Traceparent,
            });

        Assert.Equal(Traceparent, Assert.Single(AsbTraceHeaders.Extract(received)).Value);
    }

    // O6: the envelope Telemetry.PublishAsync builds is the one that is sent, so the producer span's
    // messaging.message.id is the meta.id on the wire (and the traceparent rides beside the envelope).
    [Fact]
    public async Task TelemetryPublishEnvelopeRecordsTheWireMessageId()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new AsbPublisher(sender.Object);
        const string envelopeUrn = "urn:babel:orders:envelope-id";
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer && Equals(activity.GetTagItem("babelqueue.urn"), envelopeUrn))
                {
                    lock (activities)
                    {
                        activities.Add(activity);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var headers = new Dictionary<string, string>();
        var id = await Telemetry.PublishAsync(
            envelopeUrn,
            new Dictionary<string, object?> { ["order_id"] = 7 },
            headers,
            env => publisher.PublishEnvelopeAsync(env, headers),
            "orders");

        var producerSpan = Assert.Single(activities);
        var message = Assert.Single(sent);
        var body = message.Body.ToString();
        var traceparent = message.ApplicationProperties.TryGetValue("traceparent", out var tp) ? (string)tp : null;
        var wire = EnvelopeCodec.Decode(body);

        Assert.Equal(id, wire.Meta!.Id);
        Assert.Equal(wire.Meta.Id, producerSpan.GetTagItem("messaging.message.id"));
        Assert.Equal(Telemetry.UuidOf(producerSpan.TraceId), wire.TraceId);
        Assert.Equal(wire.TraceId, producerSpan.GetTagItem("messaging.message.conversation_id"));
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta.SchemaVersion);
        Assert.Equal(headers["traceparent"], traceparent);
        Assert.Contains(producerSpan.TraceId.ToHexString(), traceparent, StringComparison.Ordinal);
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublishEnvelopeSendsTheGivenEnvelopeUnchanged()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new AsbPublisher(sender.Object);
        var made = EnvelopeCodec.Make(
            "urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-fixed");
        var envelope = made with
        {
            Extras = new Dictionary<string, JsonElement> { ["x_tenant"] = JsonSerializer.SerializeToElement("t1") },
            Meta = made.Meta! with
            {
                Extras = new Dictionary<string, JsonElement> { ["x_region"] = JsonSerializer.SerializeToElement("eu") },
            },
        };

        var id = await publisher.PublishEnvelopeAsync(envelope);

        var message = Assert.Single(sent);
        var body = message.Body.ToString();
        var traceparent = message.ApplicationProperties.TryGetValue("traceparent", out var tp) ? (string)tp : null;
        Assert.Equal(envelope.Meta!.Id, id);
        Assert.Equal(EnvelopeCodec.Encode(envelope), body);
        var wire = EnvelopeCodec.Decode(body);
        Assert.Equal("trace-fixed", wire.TraceId);
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta!.SchemaVersion);
        Assert.Equal(envelope.Meta.Id, wire.Meta.Id);
        Assert.Equal("t1", wire.Extras!["x_tenant"].GetString());
        Assert.Equal("eu", wire.Meta.Extras!["x_region"].GetString());
        Assert.Null(traceparent);
    }

    // Documents the PublishEnvelopeAsync contract: meta.queue is whatever the envelope carries. Telemetry stamps
    // its queue argument (default "default"), not this publisher's target, so callers must pass the queue.
    [Fact]
    public async Task TelemetryPublishWithoutAQueueArgumentStampsDefaultMetaQueue()
    {
        var (sender, sent) = SenderOn("orders");
        var publisher = new AsbPublisher(sender.Object);
        await Telemetry.PublishAsync(
            "urn:babel:orders:created", null, env => publisher.PublishEnvelopeAsync(env));

        var message = Assert.Single(sent);
        var body = message.Body.ToString();
        var traceparent = message.ApplicationProperties.TryGetValue("traceparent", out var tp) ? (string)tp : null;
        Assert.Equal("default", EnvelopeCodec.Decode(body).Meta!.Queue);
        Assert.Null(traceparent);
    }
}

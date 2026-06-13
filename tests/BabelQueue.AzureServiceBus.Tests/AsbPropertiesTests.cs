using BabelQueue;
using BabelQueue.AzureServiceBus;
using Xunit;

namespace BabelQueue.AzureServiceBus.Tests;

/// <summary>
/// §4 native projection (no broker): Subject = URN, CorrelationId = trace_id,
/// MessageId = meta.id, plus the bq- ApplicationProperties; the body stays the
/// canonical envelope and a delay schedules native enqueue time.
/// </summary>
public sealed class AsbPropertiesTests
{
    private static Envelope Sample() =>
        EnvelopeCodec.Make(
            "urn:babel:orders:created",
            new Dictionary<string, object?> { ["order_id"] = 1042 },
            "orders",
            "trace-xyz");

    [Fact]
    public void ProjectsNativeFieldsAndApplicationProperties()
    {
        var env = Sample();
        var msg = AsbProperties.ToMessage(env);

        Assert.Equal("urn:babel:orders:created", msg.Subject);
        Assert.Equal("trace-xyz", msg.CorrelationId);
        Assert.Equal(env.Meta!.Id, msg.MessageId);
        Assert.Equal("application/json", msg.ContentType);
        Assert.Equal(env.Meta.SchemaVersion, msg.ApplicationProperties["bq-schema-version"]);
        Assert.Equal(env.Meta.Lang, msg.ApplicationProperties["bq-source-lang"]);
        Assert.Equal(env.Meta.CreatedAt, msg.ApplicationProperties["bq-created-at"]);
    }

    [Fact]
    public void BodyIsTheCanonicalEnvelope()
    {
        var env = Sample();
        var msg = AsbProperties.ToMessage(env);

        var decoded = EnvelopeCodec.Decode(msg.Body.ToString());
        Assert.True(EnvelopeCodec.Accepts(decoded));
        Assert.Equal("urn:babel:orders:created", EnvelopeCodec.Urn(decoded));
    }

    [Fact]
    public void DelaySchedulesNativeEnqueueTime()
    {
        var env = Sample();
        var before = DateTimeOffset.UtcNow;

        var msg = AsbProperties.ToMessage(env, TimeSpan.FromSeconds(30));

        Assert.True(msg.ScheduledEnqueueTime >= before.AddSeconds(29));
        Assert.Equal(30000L, msg.ApplicationProperties["bq-delay"]);
    }

    [Fact]
    public void NoDelayLeavesEnqueueTimeUnscheduled()
    {
        var msg = AsbProperties.ToMessage(Sample());
        Assert.Equal(default, msg.ScheduledEnqueueTime);
        Assert.False(msg.ApplicationProperties.ContainsKey("bq-delay"));
    }
}

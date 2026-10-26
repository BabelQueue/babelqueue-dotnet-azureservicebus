using System.Globalization;
using System.Text;
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

    // -- poison_dlq (ADR-0035) --------------------------------------------------------------------------------------

    private static JsonElement Section(string name)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty(name).Clone();
    }

    private static readonly JsonDocumentOptions DeepJson = new() { MaxDepth = 10_100 };

    [Fact]
    public void PoisonDlqRecordShapeMatchesGolden()
    {
        var section = Section("poison_dlq");
        var originalQueue = section.GetProperty("original_queue").GetString()!;
        var cases = section.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(43, cases.Count);
        foreach (var testCase in cases)
        {
            var name = testCase.GetProperty("name").GetString();
            var bytes = testCase.TryGetProperty("body_base64", out var b64)
                ? Convert.FromBase64String(b64.GetString()!)
                : Encoding.UTF8.GetBytes(testCase.GetProperty("body").GetString()!);
            var expect = testCase.GetProperty("expect");
            var shape = expect.GetProperty("shape").GetString()!;

            var record = AsbConsumer.PoisonBody(bytes, "poison", "System.Exception", 1700000000000L, originalQueue, 0);
            var text = Encoding.UTF8.GetString(record);
            Assert.False(text.Contains('\uFFFD', StringComparison.Ordinal), $"{name}: the record holds U+FFFD");

            using var doc = JsonDocument.Parse(record, DeepJson);
            Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
            var deadLetter = doc.RootElement.EnumerateObject().Where(p => p.Name == "dead_letter").ToList();
            Assert.True(deadLetter.Count == 1, $"{name}: exactly one top-level dead_letter");
            Assert.Equal("poison", deadLetter[0].Value.GetProperty("reason").GetString());
            Assert.Equal(originalQueue, deadLetter[0].Value.GetProperty("original_queue").GetString());
            var rest = doc.RootElement.EnumerateObject().Where(p => p.Name != "dead_letter").ToList();

            var want = expect.GetProperty("record");
            if (shape == "splice_or_wrapper" && !SameMembers(rest, want))
            {
                foreach (var literal in expect.GetProperty("splice_literals").EnumerateArray())
                {
                    Assert.True(text.Contains(literal.GetString()!, StringComparison.Ordinal), $"{name}: missing literal {literal.GetString()}");
                }

                Assert.DoesNotContain(rest, p => p.Name is "raw" or "raw_encoding");
                var keys = rest.Select(p => p.Name).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
                var wantKeys = expect.GetProperty("splice_keys").EnumerateArray().Select(k => k.GetString()!).OrderBy(n => n, StringComparer.Ordinal).ToList();
                Assert.Equal(wantKeys, keys);
                continue;
            }

            Assert.True(SameMembers(rest, want), $"{name}: the record without dead_letter differs: {text}");
        }
    }

    private static bool SameMembers(List<JsonProperty> members, JsonElement want)
    {
        var wantMembers = want.EnumerateObject().ToList();
        return members.Count == wantMembers.Count
            && members.All(m => wantMembers.Count(w => w.Name == m.Name && SameJson(m.Value, w.Value)) == 1);
    }

    private static bool SameJson(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        return a.ValueKind switch
        {
            JsonValueKind.Object => SameMembers(a.EnumerateObject().ToList(), b),
            JsonValueKind.Array => a.GetArrayLength() == b.GetArrayLength()
                && a.EnumerateArray().Zip(b.EnumerateArray(), SameJson).All(x => x),
            JsonValueKind.String => a.GetString() == b.GetString(),
            JsonValueKind.Number => a.GetRawText() == b.GetRawText(),
            _ => true,
        };
    }

    // -- version_gate_asb (ADR-0037 decision 1) ---------------------------------------------------------------------

    [Fact]
    public async Task VersionGateSettleMatchesGolden()
    {
        var section = Section("version_gate_asb");
        var cases = section.GetProperty("cases").EnumerateArray().ToList();
        Assert.Equal(17, cases.Count);
        var bodies = section.GetProperty("incoming_bodies").EnumerateArray()
            .Select(b => (Name: b.GetProperty("name").GetString()!, Text: b.GetProperty("body").GetString()!))
            .ToList();
        Assert.Equal(3, bodies.Count);

        var asserted = 0;
        foreach (var testCase in cases)
        {
            // The .NET Azure Service Bus consumer has only an OnUnknownUrn callback and no strategy option: it behaves as
            // `fail`, so rows with any other on_unknown_urn value are skipped (ADR-0037), not passed.
            if (testCase.TryGetProperty("on_unknown_urn", out var strategy) && strategy.GetString() != "fail")
            {
                continue;
            }

            asserted++;
            var caseName = testCase.GetProperty("name").GetString();
            var deadLetterOn = testCase.GetProperty("dead_letter").GetString() == "on";
            var writeFails = testCase.TryGetProperty("dlq_write", out var w) && w.GetString() == "fails";
            var expect = testCase.GetProperty("expect");
            var outcome = !deadLetterOn && expect.TryGetProperty("outcome_no_writer", out var nw)
                ? nw.GetString()!
                : expect.GetProperty("outcome").GetString()!;

            foreach (var (bodyName, bodyText) in bodies)
            {
                var label = $"{caseName} / {bodyName}";
                var expected = Encoding.UTF8.GetBytes(bodyText);
                var properties = new Dictionary<string, object> { ["bq-schema-version"] = 2 };
                if (testCase.TryGetProperty("bq_attempts", out var attempts))
                {
                    properties["bq-attempts"] = attempts.GetString()!;
                }

                var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
                    body: BinaryData.FromBytes(expected),
                    deliveryCount: 9,
                    properties: properties);

                var receiver = new Mock<ServiceBusReceiver>();
                receiver.Setup(r => r.ReceiveMessagesAsync(It.IsAny<int>(), It.IsAny<TimeSpan?>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync((IReadOnlyList<ServiceBusReceivedMessage>)new[] { message });
                receiver.Setup(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
                receiver.Setup(r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
                receiver.Setup(r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);

                var sent = new List<ServiceBusMessage>();
                var sender = new Mock<ServiceBusSender>();
                var send = sender.Setup(x => x.SendMessageAsync(It.IsAny<ServiceBusMessage>(), It.IsAny<CancellationToken>()));
                if (writeFails)
                {
                    send.ThrowsAsync(new InvalidOperationException("dlq down"));
                }
                else
                {
                    send.Callback<ServiceBusMessage, CancellationToken>((m, _) => sent.Add(m)).Returns(Task.CompletedTask);
                }

                var handled = 0;
                var handlers = new Dictionary<string, BabelHandler>
                {
                    ["urn:babel:orders:created"] = (_, _, _) => { handled++; return Task.CompletedTask; },
                };
                var options = new AsbConsumerOptions { DeadLetterSender = deadLetterOn ? sender.Object : null };

                await new AsbConsumer(receiver.Object, handlers, options).PollAsync();

                Assert.True(handled == 0, $"{label}: a version-gated delivery reached a handler");
                switch (outcome)
                {
                    case "dlq_raw":
                        receiver.Verify(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()), Times.Once());
                        receiver.Verify(r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()), Times.Never());
                        var record = Assert.Single(sent);
                        Assert.True(record.Body.ToArray().SequenceEqual(expected), $"{label}: the DLQ record is not the incoming body byte for byte");
                        break;
                    case "unsettled":
                        receiver.Verify(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()), Times.Never());
                        receiver.Verify(r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never());
                        receiver.Verify(r => r.AbandonMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<IDictionary<string, object>>(), It.IsAny<CancellationToken>()), Times.Once());
                        break;
                    case "native":
                        // Either an explicit native DeadLetter or an Abandon until MaxDeliveryCount is conformant; this
                        // consumer dead-letters explicitly. Never completed, never written.
                        receiver.Verify(r => r.CompleteMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<CancellationToken>()), Times.Never());
                        receiver.Verify(r => r.DeadLetterMessageAsync(It.IsAny<ServiceBusReceivedMessage>(), It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once());
                        Assert.Empty(sent);
                        break;
                    default:
                        Assert.Fail($"{label}: unknown outcome {outcome}");
                        break;
                }
            }
        }

        Assert.Equal(8, asserted);
    }
}

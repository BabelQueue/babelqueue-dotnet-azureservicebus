# BabelQueue.AzureServiceBus

Azure Service Bus transport for [BabelQueue](https://babelqueue.com) — "Polyglot Queues,
Simplified." Built on `Azure.Messaging.ServiceBus` and the framework-agnostic
[`BabelQueue.Core`](https://www.nuget.org/packages/BabelQueue.Core).

A canonical-envelope **publisher** and a URN-routed **consumer**, so an Azure Service
Bus-based .NET service speaks the same wire contract (envelope shape, URN identity, trace
propagation) as the Java, Python, Go and Node SDKs. Implements
[§4 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#azure-service-bus).

## Install

```bash
dotnet add package BabelQueue.AzureServiceBus
```

It pulls `BabelQueue.Core` and `Azure.Messaging.ServiceBus` transitively.

## Use

```csharp
using Azure.Messaging.ServiceBus;
using BabelQueue.AzureServiceBus;

await using var client = new ServiceBusClient("<connection-string>"); // or a namespace + TokenCredential

// produce
var sender = client.CreateSender("orders");
var id = await new AsbPublisher(sender)
    .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1042 });

// consume (PeekLock)
var receiver = client.CreateReceiver("orders");
var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = async (env, message, ct) =>
    {
        // env.Data, env.TraceId, env.Attempts ...
    },
};
var consumer = new AsbConsumer(receiver, handlers, new AsbConsumerOptions
{
    OnError = (err, env, msg) => Console.Error.WriteLine(err),
});
await consumer.RunAsync(cancellationToken);
```

**Schema-version gate (ADR-0037).** Before decoding, the consumer reads the `bq-schema-version` application property. Absent or blank decodes as usual; integer `1` (any integral type) and string `"1"` are accepted; any other value is rejected without decoding the body and settled on its **first delivery**, never retried and never handed to a handler or `OnUnknownUrn`. With no `DeadLetterSender` it is **dead-lettered explicitly** to the native sub-queue (reason "unsupported schema version"), without Abandon. With a `DeadLetterSender` (a sender on `<queue>.dlq`) its body is written **exactly as received** (no `dead_letter` block) to `<queue>.dlq` and then `Complete`d; a failed write `Abandon`s it (reported). The rejected value is echoed in the description and `OnError` message as at most its first 64 characters (`...` appended when cut), since Service Bus caps the description at 4096 characters. This consumer has only an `OnUnknownUrn` callback and no strategy option: it behaves as `fail`, and the callback never applies to a gated message.

**Poison (ADR-0037).** A body that can never reach a handler — invalid UTF-8, not a JSON object, an object without a valid `job`/`urn`, or any other non-conformant envelope — is never handed to one. With a `DeadLetterSender` it is written to `<queue>.dlq` as the ADR-0035 record (`reason: "poison"`; a strict JSON object is spliced byte for byte, anything else is wrapped as `raw`, base64 for invalid UTF-8) and `Complete`d only after the write; a failed write `Abandon`s it. `dead_letter.original_queue` is `AsbConsumerOptions.Queue`, else the receiver entity path. With no `DeadLetterSender` it is `Abandon`ed, so the broker moves it to the native dead-letter sub-queue at `MaxDeliveryCount` (today's behaviour). **Behaviour change:** with a `DeadLetterSender`, a poison body is dead-lettered at its first delivery instead of being abandoned until `MaxDeliveryCount`.

```csharp
await using var dlq = client.CreateSender("orders.dlq");   // an ordinary queue (§4.5)
var consumer = new AsbConsumer(receiver, handlers, new AsbConsumerOptions { DeadLetterSender = dlq });
```

Delayed delivery: `PublishAsync(urn, data, delay: TimeSpan.FromMinutes(5))` → native
`ScheduledEnqueueTime`. Auth: a connection string, or the fully-qualified namespace + a
`TokenCredential` (`DefaultAzureCredential`).

## Contract mapping (§4)

| Envelope | Azure Service Bus |
| :--- | :--- |
| body | `Body` (byte-identical across SDKs) |
| `job` (URN) | `Subject` |
| `trace_id` | `CorrelationId` |
| `meta.id` | `MessageId` |
| `meta.schema_version` | `ApplicationProperties["bq-schema-version"]` |
| `meta.lang` | `ApplicationProperties["bq-source-lang"]` |
| `meta.created_at` | `ApplicationProperties["bq-created-at"]` (ms) |
| `attempts` | `DeliveryCount − 1` (broker-authoritative) |
| reserve / ack / retry | PeekLock → `Complete` / `Abandon` |

`DeliveryCount` is the **authoritative** attempts source on ASB (native, 1-based) — the
contract `attempts` is `DeliveryCount − 1`. A throwing handler `Abandon`s the message, so
the broker redelivers it and increments `DeliveryCount` (at-least-once); at
`MaxDeliveryCount` it auto-moves to the native dead-letter sub-queue. The poll loop never
stops on a bad message — observe via `OnError` / `OnUnknownUrn`. The envelope is unchanged
(`schema_version` stays `1`); Azure Service Bus is purely additive.

## Build & test

```bash
dotnet test
```

`ServiceBusSender` / `ServiceBusReceiver` are mockable, so the unit tests use Moq +
`ServiceBusModelFactory` — no Azure, no network. The vendored conformance suite (v1.8.0: `poison_dlq`, `version_gate_asb`) is run too.

## License

MIT

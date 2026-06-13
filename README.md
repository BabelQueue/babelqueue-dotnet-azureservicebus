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
`ServiceBusModelFactory` — no Azure, no network.

## License

MIT

# Changelog

All notable changes to `BabelQueue.AzureServiceBus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Azure Service Bus transport on `BabelQueue.Core` +
  `Azure.Messaging.ServiceBus`: `AsbPublisher` (canonical-envelope send with the §4 native
  projection — `Subject` = URN, `CorrelationId` = `trace_id`, `MessageId` = `meta.id`, plus
  `bq-schema-version`/`bq-source-lang`/`bq-created-at` `ApplicationProperties`; native
  `ScheduledEnqueueTime` for delays) and `AsbConsumer` (PeekLock receive → URN-routed
  `BabelHandler`s → `Complete`; a throwing handler `Abandon`s for at-least-once redelivery;
  `attempts` reconciled to the broker-authoritative `DeliveryCount − 1`; `OnError`/
  `OnUnknownUrn` hooks). `net8.0`, Roslyn analyzers (latest-recommended, warnings-as-errors);
  13 xUnit tests run against a Moq-mocked sender/receiver (no Azure, no network). The envelope
  is unchanged (`schema_version: 1`); Azure Service Bus is purely additive.

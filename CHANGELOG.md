# Changelog

All notable changes to `BabelQueue.AzureServiceBus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.1.0] - 2026-10-26

Implements ADR-0037 decisions 1 and 2. The envelope stays at `schema_version: 1`; every change is a consumer-side settle
decision. Ships as a **MINOR**.

### Added

- `AsbConsumerOptions.DeadLetterSender` (a `ServiceBusSender` on `<queue>.dlq`, opt-in): turns BabelQueue dead-lettering on.
  With it set, a poison body is written as the ADR-0035 record (`reason: "poison"`) and a `bq-schema-version`-gated message
  as the raw incoming body (no `dead_letter` block), each on its first delivery, and `Complete`d after the write; a failed
  write `Abandon`s the message and is reported.
- `AsbConsumerOptions.Queue`: the consumer's queue name for `dead_letter.original_queue` of a poison record (default: the
  receiver entity path, else the sender path without `.dlq`).

### Changed

- **Behaviour change (with `DeadLetterSender` set): a poison body is dead-lettered at its first delivery.** Before, it was
  abandoned until `MaxDeliveryCount` moved it to the native sub-queue. It is written via `BabelQueue.Core`'s `PoisonRecord`
  (a strict JSON object is spliced byte for byte, anything else is wrapped as `raw`, base64 for invalid UTF-8) and is
  never handed to a handler. Invalid UTF-8 is now poison (before, it was decoded leniently).
- **Behaviour change (with `DeadLetterSender` set): a gated message goes to `<queue>.dlq` raw.** Before, it was dead-lettered
  to the native `$DeadLetterQueue`. With no `DeadLetterSender` it is still dead-lettered explicitly to the native sub-queue
  (reason "unsupported schema version"), and a poison body is still abandoned (unchanged).
- The `OnUnknownUrn` callback applies to an unknown URN only; this consumer has no strategy option and behaves as `fail` for a
  poison or gated message (ADR-0037 "Configured strategy only").
- `BabelQueue.Core` dependency raised from `1.0.0` to `1.13.0` (`PoisonRecord`).
- Vendored conformance suite updated to v1.8.0; CI pins v1.8.0. The runners cover the `poison_dlq` and `version_gate_asb`
  sections (rows with an `on_unknown_urn` value other than `fail` are not asserted: the consumer has no strategy option,
  ADR-0037).
- CI pins the `fetch.sh` it downloads by sha256 (`BQ_FETCH_SH_SHA256`) before running it.

## [1.0.1] - 2026-10-10

### Fixed
- `AsbConsumer` now checks the `bq-schema-version` application property **before** decoding the body (§4.7). An unsupported value is dead-lettered explicitly (`DeadLetterMessageAsync`, reason "unsupported schema version") without decoding and without abandoning. The string `"1"` and any integral AMQP type with value `1` are accepted (producers write integer `1`); absent or blank (empty / ASCII whitespace) falls through to the normal decode. The rejected value is echoed truncated to 64 characters (`...` appended) so an oversized value cannot make `DeadLetterMessageAsync` throw. No public API change.

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

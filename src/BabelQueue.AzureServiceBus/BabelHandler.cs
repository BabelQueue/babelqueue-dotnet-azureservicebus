using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>
/// Processes one decoded, validated envelope and the raw Service Bus message it arrived on.
/// Completing normally acknowledges it (the consumer <c>Complete</c>s it); throwing leaves it
/// for the broker to redeliver (the consumer <c>Abandon</c>s it, incrementing
/// <c>DeliveryCount</c>).
/// </summary>
/// <remarks>
/// To continue a W3C <c>traceparent</c> that rode beside the envelope (ADR-0028), hand
/// <c>AsbTraceHeaders.Extract(message)</c> to <c>Telemetry.Wrap(handler, headers)</c>; the consumer span then becomes a true child of the
/// producer span. With no <c>traceparent</c> in the application properties the core falls back to the v0.1 <c>trace_id</c> mapping.
/// </remarks>
public delegate Task BabelHandler(Envelope envelope, ServiceBusReceivedMessage message, CancellationToken cancellationToken);

using Azure.Messaging.ServiceBus;

namespace BabelQueue.AzureServiceBus;

/// <summary>
/// Processes one decoded, validated envelope and the raw Service Bus message it arrived on.
/// Completing normally acknowledges it (the consumer <c>Complete</c>s it); throwing leaves it
/// for the broker to redeliver (the consumer <c>Abandon</c>s it, incrementing
/// <c>DeliveryCount</c>).
/// </summary>
public delegate Task BabelHandler(Envelope envelope, ServiceBusReceivedMessage message, CancellationToken cancellationToken);

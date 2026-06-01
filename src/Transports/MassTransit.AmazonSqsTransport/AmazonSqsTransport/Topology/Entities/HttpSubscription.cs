namespace MassTransit.AmazonSqsTransport.Topology;


/// <summary>
/// Represents an SNS topic subscription with HTTP/HTTPS protocol
/// </summary>
public interface HttpSubscription
{
    /// <summary>
    /// The SNS topic source
    /// </summary>
    Topic Source { get; }

    /// <summary>
    /// The HTTP/HTTPS endpoint URL that will receive the messages
    /// </summary>
    string EndpointUrl { get; }

    /// <summary>
    /// When true, SNS delivers the raw message body without the JSON wrapper
    /// </summary>
    bool RawMessageDelivery { get; }

    /// <summary>
    /// When true, a Dead Letter Queue (SQS) is automatically created for this HTTP subscription.
    /// </summary>
    bool DeadLetterQueueEnabled { get; }

    /// <summary>
    /// Custom name for the DLQ. If null/empty, defaults to {TopicName}-http-dlq.
    /// </summary>
    string? DeadLetterQueueName { get; }

    /// <summary>
    /// Maximum number of delivery retries before sending the message to the DLQ. Range: 1-100. Default: 3.
    /// </summary>
    int MaxReceiveCount { get; }

    /// <summary>
    /// Minimum delay (in seconds) between delivery retries. Default: 20.
    /// </summary>
    int MinDelayTarget { get; }

    /// <summary>
    /// Maximum delay (in seconds) between delivery retries. Default: 20.
    /// </summary>
    int MaxDelayTarget { get; }

    /// <summary>
    /// Backoff function for retry delays. Default: "linear".
    /// </summary>
    string BackoffFunction { get; }
}

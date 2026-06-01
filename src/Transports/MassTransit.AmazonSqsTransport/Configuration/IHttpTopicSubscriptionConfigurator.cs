namespace MassTransit;


public interface IHttpTopicSubscriptionConfigurator
{
    /// <summary>
    /// The SNS topic name to subscribe to
    /// </summary>
    string TopicName { get; }

    /// <summary>
    /// The HTTP/HTTPS endpoint URL that will receive the messages
    /// </summary>
    string EndpointUrl { get; set; }

    /// <summary>
    /// When true, SNS delivers the raw message body without the JSON wrapper.
    /// When false (default), SNS delivers the full envelope with Type, MessageId, TopicArn, and Message fields.
    /// Defaults to false.
    /// </summary>
    bool RawMessageDelivery { get; set; }

    /// <summary>
    /// Whether the topic is durable (survives broker restart). Defaults to true.
    /// </summary>
    bool Durable { get; set; }

    /// <summary>
    /// Whether the topic is auto-deleted when the connection is closed. Defaults to false.
    /// </summary>
    bool AutoDelete { get; set; }

    /// <summary>
    /// Habilita la creación automática de una cola SQS DLQ para la suscripción HTTP.
    /// Defaults to false.
    /// </summary>
    bool DeadLetterQueueEnabled { get; set; }

    /// <summary>
    /// Nombre personalizado para la cola DLQ. Si es null/vacío, se genera como {TopicName}-http-dlq.
    /// </summary>
    string? DeadLetterQueueName { get; set; }

    /// <summary>
    /// Número máximo de reintentos de entrega antes de enviar a la DLQ. Rango: 1-100. Default: 3.
    /// </summary>
    int MaxReceiveCount { get; set; }

    /// <summary>
    /// Minimum delay (in seconds) between delivery retries. Range: 1-3600. Default: 20.
    /// </summary>
    int MinDelayTarget { get; set; }

    /// <summary>
    /// Maximum delay (in seconds) between delivery retries. Range: 1-3600. Default: 20.
    /// Must be >= MinDelayTarget.
    /// </summary>
    int MaxDelayTarget { get; set; }

    /// <summary>
    /// Backoff function for retry delays. Default: "linear".
    /// Valid values: "linear", "arithmetic", "geometric", "exponential".
    /// </summary>
    string BackoffFunction { get; set; }
}

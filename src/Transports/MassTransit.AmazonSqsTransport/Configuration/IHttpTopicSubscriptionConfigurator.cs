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
}

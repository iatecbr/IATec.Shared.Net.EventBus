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
}

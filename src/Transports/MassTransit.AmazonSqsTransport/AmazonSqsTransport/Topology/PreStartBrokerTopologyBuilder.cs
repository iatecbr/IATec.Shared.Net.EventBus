namespace MassTransit.AmazonSqsTransport.Topology;


/// <summary>
/// Builds a broker topology that combines publish topology entries and HTTP subscription entries.
/// Used in the PreStartPipe to apply both in a single pass.
/// </summary>
public class PreStartBrokerTopologyBuilder :
    BrokerTopologyBuilder,
    IPublishEndpointBrokerTopologyBuilder,
    IReceiveEndpointBrokerTopologyBuilder
{
    public TopicHandle? Topic { get; set; }
    public QueueHandle? Queue { get; set; }

    public BrokerTopology Build()
    {
        return new AmazonSqsBrokerTopology(Topics, Queues, QueueSubscriptions, TopicSubscriptions, HttpSubscriptions);
    }
}

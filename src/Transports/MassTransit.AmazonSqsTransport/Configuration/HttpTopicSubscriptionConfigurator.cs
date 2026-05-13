namespace MassTransit;


public class HttpTopicSubscriptionConfigurator :
    IHttpTopicSubscriptionConfigurator
{
    public HttpTopicSubscriptionConfigurator(string topicName, string endpointUrl, bool durable = true, bool autoDelete = false)
    {
        TopicName = topicName;
        EndpointUrl = endpointUrl;
        Durable = durable;
        AutoDelete = autoDelete;
        RawMessageDelivery = false;
    }

    public string TopicName { get; }
    public string EndpointUrl { get; set; }
    public bool RawMessageDelivery { get; set; }
    public bool Durable { get; set; }
    public bool AutoDelete { get; set; }
}

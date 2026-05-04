namespace MassTransit.AmazonSqsTransport.Topology;

using MassTransit.Topology;


public interface HttpSubscriptionHandle : EntityHandle
{
    HttpSubscription HttpSubscription { get; }
}

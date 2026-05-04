namespace MassTransit.AmazonSqsTransport.Configuration;

using System;
using MassTransit.Configuration;
using Topology;


public interface IAmazonSqsHostConfiguration :
    IHostConfiguration,
    IReceiveConfigurator<IAmazonSqsReceiveEndpointConfigurator>
{
    AmazonSqsHostSettings Settings { get; set; }

    IConnectionContextSupervisor ConnectionContextSupervisor { get; }

    new IAmazonSqsBusTopology Topology { get; }

    /// <summary>
    /// Apply the endpoint definition to the receive endpoint configurator
    /// </summary>
    void ApplyEndpointDefinition(IAmazonSqsReceiveEndpointConfigurator configurator, IEndpointDefinition definition);

    /// <summary>
    /// Create a receive endpoint configuration using the specified host
    /// </summary>
    IAmazonSqsReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(string queueName,
        Action<IAmazonSqsReceiveEndpointConfigurator>? configure = null);

    /// <summary>
    /// Create a receive endpoint configuration for the default host
    /// </summary>
    IAmazonSqsReceiveEndpointConfiguration CreateReceiveEndpointConfiguration(QueueReceiveSettings settings,
        IAmazonSqsEndpointConfiguration endpointConfiguration, Action<IAmazonSqsReceiveEndpointConfigurator>? configure = null);

    /// <summary>
    /// Adds an HTTP subscription specification to be applied on bus startup via the PreStartPipe
    /// </summary>
    void AddHttpSubscriptionSpecification(IAmazonSqsConsumeTopologySpecification specification);

    /// <summary>
    /// Returns true if there are HTTP subscription specifications registered
    /// </summary>
    bool HasHttpSubscriptions();

    /// <summary>
    /// Builds a BrokerTopology that includes both the publish topology and all HTTP subscriptions
    /// </summary>
    BrokerTopology BuildPreStartBrokerTopology();
}

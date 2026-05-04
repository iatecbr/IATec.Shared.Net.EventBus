namespace MassTransit;

using System;
using System.Collections.Generic;
using AmazonSqsTransport.Configuration;
using Internals;
using MassTransit.AmazonSqsTransport.Topology;


/// <summary>
/// Applies an SNS HTTP/HTTPS subscription to the broker topology builder.
/// This creates the SNS topic (if needed) and registers the HTTP subscription.
/// Topic names respect the scope prefix when ScopeTopics is enabled.
/// </summary>
public class HttpSubscriptionConsumeTopologySpecification :
    AmazonSqsTopicSubscriptionConfigurator,
    IAmazonSqsConsumeTopologySpecification
{
    readonly string _endpointUrl;
    readonly Uri? _hostAddress;
    readonly IAmazonSqsPublishTopology _publishTopology;
    readonly bool _rawMessageDelivery;

    public HttpSubscriptionConsumeTopologySpecification(
        IAmazonSqsPublishTopology publishTopology,
        Uri hostAddress,
        string topicName,
        string endpointUrl,
        bool rawMessageDelivery = true,
        bool durable = true,
        bool autoDelete = false)
        : base(topicName, durable, autoDelete)
    {
        _publishTopology = publishTopology;
        _hostAddress = hostAddress;
        _endpointUrl = endpointUrl;
        _rawMessageDelivery = rawMessageDelivery;
    }

    // Constructor for backward compatibility (when HostAddress is not available)
    public HttpSubscriptionConsumeTopologySpecification(
        IAmazonSqsPublishTopology publishTopology,
        string topicName,
        string endpointUrl,
        bool rawMessageDelivery = true,
        bool durable = true,
        bool autoDelete = false)
        : base(topicName, durable, autoDelete)
    {
        _publishTopology = publishTopology;
        _hostAddress = null;
        _endpointUrl = endpointUrl;
        _rawMessageDelivery = rawMessageDelivery;
    }

    public IEnumerable<ValidationResult> Validate()
    {
        if (string.IsNullOrWhiteSpace(EntityName))
            yield return this.Failure("TopicName", "must not be empty");

        if (string.IsNullOrWhiteSpace(_endpointUrl))
            yield return this.Failure("EndpointUrl", "must not be empty");

        if (!string.IsNullOrWhiteSpace(_endpointUrl)
            && !_endpointUrl.StartsWith("http://", System.StringComparison.OrdinalIgnoreCase)
            && !_endpointUrl.StartsWith("https://", System.StringComparison.OrdinalIgnoreCase))
            yield return this.Failure("EndpointUrl", "must start with http:// or https://");
    }

    public void Apply(IReceiveEndpointBrokerTopologyBuilder builder)
    {
        string topicName;
        
        // Apply scope prefix when HostAddress is available (using the same logic as GetDestinationAddress)
        if (_hostAddress != null)
        {
            var address = new AmazonSqsEndpointAddress(_hostAddress, new Uri($"topic:{EntityName}"));
            topicName = address.Name;
        }
        else
        {
            topicName = EntityName;
        }

        var topicHandle = builder.CreateTopic(
            topicName,
            Durable,
            AutoDelete,
            _publishTopology.TopicAttributes.MergeLeft(TopicAttributes),
            _publishTopology.TopicSubscriptionAttributes.MergeLeft(TopicSubscriptionAttributes),
            _publishTopology.TopicTags.MergeLeft(Tags));

        builder.CreateHttpSubscription(topicHandle, _endpointUrl, _rawMessageDelivery);
    }
}

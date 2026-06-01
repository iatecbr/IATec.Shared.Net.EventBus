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
    readonly bool _deadLetterQueueEnabled;
    readonly string? _deadLetterQueueName;
    readonly int _maxReceiveCount;
    readonly int _minDelayTarget;
    readonly int _maxDelayTarget;
    readonly string _backoffFunction;

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
        _deadLetterQueueEnabled = false;
        _deadLetterQueueName = null;
        _maxReceiveCount = 3;
        _minDelayTarget = 20;
        _maxDelayTarget = 20;
        _backoffFunction = "linear";
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
        _deadLetterQueueEnabled = false;
        _deadLetterQueueName = null;
        _maxReceiveCount = 3;
        _minDelayTarget = 20;
        _maxDelayTarget = 20;
        _backoffFunction = "linear";
    }

    /// <summary>
    /// Constructor with DLQ parameters for Dead Letter Queue support.
    /// </summary>
    public HttpSubscriptionConsumeTopologySpecification(
        IAmazonSqsPublishTopology publishTopology,
        Uri hostAddress,
        string topicName,
        string endpointUrl,
        bool rawMessageDelivery,
        bool durable,
        bool autoDelete,
        bool deadLetterQueueEnabled,
        string? deadLetterQueueName,
        int maxReceiveCount,
        int minDelayTarget = 20,
        int maxDelayTarget = 20,
        string backoffFunction = "linear")
        : base(topicName, durable, autoDelete)
    {
        _publishTopology = publishTopology;
        _hostAddress = hostAddress;
        _endpointUrl = endpointUrl;
        _rawMessageDelivery = rawMessageDelivery;
        _deadLetterQueueEnabled = deadLetterQueueEnabled;
        _deadLetterQueueName = deadLetterQueueName;
        _maxReceiveCount = maxReceiveCount;
        _minDelayTarget = minDelayTarget;
        _maxDelayTarget = maxDelayTarget;
        _backoffFunction = backoffFunction;
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

        if (_deadLetterQueueEnabled)
        {
            var resolvedName = ResolveDeadLetterQueueName();

            if (string.IsNullOrWhiteSpace(resolvedName))
                yield return this.Failure("DeadLetterQueueName", "must not be empty");
            else if (resolvedName.Length > 80)
                yield return this.Failure("DeadLetterQueueName", "must not exceed 80 characters");
            else if (!IsValidQueueName(resolvedName))
                yield return this.Failure("DeadLetterQueueName",
                    "must contain only alphanumeric characters, hyphens (-), and underscores (_)");
        }
    }

    public void Apply(IReceiveEndpointBrokerTopologyBuilder builder)
    {
        string topicName;
        string scopePrefix = "";

        // Apply scope prefix when HostAddress is available (using the same logic as GetDestinationAddress)
        if (_hostAddress != null)
        {
            var address = new AmazonSqsEndpointAddress(_hostAddress, new Uri($"topic:{EntityName}"));
            topicName = address.Name;

            // Extract scope prefix for DLQ naming
            if (address.Scope != "/")
                scopePrefix = address.Scope.Trim('/') + "_";
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

        if (_deadLetterQueueEnabled)
        {
            var dlqName = scopePrefix + ResolveDeadLetterQueueName();
            var queueAttributes = new Dictionary<string, object>
            {
                ["MessageRetentionPeriod"] = "2592000"
            };

            builder.CreateQueue(dlqName, Durable, AutoDelete, queueAttributes);
        }

        builder.CreateHttpSubscription(topicHandle, _endpointUrl, _rawMessageDelivery,
            _deadLetterQueueEnabled, _deadLetterQueueEnabled ? scopePrefix + ResolveDeadLetterQueueName() : _deadLetterQueueName, _maxReceiveCount,
            _minDelayTarget, _maxDelayTarget, _backoffFunction);
    }

    string ResolveDeadLetterQueueName()
    {
        return string.IsNullOrWhiteSpace(_deadLetterQueueName)
            ? $"{EntityName}-http-dlq"
            : _deadLetterQueueName;
    }

    static bool IsValidQueueName(string name)
    {
        foreach (var c in name)
        {
            if (!char.IsLetterOrDigit(c) && c != '-' && c != '_')
                return false;
        }

        return true;
    }
}

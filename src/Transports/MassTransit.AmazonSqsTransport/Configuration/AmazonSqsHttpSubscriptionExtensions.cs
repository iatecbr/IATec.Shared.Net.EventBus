namespace MassTransit;

using System;
using AmazonSqsTransport.Configuration;


public static class AmazonSqsHttpSubscriptionExtensions
{
    /// <summary>
    /// Adds an SNS HTTP/HTTPS subscription for the specified topic name.
    /// SNS will POST messages to the given endpoint URL.
    /// The endpoint must handle the SNS SubscriptionConfirmation request — use
    /// <see cref="AmazonSqsTransport.SnsSubscriptionConfirmationHandler"/> in your HTTP handler.
    /// </summary>
    public static void SubscribeTopicToHttpEndpoint(
        this IAmazonSqsBusFactoryConfigurator configurator,
        string topicName,
        string endpointUrl,
        Action<IHttpTopicSubscriptionConfigurator>? configure = null)
    {
        if (configurator == null)
            throw new ArgumentNullException(nameof(configurator));
        if (string.IsNullOrWhiteSpace(topicName))
            throw new ArgumentNullException(nameof(topicName));
        if (string.IsNullOrWhiteSpace(endpointUrl))
            throw new ArgumentNullException(nameof(endpointUrl));

        if (configurator is not AmazonSqsBusFactoryConfigurator busConfigurator)
            throw new InvalidOperationException(
                $"SubscribeTopicToHttpEndpoint requires {nameof(AmazonSqsBusFactoryConfigurator)}");

        var subscriptionConfigurator = new HttpTopicSubscriptionConfigurator(topicName, endpointUrl);
        configure?.Invoke(subscriptionConfigurator);

        var specification = new HttpSubscriptionConsumeTopologySpecification(
            configurator.PublishTopology,
            busConfigurator.HostAddress,
            subscriptionConfigurator.TopicName,
            subscriptionConfigurator.EndpointUrl,
            subscriptionConfigurator.RawMessageDelivery,
            subscriptionConfigurator.Durable,
            subscriptionConfigurator.AutoDelete,
            subscriptionConfigurator.DeadLetterQueueEnabled,
            subscriptionConfigurator.DeadLetterQueueName,
            subscriptionConfigurator.MaxReceiveCount,
            subscriptionConfigurator.MinDelayTarget,
            subscriptionConfigurator.MaxDelayTarget,
            subscriptionConfigurator.BackoffFunction);

        busConfigurator.AddHttpSubscriptionSpecification(specification);
    }

    /// <summary>
    /// Adds an SNS HTTP/HTTPS subscription for the message type <typeparamref name="T"/>.
    /// The topic name is derived from the message type using the default naming convention.
    /// </summary>
    public static void SubscribeTopicToHttpEndpoint<T>(
        this IAmazonSqsBusFactoryConfigurator configurator,
        string endpointUrl,
        Action<IHttpTopicSubscriptionConfigurator>? configure = null)
        where T : class
    {
        var topicName = configurator.PublishTopology.GetMessageTopology<T>().Topic.EntityName;

        configurator.SubscribeTopicToHttpEndpoint(topicName, endpointUrl, configure);
    }
}

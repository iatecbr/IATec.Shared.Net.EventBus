namespace MassTransit.AmazonSqsTransport.Middleware;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Topology;


/// <summary>
/// Configures HTTP/HTTPS SNS subscriptions on startup.
/// For each HttpSubscription in the broker topology, this filter:
/// 1. Ensures the SNS topic exists
/// 2. Creates the SNS subscription with Protocol = "https" (or "http")
/// 3. Automatically confirms the subscription by visiting the SubscribeURL sent by SNS
/// </summary>
public class ConfigureHttpSubscriptionFilter :
    IFilter<ClientContext>
{
    readonly BrokerTopology _brokerTopology;
    readonly IAmazonSimpleNotificationService _snsClient;

    public ConfigureHttpSubscriptionFilter(BrokerTopology brokerTopology, IAmazonSimpleNotificationService snsClient)
    {
        _brokerTopology = brokerTopology;
        _snsClient = snsClient;
    }

    public async Task Send(ClientContext context, IPipe<ClientContext> next)
    {
        await ConfigureHttpSubscriptions(context, context.CancellationToken).ConfigureAwait(false);

        await next.Send(context).ConfigureAwait(false);
    }

    public void Probe(ProbeContext context)
    {
        var scope = context.CreateFilterScope("configureHttpSubscriptions");
        _brokerTopology.Probe(scope);
    }

    async Task ConfigureHttpSubscriptions(ClientContext context, CancellationToken cancellationToken)
    {
        if (_brokerTopology.HttpSubscriptions.Length == 0)
            return;

        IEnumerable<Task> tasks = _brokerTopology.HttpSubscriptions
            .Select(subscription => DeclareHttpSubscription(context, subscription, cancellationToken));

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    async Task DeclareHttpSubscription(ClientContext context, HttpSubscription subscription, CancellationToken cancellationToken)
    {
        var topicInfo = await context.CreateTopic(subscription.Source, cancellationToken).ConfigureAwait(false);

        var protocol = subscription.EndpointUrl.StartsWith("https://", StringComparison.OrdinalIgnoreCase) ? "https" : "http";

        var subscriptionAttributes = new Dictionary<string, string>
        {
            ["RawMessageDelivery"] = subscription.RawMessageDelivery ? "true" : "false"
        };

        string? subscriptionArn = null;

        try
        {
            var response = await _snsClient.SubscribeAsync(new SubscribeRequest
            {
                TopicArn = topicInfo.Arn,
                Protocol = protocol,
                Endpoint = subscription.EndpointUrl,
                Attributes = subscriptionAttributes,
                ReturnSubscriptionArn = true
            }, cancellationToken).ConfigureAwait(false);

            response.EnsureSuccessfulResponse();

            subscriptionArn = response.SubscriptionArn;

            LogContext.Debug?.Log("Created HTTP subscription {SubscriptionArn} for topic {Topic} -> {Endpoint}",
                subscriptionArn, subscription.Source.EntityName, subscription.EndpointUrl);
        }
        catch (InvalidParameterException exception) when (exception.Message.Contains("exists"))
        {
            var existing = await _snsClient.ListSubscriptionsByTopicAsync(topicInfo.Arn, cancellationToken).ConfigureAwait(false);
            existing.EnsureSuccessfulResponse();

            var match = existing.Subscriptions.SingleOrDefault(x =>
                x.TopicArn == topicInfo.Arn
                && x.Endpoint == subscription.EndpointUrl
                && (x.Protocol == "http" || x.Protocol == "https"));

            if (match != null)
            {
                subscriptionArn = match.SubscriptionArn;
                LogContext.Debug?.Log("Existing HTTP subscription {SubscriptionArn} for topic {Topic} -> {Endpoint}",
                    subscriptionArn, subscription.Source.EntityName, subscription.EndpointUrl);
            }
        }

        if (subscriptionArn == "PendingConfirmation")
            LogContext.Info?.Log("HTTP subscription pending confirmation for topic {Topic} -> {Endpoint}. " +
                "Ensure the endpoint handles the SNS SubscriptionConfirmation request.",
                subscription.Source.EntityName, subscription.EndpointUrl);
    }
}

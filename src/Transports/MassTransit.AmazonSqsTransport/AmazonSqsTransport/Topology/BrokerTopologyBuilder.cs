namespace MassTransit.AmazonSqsTransport.Topology;

using System.Collections.Generic;
using System.Threading;
using MassTransit.Topology;


public abstract class BrokerTopologyBuilder
{
    protected readonly NamedEntityCollection<QueueEntity, QueueHandle> Queues;
    protected readonly NamedEntityCollection<QueueSubscriptionEntity, QueueSubscriptionHandle> QueueSubscriptions;
    protected readonly NamedEntityCollection<TopicEntity, TopicHandle> Topics;
    protected readonly NamedEntityCollection<TopicSubscriptionEntity, TopicSubscriptionHandle> TopicSubscriptions;
    protected readonly NamedEntityCollection<HttpSubscriptionEntity, HttpSubscriptionHandle> HttpSubscriptions;
    long _nextId;

    protected BrokerTopologyBuilder()
    {
        Topics = new NamedEntityCollection<TopicEntity, TopicHandle>(TopicEntity.EntityComparer, TopicEntity.NameComparer);
        Queues = new NamedEntityCollection<QueueEntity, QueueHandle>(QueueEntity.QueueComparer, QueueEntity.NameComparer);
        QueueSubscriptions =
            new NamedEntityCollection<QueueSubscriptionEntity, QueueSubscriptionHandle>(QueueSubscriptionEntity.EntityComparer,
                QueueSubscriptionEntity.NameComparer);
        TopicSubscriptions =
            new NamedEntityCollection<TopicSubscriptionEntity, TopicSubscriptionHandle>(TopicSubscriptionEntity.EntityComparer,
                TopicSubscriptionEntity.NameComparer);
        HttpSubscriptions =
            new NamedEntityCollection<HttpSubscriptionEntity, HttpSubscriptionHandle>(HttpSubscriptionEntity.EntityComparer,
                HttpSubscriptionEntity.NameComparer);
    }

    long GetNextId()
    {
        return Interlocked.Increment(ref _nextId);
    }

    public TopicHandle CreateTopic(string name, bool durable, bool autoDelete, IDictionary<string, object>? topicAttributes = null,
        IDictionary<string, object>? topicSubscriptionAttributes = null, IDictionary<string, string>? tags = null)
    {
        var id = GetNextId();

        var topicEntity = new TopicEntity(id, name, durable, autoDelete, topicAttributes, topicSubscriptionAttributes, tags);

        return Topics.GetOrAdd(topicEntity);
    }

    public QueueHandle CreateQueue(string name, bool durable, bool autoDelete, IDictionary<string, object>? queueAttributes = null,
        IDictionary<string, object>? queueSubscriptionAttributes = null, IDictionary<string, string>? tags = null)
    {
        var id = GetNextId();

        var queueEntity = new QueueEntity(id, name, durable, autoDelete, queueAttributes, queueSubscriptionAttributes, tags);

        return Queues.GetOrAdd(queueEntity);
    }

    public QueueSubscriptionHandle CreateQueueSubscription(TopicHandle topic, QueueHandle queue)
    {
        var id = GetNextId();

        var topicEntity = Topics.Get(topic);

        var queueEntity = Queues.Get(queue);

        var binding = new QueueSubscriptionEntity(id, topicEntity, queueEntity);

        return QueueSubscriptions.GetOrAdd(binding);
    }

    public TopicSubscriptionHandle CreateTopicSubscription(TopicHandle source, TopicHandle destination)
    {
        var id = GetNextId();

        var sourceEntity = Topics.Get(source);

        var destinationEntity = Topics.Get(destination);

        var binding = new TopicSubscriptionEntity(id, sourceEntity, destinationEntity);

        return TopicSubscriptions.GetOrAdd(binding);
    }

    public HttpSubscriptionHandle CreateHttpSubscription(TopicHandle topic, string endpointUrl, bool rawMessageDelivery = true)
    {
        var id = GetNextId();

        var topicEntity = Topics.Get(topic);

        var subscription = new HttpSubscriptionEntity(id, topicEntity, endpointUrl, rawMessageDelivery);

        return HttpSubscriptions.GetOrAdd(subscription);
    }

    public HttpSubscriptionHandle CreateHttpSubscription(TopicHandle topic, string endpointUrl, bool rawMessageDelivery,
        bool deadLetterQueueEnabled, string? deadLetterQueueName, int maxReceiveCount,
        int minDelayTarget = 20, int maxDelayTarget = 20, string backoffFunction = "linear")
    {
        var id = GetNextId();

        var topicEntity = Topics.Get(topic);

        var subscription = new HttpSubscriptionEntity(id, topicEntity, endpointUrl, rawMessageDelivery,
            deadLetterQueueEnabled, deadLetterQueueName, maxReceiveCount, minDelayTarget, maxDelayTarget, backoffFunction);

        return HttpSubscriptions.GetOrAdd(subscription);
    }
}

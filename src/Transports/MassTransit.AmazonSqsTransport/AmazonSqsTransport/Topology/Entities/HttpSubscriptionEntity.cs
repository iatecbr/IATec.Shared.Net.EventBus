namespace MassTransit.AmazonSqsTransport.Topology;

using System.Collections.Generic;
using System.Linq;


public class HttpSubscriptionEntity :
    HttpSubscription,
    HttpSubscriptionHandle
{
    readonly string _endpointUrl;
    readonly TopicEntity _topic;

    public HttpSubscriptionEntity(long id, TopicEntity topic, string endpointUrl, bool rawMessageDelivery)
    {
        Id = id;
        _topic = topic;
        _endpointUrl = endpointUrl;
        RawMessageDelivery = rawMessageDelivery;
    }

    public static IEqualityComparer<HttpSubscriptionEntity> EntityComparer { get; } = new HttpSubscriptionEntityEqualityComparer();
    public static IEqualityComparer<HttpSubscriptionEntity> NameComparer { get; } = new NameEqualityComparer();

    public long Id { get; }
    public HttpSubscription HttpSubscription => this;

    public Topic Source => _topic.Topic;
    public string EndpointUrl => _endpointUrl;
    public bool RawMessageDelivery { get; }

    public override string ToString()
    {
        return string.Join(", ",
            new[] { $"source: {Source.EntityName}", $"endpoint: {_endpointUrl}" }.Where(x => !string.IsNullOrWhiteSpace(x)));
    }


    sealed class HttpSubscriptionEntityEqualityComparer : IEqualityComparer<HttpSubscriptionEntity>
    {
        public bool Equals(HttpSubscriptionEntity? x, HttpSubscriptionEntity? y)
        {
            if (ReferenceEquals(x, y))
                return true;
            if (ReferenceEquals(x, null) || ReferenceEquals(y, null))
                return false;
            if (x.GetType() != y.GetType())
                return false;

            return x._topic.Equals(y._topic) && x._endpointUrl == y._endpointUrl;
        }

        public int GetHashCode(HttpSubscriptionEntity obj)
        {
            unchecked
            {
                var hashCode = obj._topic.GetHashCode();
                hashCode = (hashCode * 397) ^ obj._endpointUrl.GetHashCode();
                return hashCode;
            }
        }
    }


    sealed class NameEqualityComparer : IEqualityComparer<HttpSubscriptionEntity>
    {
        public bool Equals(HttpSubscriptionEntity? x, HttpSubscriptionEntity? y)
        {
            if (ReferenceEquals(x, y))
                return true;
            if (ReferenceEquals(x, null) || ReferenceEquals(y, null))
                return false;
            if (x.GetType() != y.GetType())
                return false;

            return string.Equals(x._topic.EntityName, y._topic.EntityName) && x._endpointUrl == y._endpointUrl;
        }

        public int GetHashCode(HttpSubscriptionEntity obj)
        {
            unchecked
            {
                return (obj._topic.EntityName.GetHashCode() * 397) ^ obj._endpointUrl.GetHashCode();
            }
        }
    }
}

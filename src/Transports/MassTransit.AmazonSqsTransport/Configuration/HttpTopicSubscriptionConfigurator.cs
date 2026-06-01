namespace MassTransit;

using System;


public class HttpTopicSubscriptionConfigurator :
    IHttpTopicSubscriptionConfigurator
{
    int _maxReceiveCount = 3;
    string? _deadLetterQueueName;

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

    public bool DeadLetterQueueEnabled { get; set; }

    public string? DeadLetterQueueName
    {
        get => _deadLetterQueueName;
        set
        {
            if (value != null)
            {
                if (value.Length > 80)
                    throw new ArgumentException("DeadLetterQueueName must not exceed 80 characters.", nameof(value));
                if (!IsValidQueueName(value))
                    throw new ArgumentException(
                        "DeadLetterQueueName must contain only alphanumeric characters, hyphens (-), and underscores (_).",
                        nameof(value));
            }

            _deadLetterQueueName = value;
        }
    }

    public int MaxReceiveCount
    {
        get => _maxReceiveCount;
        set
        {
            if (value < 1 || value > 100)
                throw new ArgumentOutOfRangeException(nameof(value), value,
                    "MaxReceiveCount must be between 1 and 100 (inclusive).");
            _maxReceiveCount = value;
        }
    }

    int _minDelayTarget = 20;
    int _maxDelayTarget = 20;

    public int MinDelayTarget
    {
        get => _minDelayTarget;
        set
        {
            if (value < 1 || value > 3600)
                throw new ArgumentOutOfRangeException(nameof(value), value,
                    "MinDelayTarget must be between 1 and 3600 seconds.");
            _minDelayTarget = value;
        }
    }

    public int MaxDelayTarget
    {
        get => _maxDelayTarget;
        set
        {
            if (value < 1 || value > 3600)
                throw new ArgumentOutOfRangeException(nameof(value), value,
                    "MaxDelayTarget must be between 1 and 3600 seconds.");
            _maxDelayTarget = value;
        }
    }

    public string BackoffFunction { get; set; } = "linear";

    /// <summary>
    /// Resolves the final DLQ name: uses DeadLetterQueueName if defined,
    /// or generates {TopicName}-http-dlq.
    /// </summary>
    internal string ResolveDeadLetterQueueName()
    {
        return string.IsNullOrWhiteSpace(DeadLetterQueueName)
            ? $"{TopicName}-http-dlq"
            : DeadLetterQueueName;
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

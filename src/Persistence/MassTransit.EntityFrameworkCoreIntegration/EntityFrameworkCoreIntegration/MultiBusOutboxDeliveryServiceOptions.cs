namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System;


    /// <summary>
    /// Per-bus delivery service options for a MultiBus bus outbox. Isolated by <typeparamref name="TBus" /> so that
    /// each typed bus's delivery service has its own configuration, independent of the default
    /// <see cref="OutboxDeliveryServiceOptions" /> used by the primary <see cref="IBus" />.
    /// </summary>
    /// <typeparam name="TBus">The typed bus (e.g. ISnsBus)</typeparam>
    public class MultiBusOutboxDeliveryServiceOptions<TBus>
        where TBus : class, IBus
    {
        /// <summary>
        /// The number of messages to deliver at a time from an individual outbox
        /// </summary>
        public int MessageDeliveryLimit { get; set; } = 100;

        /// <summary>
        /// Transport Send timeout when delivering messages to the transport
        /// </summary>
        public TimeSpan MessageDeliveryTimeout { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Delay between each database sweep to load outbox messages
        /// </summary>
        public TimeSpan QueryDelay { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// The number of outboxes to query at a time
        /// </summary>
        public int QueryMessageLimit { get; set; } = 100;

        /// <summary>
        /// Database query timeout for loading outbox messages
        /// </summary>
        public TimeSpan QueryTimeout { get; set; } = TimeSpan.FromSeconds(30);

        internal OutboxDeliveryServiceOptions ToDeliveryServiceOptions()
        {
            return new OutboxDeliveryServiceOptions
            {
                MessageDeliveryLimit = MessageDeliveryLimit,
                MessageDeliveryTimeout = MessageDeliveryTimeout,
                QueryDelay = QueryDelay,
                QueryMessageLimit = QueryMessageLimit,
                QueryTimeout = QueryTimeout
            };
        }
    }
}

namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Options;
    using Middleware;
    using Middleware.Outbox;
    using Transports;


    /// <summary>
    /// Bus outbox delivery service for a specific (typed) bus in a MultiBus setup. Behaves exactly like
    /// <see cref="BusOutboxDeliveryService{TDbContext}" />, but resolves the <see cref="IBusControl" /> from the
    /// <see cref="IBusInstance{TBus}" /> of <typeparamref name="TBus" /> so that outbox messages are delivered
    /// through the correct transport (e.g. Amazon SQS/SNS) instead of the default <see cref="IBus" />.
    /// </summary>
    /// <typeparam name="TBus">The typed bus (e.g. ISnsBus)</typeparam>
    /// <typeparam name="TDbContext">The DbContext holding this bus's outbox tables</typeparam>
    public class MultiBusBusOutboxDeliveryService<TBus, TDbContext> :
        BusOutboxDeliveryService<TDbContext>
        where TBus : class, IBus
        where TDbContext : DbContext
    {
        public MultiBusBusOutboxDeliveryService(IBusInstance<TBus> busInstance,
            IOptions<MultiBusOutboxDeliveryServiceOptions<TBus>> options,
            IOptions<EntityFrameworkOutboxOptions<TDbContext>> outboxOptions, IBusOutboxNotification notification,
            ILogger<MultiBusBusOutboxDeliveryService<TBus, TDbContext>> logger, IServiceProvider provider)
            : base(busInstance.BusControl, Unwrap(options), outboxOptions, notification, logger, provider)
        {
        }

        static IOptions<OutboxDeliveryServiceOptions> Unwrap(IOptions<MultiBusOutboxDeliveryServiceOptions<TBus>> options)
        {
            return Options.Create(options.Value.ToDeliveryServiceOptions());
        }
    }
}

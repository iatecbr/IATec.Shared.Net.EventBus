#nullable enable
namespace MassTransit.Configuration
{
    using System;
    using DependencyInjection;
    using EntityFrameworkCoreIntegration;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
    using Middleware.Outbox;


    /// <summary>
    /// Bus outbox configurator for a specific (typed) bus in a MultiBus setup. Mirrors
    /// <see cref="EntityFrameworkBusOutboxConfigurator{TDbContext}" /> but registers the scoped bus context
    /// provider and the delivery service against <typeparamref name="TBus" /> instead of the default
    /// <see cref="IBus" />, so a second bus (e.g. ISnsBus) gets its own EF Core outbox and delivery service.
    /// </summary>
    public class MultiBusEntityFrameworkBusOutboxConfigurator<TBus, TDbContext> :
        IEntityFrameworkBusOutboxConfigurator
        where TBus : class, IBus
        where TDbContext : DbContext
    {
        readonly IBusRegistrationConfigurator _configurator;
        readonly MultiBusEntityFrameworkOutboxConfigurator<TBus, TDbContext> _outboxConfigurator;
        bool _registerOutboxDeliveryService;

        public MultiBusEntityFrameworkBusOutboxConfigurator(IBusRegistrationConfigurator configurator,
            MultiBusEntityFrameworkOutboxConfigurator<TBus, TDbContext> outboxConfigurator)
        {
            _outboxConfigurator = outboxConfigurator;
            _configurator = configurator;

            _registerOutboxDeliveryService = true;
        }

        /// <summary>
        /// The number of message to deliver at a time from the outbox
        /// </summary>
        public int MessageDeliveryLimit { get; set; } = 100;

        /// <summary>
        /// Transport Send timeout when delivering messages to the transport
        /// </summary>
        public TimeSpan MessageDeliveryTimeout { get; set; } = TimeSpan.FromSeconds(10);

        public void DisableDeliveryService()
        {
            _registerOutboxDeliveryService = false;
        }

        public virtual void Configure(Action<IEntityFrameworkBusOutboxConfigurator>? configure)
        {
            configure?.Invoke(this);

            // Replace the scoped bus context provider FOR THIS TYPED BUS only. The default IBus provider is
            // left untouched, so the primary bus behavior is unchanged.
            _configurator.ReplaceScoped<IScopedBusContextProvider<TBus>, EntityFrameworkScopedBusContextProvider<TBus, TDbContext>>();
            _configurator.AddSingleton<IBusOutboxNotification, BusOutboxNotification>();

            if (_registerOutboxDeliveryService)
            {
                _configurator.AddHostedService<MultiBusBusOutboxDeliveryService<TBus, TDbContext>>();
                _configurator.AddOptions<MultiBusOutboxDeliveryServiceOptions<TBus>>()
                    .Configure(options =>
                    {
                        options.QueryDelay = _outboxConfigurator.QueryDelay;
                        options.QueryMessageLimit = _outboxConfigurator.QueryMessageLimit;
                        options.QueryTimeout = _outboxConfigurator.QueryTimeout;
                        options.MessageDeliveryLimit = MessageDeliveryLimit;
                        options.MessageDeliveryTimeout = MessageDeliveryTimeout;
                    });
            }
        }
    }
}

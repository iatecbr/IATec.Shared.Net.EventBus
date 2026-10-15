#nullable enable
namespace MassTransit
{
    using System;
    using Configuration;
    using Microsoft.EntityFrameworkCore;


    public static class MultiBusEntityFrameworkOutboxConfigurationExtensions
    {
        /// <summary>
        /// Configures the Entity Framework Outbox for a specific (typed) bus in a MultiBus setup. Use this inside
        /// <c>AddMassTransit&lt;TBus&gt;(...)</c> so that the bus outbox (<see cref="IEntityFrameworkOutboxConfigurator.UseBusOutbox" />)
        /// and its delivery service are wired to <typeparamref name="TBus" /> and <typeparamref name="TDbContext" />,
        /// independent of the primary <see cref="IBus" /> outbox.
        /// </summary>
        /// <param name="configurator">The typed bus registration configurator</param>
        /// <param name="configure">Optional outbox configuration</param>
        /// <typeparam name="TBus">The typed bus (e.g. ISnsBus)</typeparam>
        /// <typeparam name="TDbContext">The DbContext holding this bus's outbox tables</typeparam>
        public static void AddMultiBusEntityFrameworkOutbox<TBus, TDbContext>(this IBusRegistrationConfigurator<TBus> configurator,
            Action<IEntityFrameworkOutboxConfigurator>? configure = null)
            where TBus : class, IBus
            where TDbContext : DbContext
        {
            var outboxConfigurator = new MultiBusEntityFrameworkOutboxConfigurator<TBus, TDbContext>(configurator);

            outboxConfigurator.Configure(configure);
        }
    }
}

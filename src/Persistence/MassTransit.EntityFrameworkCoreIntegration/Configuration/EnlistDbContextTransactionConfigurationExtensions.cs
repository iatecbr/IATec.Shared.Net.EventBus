#nullable enable
namespace MassTransit
{
    using System;
    using EntityFrameworkCoreIntegration;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.Extensions.DependencyInjection.Extensions;


    public static class EnlistDbContextTransactionConfigurationExtensions
    {
        /// <summary>
        /// Registers the services required to enlist a secondary DbContext (<typeparamref name="TSecondaryDbContext" />)
        /// in the consumer outbox transaction of a primary DbContext (<typeparamref name="TPrimaryDbContext" />).
        /// Call this on the bus registration configurator (inside <c>AddMassTransit</c>), then call
        /// <see cref="UseEnlistDbContextTransaction" /> on the receive endpoint that must share the transaction.
        ///
        /// IMPORTANT: both DbContexts MUST share the same scoped <see cref="System.Data.Common.DbConnection" />,
        /// otherwise there is no shared transaction to enlist in (on SQL Server, using separate connections would
        /// require MSDTC, which is not supported).
        /// </summary>
        public static IBusRegistrationConfigurator AddDbContextTransactionEnlister<TPrimaryDbContext, TSecondaryDbContext>(
            this IBusRegistrationConfigurator configurator)
            where TPrimaryDbContext : DbContext
            where TSecondaryDbContext : DbContext
        {
            if (configurator == null)
                throw new ArgumentNullException(nameof(configurator));

            configurator.TryAddScoped<IDbContextTransactionEnlister,
                DbContextTransactionEnlister<TPrimaryDbContext, TSecondaryDbContext>>();

            return configurator;
        }

        /// <summary>
        /// Adds a consume filter to this receive endpoint that enlists the secondary DbContext in the consumer
        /// outbox transaction before the consumer runs. Requires
        /// <see cref="AddDbContextTransactionEnlister{TPrimaryDbContext,TSecondaryDbContext}" /> to have been called.
        /// </summary>
        public static void UseEnlistDbContextTransaction(this IConsumePipeConfigurator configurator, IRegistrationContext context)
        {
            if (configurator == null)
                throw new ArgumentNullException(nameof(configurator));
            if (context == null)
                throw new ArgumentNullException(nameof(context));

            configurator.UseConsumeFilter(typeof(EnlistDbContextTransactionFilter<>), context);
        }
    }
}

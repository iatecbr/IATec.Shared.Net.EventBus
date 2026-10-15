namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Storage;


    /// <summary>
    /// Enlists <typeparamref name="TSecondaryDbContext" /> in the ambient transaction opened by the consumer
    /// outbox on <typeparamref name="TPrimaryDbContext" />. Both DbContexts MUST share the same underlying
    /// <see cref="System.Data.Common.DbConnection" /> (register it as a scoped service and pass it to both
    /// <c>AddDbContext</c> registrations), otherwise there is no shared transaction to enlist in.
    /// </summary>
    /// <typeparam name="TPrimaryDbContext">The DbContext owning the consumer outbox transaction (e.g. RabbitMQ)</typeparam>
    /// <typeparam name="TSecondaryDbContext">The DbContext to enlist (e.g. the SNS outbox DbContext)</typeparam>
    public class DbContextTransactionEnlister<TPrimaryDbContext, TSecondaryDbContext> :
        IDbContextTransactionEnlister
        where TPrimaryDbContext : DbContext
        where TSecondaryDbContext : DbContext
    {
        readonly TPrimaryDbContext _primaryDbContext;
        readonly TSecondaryDbContext _secondaryDbContext;

        public DbContextTransactionEnlister(TPrimaryDbContext primaryDbContext, TSecondaryDbContext secondaryDbContext)
        {
            _primaryDbContext = primaryDbContext;
            _secondaryDbContext = secondaryDbContext;
        }

        public async Task EnlistAsync(CancellationToken cancellationToken)
        {
            IDbContextTransaction primaryTransaction = _primaryDbContext.Database.CurrentTransaction;

            if (primaryTransaction == null)
                return;

            var dbTransaction = primaryTransaction.GetDbTransaction();

            IDbContextTransaction secondaryTransaction = _secondaryDbContext.Database.CurrentTransaction;

            // Already enlisted in the same physical transaction? nothing to do.
            if (secondaryTransaction != null && secondaryTransaction.GetDbTransaction() == dbTransaction)
                return;

            await _secondaryDbContext.Database.UseTransactionAsync(dbTransaction, cancellationToken).ConfigureAwait(false);
        }

        public async Task SaveChangesAsync(CancellationToken cancellationToken)
        {
            // Only flush if we actually enlisted in the primary's transaction. Enlisting shares the transaction,
            // but each DbContext still needs its own SaveChanges to send its commands within that transaction.
            if (_primaryDbContext.Database.CurrentTransaction == null)
                return;

            await _secondaryDbContext.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}

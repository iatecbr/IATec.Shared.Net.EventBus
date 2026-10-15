namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System.Threading;
    using System.Threading.Tasks;


    /// <summary>
    /// Enlists a secondary DbContext in the ambient transaction opened by the consumer outbox on a primary
    /// DbContext, so writes to the secondary DbContext are atomic with the consumer outbox commit. The concrete
    /// implementation knows the two DbContext types; the filter stays generic only over the message type.
    /// </summary>
    public interface IDbContextTransactionEnlister
    {
        /// <summary>
        /// Enlists the secondary DbContext in the primary DbContext's current transaction, if one is present.
        /// No-op when there is no ambient transaction.
        /// </summary>
        Task EnlistAsync(CancellationToken cancellationToken);

        /// <summary>
        /// Flushes the secondary DbContext's pending changes within the shared transaction. Must be called after
        /// the consumer runs and before the consumer outbox commits, so the secondary writes (status + outbox
        /// message) participate in the same commit. Only flushes when the secondary is enlisted in the primary's
        /// transaction; otherwise it is a no-op (the secondary would be committed independently elsewhere).
        /// </summary>
        Task SaveChangesAsync(CancellationToken cancellationToken);
    }
}

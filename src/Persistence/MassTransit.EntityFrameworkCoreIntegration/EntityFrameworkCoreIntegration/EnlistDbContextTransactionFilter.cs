namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System.Threading.Tasks;


    /// <summary>
    /// Consume filter that enlists a secondary DbContext in the ambient transaction opened by the consumer
    /// outbox on the primary DbContext, making writes to the secondary DbContext (e.g. the typed-bus outbox
    /// tables plus a shared status entity) atomic with the consumer outbox commit.
    ///
    /// The actual DbContext types are known by the injected <see cref="IDbContextTransactionEnlister" />, so this
    /// filter stays generic only over the message type (required by the scoped consume filter infrastructure).
    ///
    /// No-op when there is no ambient transaction on the primary DbContext (e.g. the consumer outbox is not
    /// configured on this endpoint).
    /// </summary>
    /// <typeparam name="TMessage">The consumed message type</typeparam>
    public class EnlistDbContextTransactionFilter<TMessage> :
        IFilter<ConsumeContext<TMessage>>
        where TMessage : class
    {
        readonly IDbContextTransactionEnlister _enlister;

        public EnlistDbContextTransactionFilter(IDbContextTransactionEnlister enlister)
        {
            _enlister = enlister;
        }

        public async Task Send(ConsumeContext<TMessage> context, IPipe<ConsumeContext<TMessage>> next)
        {
            // Before the consumer: enlist the secondary DbContext in the consumer outbox transaction.
            await _enlister.EnlistAsync(context.CancellationToken).ConfigureAwait(false);

            // Run the consumer (which updates the secondary DbContext and publishes to the secondary bus).
            await next.Send(context).ConfigureAwait(false);

            // After the consumer, still inside the transaction: flush the secondary DbContext so its writes
            // (status + outbox message) are sent within the shared transaction and committed together by the
            // consumer outbox. If the consumer threw, we never reach here and everything rolls back.
            await _enlister.SaveChangesAsync(context.CancellationToken).ConfigureAwait(false);
        }

        public void Probe(ProbeContext context)
        {
            context.CreateFilterScope("enlistDbContextTransaction");
        }
    }
}

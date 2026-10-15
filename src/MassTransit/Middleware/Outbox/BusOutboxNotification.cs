namespace MassTransit.Middleware.Outbox
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Options;


    public class BusOutboxNotification :
        IBusOutboxNotification
    {
        readonly object _lock = new object();
        readonly IOptions<OutboxDeliveryServiceOptions> _options;

        // Supports multiple concurrent waiters (e.g. one delivery service per bus in a MultiBus setup).
        // A single shared CancellationTokenSource was not safe when more than one delivery service waited
        // on the same notification instance, causing a NullReferenceException.
        readonly HashSet<CancellationTokenSource> _waiters = new HashSet<CancellationTokenSource>();

        public BusOutboxNotification(IOptions<OutboxDeliveryServiceOptions> options)
        {
            _options = options;
        }

        public async Task WaitForDelivery(CancellationToken cancellationToken)
        {
            var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            lock (_lock)
                _waiters.Add(cancellationTokenSource);

            try
            {
                var delay = await Task.Delay(_options.Value.QueryDelay, cancellationTokenSource.Token)
                    .ContinueWith(t => t, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default)
                    .ConfigureAwait(false);

                if (delay.IsCanceled)
                    cancellationToken.ThrowIfCancellationRequested();
            }
            finally
            {
                lock (_lock)
                    _waiters.Remove(cancellationTokenSource);

                cancellationTokenSource.Dispose();
            }
        }

        public void Delivered()
        {
            lock (_lock)
            {
                foreach (var waiter in _waiters)
                    waiter.Cancel();
            }
        }
    }
}

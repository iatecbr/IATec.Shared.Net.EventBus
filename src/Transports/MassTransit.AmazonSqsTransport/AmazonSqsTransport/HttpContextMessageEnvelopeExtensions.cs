namespace MassTransit.AmazonSqsTransport;

#if !NETSTANDARD2_0

using Microsoft.AspNetCore.Http;
using Serialization;


/// <summary>
/// Extension to retrieve the <see cref="MessageEnvelope"/> extracted from SNS.Message by <see cref="SnsSubscriptionConfirmationFilter"/>.
/// </summary>
public static class HttpContextMessageEnvelopeExtensions
{
    static readonly object EnvelopeKey = typeof(MessageEnvelope);

    /// <summary>
    /// Gets the MassTransit MessageEnvelope extracted from the SNS.Message field by the SnsSubscriptionConfirmationFilter.
    /// Returns null if the SNS message does not contain a MassTransit envelope or if the filter was not applied.
    /// </summary>
    public static MessageEnvelope? GetMessageEnvelope(this HttpContext context)
    {
        return context.Items.TryGetValue(EnvelopeKey, out var value) ? value as MessageEnvelope : null;
    }
}

#endif

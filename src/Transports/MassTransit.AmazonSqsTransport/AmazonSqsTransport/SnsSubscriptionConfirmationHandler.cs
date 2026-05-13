namespace MassTransit.AmazonSqsTransport;

using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;


/// <summary>
/// SNS envelope received on HTTP/HTTPS subscriptions.
/// The <see cref="Message"/> property contains the raw JSON string of the actual payload.
/// Use <see cref="DeserializeMessage{T}"/> to get the typed payload.
/// </summary>
public class SnsEnvelope
{
    static readonly JsonSerializerOptions CaseInsensitive = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

    public string? Type { get; set; }
    public string? TopicArn { get; set; }
    public string? Subject { get; set; }
    public string? Message { get; set; }
    public string? MessageId { get; set; }
    public string? Timestamp { get; set; }
    public string? SubscribeURL { get; set; }
    public string? Token { get; set; }

    /// <summary>
    /// Deserializes the <see cref="Message"/> JSON string into <typeparamref name="T"/>.
    /// </summary>
    public T? DeserializeMessage<T>() where T : class
    {
        if (string.IsNullOrWhiteSpace(Message))
            return null;

        return JsonSerializer.Deserialize<T>(Message, CaseInsensitive);
    }
}


/// <summary>
/// Handles SNS HTTP subscription lifecycle messages (SubscriptionConfirmation and UnsubscribeConfirmation).
///
/// Call <see cref="TryHandleAsync"/> with the raw JSON body.
/// Returns true when the request was a lifecycle message and was handled.
/// Returns false when it is a regular notification.
///
/// Compatible with netstandard2.0, net8.0, net9.0, net10.0.
/// </summary>
public class SnsSubscriptionConfirmationHandler
{
    readonly HttpClient _httpClient;

    public SnsSubscriptionConfirmationHandler(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    /// <summary>
    /// Tries to handle an SNS lifecycle message from the deserialized envelope.
    /// </summary>
    /// <returns>True if the message was a SubscriptionConfirmation or UnsubscribeConfirmation and was handled.</returns>
    public async Task<bool> TryHandleAsync(SnsEnvelope envelope, CancellationToken cancellationToken = default)
    {
        if (envelope == null)
            return false;

        if (string.Equals(envelope.Type, "SubscriptionConfirmation", StringComparison.OrdinalIgnoreCase)
            || string.Equals(envelope.Type, "UnsubscribeConfirmation", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(envelope.SubscribeURL))
            {
                LogContext.Warning?.Log("SNS {Type} received but SubscribeURL is missing", envelope.Type);
                return true;
            }

            try
            {
                var response = await _httpClient.GetAsync(envelope.SubscribeURL, cancellationToken).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();

                LogContext.Info?.Log("SNS {Type} confirmed for topic {TopicArn}", envelope.Type, envelope.TopicArn);
            }
            catch (Exception ex)
            {
                LogContext.Warning?.Log(ex, "Failed to confirm SNS {Type} for topic {TopicArn}", envelope.Type, envelope.TopicArn);
            }

            return true;
        }

        return false;
    }
}

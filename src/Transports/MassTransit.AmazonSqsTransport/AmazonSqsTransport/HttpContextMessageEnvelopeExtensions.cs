namespace MassTransit.AmazonSqsTransport;

#if !NETSTANDARD2_0

using System;
using System.Linq;
using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Serialization;


/// <summary>
/// Extensions to retrieve the <see cref="MessageEnvelope"/> and resolve typed messages
/// from the HTTP context populated by <see cref="SnsSubscriptionConfirmationFilter"/>.
/// </summary>
public static class HttpContextMessageEnvelopeExtensions
{
    static readonly object EnvelopeKey = typeof(MessageEnvelope);
    static readonly object RawPayloadKey = typeof(SnsSubscriptionConfirmationFilter);

    /// <summary>
    /// Gets the MassTransit MessageEnvelope extracted by the SnsSubscriptionConfirmationFilter.
    /// Works for both scenarios:
    /// - RawMessageDelivery=false: extracted from the SNS.Message field
    /// - RawMessageDelivery=true: deserialized directly from the raw HTTP body
    /// Returns null if the payload does not contain a valid MassTransit envelope or if the filter was not applied.
    /// </summary>
    public static MessageEnvelope? GetMessageEnvelope(this HttpContext context)
    {
        return context.Items.TryGetValue(EnvelopeKey, out var value) ? value as MessageEnvelope : null;
    }

    /// <summary>
    /// Gets the raw payload string from the HTTP body when the SnsSubscriptionConfirmationFilter
    /// could not parse it as an SNS envelope (typically when RawMessageDelivery=true).
    /// Returns null if the filter was not applied or the body was a valid SNS envelope.
    /// </summary>
    public static string? GetRawPayload(this HttpContext context)
    {
        return context.Items.TryGetValue(RawPayloadKey, out var value) ? value as string : null;
    }

    /// <summary>
    /// Resolves the message type from the MessageEnvelope's messageType URNs and deserializes
    /// the message payload into the resolved .NET type.
    /// Returns (null, null) if no matching type is found in the loaded assemblies.
    /// </summary>
    /// <example>
    /// var (type, message) = HttpContext.ResolveMessage();
    /// if (message is INotification notification)
    ///     await mediator.Publish(notification);
    /// </example>
    public static (Type? Type, object? Message) ResolveMessage(this HttpContext context)
    {
        var envelope = context.GetMessageEnvelope();
        if (envelope?.MessageType == null || envelope.Message == null)
            return (null, null);

        foreach (var urn in envelope.MessageType)
        {
            var typeName = ConvertUrnToTypeName(urn);
            if (string.IsNullOrWhiteSpace(typeName))
                continue;

            var type = AppDomain.CurrentDomain.GetAssemblies()
                .Where(a => !a.IsDynamic)
                .Select(a => a.GetType(typeName))
                .FirstOrDefault(t => t != null);

            if (type != null)
            {
                try
                {
                    var json = JsonSerializer.Serialize(envelope.Message, SystemTextJsonMessageSerializer.Options);
                    var message = JsonSerializer.Deserialize(json, type, SystemTextJsonMessageSerializer.Options);
                    return (type, message);
                }
                catch (JsonException)
                {
                    continue;
                }
            }
        }

        return (null, null);
    }

    /// <summary>
    /// Resolves and deserializes the message to the specified type <typeparamref name="T"/>.
    /// Returns null if the MessageEnvelope is not available or deserialization fails.
    /// </summary>
    public static T? GetMessage<T>(this HttpContext context) where T : class
    {
        var envelope = context.GetMessageEnvelope();
        if (envelope?.Message == null)
            return null;

        try
        {
            var json = JsonSerializer.Serialize(envelope.Message, SystemTextJsonMessageSerializer.Options);
            return JsonSerializer.Deserialize<T>(json, SystemTextJsonMessageSerializer.Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Converts a MassTransit message type URN to a .NET fully qualified type name.
    /// Example: "urn:message:MyApp.Contracts:OrderCreatedEvent" → "MyApp.Contracts.OrderCreatedEvent"
    /// </summary>
    static string? ConvertUrnToTypeName(string urn)
    {
        if (!urn.StartsWith("urn:message:", StringComparison.OrdinalIgnoreCase))
            return null;

        var typePart = urn.Substring("urn:message:".Length);
        return typePart.Replace(':', '.');
    }
}

#endif

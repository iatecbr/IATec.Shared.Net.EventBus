namespace MassTransit.AmazonSqsTransport;

#if !NETSTANDARD2_0

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Serialization;


/// <summary>
/// Resource filter that runs before model binding, reads the SNS body once,
/// handles SubscriptionConfirmation/UnsubscribeConfirmation automatically,
/// and stores the deserialized <see cref="SnsEnvelope"/> in <see cref="HttpContext.Items"/>
/// so the controller can retrieve it via <see cref="HttpContextSnsExtensions.GetSnsEnvelope"/>.
///
/// Additionally, if the SNS Message contains a MassTransit MessageEnvelope (JSON),
/// it will be extracted and stored for access via <see cref="HttpContextMessageEnvelopeExtensions.GetMessageEnvelope"/>.
///
/// Usage — register globally:
/// <code>
/// builder.Services.AddControllers(options =>
///     options.Filters.Add&lt;SnsSubscriptionConfirmationFilter&gt;());
/// </code>
///
/// Or per controller/action:
/// <code>
/// [SnsSubscriptionConfirmation]
/// public async Task&lt;IActionResult&gt; SnsWebhook()
/// {
///     var snsEnvelope = HttpContext.GetSnsEnvelope();
///     var messageEnvelope = HttpContext.GetMessageEnvelope();  // MassTransit envelope
/// }
/// </code>
/// </summary>
public class SnsSubscriptionConfirmationFilter : IAsyncResourceFilter
{
    static readonly JsonSerializerOptions CaseInsensitive = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    static readonly object EnvelopeKey = typeof(SnsEnvelope);
    static readonly object MessageEnvelopeKey = typeof(MessageEnvelope);

    readonly SnsSubscriptionConfirmationHandler _handler;

    public SnsSubscriptionConfirmationFilter(SnsSubscriptionConfirmationHandler handler)
    {
        _handler = handler;
    }

    public async Task OnResourceExecutionAsync(ResourceExecutingContext context, ResourceExecutionDelegate next)
    {
        var request = context.HttpContext.Request;

        request.EnableBuffering();

        string body;
        using (var reader = new StreamReader(request.Body, Encoding.UTF8, leaveOpen: true))
        {
            body = await reader.ReadToEndAsync(context.HttpContext.RequestAborted).ConfigureAwait(false);
            request.Body.Position = 0;
        }

        SnsEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<SnsEnvelope>(body, CaseInsensitive);
        }
        catch
        {
            await next().ConfigureAwait(false);
            return;
        }

        if (envelope == null)
        {
            await next().ConfigureAwait(false);
            return;
        }

        var handled = await _handler.TryHandleAsync(envelope, context.HttpContext.RequestAborted).ConfigureAwait(false);

        if (handled)
        {
            context.Result = new OkResult();
            return;
        }

        context.HttpContext.Items[EnvelopeKey] = envelope;

        // Try to extract MassTransit MessageEnvelope from SNS.Message
        if (!string.IsNullOrWhiteSpace(envelope.Message))
        {
            try
            {
                var messageEnvelope = JsonSerializer.Deserialize<MessageEnvelope>(
                    envelope.Message,
                    SystemTextJsonMessageSerializer.Options);

                if (messageEnvelope != null)
                {
                    context.HttpContext.Items[MessageEnvelopeKey] = messageEnvelope;
                }
            }
            catch (JsonException ex)
            {
                LogContext.Warning?.Log(ex, "Failed to deserialize Masstransit message envelope");
            }
        }

        await next().ConfigureAwait(false);
    }
}


/// <summary>
/// Extension to retrieve the <see cref="SnsEnvelope"/> stored by <see cref="SnsSubscriptionConfirmationFilter"/>.
/// </summary>
public static class HttpContextSnsExtensions
{
    static readonly object EnvelopeKey = typeof(SnsEnvelope);

    public static SnsEnvelope? GetSnsEnvelope(this HttpContext context)
    {
        return context.Items.TryGetValue(EnvelopeKey, out var value) ? value as SnsEnvelope : null;
    }
}


/// <summary>
/// Convenience attribute that applies <see cref="SnsSubscriptionConfirmationFilter"/> to a controller or action.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
public sealed class SnsSubscriptionConfirmationAttribute : TypeFilterAttribute
{
    public SnsSubscriptionConfirmationAttribute() : base(typeof(SnsSubscriptionConfirmationFilter))
    {
    }
}

#endif

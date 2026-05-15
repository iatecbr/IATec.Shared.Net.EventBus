namespace MassTransit.AmazonSqsTransport;

#if !NETSTANDARD2_0

using Microsoft.Extensions.DependencyInjection;


public static class SnsSubscriptionConfirmationServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="SnsSubscriptionConfirmationHandler"/> and <see cref="SnsSubscriptionConfirmationFilter"/>
    /// in the DI container, required when using the <see cref="SnsSubscriptionConfirmationAttribute"/>.
    /// </summary>
    public static IServiceCollection AddSnsSubscriptionConfirmation(this IServiceCollection services)
    {
        services.AddHttpClient<SnsSubscriptionConfirmationHandler>();
        services.AddScoped<SnsSubscriptionConfirmationFilter>();
        return services;
    }
}

#endif

# Breaking Change: SNS HTTP Subscriptions Now Respect Scope Prefix

## Summary

HTTP subscriptions created via `SubscribeTopicToHttpEndpoint()` now automatically apply the scope prefix when `ScopeTopics` is enabled, matching the behavior of regular SQS consumers.

## Version

This change was introduced in: **[Version to be defined]**

## What Changed

### Before

When using HTTP subscriptions with scoped topics, the topic was created **without** the scope prefix:

```csharp
cfg.Host(new Uri("amazonsqs://localhost:4566"), h =>
{
    h.Scope = "dev";  // Scope configured
    h.Config(new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost:4566" });
    h.Config(new AmazonSQSConfig { ServiceURL = "http://localhost:4566" });
});

// This created a topic named "Notification" (WITHOUT scope prefix)
cfg.SubscribeTopicToHttpEndpoint("Notification", "https://api.example.com/webhooks/sns");

// But GetDestinationAddress() returned "dev_Notification" (WITH scope prefix)
var address = busTopology.GetDestinationAddress("Notification");
// Result: Topics duplicated in SNS ("Notification" AND "dev_Notification")
```

### After

HTTP subscriptions now **automatically apply the scope prefix**, just like regular SQS consumers:

```csharp
cfg.Host(new Uri("amazonsqs://localhost:4566"), h =>
{
    h.Scope = "dev";
    h.Config(new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost:4566" });
    h.Config(new AmazonSQSConfig { ServiceURL = "http://localhost:4566" });
});

// Now creates a topic named "dev_Notification" (WITH scope prefix)
cfg.SubscribeTopicToHttpEndpoint("Notification", "https://api.example.com/webhooks/sns");

// GetDestinationAddress() also returns "dev_Notification"
var address = busTopology.GetDestinationAddress("Notification");
// Result: Only ONE topic "dev_Notification" is created
```

## Impact

### Who is affected?

Applications using **both**:
1. `SubscribeTopicToHttpEndpoint()` for HTTP/HTTPS subscriptions
2. Scoped topics via `h.Scope = "environment"` configuration

### What happens?

When you upgrade to this version:

1. **New topic names:** HTTP subscriptions will create topics with the scope prefix (e.g., `dev_Notification` instead of `Notification`)
2. **Old topics orphaned:** Previous topics created without the scope prefix will remain in SNS but won't receive new messages
3. **Subscriptions recreated:** HTTP subscriptions will be recreated on the new scoped topics

## Migration Guide

### Option 1: Clean Migration (Recommended)

**For dev/staging environments:**

1. **Delete old topics** from SNS/LocalStack:
   ```bash
   # Using AWS CLI (or LocalStack awslocal)
   aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:000000000000:Notification
   
   # Replace with all your non-scoped topic names
   ```

2. **Update application configuration** (no code changes needed if already using scoped configuration)

3. **Restart application** - New scoped topics will be created automatically

4. **Confirm SNS subscription** - Your HTTP endpoint will receive a SubscriptionConfirmation request

**For production environments:**

1. **Deploy new version** - Scoped topics will be created alongside old ones
2. **Monitor both topics** temporarily
3. **Verify new topics** are receiving messages correctly
4. **Delete old topics** after confirming everything works
5. **Update external systems** if they reference topic ARNs directly

### Option 2: Disable Scope Temporarily

If you need to maintain the old behavior temporarily:

```csharp
cfg.Host(new Uri("amazonsqs://localhost:4566"), h =>
{
    // Remove or comment out Scope configuration
    // h.Scope = "dev";  // ← Disabled
    
    h.Config(new AmazonSimpleNotificationServiceConfig { ServiceURL = "http://localhost:4566" });
    h.Config(new AmazonSQSConfig { ServiceURL = "http://localhost:4566" });
});
```

**Note:** This is not recommended for multi-environment deployments.

## Technical Details

### Root Cause

`HttpSubscriptionConsumeTopologySpecification` was not inheriting from `AmazonSqsTopicSubscriptionConfigurator`, which meant it bypassed the entity name formatter system that applies scope prefixes.

### Solution

Changed `HttpSubscriptionConsumeTopologySpecification` to inherit from `AmazonSqsTopicSubscriptionConfigurator`, making it consistent with `ConsumerConsumeTopologySpecification` (used for regular SQS consumers).

### Files Modified

- `src/Transports/MassTransit.AmazonSqsTransport/Configuration/HttpSubscriptionConsumeTopologySpecification.cs`

### Code Changes

**Modified Files:**
1. `HttpSubscriptionConsumeTopologySpecification.cs` - Apply scope prefix using `AmazonSqsEndpointAddress`
2. `AmazonSqsHttpSubscriptionExtensions.cs` - Pass `HostAddress` to specification
3. `AmazonSqsBusFactoryConfigurator.cs` - Expose `HostAddress` property

```diff
 public class HttpSubscriptionConsumeTopologySpecification :
     AmazonSqsTopicSubscriptionConfigurator,
     IAmazonSqsConsumeTopologySpecification
 {
     readonly string _endpointUrl;
+    readonly Uri? _hostAddress;
     readonly IAmazonSqsPublishTopology _publishTopology;
     readonly bool _rawMessageDelivery;
     
     public HttpSubscriptionConsumeTopologySpecification(
         IAmazonSqsPublishTopology publishTopology,
+        Uri hostAddress,
         string topicName,
         string endpointUrl,
         bool rawMessageDelivery = true,
         bool durable = true,
         bool autoDelete = false)
         : base(topicName, durable, autoDelete)
     {
         _publishTopology = publishTopology;
+        _hostAddress = hostAddress;
         _endpointUrl = endpointUrl;
         _rawMessageDelivery = rawMessageDelivery;
     }
     
     public void Apply(IReceiveEndpointBrokerTopologyBuilder builder)
     {
+        string topicName;
+        
+        // Apply scope prefix when HostAddress is available
+        if (_hostAddress != null)
+        {
+            var address = new AmazonSqsEndpointAddress(_hostAddress, new Uri($"topic:{EntityName}"));
+            topicName = address.Name;  // ← Scope applied here!
+        }
+        else
+        {
+            topicName = EntityName;
+        }
+        
         var topicHandle = builder.CreateTopic(
-            EntityName,
+            topicName,  // ← Uses scoped topic name
             Durable,
             AutoDelete,
             ...);
     }
 }
```

## Testing

### Verify Scope is Applied

1. **Check topic names in SNS:**
   ```bash
   aws sns list-topics
   ```
   
   You should see topics with scope prefixes (e.g., `dev_Notification`, `staging_OrderCreated`)

2. **Check HTTP subscription:**
   ```bash
   aws sns list-subscriptions
   ```
   
   Subscriptions should be attached to scoped topics

3. **Test message flow:**
   - Send a message using `bus.Send()`
   - Verify your HTTP endpoint receives it
   - Confirm topic name matches scope

### Automated Tests

If you have integration tests, update expectations:

```csharp
// Before
Assert.That(topicName, Is.EqualTo("Notification"));

// After (with scope "dev")
Assert.That(topicName, Is.EqualTo("dev_Notification"));
```

## FAQ

### Q: Do I need to change my code?

**A:** No code changes required if you're already using scoped configuration correctly. Only infrastructure (SNS topics) needs cleanup.

### Q: Will this affect my existing messages?

**A:** No. Existing messages in old topics remain. New messages will be sent to scoped topics.

### Q: Can I use different scopes for different environments?

**A:** Yes! That's the whole point of scopes:

```csharp
// Development
h.Scope = "dev";      // Creates "dev_TopicName"

// Staging
h.Scope = "staging";  // Creates "staging_TopicName"

// Production
h.Scope = "prod";     // Creates "prod_TopicName"
```

### Q: What if I don't use scopes?

**A:** If you don't set a scope (or set it to `"/"`), topic names remain unchanged (no prefix applied).

### Q: Does this affect SQS queue subscriptions?

**A:** No. This change only affects HTTP/HTTPS subscriptions. Regular SQS consumers already had this behavior.

## Related Changes

This change is part of a larger effort to add SNS HTTP subscription support with automatic message extraction:

1. ✅ Added `SnsSubscriptionConfirmationFilter` for automatic subscription confirmation
2. ✅ Added `HttpContextMessageEnvelopeExtensions.GetMessageEnvelope()` for extracting MassTransit messages from SNS
3. ✅ Changed `RawMessageDelivery` default to `false` to enable SNS envelope access
4. ✅ **This change:** Fixed scope prefix consistency for HTTP subscriptions

## Support

For questions or issues, please open a GitHub Discussion or contact the MassTransit community.

---

**Last Updated:** 2025-04-30
**Affects:** MassTransit.AmazonSqsTransport

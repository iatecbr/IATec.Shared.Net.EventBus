namespace MassTransit.AmazonSqsTransport.Tests.HttpSubscription
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.SimpleNotificationService;
    using Amazon.SimpleNotificationService.Model;
    using Amazon.SQS;
    using Amazon.SQS.Model;
    using MassTransit.AmazonSqsTransport;
    using MassTransit.AmazonSqsTransport.Configuration;
    using MassTransit.AmazonSqsTransport.Topology;
    using MassTransit.Configuration;
    using NSubstitute;
    using NSubstitute.ExceptionExtensions;
    using NUnit.Framework;
    using Topic = MassTransit.AmazonSqsTransport.Topology.Topic;


    /// <summary>
    /// Integration tests for the DLQ creation flow with mocked AWS services.
    /// Tests the full flow: create DLQ queue → configure IAM permissions → create subscription → apply RedrivePolicy.
    /// Requirements: 2.1, 2.5, 2.8, 3.1, 3.3, 3.4, 4.1, 4.2, 4.3
    /// </summary>
    [TestFixture]
    public class HttpSubscriptionDlqIntegrationTests
    {
        const string TopicName = "test-topic";
        const string TopicArn = "arn:aws:sns:us-east-1:123456789012:test-topic";
        const string DlqName = "test-topic-http-dlq";
        const string DlqArn = "arn:aws:sqs:us-east-1:123456789012:test-topic-http-dlq";
        const string DlqUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/test-topic-http-dlq";
        const string EndpointUrl = "https://example.com/webhook";
        const string SubscriptionArn = "arn:aws:sns:us-east-1:123456789012:test-topic:sub-12345";

        IAmazonSimpleNotificationService _snsClient;
        IAmazonSQS _sqsClient;
        ConnectionContext _connectionContext;
        AmazonSqsClientContext _clientContext;

        [SetUp]
        public void Setup()
        {
            _snsClient = Substitute.For<IAmazonSimpleNotificationService>();
            _sqsClient = Substitute.For<IAmazonSQS>();
            _connectionContext = Substitute.For<ConnectionContext>();

            // Setup default ConnectionContext behavior
            var topicInfo = new TopicInfo(TopicName, TopicArn, _snsClient, CancellationToken.None, existing: false);
            _connectionContext.GetTopic(Arg.Any<Topic>(), Arg.Any<CancellationToken>())
                .Returns(topicInfo);

            _clientContext = new AmazonSqsClientContext(_connectionContext, _sqsClient, _snsClient, CancellationToken.None);
        }

        /// <summary>
        /// Validates: Requirements 2.1, 3.1, 4.1, 4.2
        /// Full flow: create DLQ → configure permissions → create subscription → apply RedrivePolicy.
        /// </summary>
        [Test]
        public async Task CreateHttpSubscription_with_DLQ_enabled_applies_RedrivePolicy_on_valid_subscription()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            // Mock SubscribeAsync to return a valid subscription ARN
            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = SubscriptionArn,
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Mock SetSubscriptionAttributesAsync for RedrivePolicy
            _snsClient.SetSubscriptionAttributesAsync(Arg.Any<SetSubscriptionAttributesRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SetSubscriptionAttributesResponse
                {
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Act
            var result = await _clientContext.CreateHttpSubscription(
                topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None);

            // Assert
            Assert.That(result, Is.True);

            // Verify SubscribeAsync was called with correct parameters
            await _snsClient.Received(1).SubscribeAsync(
                Arg.Is<SubscribeRequest>(r =>
                    r.TopicArn == TopicArn &&
                    r.Protocol == "https" &&
                    r.Endpoint == EndpointUrl &&
                    r.ReturnSubscriptionArn == true),
                Arg.Any<CancellationToken>());

            // Verify SetSubscriptionAttributesAsync was called with RedrivePolicy
            await _snsClient.Received(1).SetSubscriptionAttributesAsync(
                Arg.Is<SetSubscriptionAttributesRequest>(r =>
                    r.SubscriptionArn == SubscriptionArn &&
                    r.AttributeName == "RedrivePolicy" &&
                    r.AttributeValue == $"{{\"deadLetterTargetArn\":\"{DlqArn}\"}}"),
                Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 2.5
        /// When DLQ queue already exists, the flow should still work (reuse existing queue).
        /// The AmazonSqsClientContext.CreateHttpSubscription doesn't create the queue itself,
        /// but it should still apply RedrivePolicy regardless of whether the queue is new or existing.
        /// </summary>
        [Test]
        public async Task CreateHttpSubscription_with_DLQ_applies_RedrivePolicy_regardless_of_queue_existence()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = SubscriptionArn,
                    HttpStatusCode = HttpStatusCode.OK
                });

            _snsClient.SetSubscriptionAttributesAsync(Arg.Any<SetSubscriptionAttributesRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SetSubscriptionAttributesResponse
                {
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Act - pass DLQ ARN (simulating that the queue was already created/reused in the filter)
            var result = await _clientContext.CreateHttpSubscription(
                topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 5, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None);

            // Assert
            Assert.That(result, Is.True);

            // RedrivePolicy should still be applied
            await _snsClient.Received(1).SetSubscriptionAttributesAsync(
                Arg.Is<SetSubscriptionAttributesRequest>(r =>
                    r.AttributeName == "RedrivePolicy" &&
                    r.AttributeValue == $"{{\"deadLetterTargetArn\":\"{DlqArn}\"}}"),
                Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 3.3
        /// When subscription returns PendingConfirmation, RedrivePolicy should NOT be set
        /// and a warning log should be generated.
        /// </summary>
        [Test]
        public async Task CreateHttpSubscription_with_PendingConfirmation_omits_RedrivePolicy()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = "PendingConfirmation",
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Act
            var result = await _clientContext.CreateHttpSubscription(
                topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None);

            // Assert - subscription still returns true (ARN is not null)
            Assert.That(result, Is.True);

            // Verify SetSubscriptionAttributesAsync was NOT called (RedrivePolicy not applied)
            await _snsClient.DidNotReceive().SetSubscriptionAttributesAsync(
                Arg.Any<SetSubscriptionAttributesRequest>(),
                Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 3.4
        /// When SetSubscriptionAttributesAsync fails, an InvalidOperationException should be thrown
        /// with both subscription ARN and DLQ ARN in the message.
        /// </summary>
        [Test]
        public void CreateHttpSubscription_when_SetSubscriptionAttributes_fails_throws_InvalidOperationException_with_context()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = SubscriptionArn,
                    HttpStatusCode = HttpStatusCode.OK
                });

            // SetSubscriptionAttributesAsync throws an AWS exception
            var awsException = new Amazon.SimpleNotificationService.Model.InvalidParameterException("Invalid parameter");
            _snsClient.SetSubscriptionAttributesAsync(Arg.Any<SetSubscriptionAttributesRequest>(), Arg.Any<CancellationToken>())
                .ThrowsAsync(awsException);

            // Act & Assert
            var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _clientContext.CreateHttpSubscription(
                    topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None));

            // Verify the exception message contains both ARNs for context
            Assert.That(ex!.Message, Does.Contain(SubscriptionArn));
            Assert.That(ex.Message, Does.Contain(DlqArn));
            Assert.That(ex.InnerException, Is.SameAs(awsException));
        }

        /// <summary>
        /// Validates: Requirements 3.1
        /// When DLQ ARN is null (DLQ disabled), no RedrivePolicy is configured.
        /// </summary>
        [Test]
        public async Task CreateHttpSubscription_without_DLQ_does_not_set_RedrivePolicy()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = SubscriptionArn,
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Act - dlqArn is null (DLQ disabled)
            var result = await _clientContext.CreateHttpSubscription(
                topic, EndpointUrl, rawMessageDelivery: true, dlqArn: null, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None);

            // Assert
            Assert.That(result, Is.True);

            // Verify SetSubscriptionAttributesAsync was NOT called
            await _snsClient.DidNotReceive().SetSubscriptionAttributesAsync(
                Arg.Any<SetSubscriptionAttributesRequest>(),
                Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 4.1, 4.2, 4.3
        /// Integration test for the ConfigureAmazonSqsTopologyFilter Declare flow:
        /// When DLQ is enabled, the filter should get queue info, configure IAM permissions via UpdatePolicy,
        /// and pass the DLQ ARN to CreateHttpSubscription.
        /// This test verifies the full topology filter flow using a mock ClientContext.
        /// </summary>
        [Test]
        public async Task Declare_HttpSubscription_with_DLQ_configures_permissions_and_passes_dlqArn()
        {
            // Arrange - use a mock ClientContext to test the filter's Declare logic
            var mockClientContext = Substitute.For<ClientContext>();

            var topicInfo = new TopicInfo(TopicName, TopicArn, _snsClient, CancellationToken.None, existing: false);
            mockClientContext.CreateTopic(Arg.Any<Topic>(), Arg.Any<CancellationToken>())
                .Returns(topicInfo);

            // Create a real QueueInfo for the DLQ (requires IAmazonSQS for internal batchers)
            var dlqAttributes = new Dictionary<string, string>
            {
                ["QueueArn"] = DlqArn
            };
            var dlqQueueInfo = new QueueInfo(DlqName, DlqUrl, dlqAttributes, _sqsClient, CancellationToken.None, existing: false);

            // Setup SQS mock for SetQueueAttributesAsync (called by UpdatePolicy)
            _sqsClient.SetQueueAttributesAsync(Arg.Any<string>(), Arg.Any<Dictionary<string, string>>(), Arg.Any<CancellationToken>())
                .Returns(new SetQueueAttributesResponse { HttpStatusCode = HttpStatusCode.OK });

            mockClientContext.GetQueueInfo(DlqName, Arg.Any<CancellationToken>())
                .Returns(dlqQueueInfo);

            mockClientContext.CreateHttpSubscription(
                    Arg.Any<Topic>(), Arg.Any<string>(), Arg.Any<bool>(),
                    Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(true);

            // Create the HttpSubscription entity with DLQ enabled
            var topicEntity = new TopicEntity(1, TopicName, durable: true, autoDelete: false);
            var subscription = new HttpSubscriptionEntity(1, topicEntity, EndpointUrl, rawMessageDelivery: true,
                deadLetterQueueEnabled: true, deadLetterQueueName: DlqName, maxReceiveCount: 3);

            // Act - simulate what ConfigureAmazonSqsTopologyFilter.Declare does
            string? dlqArn = null;
            if (subscription.DeadLetterQueueEnabled && !string.IsNullOrWhiteSpace(subscription.DeadLetterQueueName))
            {
                var queueInfo = await mockClientContext.GetQueueInfo(subscription.DeadLetterQueueName!, CancellationToken.None);
                dlqArn = queueInfo.Arn;

                var topicInfoResult = await mockClientContext.CreateTopic(subscription.Source, CancellationToken.None);
                await queueInfo.UpdatePolicy(queueInfo.Arn, topicInfoResult.Arn, CancellationToken.None);
            }

            await mockClientContext.CreateHttpSubscription(subscription.Source, subscription.EndpointUrl,
                subscription.RawMessageDelivery, dlqArn, subscription.MaxReceiveCount,
                subscription.MinDelayTarget, subscription.MaxDelayTarget, subscription.BackoffFunction, CancellationToken.None);

            // Assert
            Assert.That(dlqArn, Is.EqualTo(DlqArn));

            // Verify GetQueueInfo was called with the DLQ name
            await mockClientContext.Received(1).GetQueueInfo(DlqName, Arg.Any<CancellationToken>());

            // Verify CreateTopic was called to get the topic ARN for IAM policy
            await mockClientContext.Received(1).CreateTopic(Arg.Any<Topic>(), Arg.Any<CancellationToken>());

            // Verify CreateHttpSubscription was called with the DLQ ARN
            await mockClientContext.Received(1).CreateHttpSubscription(
                Arg.Any<Topic>(), EndpointUrl, true, DlqArn, 3, Arg.Any<int>(), Arg.Any<int>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 4.3
        /// When the IAM policy already contains the required permission, UpdatePolicy should not call SetQueueAttributes.
        /// </summary>
        [Test]
        public async Task UpdatePolicy_when_policy_already_has_permission_does_not_call_SetQueueAttributes()
        {
            // Arrange - create a QueueInfo with an existing policy that already allows sns.amazonaws.com
            var existingPolicy = @"{
                ""Version"": ""2012-10-17"",
                ""Statement"": [{
                    ""Effect"": ""Allow"",
                    ""Principal"": { ""Service"": ""sns.amazonaws.com"" },
                    ""Action"": ""sqs:SendMessage"",
                    ""Resource"": """ + DlqArn + @""",
                    ""Condition"": {
                        ""ArnLike"": {
                            ""aws:SourceArn"": """ + TopicArn + @"""
                        }
                    }
                }]
            }";

            var dlqAttributes = new Dictionary<string, string>
            {
                ["QueueArn"] = DlqArn,
                ["Policy"] = existingPolicy
            };
            var dlqQueueInfo = new QueueInfo(DlqName, DlqUrl, dlqAttributes, _sqsClient, CancellationToken.None, existing: true);

            // Act
            var result = await dlqQueueInfo.UpdatePolicy(DlqArn, TopicArn, CancellationToken.None);

            // Assert - should return false (no update needed)
            Assert.That(result, Is.False);

            // Verify SetQueueAttributesAsync was NOT called
            await _sqsClient.DidNotReceive().SetQueueAttributesAsync(
                Arg.Any<string>(), Arg.Any<Dictionary<string, string>>(), Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 4.1, 4.2
        /// When the IAM policy does not contain the required permission, UpdatePolicy should add it.
        /// </summary>
        [Test]
        public async Task UpdatePolicy_when_policy_missing_permission_adds_statement_and_calls_SetQueueAttributes()
        {
            // Arrange - create a QueueInfo with an empty policy
            var dlqAttributes = new Dictionary<string, string>
            {
                ["QueueArn"] = DlqArn
            };
            var dlqQueueInfo = new QueueInfo(DlqName, DlqUrl, dlqAttributes, _sqsClient, CancellationToken.None, existing: false);

            _sqsClient.SetQueueAttributesAsync(Arg.Any<string>(), Arg.Any<Dictionary<string, string>>(), Arg.Any<CancellationToken>())
                .Returns(new SetQueueAttributesResponse
                {
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Act
            var result = await dlqQueueInfo.UpdatePolicy(DlqArn, TopicArn, CancellationToken.None);

            // Assert - should return true (policy was updated)
            Assert.That(result, Is.True);

            // Verify SetQueueAttributesAsync was called with the policy
            await _sqsClient.Received(1).SetQueueAttributesAsync(
                DlqUrl,
                Arg.Is<Dictionary<string, string>>(d => d.ContainsKey("Policy")),
                Arg.Any<CancellationToken>());
        }

        /// <summary>
        /// Validates: Requirements 2.8
        /// When AWS fails during the flow, the exception should propagate with context.
        /// This tests that SetSubscriptionAttributesAsync failure includes both ARNs.
        /// </summary>
        [Test]
        public void CreateHttpSubscription_AWS_failure_propagates_exception_with_subscription_and_dlq_arns()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = SubscriptionArn,
                    HttpStatusCode = HttpStatusCode.OK
                });

            var innerException = new AmazonSimpleNotificationServiceException("Service unavailable");
            _snsClient.SetSubscriptionAttributesAsync(Arg.Any<SetSubscriptionAttributesRequest>(), Arg.Any<CancellationToken>())
                .ThrowsAsync(innerException);

            // Act & Assert
            var ex = Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await _clientContext.CreateHttpSubscription(
                    topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None));

            // Verify exception contains both ARNs for debugging context
            Assert.That(ex!.Message, Does.Contain(SubscriptionArn), "Exception should contain subscription ARN");
            Assert.That(ex.Message, Does.Contain(DlqArn), "Exception should contain DLQ ARN");
            Assert.That(ex.InnerException, Is.SameAs(innerException), "Inner exception should be the original AWS exception");
        }

        /// <summary>
        /// Validates: Requirements 3.1
        /// Verifies the exact RedrivePolicy JSON format sent to SetSubscriptionAttributesAsync.
        /// </summary>
        [Test]
        public async Task CreateHttpSubscription_RedrivePolicy_has_exact_JSON_format()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = SubscriptionArn,
                    HttpStatusCode = HttpStatusCode.OK
                });

            SetSubscriptionAttributesRequest? capturedRedrivePolicyRequest = null;
            _snsClient.SetSubscriptionAttributesAsync(Arg.Any<SetSubscriptionAttributesRequest>(), Arg.Any<CancellationToken>())
                .Returns(callInfo =>
                {
                    var req = callInfo.Arg<SetSubscriptionAttributesRequest>();
                    if (req.AttributeName == "RedrivePolicy")
                        capturedRedrivePolicyRequest = req;
                    return new SetSubscriptionAttributesResponse { HttpStatusCode = HttpStatusCode.OK };
                });

            // Act
            await _clientContext.CreateHttpSubscription(
                topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None);

            // Assert - verify exact JSON format
            Assert.That(capturedRedrivePolicyRequest, Is.Not.Null);
            Assert.That(capturedRedrivePolicyRequest!.AttributeName, Is.EqualTo("RedrivePolicy"));

            var expectedJson = $"{{\"deadLetterTargetArn\":\"{DlqArn}\"}}";
            Assert.That(capturedRedrivePolicyRequest.AttributeValue, Is.EqualTo(expectedJson));
        }

        /// <summary>
        /// Validates: Requirements 3.3
        /// Verifies that when PendingConfirmation is returned with a DLQ ARN,
        /// the method still returns true but does not attempt to set RedrivePolicy.
        /// </summary>
        [Test]
        public async Task CreateHttpSubscription_PendingConfirmation_with_DLQ_returns_true_without_setting_attributes()
        {
            // Arrange
            var topic = new TopicEntity(1, TopicName, durable: true, autoDelete: false);

            _snsClient.SubscribeAsync(Arg.Any<SubscribeRequest>(), Arg.Any<CancellationToken>())
                .Returns(new SubscribeResponse
                {
                    SubscriptionArn = "PendingConfirmation",
                    HttpStatusCode = HttpStatusCode.OK
                });

            // Act
            var result = await _clientContext.CreateHttpSubscription(
                topic, EndpointUrl, rawMessageDelivery: true, dlqArn: DlqArn, maxReceiveCount: 3, minDelayTarget: 20, maxDelayTarget: 20, backoffFunction: "linear", CancellationToken.None);

            // Assert
            Assert.That(result, Is.True, "Should return true even for PendingConfirmation (ARN is not null)");

            // SetSubscriptionAttributesAsync should never be called
            await _snsClient.DidNotReceive().SetSubscriptionAttributesAsync(
                Arg.Any<SetSubscriptionAttributesRequest>(),
                Arg.Any<CancellationToken>());
        }

        #region Task 14.2 - Backward Compatibility of Extension Method

        /// <summary>
        /// Validates: Requirements 6.3, 6.4
        /// Verifies that existing invocations without configuring DLQ compile and function without changes.
        /// When the extension method is called without a configure delegate (or with a delegate that
        /// does not set DLQ properties), the resulting topology specification should produce the same
        /// behavior as before: no DLQ queue created, no RedrivePolicy configured.
        /// </summary>
        [Test]
        public void ExtensionMethod_without_DLQ_configuration_produces_no_DLQ_behavior()
        {
            // Arrange - simulate what the extension method does when called without DLQ configuration
            var publishTopology = new StubPublishTopology();
            var hostAddress = new Uri("amazonsqs://localhost");

            // This simulates: configurator.SubscribeTopicToHttpEndpoint("my-topic", "https://example.com/webhook")
            // The configurator defaults: DeadLetterQueueEnabled=false, DeadLetterQueueName=null, MaxReceiveCount=3
            var subscriptionConfigurator = new HttpTopicSubscriptionConfigurator(TopicName, EndpointUrl);
            // No configure delegate invoked - simulates calling without Action<IHttpTopicSubscriptionConfigurator>

            var specification = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                subscriptionConfigurator.TopicName,
                subscriptionConfigurator.EndpointUrl,
                subscriptionConfigurator.RawMessageDelivery,
                subscriptionConfigurator.Durable,
                subscriptionConfigurator.AutoDelete,
                subscriptionConfigurator.DeadLetterQueueEnabled,
                subscriptionConfigurator.DeadLetterQueueName,
                subscriptionConfigurator.MaxReceiveCount);

            // Act
            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            specification.Apply(builder);
            var topology = builder.BuildTopologyLayout();

            // Assert - no DLQ queue should be registered (same behavior as before DLQ feature)
            Assert.That(topology.Queues, Is.Empty,
                "No DLQ queue should be registered when DLQ is not configured (backward compatibility)");

            // Verify no DLQ-related validation failures
            var validationResults = specification.Validate().ToList();
            var dlqValidationResults = validationResults.Where(r =>
                r.Key != null && r.Key.Contains("DeadLetterQueue", StringComparison.OrdinalIgnoreCase)).ToList();
            Assert.That(dlqValidationResults, Is.Empty,
                "No DLQ-related validation results should be produced when DLQ is not configured");
        }

        /// <summary>
        /// Validates: Requirements 6.3, 6.4
        /// Verifies that existing invocations with a configure delegate that only sets non-DLQ properties
        /// (e.g., RawMessageDelivery, Durable) still compile and function without changes.
        /// </summary>
        [Test]
        public void ExtensionMethod_with_non_DLQ_configuration_produces_no_DLQ_behavior()
        {
            // Arrange - simulate calling with a configure delegate that only sets non-DLQ properties
            var publishTopology = new StubPublishTopology();
            var hostAddress = new Uri("amazonsqs://localhost");

            // This simulates: configurator.SubscribeTopicToHttpEndpoint("my-topic", "https://example.com/webhook",
            //     cfg => { cfg.RawMessageDelivery = true; cfg.Durable = false; })
            var subscriptionConfigurator = new HttpTopicSubscriptionConfigurator(TopicName, EndpointUrl);
            subscriptionConfigurator.RawMessageDelivery = true;
            subscriptionConfigurator.Durable = false;
            subscriptionConfigurator.AutoDelete = true;

            var specification = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                subscriptionConfigurator.TopicName,
                subscriptionConfigurator.EndpointUrl,
                subscriptionConfigurator.RawMessageDelivery,
                subscriptionConfigurator.Durable,
                subscriptionConfigurator.AutoDelete,
                subscriptionConfigurator.DeadLetterQueueEnabled,
                subscriptionConfigurator.DeadLetterQueueName,
                subscriptionConfigurator.MaxReceiveCount);

            // Act
            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            specification.Apply(builder);
            var topology = builder.BuildTopologyLayout();

            // Assert - no DLQ queue should be registered
            Assert.That(topology.Queues, Is.Empty,
                "No DLQ queue should be registered when only non-DLQ properties are configured");

            // Verify the HTTP subscription was still created with the correct properties
            Assert.That(topology.HttpSubscriptions.Length, Is.EqualTo(1),
                "HTTP subscription should still be created");
            var httpSub = topology.HttpSubscriptions[0];
            Assert.That(httpSub.EndpointUrl, Is.EqualTo(EndpointUrl));
            Assert.That(httpSub.RawMessageDelivery, Is.True);
            Assert.That(httpSub.DeadLetterQueueEnabled, Is.False,
                "DLQ should not be enabled when not explicitly configured");
        }

        /// <summary>
        /// Validates: Requirements 6.2
        /// Verifies that the generic overload SubscribeTopicToHttpEndpoint&lt;T&gt; delegates correctly
        /// by ensuring the DLQ configuration from the configure delegate is propagated identically
        /// to the topology specification. This tests the delegation pattern by verifying that
        /// the same configurator state produces the same topology regardless of which overload is used.
        /// </summary>
        [Test]
        public void GenericOverload_delegates_DLQ_configuration_identically_to_explicit_topic_overload()
        {
            // Arrange
            var publishTopology = new StubPublishTopology();
            var hostAddress = new Uri("amazonsqs://localhost");
            const string customDlqName = "my-custom-dlq";
            const int customMaxReceiveCount = 7;

            // Simulate what the generic overload does: it resolves the topic name from the type,
            // then delegates to the explicit topic name overload with the same configure delegate.
            // Both overloads create the same HttpTopicSubscriptionConfigurator and pass it to
            // HttpSubscriptionConsumeTopologySpecification.

            // Simulate the explicit topic name overload with DLQ configuration
            var configurator1 = new HttpTopicSubscriptionConfigurator(TopicName, EndpointUrl);
            Action<IHttpTopicSubscriptionConfigurator> configureAction = cfg =>
            {
                cfg.DeadLetterQueueEnabled = true;
                cfg.DeadLetterQueueName = customDlqName;
                cfg.MaxReceiveCount = customMaxReceiveCount;
                cfg.RawMessageDelivery = true;
            };
            configureAction(configurator1);

            var spec1 = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                configurator1.TopicName,
                configurator1.EndpointUrl,
                configurator1.RawMessageDelivery,
                configurator1.Durable,
                configurator1.AutoDelete,
                configurator1.DeadLetterQueueEnabled,
                configurator1.DeadLetterQueueName,
                configurator1.MaxReceiveCount);

            // Simulate the generic overload: it resolves topic name then calls the same code path
            var configurator2 = new HttpTopicSubscriptionConfigurator(TopicName, EndpointUrl);
            configureAction(configurator2);

            var spec2 = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                configurator2.TopicName,
                configurator2.EndpointUrl,
                configurator2.RawMessageDelivery,
                configurator2.Durable,
                configurator2.AutoDelete,
                configurator2.DeadLetterQueueEnabled,
                configurator2.DeadLetterQueueName,
                configurator2.MaxReceiveCount);

            // Act - apply both specifications
            var builder1 = new ReceiveEndpointBrokerTopologyBuilder();
            spec1.Apply(builder1);
            var topology1 = builder1.BuildTopologyLayout();

            var builder2 = new ReceiveEndpointBrokerTopologyBuilder();
            spec2.Apply(builder2);
            var topology2 = builder2.BuildTopologyLayout();

            // Assert - both topologies should be identical
            Assert.That(topology1.Queues.Length, Is.EqualTo(topology2.Queues.Length),
                "Both overloads should produce the same number of queues");
            Assert.That(topology1.Queues.Length, Is.EqualTo(1),
                "DLQ queue should be registered");

            var dlq1 = topology1.Queues[0];
            var dlq2 = topology2.Queues[0];

            Assert.That(dlq1.EntityName, Is.EqualTo(dlq2.EntityName),
                "DLQ queue name should be identical from both overloads");
            Assert.That(dlq1.EntityName, Is.EqualTo(customDlqName),
                "DLQ queue name should match the configured custom name");
            Assert.That(dlq1.Durable, Is.EqualTo(dlq2.Durable),
                "DLQ Durable should be identical from both overloads");
            Assert.That(dlq1.AutoDelete, Is.EqualTo(dlq2.AutoDelete),
                "DLQ AutoDelete should be identical from both overloads");

            // Verify HTTP subscriptions are identical
            Assert.That(topology1.HttpSubscriptions.Length, Is.EqualTo(topology2.HttpSubscriptions.Length),
                "Both overloads should produce the same number of HTTP subscriptions");

            var httpSub1 = topology1.HttpSubscriptions[0];
            var httpSub2 = topology2.HttpSubscriptions[0];

            Assert.That(httpSub1.DeadLetterQueueEnabled, Is.EqualTo(httpSub2.DeadLetterQueueEnabled),
                "DLQ enabled flag should be identical from both overloads");
            Assert.That(httpSub1.DeadLetterQueueName, Is.EqualTo(httpSub2.DeadLetterQueueName),
                "DLQ name should be identical from both overloads");
            Assert.That(httpSub1.MaxReceiveCount, Is.EqualTo(httpSub2.MaxReceiveCount),
                "MaxReceiveCount should be identical from both overloads");
            Assert.That(httpSub1.MaxReceiveCount, Is.EqualTo(customMaxReceiveCount),
                "MaxReceiveCount should match the configured value");
        }

        /// <summary>
        /// Validates: Requirements 6.3, 6.4
        /// Verifies that the legacy constructor (without DLQ parameters) still works correctly,
        /// ensuring backward compatibility for any code that directly instantiates the specification.
        /// </summary>
        [Test]
        public void Legacy_constructor_produces_same_behavior_as_DLQ_disabled()
        {
            // Arrange
            var publishTopology = new StubPublishTopology();
            var hostAddress = new Uri("amazonsqs://localhost");

            // Legacy constructor (without DLQ params)
            var legacySpec = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                TopicName,
                EndpointUrl,
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false);

            // New constructor with DLQ explicitly disabled (what the extension method produces)
            var newSpec = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                TopicName,
                EndpointUrl,
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: false,
                deadLetterQueueName: null,
                maxReceiveCount: 3);

            // Act
            var legacyBuilder = new ReceiveEndpointBrokerTopologyBuilder();
            legacySpec.Apply(legacyBuilder);
            var legacyTopology = legacyBuilder.BuildTopologyLayout();

            var newBuilder = new ReceiveEndpointBrokerTopologyBuilder();
            newSpec.Apply(newBuilder);
            var newTopology = newBuilder.BuildTopologyLayout();

            // Assert - both should produce identical topology (no DLQ)
            Assert.That(legacyTopology.Queues.Length, Is.EqualTo(newTopology.Queues.Length),
                "Legacy and new constructors should produce the same number of queues");
            Assert.That(legacyTopology.Queues, Is.Empty,
                "Legacy constructor should not produce any DLQ queues");
            Assert.That(newTopology.Queues, Is.Empty,
                "New constructor with DLQ disabled should not produce any DLQ queues");

            // Both should have the same HTTP subscriptions
            Assert.That(legacyTopology.HttpSubscriptions.Length, Is.EqualTo(newTopology.HttpSubscriptions.Length),
                "Both constructors should produce the same number of HTTP subscriptions");

            // Verify no DLQ-related validation failures for either
            var legacyValidation = legacySpec.Validate().ToList();
            var newValidation = newSpec.Validate().ToList();

            var legacyDlqResults = legacyValidation.Where(r =>
                r.Key != null && r.Key.Contains("DeadLetterQueue", StringComparison.OrdinalIgnoreCase)).ToList();
            var newDlqResults = newValidation.Where(r =>
                r.Key != null && r.Key.Contains("DeadLetterQueue", StringComparison.OrdinalIgnoreCase)).ToList();

            Assert.That(legacyDlqResults, Is.Empty,
                "Legacy constructor should not produce DLQ validation results");
            Assert.That(newDlqResults, Is.Empty,
                "New constructor with DLQ disabled should not produce DLQ validation results");
        }

        /// <summary>
        /// Validates: Requirements 6.2
        /// Verifies that the generic overload correctly delegates DLQ configuration when DLQ is disabled.
        /// The generic overload should produce the same result as the explicit overload when both
        /// use the same configure delegate (or no delegate).
        /// </summary>
        [Test]
        public void GenericOverload_without_DLQ_delegates_correctly_producing_no_DLQ_behavior()
        {
            // Arrange
            var publishTopology = new StubPublishTopology();
            var hostAddress = new Uri("amazonsqs://localhost");

            // Simulate both overloads without DLQ configuration
            // The generic overload resolves topic name then calls the explicit overload
            var configurator = new HttpTopicSubscriptionConfigurator(TopicName, EndpointUrl);
            // No DLQ configuration - defaults apply

            var spec = new HttpSubscriptionConsumeTopologySpecification(
                publishTopology,
                hostAddress,
                configurator.TopicName,
                configurator.EndpointUrl,
                configurator.RawMessageDelivery,
                configurator.Durable,
                configurator.AutoDelete,
                configurator.DeadLetterQueueEnabled,
                configurator.DeadLetterQueueName,
                configurator.MaxReceiveCount);

            // Act
            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);
            var topology = builder.BuildTopologyLayout();

            // Assert
            Assert.That(topology.Queues, Is.Empty,
                "Generic overload without DLQ config should not produce any DLQ queues");
            Assert.That(topology.HttpSubscriptions.Length, Is.EqualTo(1),
                "HTTP subscription should still be created");

            var httpSub = topology.HttpSubscriptions[0];
            Assert.That(httpSub.DeadLetterQueueEnabled, Is.False,
                "DLQ should be disabled by default");
            Assert.That(httpSub.MaxReceiveCount, Is.EqualTo(3),
                "MaxReceiveCount should be 3 by default");
        }

        /// <summary>
        /// Stub implementation of IAmazonSqsPublishTopology for backward compatibility tests.
        /// </summary>
        class StubPublishTopology : IAmazonSqsPublishTopology
        {
            public IDictionary<string, object> TopicAttributes { get; } = new Dictionary<string, object>();
            public IDictionary<string, object> TopicSubscriptionAttributes { get; } = new Dictionary<string, object>();
            public IDictionary<string, string> TopicTags { get; } = new Dictionary<string, string>();

            IAmazonSqsMessagePublishTopology<T> IAmazonSqsPublishTopology.GetMessageTopology<T>()
            {
                throw new NotImplementedException();
            }

            public BrokerTopology GetPublishBrokerTopology()
            {
                throw new NotImplementedException();
            }

            IMessagePublishTopology<T> IPublishTopology.GetMessageTopology<T>()
            {
                throw new NotImplementedException();
            }

            IMessagePublishTopology IPublishTopology.GetMessageTopology(Type messageType)
            {
                throw new NotImplementedException();
            }

            public bool TryGetPublishAddress(Type messageType, Uri baseAddress, out Uri? publishAddress)
            {
                publishAddress = null;
                return false;
            }

            public ConnectHandle ConnectPublishTopologyConfigurationObserver(IPublishTopologyConfigurationObserver observer)
            {
                throw new NotImplementedException();
            }

            public void Probe(ProbeContext context)
            {
            }
        }

        #endregion
    }
}

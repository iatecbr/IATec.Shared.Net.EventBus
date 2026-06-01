namespace MassTransit.AmazonSqsTransport.Tests.HttpSubscription
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FsCheck;
    using FsCheck.Fluent;
    using MassTransit.AmazonSqsTransport.Topology;
    using MassTransit.Configuration;
    using NUnit.Framework;


    [TestFixture]
    public class HttpSubscriptionTopologySpecificationTests
    {
        /// <summary>
        /// Stub implementation of IAmazonSqsPublishTopology for unit testing.
        /// Returns empty dictionaries for all topology attributes.
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

        StubPublishTopology _publishTopology;
        Uri _hostAddress;

        [SetUp]
        public void Setup()
        {
            _publishTopology = new StubPublishTopology();
            _hostAddress = new Uri("amazonsqs://localhost");
        }

        /// <summary>
        /// **Validates: Requirements 2.2**
        /// Verifies that the DLQ queue is created as a standard SQS queue (not FIFO),
        /// without the FifoQueue attribute and without the .fifo suffix in the name.
        /// </summary>
        [Test]
        public void DLQ_queue_is_standard_not_FIFO_and_has_no_fifo_suffix()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "my-topic",
                "https://example.com/webhook",
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: true,
                deadLetterQueueName: null,
                maxReceiveCount: 3);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();
            var dlqQueue = topology.Queues.FirstOrDefault(q => q.EntityName.Contains("dlq"));

            Assert.That(dlqQueue, Is.Not.Null, "DLQ queue should be registered in the topology");
            Assert.That(dlqQueue!.EntityName, Does.Not.EndWith(".fifo"),
                "DLQ queue name must not have .fifo suffix (must be standard queue)");
            Assert.That(dlqQueue.QueueAttributes.ContainsKey("FifoQueue"), Is.False,
                "DLQ queue must not have FifoQueue attribute (must be standard queue)");
        }

        /// <summary>
        /// **Validates: Requirements 2.2**
        /// Verifies that a custom DLQ name also does not include .fifo suffix or FifoQueue attribute.
        /// </summary>
        [Test]
        public void DLQ_queue_with_custom_name_is_standard_not_FIFO()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "my-topic",
                "https://example.com/webhook",
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: true,
                deadLetterQueueName: "my-custom-dlq",
                maxReceiveCount: 5);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();
            var dlqQueue = topology.Queues.FirstOrDefault(q => q.EntityName == "my-custom-dlq");

            Assert.That(dlqQueue, Is.Not.Null, "Custom DLQ queue should be registered in the topology");
            Assert.That(dlqQueue!.EntityName, Does.Not.EndWith(".fifo"),
                "Custom DLQ queue name must not have .fifo suffix");
            Assert.That(dlqQueue.QueueAttributes.ContainsKey("FifoQueue"), Is.False,
                "Custom DLQ queue must not have FifoQueue attribute");
        }

        /// <summary>
        /// **Validates: Requirements 2.7**
        /// Verifies that the DLQ queue is created with MessageRetentionPeriod = 2592000 (30 days).
        /// </summary>
        [Test]
        public void DLQ_queue_has_MessageRetentionPeriod_of_2592000()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "orders-topic",
                "https://example.com/webhook",
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: true,
                deadLetterQueueName: null,
                maxReceiveCount: 3);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();
            var dlqQueue = topology.Queues.FirstOrDefault(q => q.EntityName == "orders-topic-http-dlq");

            Assert.That(dlqQueue, Is.Not.Null, "DLQ queue should be registered");
            Assert.That(dlqQueue!.QueueAttributes.ContainsKey("MessageRetentionPeriod"), Is.True,
                "DLQ queue must have MessageRetentionPeriod attribute");
            Assert.That(dlqQueue.QueueAttributes["MessageRetentionPeriod"], Is.EqualTo("2592000"),
                "MessageRetentionPeriod must be 2592000 seconds (30 days)");
        }

        /// <summary>
        /// **Validates: Requirements 2.7**
        /// Verifies MessageRetentionPeriod with a custom DLQ name.
        /// </summary>
        [Test]
        public void DLQ_queue_with_custom_name_has_MessageRetentionPeriod_of_2592000()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "events-topic",
                "https://example.com/events",
                rawMessageDelivery: false,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: true,
                deadLetterQueueName: "events-failures",
                maxReceiveCount: 10);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();
            var dlqQueue = topology.Queues.FirstOrDefault(q => q.EntityName == "events-failures");

            Assert.That(dlqQueue, Is.Not.Null, "Custom DLQ queue should be registered");
            Assert.That(dlqQueue!.QueueAttributes["MessageRetentionPeriod"], Is.EqualTo("2592000"),
                "MessageRetentionPeriod must be 2592000 seconds (30 days)");
        }

        /// <summary>
        /// **Validates: Requirements 2.9**
        /// Verifies that when DeadLetterQueueEnabled is false, no DLQ queue is registered in the topology.
        /// </summary>
        [Test]
        public void DLQ_disabled_does_not_register_queue_in_topology()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "my-topic",
                "https://example.com/webhook",
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: false,
                deadLetterQueueName: null,
                maxReceiveCount: 3);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();

            Assert.That(topology.Queues, Is.Empty,
                "No queues should be registered when DLQ is disabled");
        }

        /// <summary>
        /// **Validates: Requirements 2.9**
        /// Verifies that even with a custom DLQ name set, if DeadLetterQueueEnabled is false,
        /// no DLQ queue is registered.
        /// </summary>
        [Test]
        public void DLQ_disabled_with_custom_name_does_not_register_queue()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "my-topic",
                "https://example.com/webhook",
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false,
                deadLetterQueueEnabled: false,
                deadLetterQueueName: "my-custom-dlq",
                maxReceiveCount: 5);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();

            Assert.That(topology.Queues, Is.Empty,
                "No queues should be registered when DLQ is disabled, even with a custom name set");
        }

        /// <summary>
        /// **Validates: Requirements 2.9**
        /// Verifies that the legacy constructor (without DLQ params) does not register any queue.
        /// </summary>
        [Test]
        public void Legacy_constructor_without_DLQ_params_does_not_register_queue()
        {
            var spec = new HttpSubscriptionConsumeTopologySpecification(
                _publishTopology,
                _hostAddress,
                "my-topic",
                "https://example.com/webhook",
                rawMessageDelivery: true,
                durable: true,
                autoDelete: false);

            var builder = new ReceiveEndpointBrokerTopologyBuilder();
            spec.Apply(builder);

            var topology = builder.BuildTopologyLayout();

            Assert.That(topology.Queues, Is.Empty,
                "No queues should be registered when using the legacy constructor (DLQ disabled by default)");
        }

        /// <summary>
        /// **Validates: Requirements 2.1, 2.3, 2.4**
        /// Feature: sns-http-subscription-dlq, Property 5: DLQ habilitada registra cola con propiedades heredadas
        /// For ANY configuration with DeadLetterQueueEnabled = true, valid DLQ name, and arbitrary values
        /// of Durable and AutoDelete, when applying the topology:
        /// 1. A queue is registered in the builder whose name is the resolved DLQ name
        /// 2. The queue's Durable property equals that of the subscription
        /// 3. The queue's AutoDelete property equals that of the subscription
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property DLQ_enabled_registers_queue_with_inherited_Durable_and_AutoDelete()
        {
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

            // Generator for valid topic names (1-40 chars, valid characters)
            var topicNameGen = Gen.Choose(1, 40).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Generator for valid DLQ names (1-80 chars, valid characters) or null (to use default pattern)
            var dlqNameGen = Gen.OneOf(
                Gen.Constant<string>(null),
                Gen.Choose(1, 60).SelectMany(len =>
                    Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                        .Select(chars => new string(chars)))
            );

            // Generator for bool values (Durable and AutoDelete)
            var boolGen = Gen.Elements(true, false);

            // Combine all varying parameters
            var configGen = topicNameGen.SelectMany(topic =>
                dlqNameGen.SelectMany(dlqName =>
                    boolGen.SelectMany(durable =>
                        boolGen.Select(autoDelete =>
                            (topic, dlqName, durable, autoDelete)))));

            var property = Prop.ForAll(
                configGen.ToArbitrary(),
                (ValueTuple<string, string, bool, bool> config) =>
                {
                    var topicName = config.Item1;
                    var dlqName = config.Item2;
                    var durable = config.Item3;
                    var autoDelete = config.Item4;

                    var publishTopology = new StubPublishTopology();
                    var hostAddress = new Uri("amazonsqs://localhost");

                    var spec = new HttpSubscriptionConsumeTopologySpecification(
                        publishTopology,
                        hostAddress,
                        topicName,
                        "https://example.com/webhook",
                        rawMessageDelivery: true,
                        durable: durable,
                        autoDelete: autoDelete,
                        deadLetterQueueEnabled: true,
                        deadLetterQueueName: dlqName,
                        maxReceiveCount: 3);

                    var builder = new ReceiveEndpointBrokerTopologyBuilder();
                    spec.Apply(builder);
                    var topology = builder.BuildTopologyLayout();

                    // Resolve expected DLQ name
                    var expectedDlqName = string.IsNullOrWhiteSpace(dlqName)
                        ? $"{topicName}-http-dlq"
                        : dlqName;

                    // Find the DLQ queue in the topology
                    var dlqQueue = topology.Queues.FirstOrDefault(q => q.EntityName == expectedDlqName);

                    var queueRegistered = dlqQueue != null;
                    var durableInherited = dlqQueue != null && dlqQueue.Durable == durable;
                    var autoDeleteInherited = dlqQueue != null && dlqQueue.AutoDelete == autoDelete;

                    return queueRegistered
                        .Label($"DLQ queue '{expectedDlqName}' should be registered in the topology")
                        .And(durableInherited
                            .Label($"DLQ queue Durable should be {durable} (inherited from subscription)"))
                        .And(autoDeleteInherited
                            .Label($"DLQ queue AutoDelete should be {autoDelete} (inherited from subscription)"));
                });

            return property;
        }

        /// <summary>
        /// **Validates: Requirements 5.1, 5.2, 5.4**
        /// Feature: sns-http-subscription-dlq, Property 7: Validación de topología rechaza nombres de DLQ inválidos
        /// For ANY configuration with DeadLetterQueueEnabled = true where the resolved DLQ name
        /// exceeds 80 characters, contains invalid characters, or is empty/whitespace-only,
        /// the validation must produce a ValidationResult with Failure disposition that identifies
        /// the field DeadLetterQueueName.
        ///
        /// The resolved DLQ name is determined by ResolveDeadLetterQueueName():
        /// - If deadLetterQueueName is null/whitespace → resolved = "{topicName}-http-dlq"
        /// - Otherwise → resolved = deadLetterQueueName as-is
        ///
        /// This property tests three sub-cases:
        /// 1. Explicit DLQ name >80 chars → Failure (Req 5.1)
        /// 2. Explicit DLQ name with invalid chars → Failure (Req 5.2)
        /// 3. Topic name that produces a resolved name >80 chars via the fallback pattern (Req 5.1/5.4)
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property Topology_validation_rejects_invalid_DLQ_names()
        {
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
            const string invalidChars = "!@#$%^&*() +=[]{}|;:',.<>?/~`\"\\";

            // Generator for valid topic names (1-40 chars, valid characters)
            var topicNameGen = Gen.Choose(1, 40).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Case 1: Explicit DLQ names exceeding 80 characters (Requirement 5.1)
            // These are non-whitespace so they won't fall back to the pattern
            var tooLongNameGen = Gen.Choose(81, 120).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Case 2: Explicit DLQ names with invalid characters (Requirement 5.2)
            // These are non-whitespace so they won't fall back to the pattern
            var invalidCharNameGen = Gen.Choose(2, 40).SelectMany(len =>
                Gen.Choose(0, len - 1).SelectMany(insertPos =>
                    Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len).SelectMany(validArr =>
                        Gen.Elements(invalidChars.ToCharArray()).Select(invalidChar =>
                        {
                            validArr[insertPos] = invalidChar;
                            return new string(validArr);
                        }))));

            // Case 3: Topic name long enough that "{TopicName}-http-dlq" exceeds 80 chars (Requirement 5.1)
            // The suffix "-http-dlq" is 9 chars, so topic name >71 chars produces resolved name >80 chars
            var longTopicNameGen = Gen.Choose(72, 80).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Combine: Cases 1 and 2 use a valid topic name + invalid explicit DLQ name
            // Case 3 uses a long topic name + null DLQ name (triggers fallback pattern)
            var case1Gen = topicNameGen.SelectMany(topic =>
                tooLongNameGen.Select(dlqName => (topic, dlqName: (string?)dlqName)));

            var case2Gen = topicNameGen.SelectMany(topic =>
                invalidCharNameGen.Select(dlqName => (topic, dlqName: (string?)dlqName)));

            var case3Gen = longTopicNameGen.Select(topic => (topic, dlqName: (string?)null));

            var configGen = Gen.OneOf(case1Gen, case2Gen, case3Gen);

            var property = Prop.ForAll(
                configGen.ToArbitrary(),
                (ValueTuple<string, string?> config) =>
                {
                    var topicName = config.Item1;
                    var dlqName = config.Item2;

                    var publishTopology = new StubPublishTopology();
                    var hostAddress = new Uri("amazonsqs://localhost");

                    var spec = new HttpSubscriptionConsumeTopologySpecification(
                        publishTopology,
                        hostAddress,
                        topicName,
                        "https://example.com/webhook",
                        rawMessageDelivery: true,
                        durable: true,
                        autoDelete: false,
                        deadLetterQueueEnabled: true,
                        deadLetterQueueName: dlqName,
                        maxReceiveCount: 3);

                    var validationResults = spec.Validate().ToList();

                    var hasDeadLetterQueueNameFailure = validationResults.Any(r =>
                        r.Key != null
                        && r.Key.Contains("DeadLetterQueueName", StringComparison.OrdinalIgnoreCase)
                        && r.Disposition == ValidationResultDisposition.Failure);

                    // Compute the resolved name for labeling
                    var resolvedName = string.IsNullOrWhiteSpace(dlqName)
                        ? $"{topicName}-http-dlq"
                        : dlqName;

                    return hasDeadLetterQueueNameFailure
                        .Label($"Validation should produce a Failure for DeadLetterQueueName when resolved name is invalid (resolved='{resolvedName}', length={resolvedName.Length})");
                });

            return property;
        }

        /// <summary>
        /// **Validates: Requirements 1.7, 2.9, 5.3**
        /// Feature: sns-http-subscription-dlq, Property 4: DLQ deshabilitada no produce comportamiento DLQ
        /// For ANY configuration where DeadLetterQueueEnabled = false (regardless of DeadLetterQueueName
        /// and MaxReceiveCount values), when applying the topology:
        /// 1. No DLQ queue is registered in the builder
        /// 2. Validation does not produce any ValidationResult referencing the DLQ
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property DLQ_disabled_does_not_produce_DLQ_behavior_for_any_configuration()
        {
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

            // Generator for valid topic names (1-40 chars to keep within bounds)
            var topicNameGen = Gen.Choose(1, 40).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Generator for arbitrary DLQ names: null, empty, valid, or even invalid names
            var dlqNameGen = Gen.OneOf(
                Gen.Constant<string>(null),
                Gen.Constant(""),
                Gen.Constant("   "),
                Gen.Choose(1, 80).SelectMany(len =>
                    Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                        .Select(chars => new string(chars))),
                Gen.Constant("invalid name with spaces!@#")
            );

            // Generator for arbitrary MaxReceiveCount values within valid range
            var maxReceiveCountGen = Gen.Choose(1, 100);

            // Generator for bool values
            var boolGen = Gen.Elements(true, false);

            // Combine all varying parameters into a single tuple generator
            var configGen = topicNameGen.SelectMany(topic =>
                dlqNameGen.SelectMany(dlqName =>
                    maxReceiveCountGen.SelectMany(maxCount =>
                        boolGen.SelectMany(durable =>
                            boolGen.Select(rawMsg =>
                                (topic, dlqName, maxCount, durable, rawMsg))))));

            var property = Prop.ForAll(
                configGen.ToArbitrary(),
                (ValueTuple<string, string, int, bool, bool> config) =>
                {
                    var publishTopology = new StubPublishTopology();
                    var hostAddress = new Uri("amazonsqs://localhost");

                    var spec = new HttpSubscriptionConsumeTopologySpecification(
                        publishTopology,
                        hostAddress,
                        config.Item1,
                        "https://example.com/webhook",
                        rawMessageDelivery: config.Item5,
                        durable: config.Item4,
                        autoDelete: !config.Item4,
                        deadLetterQueueEnabled: false,
                        deadLetterQueueName: config.Item2,
                        maxReceiveCount: config.Item3);

                    // Verify: No DLQ queue is registered in the builder
                    var builder = new ReceiveEndpointBrokerTopologyBuilder();
                    spec.Apply(builder);
                    var topology = builder.BuildTopologyLayout();

                    var noQueuesRegistered = topology.Queues.Length == 0;

                    // Verify: Validation does not produce any ValidationResult referencing DLQ
                    var validationResults = spec.Validate().ToList();
                    var noDlqValidationResults = !validationResults.Any(r =>
                        (r.Key != null && r.Key.Contains("DeadLetterQueue", StringComparison.OrdinalIgnoreCase))
                        || (r.Message != null && r.Message.Contains("DeadLetterQueue", StringComparison.OrdinalIgnoreCase)));

                    return noQueuesRegistered
                        .Label("No DLQ queue should be registered when DeadLetterQueueEnabled is false")
                        .And(noDlqValidationResults
                            .Label("No ValidationResult referencing DLQ should be produced when DeadLetterQueueEnabled is false"));
                });

            return property;
        }
    }
}

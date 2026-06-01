namespace MassTransit.AmazonSqsTransport.Tests.HttpSubscription
{
    using System;
    using System.Reflection;
    using FsCheck;
    using FsCheck.Fluent;
    using NUnit.Framework;


    [TestFixture]
    public class HttpTopicSubscriptionConfiguratorTests
    {
        /// <summary>
        /// Invokes the internal ResolveDeadLetterQueueName() method via reflection.
        /// </summary>
        static string InvokeResolveDeadLetterQueueName(HttpTopicSubscriptionConfigurator configurator)
        {
            var method = typeof(HttpTopicSubscriptionConfigurator)
                .GetMethod("ResolveDeadLetterQueueName", BindingFlags.Instance | BindingFlags.NonPublic);
            return (string)method!.Invoke(configurator, null)!;
        }

        /// <summary>
        /// **Validates: Requirements 1.3**
        /// Property 1: Generación del nombre de DLQ sigue el patrón.
        /// For any valid TopicName (alphanumeric, hyphens, underscores, ≤80 chars) and
        /// DeadLetterQueueName that is null or composed only of whitespace,
        /// ResolveDeadLetterQueueName() must return exactly "{TopicName}-http-dlq".
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property ResolveDeadLetterQueueName_returns_TopicName_http_dlq_when_name_is_null_or_whitespace()
        {
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

            // Generator for valid topic names: alphanumeric, hyphens, underscores, 1-80 chars
            var topicNameGen = Gen.Choose(1, 80).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Generator for null or whitespace-only strings (null, "", " ", "  ", "\t", etc.)
            var nullOrWhitespaceGen = Gen.OneOf(
                Gen.Constant((string?)null),
                Gen.Constant((string?)""),
                Gen.Choose(1, 10).Select(len => new string(' ', len) as string),
                Gen.Constant((string?)"\t"),
                Gen.Constant((string?)" \t ")
            );

            var property = Prop.ForAll(
                topicNameGen.ToArbitrary(),
                nullOrWhitespaceGen.ToArbitrary(),
                (string topicName, string? dlqName) =>
                {
                    var configurator = new HttpTopicSubscriptionConfigurator(topicName, "https://example.com/webhook");
                    configurator.DeadLetterQueueEnabled = true;

                    // Note: DeadLetterQueueName setter validates non-null values,
                    // but whitespace strings contain invalid chars. We need to set the backing field
                    // or only use null. The ResolveDeadLetterQueueName checks IsNullOrWhiteSpace,
                    // but the setter rejects whitespace chars. So we only test null/empty here
                    // since the setter validation prevents whitespace-only strings from being set.
                    // However, the internal method still handles it correctly if the field were set directly.
                    // For this property test, we verify with null (the primary case).
                    if (dlqName == null || dlqName.Length == 0)
                    {
                        configurator.DeadLetterQueueName = dlqName;
                    }
                    else
                    {
                        // For whitespace strings, use reflection to set the backing field directly
                        // since the setter validates characters but ResolveDeadLetterQueueName
                        // uses string.IsNullOrWhiteSpace which handles whitespace
                        var field = typeof(HttpTopicSubscriptionConfigurator)
                            .GetField("_deadLetterQueueName", BindingFlags.Instance | BindingFlags.NonPublic);
                        field!.SetValue(configurator, dlqName);
                    }

                    var resolved = InvokeResolveDeadLetterQueueName(configurator);
                    var expected = $"{topicName}-http-dlq";

                    return (resolved == expected)
                        .Label($"Expected '{expected}' but got '{resolved}' (dlqName was '{dlqName ?? "null"}')");
                });

            return property;
        }

        HttpTopicSubscriptionConfigurator _configurator;

        [SetUp]
        public void Setup()
        {
            _configurator = new HttpTopicSubscriptionConfigurator("test-topic", "https://example.com/webhook");
        }

        [Test]
        public void DeadLetterQueueEnabled_should_default_to_false()
        {
            Assert.That(_configurator.DeadLetterQueueEnabled, Is.False);
        }

        [Test]
        public void MaxReceiveCount_should_default_to_3()
        {
            Assert.That(_configurator.MaxReceiveCount, Is.EqualTo(3));
        }

        [Test]
        public void DeadLetterQueueName_should_default_to_null()
        {
            Assert.That(_configurator.DeadLetterQueueName, Is.Null);
        }

        [Test]
        public void When_DLQ_disabled_setting_arbitrary_MaxReceiveCount_does_not_throw()
        {
            _configurator.DeadLetterQueueEnabled = false;

            Assert.DoesNotThrow(() => _configurator.MaxReceiveCount = 50);
        }

        [Test]
        public void When_DLQ_disabled_setting_valid_DeadLetterQueueName_does_not_throw()
        {
            _configurator.DeadLetterQueueEnabled = false;

            Assert.DoesNotThrow(() => _configurator.DeadLetterQueueName = "my-custom-dlq");
        }

        [Test]
        public void When_DLQ_disabled_setting_null_DeadLetterQueueName_does_not_throw()
        {
            _configurator.DeadLetterQueueEnabled = false;

            Assert.DoesNotThrow(() => _configurator.DeadLetterQueueName = null);
        }

        [Test]
        [TestCase(0)]
        [TestCase(-1)]
        [TestCase(101)]
        [TestCase(int.MinValue)]
        [TestCase(int.MaxValue)]
        public void MaxReceiveCount_outside_1_to_100_throws_ArgumentOutOfRangeException(int invalidValue)
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => _configurator.MaxReceiveCount = invalidValue);
        }

        [Test]
        [TestCase(1)]
        [TestCase(50)]
        [TestCase(100)]
        public void MaxReceiveCount_within_1_to_100_does_not_throw(int validValue)
        {
            Assert.DoesNotThrow(() => _configurator.MaxReceiveCount = validValue);
            Assert.That(_configurator.MaxReceiveCount, Is.EqualTo(validValue));
        }

        [Test]
        public void DeadLetterQueueName_exceeding_80_chars_throws_ArgumentException()
        {
            var longName = new string('a', 81);

            Assert.Throws<ArgumentException>(() => _configurator.DeadLetterQueueName = longName);
        }

        [Test]
        [TestCase("invalid name with spaces")]
        [TestCase("invalid.name.with.dots")]
        [TestCase("invalid/name")]
        [TestCase("invalid@name")]
        [TestCase("name!")]
        public void DeadLetterQueueName_with_invalid_characters_throws_ArgumentException(string invalidName)
        {
            Assert.Throws<ArgumentException>(() => _configurator.DeadLetterQueueName = invalidName);
        }

        [Test]
        [TestCase("valid-name")]
        [TestCase("valid_name")]
        [TestCase("ValidName123")]
        [TestCase("a")]
        [TestCase("my-queue-dlq_01")]
        public void DeadLetterQueueName_with_valid_characters_does_not_throw(string validName)
        {
            Assert.DoesNotThrow(() => _configurator.DeadLetterQueueName = validName);
            Assert.That(_configurator.DeadLetterQueueName, Is.EqualTo(validName));
        }

        [Test]
        public void DeadLetterQueueName_at_exactly_80_chars_does_not_throw()
        {
            var name = new string('a', 80);

            Assert.DoesNotThrow(() => _configurator.DeadLetterQueueName = name);
            Assert.That(_configurator.DeadLetterQueueName, Is.EqualTo(name));
        }

        [Test]
        public void DeadLetterQueueName_set_to_null_does_not_throw()
        {
            _configurator.DeadLetterQueueName = "some-name";

            Assert.DoesNotThrow(() => _configurator.DeadLetterQueueName = null);
            Assert.That(_configurator.DeadLetterQueueName, Is.Null);
        }

        /// <summary>
        /// **Validates: Requirements 1.5**
        /// Property 2: MaxReceiveCount rechaza valores fuera de rango.
        /// For any int outside [1,100] → ArgumentOutOfRangeException; within [1,100] → no exception and value stored.
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property MaxReceiveCount_outside_range_throws_ArgumentOutOfRangeException_property()
        {
            // Generator for values outside [1, 100]
            var outOfRangeGen = Gen.OneOf(
                Gen.Choose(int.MinValue, 0),
                Gen.Choose(101, int.MaxValue)
            );

            var property = Prop.ForAll(
                outOfRangeGen.ToArbitrary(),
                value =>
                {
                    var configurator = new HttpTopicSubscriptionConfigurator("test-topic", "https://example.com/webhook");
                    var threw = false;
                    try
                    {
                        configurator.MaxReceiveCount = value;
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        threw = true;
                    }

                    return threw.Label($"Expected ArgumentOutOfRangeException for value {value}");
                });

            return property;
        }

        /// <summary>
        /// **Validates: Requirements 1.5**
        /// Property 2: MaxReceiveCount within [1,100] does not throw and stores value correctly.
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property MaxReceiveCount_within_range_does_not_throw_and_stores_value()
        {
            // Generator for values within [1, 100]
            var inRangeGen = Gen.Choose(1, 100);

            var property = Prop.ForAll(
                inRangeGen.ToArbitrary(),
                value =>
                {
                    var configurator = new HttpTopicSubscriptionConfigurator("test-topic", "https://example.com/webhook");
                    configurator.MaxReceiveCount = value;

                    return (configurator.MaxReceiveCount == value)
                        .Label($"Expected MaxReceiveCount to be {value} but got {configurator.MaxReceiveCount}");
                });

            return property;
        }

        /// <summary>
        /// **Validates: Requirements 1.6**
        /// Feature: sns-http-subscription-dlq, Property 3: DeadLetterQueueName rechaza nombres inválidos
        /// For any string containing at least one character outside [a-zA-Z0-9\-_] OR exceeding 80 characters,
        /// setting DeadLetterQueueName throws ArgumentException.
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property DeadLetterQueueName_with_invalid_characters_throws_ArgumentException_property()
        {
            // Generator for strings with at least one invalid character (outside [a-zA-Z0-9\-_])
            const string invalidChars = " !@#$%^&*()+=[]{}|;:',.<>?/`~\"\\\t\n";

            var invalidCharGen = Gen.Elements(invalidChars.ToCharArray());
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
            var validCharGen = Gen.Elements(validChars.ToCharArray());

            // Build a string that contains at least one invalid character, length 1-80
            var invalidNameGen = Gen.Choose(1, 79).SelectMany(validLen =>
                Gen.ArrayOf(validCharGen, validLen).SelectMany(validPart =>
                    invalidCharGen.Select(invalidChar =>
                    {
                        // Insert the invalid char at a random position
                        var result = new char[validPart.Length + 1];
                        var insertPos = validPart.Length / 2;
                        Array.Copy(validPart, 0, result, 0, insertPos);
                        result[insertPos] = invalidChar;
                        Array.Copy(validPart, insertPos, result, insertPos + 1, validPart.Length - insertPos);
                        return new string(result);
                    })));

            var property = Prop.ForAll(
                invalidNameGen.ToArbitrary(),
                (string name) =>
                {
                    var configurator = new HttpTopicSubscriptionConfigurator("test-topic", "https://example.com/webhook");
                    var threw = false;
                    try
                    {
                        configurator.DeadLetterQueueName = name;
                    }
                    catch (ArgumentException)
                    {
                        threw = true;
                    }

                    return threw.Label($"Expected ArgumentException for name '{name}'");
                });

            return property;
        }

        /// <summary>
        /// **Validates: Requirements 1.6**
        /// Feature: sns-http-subscription-dlq, Property 3: DeadLetterQueueName rechaza nombres inválidos
        /// For any string exceeding 80 characters (even with valid chars only),
        /// setting DeadLetterQueueName throws ArgumentException.
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property DeadLetterQueueName_exceeding_80_chars_throws_ArgumentException_property()
        {
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

            // Generator for strings with valid chars but length > 80
            var tooLongGen = Gen.Choose(81, 200).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            var property = Prop.ForAll(
                tooLongGen.ToArbitrary(),
                (string name) =>
                {
                    var configurator = new HttpTopicSubscriptionConfigurator("test-topic", "https://example.com/webhook");
                    var threw = false;
                    try
                    {
                        configurator.DeadLetterQueueName = name;
                    }
                    catch (ArgumentException)
                    {
                        threw = true;
                    }

                    return threw.Label($"Expected ArgumentException for name of length {name.Length}");
                });

            return property;
        }

        /// <summary>
        /// **Validates: Requirements 1.6**
        /// Feature: sns-http-subscription-dlq, Property 3: DeadLetterQueueName rechaza nombres inválidos
        /// For any string composed only of valid characters [a-zA-Z0-9\-_] AND length ≤80,
        /// setting DeadLetterQueueName completes without exception.
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property DeadLetterQueueName_with_valid_characters_and_length_does_not_throw_property()
        {
            const string validChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

            // Generator for strings with only valid chars and length 1-80
            var validNameGen = Gen.Choose(1, 80).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(validChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            var property = Prop.ForAll(
                validNameGen.ToArbitrary(),
                (string name) =>
                {
                    var configurator = new HttpTopicSubscriptionConfigurator("test-topic", "https://example.com/webhook");
                    var threw = false;
                    try
                    {
                        configurator.DeadLetterQueueName = name;
                    }
                    catch (Exception)
                    {
                        threw = true;
                    }

                    return (!threw).Label($"Unexpected exception for valid name '{name}'")
                        .And((configurator.DeadLetterQueueName == name)
                            .Label($"Expected DeadLetterQueueName to be '{name}' but got '{configurator.DeadLetterQueueName}'"));
                });

            return property;
        }
    }
}

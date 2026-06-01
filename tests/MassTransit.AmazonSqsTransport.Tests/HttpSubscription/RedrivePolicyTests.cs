namespace MassTransit.AmazonSqsTransport.Tests.HttpSubscription
{
    using System.Text.Json;
    using FsCheck;
    using FsCheck.Fluent;
    using NUnit.Framework;


    [TestFixture]
    public class RedrivePolicyTests
    {
        /// <summary>
        /// Generates the RedrivePolicy JSON string using the same interpolation pattern
        /// as AmazonSqsClientContext.CreateHttpSubscription.
        /// </summary>
        static string GenerateRedrivePolicy(string dlqArn)
        {
            return $"{{\"deadLetterTargetArn\":\"{dlqArn}\"}}";
        }

        /// <summary>
        /// **Validates: Requirements 3.2**
        /// Feature: sns-http-subscription-dlq, Property 6: Formato del RedrivePolicy JSON
        /// For any valid DLQ ARN (format arn:aws:sqs:{region}:{account}:{name}),
        /// the generated JSON must be exactly {"deadLetterTargetArn":"&lt;ARN&gt;"}
        /// without extra spaces or fields.
        /// </summary>
        [FsCheck.NUnit.Property(MaxTest = 100)]
        public FsCheck.Property RedrivePolicy_JSON_format_is_exact_for_any_valid_DLQ_ARN()
        {
            const string digits = "0123456789";
            const string queueNameChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";

            // Generator for AWS region (e.g., us-east-1, eu-west-2, ap-southeast-1)
            var regionGen = Gen.Elements(
                "us-east-1", "us-east-2", "us-west-1", "us-west-2",
                "eu-west-1", "eu-west-2", "eu-central-1",
                "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
                "sa-east-1", "ca-central-1", "me-south-1");

            // Generator for AWS account ID (12 digits)
            var accountGen = Gen.ArrayOf(Gen.Elements(digits.ToCharArray()), 12)
                .Select(chars => new string(chars));

            // Generator for SQS queue name (1-80 chars, valid characters)
            var queueNameGen = Gen.Choose(1, 80).SelectMany(len =>
                Gen.ArrayOf(Gen.Elements(queueNameChars.ToCharArray()), len)
                    .Select(chars => new string(chars)));

            // Combine into a valid ARN: arn:aws:sqs:{region}:{account}:{name}
            var arnGen = regionGen.SelectMany(region =>
                accountGen.SelectMany(account =>
                    queueNameGen.Select(name =>
                        $"arn:aws:sqs:{region}:{account}:{name}")));

            var property = Prop.ForAll(
                arnGen.ToArbitrary(),
                (string dlqArn) =>
                {
                    var redrivePolicy = GenerateRedrivePolicy(dlqArn);

                    // 1. Must be exactly {"deadLetterTargetArn":"<ARN>"}
                    var expectedJson = $"{{\"deadLetterTargetArn\":\"{dlqArn}\"}}";
                    var exactMatch = redrivePolicy == expectedJson;

                    // 2. Must be valid JSON
                    var isValidJson = false;
                    try
                    {
                        using var doc = JsonDocument.Parse(redrivePolicy);
                        isValidJson = true;
                    }
                    catch
                    {
                        isValidJson = false;
                    }

                    // 3. Must have no extra spaces (no space after { or before }, no space after : or ,)
                    var noExtraSpaces = !redrivePolicy.Contains(" ");

                    // 4. Must have exactly one field (deadLetterTargetArn)
                    var hasExactlyOneField = false;
                    try
                    {
                        using var doc = JsonDocument.Parse(redrivePolicy);
                        var root = doc.RootElement;
                        var propertyCount = 0;
                        foreach (var _ in root.EnumerateObject())
                            propertyCount++;
                        hasExactlyOneField = propertyCount == 1;
                    }
                    catch
                    {
                        hasExactlyOneField = false;
                    }

                    // 5. The single field must be "deadLetterTargetArn" with value equal to the ARN
                    var correctFieldValue = false;
                    try
                    {
                        using var doc = JsonDocument.Parse(redrivePolicy);
                        var root = doc.RootElement;
                        if (root.TryGetProperty("deadLetterTargetArn", out var arnElement))
                        {
                            correctFieldValue = arnElement.GetString() == dlqArn;
                        }
                    }
                    catch
                    {
                        correctFieldValue = false;
                    }

                    return exactMatch
                        .Label($"RedrivePolicy must be exactly '{{\"deadLetterTargetArn\":\"{dlqArn}\"}}' but got '{redrivePolicy}'")
                        .And(isValidJson
                            .Label("RedrivePolicy must be valid JSON"))
                        .And(noExtraSpaces
                            .Label("RedrivePolicy must not contain extra spaces"))
                        .And(hasExactlyOneField
                            .Label("RedrivePolicy must have exactly one JSON field"))
                        .And(correctFieldValue
                            .Label("deadLetterTargetArn field must contain the exact DLQ ARN"));
                });

            return property;
        }
    }
}

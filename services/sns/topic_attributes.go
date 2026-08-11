package sns

import (
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
)

// GetTopicAttributes returns the attributes of a topic.
func (b *InMemoryBackend) GetTopicAttributes(topicArn string) (map[string]string, error) {
	b.mu.RLock("GetTopicAttributes")
	defer b.mu.RUnlock()

	topic, exists := b.topics.Get(topicArn)
	if !exists {
		return nil, ErrTopicNotFound
	}

	attrs := make(map[string]string, len(topic.Attributes)+computedTopicAttrCount)
	maps.Copy(attrs, topic.Attributes)
	// DataProtectionPolicy is stored on topic.Attributes internally but is only
	// ever surfaced via the dedicated GetDataProtectionPolicy operation, not here
	// (see isKnownTopicAttribute).
	delete(attrs, "DataProtectionPolicy")

	// Ensure Policy is always a valid JSON string with an empty Statement array so
	// Terraform's PolicyHasValidAWSPrincipals JMESPath check returns []any{}.
	if attrs["Policy"] == "" {
		attrs["Policy"] = defaultPolicyJSON
	}

	// Populate computed attributes that AWS returns but we store dynamically.
	if attrs["Owner"] == "" {
		attrs["Owner"] = b.accountID
	}

	// AWS always returns TopicArn as an attribute in GetTopicAttributes.
	if attrs[topicArnKey] == "" {
		attrs[topicArnKey] = topicArn
	}

	// AWS always returns FifoTopic in GetTopicAttributes: "true" for FIFO topics,
	// "false" for standard topics. Non-FIFO topics have no stored value — default to "false".
	if attrs["FifoTopic"] == "" {
		attrs["FifoTopic"] = boolFalseStr
	}

	// EffectiveDeliveryPolicy is the resolved delivery policy (defaults to
	// AWS standard retry configuration when no custom DeliveryPolicy is set).
	if attrs["EffectiveDeliveryPolicy"] == "" {
		if attrs["DeliveryPolicy"] != "" {
			attrs["EffectiveDeliveryPolicy"] = attrs["DeliveryPolicy"]
		} else {
			attrs["EffectiveDeliveryPolicy"] = defaultEffectiveDeliveryPolicyJSON
		}
	}

	// Count subscriptions for this topic.
	// SubscriptionsDeleted is not tracked in memory — AWS also resets this counter
	// periodically, so we report 0 for consistency with a fresh mock environment.
	confirmed, pending := 0, 0

	for _, sub := range b.subscriptionsByTopic.Get(topicArn) {
		if sub.PendingConfirmation {
			pending++
		} else {
			confirmed++
		}
	}

	attrs["SubscriptionsConfirmed"] = strconv.Itoa(confirmed)
	attrs["SubscriptionsPending"] = strconv.Itoa(pending)
	attrs["SubscriptionsDeleted"] = "0"

	return attrs, nil
}

// isKnownTopicAttribute reports whether name is a settable SNS topic attribute.
// Includes the core attributes plus all delivery-status logging attributes.
func isKnownTopicAttribute(name string) bool {
	switch name {
	// Core topic attributes.
	// DataProtectionPolicy is deliberately excluded: real AWS manages it only
	// through the dedicated GetDataProtectionPolicy/PutDataProtectionPolicy
	// operations, never via Set/GetTopicAttributes (confirmed absent from both
	// operations' documented Attributes list in the SNS API reference).
	case "DeliveryPolicy", "DisplayName", "FifoTopic", "ContentBasedDeduplication",
		"KmsMasterKeyId", "Policy", "TracingConfig", "FifoThroughputScope",
		"ArchivePolicy", "SignatureVersion":
		return true
	// HTTP/HTTPS delivery status logging.
	case "HTTPSuccessFeedbackRoleArn",
		"HTTPSuccessFeedbackSampleRate",
		"HTTPFailureFeedbackRoleArn",
		"HTTPSSuccessFeedbackRoleArn",
		"HTTPSSuccessFeedbackSampleRate",
		"HTTPSFailureFeedbackRoleArn":
		return true
	// SQS delivery status logging.
	case "SQSSuccessFeedbackRoleArn", "SQSSuccessFeedbackSampleRate", "SQSFailureFeedbackRoleArn":
		return true
	// Lambda delivery status logging.
	case "LambdaSuccessFeedbackRoleArn",
		"LambdaSuccessFeedbackSampleRate",
		"LambdaFailureFeedbackRoleArn":
		return true
	// Firehose delivery status logging.
	case "FirehoseSuccessFeedbackRoleArn",
		"FirehoseSuccessFeedbackSampleRate",
		"FirehoseFailureFeedbackRoleArn":
		return true
	// Mobile application (GCM/APNS/etc.) delivery status logging.
	case "ApplicationSuccessFeedbackRoleArn",
		"ApplicationSuccessFeedbackSampleRate",
		"ApplicationFailureFeedbackRoleArn":
		return true
	}

	return false
}

// SetTopicAttributes sets a single attribute on a topic.
func (b *InMemoryBackend) SetTopicAttributes(topicArn, attrName, attrValue string) error {
	b.mu.Lock("SetTopicAttributes")
	defer b.mu.Unlock()

	topic, exists := b.topics.Get(topicArn)
	if !exists {
		return ErrTopicNotFound
	}

	// Reject read-only/computed attributes that cannot be mutated directly.
	if isReadOnlyTopicAttribute(attrName) {
		return fmt.Errorf(
			"%w: Invalid parameter: Attribute %s is a read-only attribute and cannot be set",
			ErrInvalidParameter,
			attrName,
		)
	}

	if !isKnownTopicAttribute(attrName) {
		return fmt.Errorf(
			"%w: Invalid parameter: Attribute name %s is not a known topic attribute",
			ErrInvalidParameter,
			attrName,
		)
	}

	if err := validateTopicAttributeValue(topic, attrName, attrValue); err != nil {
		return err
	}

	// When clearing EffectiveDeliveryPolicy derived from DeliveryPolicy, reset it.
	if attrName == "DeliveryPolicy" {
		if attrValue == "" {
			delete(topic.Attributes, "EffectiveDeliveryPolicy")
		} else {
			topic.Attributes["EffectiveDeliveryPolicy"] = attrValue
		}
	}

	topic.Attributes[attrName] = attrValue

	return nil
}

// validateTopicAttributeValue validates attrValue against the FIFO-only and
// format constraints for attrName (ContentBasedDeduplication/FifoThroughputScope/
// ArchivePolicy require a FIFO topic; SignatureVersion only accepts "1"/"2";
// KmsMasterKeyId must be a plausible key reference). Extracted from
// SetTopicAttributes to keep it under the cyclomatic complexity budget.
func validateTopicAttributeValue(topic *Topic, attrName, attrValue string) error {
	isFifo := topic.Attributes["FifoTopic"] == fifoTopicAttrValue

	switch attrName {
	case "ContentBasedDeduplication":
		if !isFifo {
			return fmt.Errorf(
				"%w: Invalid parameter: ContentBasedDeduplication is only applicable to FIFO topics",
				ErrInvalidParameter,
			)
		}
	case "FifoThroughputScope":
		if !isFifo {
			return fmt.Errorf(
				"%w: Invalid parameter: FifoThroughputScope is only applicable to FIFO topics",
				ErrInvalidParameter,
			)
		}
	case attrArchivePolicy:
		// Message archiving/replay is only valid on FIFO topics. See the
		// matching CreateTopicInRegion check (topics.go) for the AWS doc citation.
		if attrValue != "" && !isFifo {
			return fmt.Errorf(
				"%w: Invalid parameter: ArchivePolicy is only applicable to FIFO topics",
				ErrInvalidParameter,
			)
		}
	case attrSignatureVersion:
		// Only "1" (SHA1withRSA, the AWS default) or "2" (SHA256withRSA) are
		// accepted. See docs.aws.amazon.com/sns/latest/api/API_SetTopicAttributes.html.
		if attrValue != "" && attrValue != signatureVersion1 && attrValue != signatureVersion2 {
			return fmt.Errorf(
				"%w: Invalid parameter: SignatureVersion must be \"1\" or \"2\", got %q",
				ErrInvalidParameter, attrValue,
			)
		}
	case "KmsMasterKeyId":
		if attrValue != "" {
			return validateKmsMasterKeyID(attrValue)
		}
	}

	return nil
}

// isReadOnlyTopicAttribute returns true if the attribute name is a computed/read-only
// topic attribute that must not be set via SetTopicAttributes.
func isReadOnlyTopicAttribute(name string) bool {
	switch name {
	case "Owner", topicArnKey, "SubscriptionsConfirmed", "SubscriptionsPending",
		"SubscriptionsDeleted", "EffectiveDeliveryPolicy":
		return true
	}

	return false
}

// GetDataProtectionPolicy returns the data protection policy JSON for the given topic ARN.
// The policy is stored as the "DataProtectionPolicy" attribute on the topic.
func (b *InMemoryBackend) GetDataProtectionPolicy(resourceArn string) (string, error) {
	b.mu.RLock("GetDataProtectionPolicy")
	defer b.mu.RUnlock()

	topic, exists := b.topics.Get(resourceArn)
	if !exists {
		return "", ErrTopicNotFound
	}

	return topic.Attributes["DataProtectionPolicy"], nil
}

// dataProtectionPolicyMaxLength is the AWS-documented size cap on a
// DataProtectionPolicy document (aws-sdk-go-v2/service/sns@v1.42.4
// api_op_PutDataProtectionPolicy.go DataProtectionPolicy field doc:
// "Length Constraints: Maximum length of 30,720").
const dataProtectionPolicyMaxLength = 30720

// validateDataProtectionPolicy checks policy against the AWS-documented
// wire-level constraints for a DataProtectionPolicy document: valid JSON,
// under the size cap, and a JSON object carrying the required top-level
// keys. It does not validate the policy statement grammar (data
// identifiers, operations, principals) — that is not expressed on the wire
// and is out of scope for this backend.
func validateDataProtectionPolicy(policy string) error {
	if policy == "" {
		return nil
	}

	if len(policy) > dataProtectionPolicyMaxLength {
		return fmt.Errorf(
			"%w: DataProtectionPolicy exceeds the maximum length of %d",
			ErrInvalidParameter, dataProtectionPolicyMaxLength,
		)
	}

	var doc map[string]json.RawMessage
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return fmt.Errorf("%w: DataProtectionPolicy must be a valid JSON object", ErrInvalidParameter)
	}

	// Required top-level keys per
	// docs.aws.amazon.com/sns/latest/dg/sns-message-data-protection-policies.html
	// ("A data protection policy requires the following basic policy information
	// for identification: Name ... Version ... Statement ..."; Description is
	// explicitly marked Optional there).
	for _, key := range [...]string{"Name", "Version", "Statement"} {
		if _, ok := doc[key]; !ok {
			return fmt.Errorf(
				"%w: DataProtectionPolicy is missing required member %s",
				ErrInvalidParameter, key,
			)
		}
	}

	return nil
}

// PutDataProtectionPolicy stores a data protection policy JSON string on the given topic ARN.
func (b *InMemoryBackend) PutDataProtectionPolicy(resourceArn, policy string) error {
	if err := validateDataProtectionPolicy(policy); err != nil {
		return err
	}

	b.mu.Lock("PutDataProtectionPolicy")
	defer b.mu.Unlock()

	topic, exists := b.topics.Get(resourceArn)
	if !exists {
		return ErrTopicNotFound
	}

	topic.Attributes["DataProtectionPolicy"] = policy

	return nil
}

// kmsKeyIDPattern matches a bare KMS key ID (UUID-ish: 8-4-4-4-12 hex, lowercase).
var kmsKeyIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`,
)

// validateKmsMasterKeyID validates that v is a syntactically plausible KMS key
// reference accepted by AWS SNS: a bare alias name, an alias ARN, a key ID, a
// key ARN, or the special "alias/aws/sns" managed key alias. The check rejects
// obviously malformed values; full ARN-resource validation is delegated to KMS.
func validateKmsMasterKeyID(v string) error {
	switch {
	case strings.HasPrefix(v, "alias/"):
		// Alias names must be at least one character after the prefix.
		if len(v) <= len("alias/") {
			return fmt.Errorf("%w: KmsMasterKeyId alias must not be empty", ErrInvalidParameter)
		}

		return nil
	case strings.HasPrefix(v, "arn:"):
		// Accept any well-formed KMS ARN (key or alias).
		parts := strings.Split(v, ":")
		if len(parts) < 6 || parts[2] != "kms" {
			return fmt.Errorf(
				"%w: KmsMasterKeyId is not a valid KMS ARN: %s",
				ErrInvalidParameter,
				v,
			)
		}

		return nil
	case kmsKeyIDPattern.MatchString(v):
		return nil
	default:
		return fmt.Errorf(
			"%w: KmsMasterKeyId is not a valid key ID, ARN, or alias: %s",
			ErrInvalidParameter,
			v,
		)
	}
}

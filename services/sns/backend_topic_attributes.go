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
	case "DeliveryPolicy", "DisplayName", "FifoTopic", "ContentBasedDeduplication",
		"KmsMasterKeyId", "Policy", "TracingConfig", "FifoThroughputScope",
		"ArchivePolicy", "DataProtectionPolicy", "SignatureVersion":
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

	// ContentBasedDeduplication is only valid on FIFO topics.
	if attrName == "ContentBasedDeduplication" &&
		topic.Attributes["FifoTopic"] != fifoTopicAttrValue {
		return fmt.Errorf(
			"%w: Invalid parameter: ContentBasedDeduplication is only applicable to FIFO topics",
			ErrInvalidParameter,
		)
	}

	// FifoThroughputScope is only valid on FIFO topics.
	if attrName == "FifoThroughputScope" && topic.Attributes["FifoTopic"] != fifoTopicAttrValue {
		return fmt.Errorf(
			"%w: Invalid parameter: FifoThroughputScope is only applicable to FIFO topics",
			ErrInvalidParameter,
		)
	}

	// Validate KmsMasterKeyId format (alias name, alias ARN, key ID, or key ARN).
	if attrName == "KmsMasterKeyId" && attrValue != "" {
		if err := validateKmsMasterKeyID(attrValue); err != nil {
			return err
		}
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

// PutDataProtectionPolicy stores a data protection policy JSON string on the given topic ARN.
// The policy must be valid JSON; invalid JSON is rejected with ErrInvalidParameter.
func (b *InMemoryBackend) PutDataProtectionPolicy(resourceArn, policy string) error {
	if policy != "" && !json.Valid([]byte(policy)) {
		return fmt.Errorf("%w: DataProtectionPolicy must be valid JSON", ErrInvalidParameter)
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

package sns

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isValidTopicName returns true if the topic name is non-empty, at most 256 characters,
// consists only of alphanumeric characters, hyphens, and underscores, and if it is a
// FIFO topic (ending in ".fifo") the base name (before the suffix) follows the same rules.
// Source: https://docs.aws.amazon.com/sns/latest/api/API_CreateTopic.html
func isValidTopicName(name string) bool {
	if name == "" || len(name) > maxTopicNameLen {
		return false
	}

	base := name
	if strings.HasSuffix(name, fifoTopicSuffix) {
		base = name[:len(name)-len(fifoTopicSuffix)]
		if base == "" {
			return false
		}
	}

	for _, c := range base {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' &&
			c != '_' {
			return false
		}
	}

	return true
}

// arnRegion extracts the region component from an AWS ARN.
// ARN format: arn:{partition}:{service}:{region}:{account}:{resource}
// Returns "" for malformed ARNs.
func arnRegion(a string) string {
	parts := strings.SplitN(a, ":", arnPartCount)
	if len(parts) < arnPartCount {
		return ""
	}

	return parts[3]
}

// CreateTopic creates a new SNS topic using the backend's default region.
func (b *InMemoryBackend) CreateTopic(name string, attributes map[string]string) (*Topic, error) {
	return b.CreateTopicInRegion(name, b.region, attributes)
}

// CreateTopicInRegion creates a new SNS topic in the specified region.
// If region is empty, the backend's default region is used.
func (b *InMemoryBackend) CreateTopicInRegion(
	name, region string,
	attributes map[string]string,
) (*Topic, error) {
	if !isValidTopicName(name) {
		return nil, fmt.Errorf(
			"%w: Topic name must be 1-256 characters and contain only alphanumeric characters, hyphens, and underscores",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("CreateTopicInRegion")
	defer b.mu.Unlock()

	if region == "" {
		region = b.region
	}

	topicArn := arn.Build("sns", region, b.accountID, name)
	if existing, exists := b.topics.Get(topicArn); exists {
		// AWS SNS CreateTopic is idempotent: calling it with an existing name
		// returns the existing topic ARN rather than an error.
		return existing, nil
	}

	attrs := make(map[string]string, len(attributes)+1)
	maps.Copy(attrs, attributes)
	attrs[topicArnKey] = topicArn
	// Ensure Policy is a valid JSON string with an empty Statement array so
	// Terraform's PolicyHasValidAWSPrincipals JMESPath check returns []any{}.
	if attrs["Policy"] == "" {
		attrs["Policy"] = defaultPolicyJSON
	}

	// Validate FifoTopic attribute consistency with topic name.
	// AWS rejects CreateTopic when FifoTopic=true but name doesn't end in ".fifo".
	if attrs["FifoTopic"] == fifoTopicAttrValue && !strings.HasSuffix(name, fifoTopicSuffix) {
		return nil, fmt.Errorf(
			"%w: Topic name must end with '.fifo' for FIFO topics",
			ErrInvalidParameter,
		)
	}

	// ContentBasedDeduplication is only valid on FIFO topics.
	if attrs["ContentBasedDeduplication"] != "" &&
		attrs["FifoTopic"] != fifoTopicAttrValue &&
		!strings.HasSuffix(name, fifoTopicSuffix) {
		return nil, fmt.Errorf(
			"%w: ContentBasedDeduplication is only applicable to FIFO topics",
			ErrInvalidParameter,
		)
	}

	// FIFO topics: auto-set FifoTopic=true and ContentBasedDeduplication if not already set.
	if strings.HasSuffix(name, fifoTopicSuffix) {
		attrs["FifoTopic"] = fifoTopicAttrValue
		if attrs["ContentBasedDeduplication"] == "" {
			attrs["ContentBasedDeduplication"] = boolFalseStr
		}
	}

	// Validate KmsMasterKeyId format if present (alias name, alias ARN, key ID, or key ARN).
	if v := attrs["KmsMasterKeyId"]; v != "" {
		if err := validateKmsMasterKeyID(v); err != nil {
			return nil, err
		}
	}

	topic := &Topic{
		TopicArn:          topicArn,
		Attributes:        attrs,
		CreationTimestamp: time.Now().UTC(),
	}
	b.topics.Put(topic)

	return topic, nil
}

// DeleteTopic removes a topic by ARN.
func (b *InMemoryBackend) DeleteTopic(topicArn string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if !b.topics.Has(topicArn) {
		return ErrTopicNotFound
	}

	b.topics.Delete(topicArn)

	// Close topic tags to prevent resource leak.
	if t := b.topicTags[topicArn]; t != nil {
		t.Close()
		delete(b.topicTags, topicArn)
	}

	// Remove any orphaned subscriptions for this topic. subscriptionsByTopic.Get
	// returns the live index group directly (see store.Index.Get), so the
	// slice is copied before deleting to avoid mutating it while iterating.
	for _, sub := range append([]*Subscription(nil), b.subscriptionsByTopic.Get(topicArn)...) {
		b.subscriptions.Delete(sub.SubscriptionArn)
	}

	// Drop the message archive (ArchivePolicy replay buffer). Without this the
	// archive both leaks unboundedly for every created-then-deleted topic ARN and,
	// because SNS topic ARNs are deterministic (arn:...:<name>), silently
	// resurfaces a deleted topic's old archived messages to ReplayPolicy
	// subscribers on a newly created topic that reuses the same name.
	delete(b.topicMessageArchive, topicArn)

	return nil
}

// ListTopics returns a page of topics across all regions, ordered by ARN.
// This preserves backward compatibility for callers that don't need region filtering.
func (b *InMemoryBackend) ListTopics(nextToken string) ([]Topic, string, error) {
	return b.ListTopicsInRegion(b.region, nextToken)
}

// ListTopicsInRegion returns a page of topics belonging to region and the next pagination token.
// AWS SNS ListTopics only returns topics in the caller's region.
func (b *InMemoryBackend) ListTopicsInRegion(region, nextToken string) ([]Topic, string, error) {
	b.mu.RLock("ListTopicsInRegion")
	defer b.mu.RUnlock()

	if region == "" {
		region = b.region
	}

	all := b.sortedTopicsInRegion(region)

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	topics, next := paginate(all, offset, pageSize)

	return topics, next, nil
}

// ListAllTopics returns all topics sorted by ARN.
func (b *InMemoryBackend) ListAllTopics() []Topic {
	b.mu.RLock("ListAllTopics")
	defer b.mu.RUnlock()

	return b.sortedTopics()
}

// sortedTopics returns all topics sorted by TopicArn. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedTopics() []Topic {
	topics := make([]Topic, 0, b.topics.Len())
	for _, t := range b.topics.All() {
		topics = append(topics, *t)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].TopicArn < topics[j].TopicArn
	})

	return topics
}

// sortedTopicsInRegion returns topics in the given region sorted by TopicArn.
// Must be called with at least RLock held.
// The region is extracted from the topic ARN (arn:partition:sns:REGION:account:name).
func (b *InMemoryBackend) sortedTopicsInRegion(region string) []Topic {
	topics := make([]Topic, 0, b.topics.Len())
	for _, t := range b.topics.All() {
		if arnRegion(t.TopicArn) == region {
			topics = append(topics, *t)
		}
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].TopicArn < topics[j].TopicArn
	})

	return topics
}

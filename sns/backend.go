package sns

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/events"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrTopicNotFound        = errors.New("NotFound")
	ErrTopicAlreadyExists   = errors.New("TopicAlreadyExists")
	ErrSubscriptionNotFound = errors.New("NotFound")
	ErrInvalidParameter     = errors.New("InvalidParameter")
)

const (
	pageSize = 25
)

// StorageBackend defines the interface for an SNS storage backend.
type StorageBackend interface {
	CreateTopic(name string, attributes map[string]string) (*Topic, error)
	CreateTopicInRegion(name, region string, attributes map[string]string) (*Topic, error)
	DeleteTopic(topicArn string) error
	ListTopics(nextToken string) ([]Topic, string, error)
	GetTopicAttributes(topicArn string) (map[string]string, error)
	SetTopicAttributes(topicArn, attrName, attrValue string) error
	Subscribe(topicArn, protocol, endpoint, filterPolicy string) (*Subscription, error)
	ConfirmSubscription(topicArn, token string) (*Subscription, error)
	Unsubscribe(subscriptionArn string) error
	ListSubscriptions(nextToken string) ([]Subscription, string, error)
	ListSubscriptionsByTopic(topicArn, nextToken string) ([]Subscription, string, error)
	GetSubscriptionAttributes(subscriptionArn string) (map[string]string, error)
	Publish(topicArn, message, subject, messageStructure string, attrs map[string]MessageAttribute) (string, error)
	ListAllTopics() []Topic
	ListAllSubscriptions() []Subscription
	GetTopicTags(arn string) map[string]string
	SetTopicTags(arn string, kv *svcTags.Tags)
	RemoveTopicTags(arn string, keys []string)
}

// InMemoryBackend implements StorageBackend using an in-memory concurrency-safe store.
type InMemoryBackend struct {
	topics        map[string]*Topic
	subscriptions map[string]*Subscription
	topicTags     map[string]*svcTags.Tags
	emitter       events.EventEmitter[*events.SNSPublishedEvent]
	accountID     string
	region        string
	mu            *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		topics:        make(map[string]*Topic),
		subscriptions: make(map[string]*Subscription),
		topicTags:     make(map[string]*svcTags.Tags),
		accountID:     accountID,
		region:        region,
		mu: lockmetrics.New("sns"),
	}
}

// SetPublishEmitter registers an event emitter that fires when a message is published.
// This is used to wire SNS→SQS delivery at startup.
func (b *InMemoryBackend) SetPublishEmitter(emitter events.EventEmitter[*events.SNSPublishedEvent]) {
	b.mu.Lock("SetPublishEmitter")
	defer b.mu.Unlock()

	b.emitter = emitter
}

// CreateTopic creates a new SNS topic using the backend's default region.
func (b *InMemoryBackend) CreateTopic(name string, attributes map[string]string) (*Topic, error) {
	return b.CreateTopicInRegion(name, b.region, attributes)
}

// CreateTopicInRegion creates a new SNS topic in the specified region.
// If region is empty, the backend's default region is used.
func (b *InMemoryBackend) CreateTopicInRegion(name, region string, attributes map[string]string) (*Topic, error) {
	b.mu.Lock("CreateTopicInRegion")
	defer b.mu.Unlock()

	if region == "" {
		region = b.region
	}

	topicArn := arn.Build("sns", region, b.accountID, name)
	if _, exists := b.topics[topicArn]; exists {
		return nil, ErrTopicAlreadyExists
	}

	attrs := make(map[string]string, len(attributes)+1)
	maps.Copy(attrs, attributes)
	attrs["TopicArn"] = topicArn
	// Ensure Policy is a valid JSON string with an empty Statement array so
	// Terraform's PolicyHasValidAWSPrincipals JMESPath check returns []any{}.
	if attrs["Policy"] == "" {
		attrs["Policy"] = `{"Version":"2012-10-17","Statement":[]}`
	}

	topic := &Topic{TopicArn: topicArn, Attributes: attrs}
	b.topics[topicArn] = topic

	return topic, nil
}

// DeleteTopic removes a topic by ARN.
func (b *InMemoryBackend) DeleteTopic(topicArn string) error {
	b.mu.Lock("DeleteTopic")
	defer b.mu.Unlock()

	if _, exists := b.topics[topicArn]; !exists {
		return ErrTopicNotFound
	}

	delete(b.topics, topicArn)

	return nil
}

// ListTopics returns a page of topics and the next pagination token.
func (b *InMemoryBackend) ListTopics(nextToken string) ([]Topic, string, error) {
	b.mu.RLock("ListTopics")
	defer b.mu.RUnlock()

	all := b.sortedTopics()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	topics, next := paginate(all, offset, pageSize)

	return topics, next, nil
}

// GetTopicAttributes returns the attributes of a topic.
func (b *InMemoryBackend) GetTopicAttributes(topicArn string) (map[string]string, error) {
	b.mu.RLock("GetTopicAttributes")
	defer b.mu.RUnlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return nil, ErrTopicNotFound
	}

	attrs := make(map[string]string, len(topic.Attributes))
	maps.Copy(attrs, topic.Attributes)

	// Ensure Policy is always a valid JSON string with an empty Statement array so
	// Terraform's PolicyHasValidAWSPrincipals JMESPath check returns []any{}.
	if attrs["Policy"] == "" {
		attrs["Policy"] = `{"Version":"2012-10-17","Statement":[]}`
	}

	return attrs, nil
}

// SetTopicAttributes sets a single attribute on a topic.
func (b *InMemoryBackend) SetTopicAttributes(topicArn, attrName, attrValue string) error {
	b.mu.Lock("SetTopicAttributes")
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return ErrTopicNotFound
	}

	topic.Attributes[attrName] = attrValue

	return nil
}

// Subscribe creates a new subscription for the given topic, protocol, and endpoint.
func (b *InMemoryBackend) Subscribe(topicArn, protocol, endpoint, filterPolicy string) (*Subscription, error) {
	b.mu.Lock("Subscribe")
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return nil, ErrTopicNotFound
	}

	parts := strings.Split(topic.TopicArn, ":")
	topicName := parts[len(parts)-1]

	subArn := arn.Build("sns", b.region, b.accountID, topicName+":"+uuid.New().String())
	pending := protocol == "http" || protocol == "https"
	sub := &Subscription{
		SubscriptionArn:     subArn,
		TopicArn:            topicArn,
		Protocol:            protocol,
		Endpoint:            endpoint,
		Owner:               b.accountID,
		FilterPolicy:        filterPolicy,
		PendingConfirmation: pending,
	}

	b.subscriptions[subArn] = sub

	return sub, nil
}

// Unsubscribe removes a subscription by ARN.
func (b *InMemoryBackend) Unsubscribe(subscriptionArn string) error {
	b.mu.Lock("Unsubscribe")
	defer b.mu.Unlock()

	if _, exists := b.subscriptions[subscriptionArn]; !exists {
		return ErrSubscriptionNotFound
	}

	delete(b.subscriptions, subscriptionArn)

	return nil
}

// ConfirmSubscription "confirms" a pending subscription.
// In the mock, any non-empty token is accepted.
// The subscription must belong to the given topicArn; if found and pending,
// PendingConfirmation is cleared and the subscription ARN is returned.
func (b *InMemoryBackend) ConfirmSubscription(topicArn, token string) (*Subscription, error) {
	if token == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("ConfirmSubscription")
	defer b.mu.Unlock()

	for _, sub := range b.subscriptions {
		if sub.TopicArn == topicArn && sub.PendingConfirmation {
			sub.PendingConfirmation = false

			return sub, nil
		}
	}

	return nil, ErrSubscriptionNotFound
}

// GetSubscriptionAttributes returns the attributes of a subscription.
func (b *InMemoryBackend) GetSubscriptionAttributes(subscriptionArn string) (map[string]string, error) {
	b.mu.RLock("GetSubscriptionAttributes")
	defer b.mu.RUnlock()

	sub, exists := b.subscriptions[subscriptionArn]
	if !exists {
		return nil, ErrSubscriptionNotFound
	}

	return map[string]string{
		"SubscriptionArn":     sub.SubscriptionArn,
		"TopicArn":            sub.TopicArn,
		"Protocol":            sub.Protocol,
		"Endpoint":            sub.Endpoint,
		"Owner":               sub.Owner,
		"PendingConfirmation": "false",
	}, nil
}

// ListSubscriptions returns a page of subscriptions and the next pagination token.
func (b *InMemoryBackend) ListSubscriptions(nextToken string) ([]Subscription, string, error) {
	b.mu.RLock("ListSubscriptions")
	defer b.mu.RUnlock()

	all := b.sortedSubscriptions()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	subs, next := paginate(all, offset, pageSize)

	return subs, next, nil
}

// ListSubscriptionsByTopic returns a page of subscriptions for a topic and the next pagination token.
func (b *InMemoryBackend) ListSubscriptionsByTopic(topicArn, nextToken string) ([]Subscription, string, error) {
	b.mu.RLock("ListSubscriptionsByTopic")
	defer b.mu.RUnlock()

	if _, exists := b.topics[topicArn]; !exists {
		return nil, "", ErrTopicNotFound
	}

	all := b.sortedSubscriptions()
	filtered := make([]Subscription, 0, len(all))

	for _, s := range all {
		if s.TopicArn == topicArn {
			filtered = append(filtered, s)
		}
	}

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	subs, next := paginate(filtered, offset, pageSize)

	return subs, next, nil
}

// Publish publishes a message to a topic and returns the message ID.
// HTTP/HTTPS subscriptions receive a synchronous best-effort delivery.
// All subscriptions are also broadcast via the publish emitter (e.g. to SQS).
func (b *InMemoryBackend) Publish(
	topicArn, message, subject, messageStructure string, attrs map[string]MessageAttribute,
) (string, error) {
	b.mu.RLock("Publish")
	defer b.mu.RUnlock()

	if _, exists := b.topics[topicArn]; !exists {
		return "", ErrTopicNotFound
	}

	messageID := uuid.New().String()

	// Pre-parse the per-protocol message map if MessageStructure is "json".
	var perProtocolMessages map[string]string
	if messageStructure == "json" {
		if err := json.Unmarshal([]byte(message), &perProtocolMessages); err != nil {
			perProtocolMessages = nil
		}
	}

	// resolveMessage returns the appropriate message body for a given protocol.
	resolveMessage := func(protocol string) string {
		if perProtocolMessages == nil {
			return message
		}

		if msg, ok := perProtocolMessages[protocol]; ok {
			return msg
		}

		if msg, ok := perProtocolMessages["default"]; ok {
			return msg
		}

		return message
	}

	// Build subscription snapshot and deliver to HTTP/HTTPS endpoints.
	subs := make([]events.SNSSubscriptionSnapshot, 0)

	for _, sub := range b.subscriptions {
		if sub.TopicArn != topicArn {
			continue
		}

		if !matchesFilterPolicy(sub.FilterPolicy, attrs) {
			continue
		}

		msg := resolveMessage(sub.Protocol)

		switch sub.Protocol {
		case "http", "https":
			deliverHTTP(sub.Endpoint, msg)
		}

		subs = append(subs, events.SNSSubscriptionSnapshot{
			SubscriptionARN: sub.SubscriptionArn,
			Protocol:        sub.Protocol,
			Endpoint:        sub.Endpoint,
			FilterPolicy:    sub.FilterPolicy,
		})
	}

	// Emit event for other services (e.g. SQS) to react to.
	if b.emitter != nil {
		attrSnaps := make(map[string]events.SNSMessageAttributeSnapshot, len(attrs))
		for k, v := range attrs {
			attrSnaps[k] = events.SNSMessageAttributeSnapshot{
				DataType:    v.DataType,
				StringValue: v.StringValue,
			}
		}

		_ = b.emitter.Emit(context.Background(), &events.SNSPublishedEvent{
			TopicARN:      topicArn,
			MessageID:     messageID,
			Message:       message,
			Subject:       subject,
			Subscriptions: subs,
			Attributes:    attrSnaps,
		})
	}

	return messageID, nil
}

// matchesFilterPolicy returns true if the message attributes satisfy all conditions in the filter policy.
// If filterPolicy is empty or invalid JSON, it returns true (no filtering).
func matchesFilterPolicy(filterPolicy string, attrs map[string]MessageAttribute) bool {
	if filterPolicy == "" {
		return true
	}

	var policy map[string]json.RawMessage
	if err := json.Unmarshal([]byte(filterPolicy), &policy); err != nil {
		return true
	}

	for key, rawConditions := range policy {
		attr, ok := attrs[key]
		if !ok {
			return false
		}

		var conditions []json.RawMessage
		if err := json.Unmarshal(rawConditions, &conditions); err != nil {
			return true
		}

		if !matchesConditions(attr.StringValue, conditions) {
			return false
		}
	}

	return true
}

func matchObjectCondition(value string, obj map[string]string) bool {
	if prefix, ok := obj["prefix"]; ok {
		return strings.HasPrefix(value, prefix)
	}

	if excluded, ok := obj["anything-but"]; ok {
		return value != excluded
	}

	return false
}

func matchCondition(value string, raw json.RawMessage) bool {
	var obj map[string]string
	if err := json.Unmarshal(raw, &obj); err == nil {
		return matchObjectCondition(value, obj)
	}

	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return value == s
	}

	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		return value == n.String()
	}

	return false
}

// matchesConditions returns true if value satisfies at least one condition in the list.
func matchesConditions(
	value string,
	conditions []json.RawMessage,
) bool {
	for _, raw := range conditions {
		if matchCondition(value, raw) {
			return true
		}
	}

	return false
}

// ListAllTopics returns all topics sorted by ARN.
func (b *InMemoryBackend) ListAllTopics() []Topic {
	b.mu.RLock("ListAllTopics")
	defer b.mu.RUnlock()

	return b.sortedTopics()
}

// ListAllSubscriptions returns all subscriptions sorted by ARN.
func (b *InMemoryBackend) ListAllSubscriptions() []Subscription {
	b.mu.RLock("ListAllSubscriptions")
	defer b.mu.RUnlock()

	return b.sortedSubscriptions()
}

// sortedTopics returns topics sorted by TopicArn. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedTopics() []Topic {
	topics := make([]Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, *t)
	}

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].TopicArn < topics[j].TopicArn
	})

	return topics
}

// sortedSubscriptions returns subscriptions sorted by SubscriptionArn. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedSubscriptions() []Subscription {
	subs := make([]Subscription, 0, len(b.subscriptions))
	for _, s := range b.subscriptions {
		subs = append(subs, *s)
	}

	sort.Slice(subs, func(i, j int) bool {
		return subs[i].SubscriptionArn < subs[j].SubscriptionArn
	})

	return subs
}

// deliverHTTP sends a best-effort HTTP POST with the message body to the endpoint.
// Errors are intentionally ignored: delivery is fire-and-forget for HTTP/HTTPS subscriptions.
func deliverHTTP(endpoint, body string) {
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		endpoint,
		strings.NewReader(body),
	)
	if err != nil {
		return
	}

	// HTTP client used for SNS HTTP endpoint delivery, not internet requests
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: intentional HTTP delivery to SNS subscribers
	if err != nil {
		return
	}

	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
}

// decodeToken decodes a base64 pagination token into an integer offset.
// An empty token is treated as offset 0.
func decodeToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return 0, err
	}

	offset, err := strconv.Atoi(string(decoded))
	if err != nil {
		return 0, err
	}

	return offset, nil
}

// encodeToken encodes an integer offset as a base64 pagination token.
func encodeToken(offset int) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

// paginate returns a page of items and the next token, or an empty token when exhausted.
func paginate[T any](items []T, offset, size int) ([]T, string) {
	if offset >= len(items) {
		return []T{}, ""
	}

	end := offset + size
	nextToken := ""

	if end < len(items) {
		nextToken = encodeToken(end)
	} else {
		end = len(items)
	}

	return items[offset:end], nextToken
}

// GetTopicTags returns tags for the given topic ARN.
func (b *InMemoryBackend) GetTopicTags(arn string) map[string]string {
	b.mu.RLock("GetTopicTags")
	defer b.mu.RUnlock()
	if b.topicTags[arn] == nil {
		return map[string]string{}
	}

	return b.topicTags[arn].Clone()
}

// SetTopicTags stores tags for the given topic ARN.
func (b *InMemoryBackend) SetTopicTags(arn string, kv *svcTags.Tags) {
	b.mu.Lock("SetTopicTags")
	defer b.mu.Unlock()
	if kv == nil {
		return
	}
	if b.topicTags[arn] == nil {
		b.topicTags[arn] = svcTags.New("sns." + arn + ".tags")
	}
	b.topicTags[arn].Merge(kv.Clone())
}

// RemoveTopicTags removes specified tag keys for the given topic ARN.
func (b *InMemoryBackend) RemoveTopicTags(arn string, keys []string) {
	b.mu.Lock("RemoveTopicTags")
	defer b.mu.Unlock()
	if b.topicTags[arn] != nil {
		b.topicTags[arn].DeleteKeys(keys)
	}
}

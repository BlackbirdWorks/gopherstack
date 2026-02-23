package sns

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrTopicNotFound        = errors.New("NotFound")
	ErrTopicAlreadyExists   = errors.New("TopicAlreadyExists")
	ErrSubscriptionNotFound = errors.New("NotFound")
	ErrInvalidParameter     = errors.New("InvalidParameter")
)

const (
	defaultAccountID = "000000000000"
	defaultRegion    = "us-east-1"
	pageSize         = 25
)

// StorageBackend defines the interface for an SNS storage backend.
type StorageBackend interface {
	CreateTopic(name string, attributes map[string]string) (*Topic, error)
	DeleteTopic(topicArn string) error
	ListTopics(nextToken string) ([]Topic, string, error)
	GetTopicAttributes(topicArn string) (map[string]string, error)
	SetTopicAttributes(topicArn, attrName, attrValue string) error
	Subscribe(topicArn, protocol, endpoint, filterPolicy string) (*Subscription, error)
	Unsubscribe(subscriptionArn string) error
	ListSubscriptions(nextToken string) ([]Subscription, string, error)
	ListSubscriptionsByTopic(topicArn, nextToken string) ([]Subscription, string, error)
	Publish(topicArn, message, subject string, attrs map[string]MessageAttribute) (string, error)
	ListAllTopics() []Topic
	ListAllSubscriptions() []Subscription
}

// InMemoryBackend implements StorageBackend using an in-memory concurrency-safe store.
type InMemoryBackend struct {
	topics        map[string]*Topic
	subscriptions map[string]*Subscription
	accountID     string
	region        string
	mu            sync.RWMutex
}

// arnPrefix returns the SNS ARN prefix for this backend's account and region.
func (b *InMemoryBackend) arnPrefix() string {
	return "arn:aws:sns:" + b.region + ":" + b.accountID + ":"
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(defaultAccountID, defaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		topics:        make(map[string]*Topic),
		subscriptions: make(map[string]*Subscription),
		accountID:     accountID,
		region:        region,
	}
}

// CreateTopic creates a new SNS topic with the given name and attributes.
func (b *InMemoryBackend) CreateTopic(name string, attributes map[string]string) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	topicArn := b.arnPrefix() + name
	if _, exists := b.topics[topicArn]; exists {
		return nil, ErrTopicAlreadyExists
	}

	attrs := make(map[string]string, len(attributes)+1)
	maps.Copy(attrs, attributes)
	attrs["TopicArn"] = topicArn

	topic := &Topic{TopicArn: topicArn, Attributes: attrs}
	b.topics[topicArn] = topic

	return topic, nil
}

// DeleteTopic removes a topic by ARN.
func (b *InMemoryBackend) DeleteTopic(topicArn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[topicArn]; !exists {
		return ErrTopicNotFound
	}

	delete(b.topics, topicArn)

	return nil
}

// ListTopics returns a page of topics and the next pagination token.
func (b *InMemoryBackend) ListTopics(nextToken string) ([]Topic, string, error) {
	b.mu.RLock()
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
	b.mu.RLock()
	defer b.mu.RUnlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return nil, ErrTopicNotFound
	}

	attrs := make(map[string]string, len(topic.Attributes))
	maps.Copy(attrs, topic.Attributes)

	return attrs, nil
}

// SetTopicAttributes sets a single attribute on a topic.
func (b *InMemoryBackend) SetTopicAttributes(topicArn, attrName, attrValue string) error {
	b.mu.Lock()
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
	b.mu.Lock()
	defer b.mu.Unlock()

	topic, exists := b.topics[topicArn]
	if !exists {
		return nil, ErrTopicNotFound
	}

	parts := strings.Split(topic.TopicArn, ":")
	topicName := parts[len(parts)-1]

	subArn := fmt.Sprintf("%s%s:%s", b.arnPrefix(), topicName, uuid.New().String())
	sub := &Subscription{
		SubscriptionArn: subArn,
		TopicArn:        topicArn,
		Protocol:        protocol,
		Endpoint:        endpoint,
		Owner:           b.accountID,
		FilterPolicy:    filterPolicy,
	}

	b.subscriptions[subArn] = sub

	return sub, nil
}

// Unsubscribe removes a subscription by ARN.
func (b *InMemoryBackend) Unsubscribe(subscriptionArn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.subscriptions[subscriptionArn]; !exists {
		return ErrSubscriptionNotFound
	}

	delete(b.subscriptions, subscriptionArn)

	return nil
}

// ListSubscriptions returns a page of subscriptions and the next pagination token.
func (b *InMemoryBackend) ListSubscriptions(nextToken string) ([]Subscription, string, error) {
	b.mu.RLock()
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
	b.mu.RLock()
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
// The subject and attrs parameters are accepted for interface compatibility but not used in delivery.
func (b *InMemoryBackend) Publish(
	topicArn, message, _ string, _ map[string]MessageAttribute,
) (string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.topics[topicArn]; !exists {
		return "", ErrTopicNotFound
	}

	messageID := uuid.New().String()

	for _, sub := range b.subscriptions {
		if sub.TopicArn != topicArn {
			continue
		}

		switch sub.Protocol {
		case "http", "https":
			deliverHTTP(sub.Endpoint, message)
		default:
			// SQS, Lambda, email, etc. — delivery not implemented.
		}
	}

	return messageID, nil
}

// ListAllTopics returns all topics sorted by ARN.
func (b *InMemoryBackend) ListAllTopics() []Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.sortedTopics()
}

// ListAllSubscriptions returns all subscriptions sorted by ARN.
func (b *InMemoryBackend) ListAllSubscriptions() []Subscription {
	b.mu.RLock()
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
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // HTTP endpoint delivery
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

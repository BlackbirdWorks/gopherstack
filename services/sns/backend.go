package sns

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/events"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrTopicNotFound                    = errors.New("NotFound")
	ErrTopicAlreadyExists               = errors.New("TopicAlreadyExists")
	ErrSubscriptionNotFound             = errors.New("NotFound")
	ErrPlatformApplicationNotFound      = errors.New("NotFound")
	ErrPlatformApplicationAlreadyExists = errors.New("PlatformApplicationAlreadyExists")
	ErrEndpointNotFound                 = errors.New("NotFound")
	ErrInvalidParameter                 = errors.New("InvalidParameter")
)

const (
	pageSize = 25

	attrFilterPolicy       = "FilterPolicy"
	attrRawMessageDelivery = "RawMessageDelivery"
	attrRedrivePolicy      = "RedrivePolicy"

	// platformARNResourceParts is the expected number of slash-delimited parts
	// in a platform application ARN resource component: "app/{Platform}/{AppName}".
	platformARNResourceParts = 3

	// endpointExtraAttrs is the number of extra attributes added to a new endpoint
	// beyond what the caller provides: Token and Enabled.
	endpointExtraAttrs = 2

	// snsHTTPTimeout is the timeout applied to SNS HTTP/HTTPS endpoint deliveries.
	snsHTTPTimeout = 5 * time.Second

	// snsMaxConcurrentDeliveries caps the number of HTTP/HTTPS subscription
	// deliveries that may run concurrently for a single Publish call.
	snsMaxConcurrentDeliveries = 8
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
	SetSubscriptionAttributes(subscriptionArn, attrName, attrValue string) error
	Publish(topicArn, message, subject, messageStructure string, attrs map[string]MessageAttribute) (string, error)
	ListAllTopics() []Topic
	ListAllSubscriptions() []Subscription
	ListAllPlatformApplications() []PlatformApplication
	GetTopicTags(arn string) map[string]string
	SetTopicTags(arn string, kv *svcTags.Tags)
	RemoveTopicTags(arn string, keys []string)
	// Platform application operations.
	CreatePlatformApplication(name, platform string, attributes map[string]string) (*PlatformApplication, error)
	GetPlatformApplicationAttributes(platformApplicationArn string) (map[string]string, error)
	SetPlatformApplicationAttributes(platformApplicationArn string, attributes map[string]string) error
	ListPlatformApplications(nextToken string) ([]PlatformApplication, string, error)
	DeletePlatformApplication(platformApplicationArn string) error
	// Platform endpoint operations.
	CreatePlatformEndpoint(
		platformApplicationArn, token string,
		attributes map[string]string,
	) (*PlatformEndpoint, error)
	GetEndpointAttributes(endpointArn string) (map[string]string, error)
	SetEndpointAttributes(endpointArn string, attributes map[string]string) error
	ListEndpointsByPlatformApplication(platformApplicationArn, nextToken string) ([]PlatformEndpoint, string, error)
	DeleteEndpoint(endpointArn string) error
}

// InMemoryBackend implements StorageBackend using an in-memory concurrency-safe store.
type InMemoryBackend struct {
	emitter              events.EventEmitter[*events.SNSPublishedEvent]
	httpClient           *http.Client
	topics               map[string]*Topic
	subscriptions        map[string]*Subscription
	topicTags            map[string]*svcTags.Tags
	platformApplications map[string]*PlatformApplication
	platformEndpoints    map[string]*PlatformEndpoint
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
}

// NewInMemoryBackend creates a new empty InMemoryBackend with default account/region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given account ID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		topics:               make(map[string]*Topic),
		subscriptions:        make(map[string]*Subscription),
		topicTags:            make(map[string]*svcTags.Tags),
		platformApplications: make(map[string]*PlatformApplication),
		platformEndpoints:    make(map[string]*PlatformEndpoint),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("sns"),
		httpClient:           &http.Client{Timeout: snsHTTPTimeout},
	}
}

// SetHTTPDeliveryClient configures the HTTP client used for HTTP/HTTPS subscription delivery.
// If not set, a dedicated client with a 5-second timeout is used.
func (b *InMemoryBackend) SetHTTPDeliveryClient(c *http.Client) {
	b.mu.Lock("SetHTTPDeliveryClient")
	defer b.mu.Unlock()

	b.httpClient = c
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

	attrs := map[string]string{
		"SubscriptionArn":      sub.SubscriptionArn,
		"TopicArn":             sub.TopicArn,
		"Protocol":             sub.Protocol,
		"Endpoint":             sub.Endpoint,
		"Owner":                sub.Owner,
		"PendingConfirmation":  strconv.FormatBool(sub.PendingConfirmation),
		attrRawMessageDelivery: strconv.FormatBool(sub.RawMessageDelivery),
	}

	if sub.FilterPolicy != "" {
		attrs[attrFilterPolicy] = sub.FilterPolicy
	}

	if sub.RedrivePolicy != "" {
		attrs[attrRedrivePolicy] = sub.RedrivePolicy
	}

	return attrs, nil
}

// SetSubscriptionAttributes sets a single attribute on a subscription.
func (b *InMemoryBackend) SetSubscriptionAttributes(subscriptionArn, attrName, attrValue string) error {
	b.mu.Lock("SetSubscriptionAttributes")
	defer b.mu.Unlock()

	sub, exists := b.subscriptions[subscriptionArn]
	if !exists {
		return ErrSubscriptionNotFound
	}

	switch attrName {
	case attrRawMessageDelivery:
		sub.RawMessageDelivery = strings.EqualFold(attrValue, "true")
	case attrFilterPolicy:
		sub.FilterPolicy = attrValue
	case attrRedrivePolicy:
		sub.RedrivePolicy = attrValue
	default:
		return ErrInvalidParameter
	}

	return nil
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

// httpDelivery holds the endpoint and message body for an HTTP/HTTPS delivery.
type httpDelivery struct {
	endpoint string
	body     string
}

// publishTargets holds the subscription snapshots and HTTP deliveries collected for a publish call.
type publishTargets struct {
	subs           []events.SNSSubscriptionSnapshot
	httpDeliveries []httpDelivery
}

// buildMessageResolver returns a function that picks the correct message body for a given protocol,
// respecting MessageStructure "json" per-protocol map when provided.
func buildMessageResolver(defaultMsg string, perProtocol map[string]string) func(string) string {
	return func(protocol string) string {
		if perProtocol == nil {
			return defaultMsg
		}

		if msg, ok := perProtocol[protocol]; ok {
			return msg
		}

		if msg, ok := perProtocol["default"]; ok {
			return msg
		}

		return defaultMsg
	}
}

// collectPublishTargets scans b.subscriptions for a given topicArn and returns
// subscription snapshots and HTTP/HTTPS deliveries to dispatch.
// Must be called with at least RLock held.
func (b *InMemoryBackend) collectPublishTargets(
	topicArn string,
	resolveMsg func(string) string,
	attrs map[string]MessageAttribute,
) publishTargets {
	var out publishTargets

	for _, sub := range b.subscriptions {
		if sub.TopicArn != topicArn {
			continue
		}

		if !matchesFilterPolicy(sub.FilterPolicy, attrs) {
			continue
		}

		msg := resolveMsg(sub.Protocol)

		if sub.Protocol == "http" || sub.Protocol == "https" {
			out.httpDeliveries = append(out.httpDeliveries, httpDelivery{endpoint: sub.Endpoint, body: msg})
		}

		out.subs = append(out.subs, events.SNSSubscriptionSnapshot{
			SubscriptionARN:    sub.SubscriptionArn,
			Protocol:           sub.Protocol,
			Endpoint:           sub.Endpoint,
			FilterPolicy:       sub.FilterPolicy,
			RawMessageDelivery: sub.RawMessageDelivery,
			RedrivePolicy:      sub.RedrivePolicy,
		})
	}

	return out
}

// Publish publishes a message to a topic and returns the message ID.
// HTTP/HTTPS subscriptions receive an asynchronous best-effort delivery after
// the read lock is released, avoiding lock starvation from slow endpoints.
// All subscriptions are also broadcast via the publish emitter (e.g. to SQS).
func (b *InMemoryBackend) Publish(
	topicArn, message, subject, messageStructure string, attrs map[string]MessageAttribute,
) (string, error) {
	b.mu.RLock("Publish")

	if _, exists := b.topics[topicArn]; !exists {
		b.mu.RUnlock()

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

	// resolveMsg returns the appropriate message body for a given protocol.
	resolveMsg := buildMessageResolver(message, perProtocolMessages)

	// Build subscription snapshot and collect HTTP deliveries — all under RLock.
	targets := b.collectPublishTargets(topicArn, resolveMsg, attrs)

	// Capture emitter and httpClient under the read lock to avoid data races
	// with concurrent SetPublishEmitter / SetHTTPDeliveryClient calls.
	emitter := b.emitter
	client := b.httpClient

	// Release the read lock before performing any network I/O so that slow or
	// unresponsive HTTP endpoints do not block write operations on the backend.
	b.mu.RUnlock()

	// Deliver to HTTP/HTTPS endpoints asynchronously with bounded concurrency.
	// The semaphore channel limits the number of in-flight goroutines to
	// snsMaxConcurrentDeliveries so that a large subscription list cannot
	// exhaust OS resources.
	if len(targets.httpDeliveries) > 0 {
		sem := make(chan struct{}, snsMaxConcurrentDeliveries)

		for _, d := range targets.httpDeliveries {
			sem <- struct{}{}

			go func() {
				defer func() { <-sem }()
				deliverHTTP(d.endpoint, d.body, client)
			}()
		}
	}

	// Emit event for other services (e.g. SQS) to react to.
	if emitter != nil {
		attrSnaps := make(map[string]events.SNSMessageAttributeSnapshot, len(attrs))
		for k, v := range attrs {
			attrSnaps[k] = events.SNSMessageAttributeSnapshot{
				DataType:    v.DataType,
				StringValue: v.StringValue,
			}
		}

		_ = emitter.Emit(context.Background(), &events.SNSPublishedEvent{
			TopicARN:      topicArn,
			MessageID:     messageID,
			Message:       message,
			Subject:       subject,
			Subscriptions: targets.subs,
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

// ListAllPlatformApplications returns all platform applications sorted by ARN.
func (b *InMemoryBackend) ListAllPlatformApplications() []PlatformApplication {
	b.mu.RLock("ListAllPlatformApplications")
	defer b.mu.RUnlock()

	apps := make([]PlatformApplication, 0, len(b.platformApplications))
	for _, app := range b.platformApplications {
		apps = append(apps, *app)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].PlatformApplicationArn < apps[j].PlatformApplicationArn
	})

	return apps
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

// deliverHTTP sends a best-effort HTTP POST with the message body to the endpoint
// using the provided client. Errors are intentionally ignored: delivery is
// fire-and-forget for HTTP/HTTPS subscriptions.
func deliverHTTP(endpoint, body string, client *http.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), snsHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		endpoint,
		strings.NewReader(body),
	)
	if err != nil {
		return
	}

	resp, err := client.Do(req)
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

// TaggedTopicInfo contains a topic's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedTopicInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedTopics returns a snapshot of all SNS topics with their tags.
// Intended for use by the Resource Groups Tagging API provider.
func (b *InMemoryBackend) TaggedTopics() []TaggedTopicInfo {
	b.mu.RLock("TaggedTopics")
	defer b.mu.RUnlock()

	result := make([]TaggedTopicInfo, 0, len(b.topics))

	for topicARN := range b.topics {
		var tagMap map[string]string
		if b.topicTags[topicARN] != nil {
			tagMap = b.topicTags[topicARN].Clone()
		}

		result = append(result, TaggedTopicInfo{ARN: topicARN, Tags: tagMap})
	}

	return result
}

// TagTopicByARN applies tags to the SNS topic identified by its ARN.
func (b *InMemoryBackend) TagTopicByARN(topicARN string, newTags map[string]string) error {
	b.mu.Lock("TagTopicByARN")
	defer b.mu.Unlock()

	if _, ok := b.topics[topicARN]; !ok {
		return fmt.Errorf("%w: topic %s", ErrTopicNotFound, topicARN)
	}

	if b.topicTags[topicARN] == nil {
		b.topicTags[topicARN] = svcTags.New("sns." + topicARN + ".tags")
	}

	b.topicTags[topicARN].Merge(newTags)

	return nil
}

// UntagTopicByARN removes the specified tag keys from the SNS topic identified by its ARN.
func (b *InMemoryBackend) UntagTopicByARN(topicARN string, tagKeys []string) error {
	b.mu.Lock("UntagTopicByARN")
	defer b.mu.Unlock()

	if _, ok := b.topics[topicARN]; !ok {
		return fmt.Errorf("%w: topic %s", ErrTopicNotFound, topicARN)
	}

	if b.topicTags[topicARN] != nil {
		b.topicTags[topicARN].DeleteKeys(tagKeys)
	}

	return nil
}

// CreatePlatformApplication creates a new SNS platform application (e.g. GCM, APNS).
func (b *InMemoryBackend) CreatePlatformApplication(
	name, platform string,
	attributes map[string]string,
) (*PlatformApplication, error) {
	if strings.ContainsAny(name, "/") || strings.ContainsAny(platform, "/") {
		return nil, fmt.Errorf("%w: Name and Platform must not contain '/'", ErrInvalidParameter)
	}

	b.mu.Lock("CreatePlatformApplication")
	defer b.mu.Unlock()

	appArn := arn.Build("sns", b.region, b.accountID, "app/"+platform+"/"+name)

	if _, exists := b.platformApplications[appArn]; exists {
		return nil, ErrPlatformApplicationAlreadyExists
	}

	attrs := make(map[string]string, len(attributes))
	maps.Copy(attrs, attributes)

	app := &PlatformApplication{
		PlatformApplicationArn: appArn,
		Attributes:             attrs,
	}
	b.platformApplications[appArn] = app

	return app, nil
}

// GetPlatformApplicationAttributes returns the attributes of a platform application.
func (b *InMemoryBackend) GetPlatformApplicationAttributes(platformApplicationArn string) (map[string]string, error) {
	b.mu.RLock("GetPlatformApplicationAttributes")
	defer b.mu.RUnlock()

	app, exists := b.platformApplications[platformApplicationArn]
	if !exists {
		return nil, ErrPlatformApplicationNotFound
	}

	attrs := make(map[string]string, len(app.Attributes))
	maps.Copy(attrs, app.Attributes)

	return attrs, nil
}

// SetPlatformApplicationAttributes updates attributes on a platform application.
func (b *InMemoryBackend) SetPlatformApplicationAttributes(
	platformApplicationArn string,
	attributes map[string]string,
) error {
	b.mu.Lock("SetPlatformApplicationAttributes")
	defer b.mu.Unlock()

	app, exists := b.platformApplications[platformApplicationArn]
	if !exists {
		return ErrPlatformApplicationNotFound
	}

	maps.Copy(app.Attributes, attributes)

	return nil
}

// ListPlatformApplications returns a page of platform applications and the next pagination token.
func (b *InMemoryBackend) ListPlatformApplications(nextToken string) ([]PlatformApplication, string, error) {
	b.mu.RLock("ListPlatformApplications")
	defer b.mu.RUnlock()

	all := b.sortedPlatformApplications()

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	apps, next := paginate(all, offset, pageSize)

	return apps, next, nil
}

// DeletePlatformApplication removes a platform application and its endpoints by ARN.
func (b *InMemoryBackend) DeletePlatformApplication(platformApplicationArn string) error {
	b.mu.Lock("DeletePlatformApplication")
	defer b.mu.Unlock()

	if _, exists := b.platformApplications[platformApplicationArn]; !exists {
		return ErrPlatformApplicationNotFound
	}

	delete(b.platformApplications, platformApplicationArn)

	// Remove all endpoints associated with this platform application.
	for endpointArn, ep := range b.platformEndpoints {
		if ep.PlatformApplicationArn == platformApplicationArn {
			delete(b.platformEndpoints, endpointArn)
		}
	}

	return nil
}

// CreatePlatformEndpoint registers a device token as an endpoint for a platform application.
func (b *InMemoryBackend) CreatePlatformEndpoint(
	platformApplicationArn, token string,
	attributes map[string]string,
) (*PlatformEndpoint, error) {
	b.mu.Lock("CreatePlatformEndpoint")
	defer b.mu.Unlock()

	app, exists := b.platformApplications[platformApplicationArn]
	if !exists {
		return nil, ErrPlatformApplicationNotFound
	}

	// Derive the platform and app name from the platform application ARN.
	// ARN format: arn:aws:sns:{region}:{accountID}:app/{Platform}/{AppName}
	parts := strings.Split(app.PlatformApplicationArn, ":")
	resource := parts[len(parts)-1] // "app/{Platform}/{AppName}"
	resourceParts := strings.SplitN(resource, "/", platformARNResourceParts)

	if len(resourceParts) != platformARNResourceParts {
		return nil, fmt.Errorf(
			"%w: malformed platform application ARN: %s",
			ErrInvalidParameter,
			platformApplicationArn,
		)
	}

	platform := resourceParts[1]
	appName := resourceParts[2]

	endpointArn := arn.Build("sns", b.region, b.accountID,
		"endpoint/"+platform+"/"+appName+"/"+uuid.New().String())

	// Allocate with room for Token and Enabled (endpointExtraAttrs) beyond caller-supplied attrs.
	attrs := make(map[string]string, len(attributes)+endpointExtraAttrs)
	maps.Copy(attrs, attributes)
	attrs["Token"] = token
	attrs["Enabled"] = "true"

	ep := &PlatformEndpoint{
		EndpointArn:            endpointArn,
		PlatformApplicationArn: platformApplicationArn,
		Attributes:             attrs,
	}
	b.platformEndpoints[endpointArn] = ep

	return ep, nil
}

// GetEndpointAttributes returns the attributes of a platform endpoint.
func (b *InMemoryBackend) GetEndpointAttributes(endpointArn string) (map[string]string, error) {
	b.mu.RLock("GetEndpointAttributes")
	defer b.mu.RUnlock()

	ep, exists := b.platformEndpoints[endpointArn]
	if !exists {
		return nil, ErrEndpointNotFound
	}

	attrs := make(map[string]string, len(ep.Attributes))
	maps.Copy(attrs, ep.Attributes)

	return attrs, nil
}

// SetEndpointAttributes updates attributes on a platform endpoint.
func (b *InMemoryBackend) SetEndpointAttributes(endpointArn string, attributes map[string]string) error {
	b.mu.Lock("SetEndpointAttributes")
	defer b.mu.Unlock()

	ep, exists := b.platformEndpoints[endpointArn]
	if !exists {
		return ErrEndpointNotFound
	}

	maps.Copy(ep.Attributes, attributes)

	return nil
}

// ListEndpointsByPlatformApplication returns a page of endpoints for a platform application.
func (b *InMemoryBackend) ListEndpointsByPlatformApplication(
	platformApplicationArn, nextToken string,
) ([]PlatformEndpoint, string, error) {
	b.mu.RLock("ListEndpointsByPlatformApplication")
	defer b.mu.RUnlock()

	if _, exists := b.platformApplications[platformApplicationArn]; !exists {
		return nil, "", ErrPlatformApplicationNotFound
	}

	all := b.sortedEndpoints()
	filtered := make([]PlatformEndpoint, 0, len(all))

	for _, ep := range all {
		if ep.PlatformApplicationArn == platformApplicationArn {
			filtered = append(filtered, ep)
		}
	}

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	eps, next := paginate(filtered, offset, pageSize)

	return eps, next, nil
}

// DeleteEndpoint removes a platform endpoint by ARN.
func (b *InMemoryBackend) DeleteEndpoint(endpointArn string) error {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.platformEndpoints[endpointArn]; !exists {
		return ErrEndpointNotFound
	}

	delete(b.platformEndpoints, endpointArn)

	return nil
}

// sortedPlatformApplications returns platform applications sorted by ARN. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedPlatformApplications() []PlatformApplication {
	apps := make([]PlatformApplication, 0, len(b.platformApplications))
	for _, a := range b.platformApplications {
		apps = append(apps, *a)
	}

	sort.Slice(apps, func(i, j int) bool {
		return apps[i].PlatformApplicationArn < apps[j].PlatformApplicationArn
	})

	return apps
}

// sortedEndpoints returns platform endpoints sorted by ARN. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedEndpoints() []PlatformEndpoint {
	eps := make([]PlatformEndpoint, 0, len(b.platformEndpoints))
	for _, ep := range b.platformEndpoints {
		eps = append(eps, *ep)
	}

	sort.Slice(eps, func(i, j int) bool {
		return eps[i].EndpointArn < eps[j].EndpointArn
	})

	return eps
}

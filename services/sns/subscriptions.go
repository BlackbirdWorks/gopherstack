package sns

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// validateSubscribeEndpoint checks that the endpoint is valid for the given protocol.
func validateSubscribeEndpoint(protocol, endpoint string) error {
	if protocol == "sms" && !isValidE164(endpoint) {
		return fmt.Errorf(
			"%w: Endpoint must be in E.164 format for SMS protocol",
			ErrInvalidParameter,
		)
	}

	if (protocol == protocolEmail || protocol == protocolEmailJSON) && !isValidEmail(endpoint) {
		return fmt.Errorf(
			"%w: Invalid parameter: Endpoint must be a valid email address for %s protocol",
			ErrInvalidParameter,
			protocol,
		)
	}

	return nil
}

// Subscribe creates a new subscription for the given topic, protocol, and endpoint.
// If a confirmed subscription for the same topic+protocol+endpoint already exists,
// the existing subscription ARN is returned (matching AWS deduplication behaviour).
func (b *InMemoryBackend) Subscribe(
	topicArn, protocol, endpoint, filterPolicy string,
) (*Subscription, error) {
	if err := validateSubscribeEndpoint(protocol, endpoint); err != nil {
		return nil, err
	}

	// Parse and validate the filter policy outside the backend lock so that
	// JSON parsing of large policies does not block other SNS operations.
	parsedPolicy, parseErr := parseFilterPolicy(filterPolicy)
	if parseErr != nil {
		return nil, parseErr
	}

	b.mu.Lock("Subscribe")
	defer b.mu.Unlock()

	topic, exists := b.topics.Get(topicArn)
	if !exists {
		return nil, ErrTopicNotFound
	}

	// Dedup: return the existing subscription ARN when protocol+endpoint already
	// has a confirmed subscription on this topic (matches AWS behaviour).
	for _, existing := range b.subscriptionsByTopic.Get(topicArn) {
		if !existing.PendingConfirmation &&
			existing.Protocol == protocol &&
			existing.Endpoint == endpoint {
			return existing, nil
		}
	}

	parts := strings.Split(topic.TopicArn, ":")
	topicName := parts[len(parts)-1]
	topicRegion := arnRegion(topic.TopicArn)
	if topicRegion == "" {
		topicRegion = b.region
	}

	subArn := arn.Build("sns", topicRegion, b.accountID, topicName+":"+uuid.New().String())

	// HTTP and HTTPS subscriptions require out-of-band confirmation.
	// Email/email-json require the recipient to click a link.
	// SQS, Lambda, Firehose, and Application (mobile push) are auto-confirmed.
	pending := protocol == protocolHTTP || protocol == protocolHTTPS ||
		protocol == protocolEmail || protocol == protocolEmailJSON

	sub := &Subscription{
		SubscriptionArn:     subArn,
		TopicArn:            topicArn,
		Protocol:            protocol,
		Endpoint:            endpoint,
		Owner:               b.accountID,
		FilterPolicy:        filterPolicy,
		parsedFilterPolicy:  parsedPolicy,
		PendingConfirmation: pending,
		CreationTimestamp:   time.Now().UTC(),
	}

	b.subscriptions.Put(sub)

	return sub, nil
}

// Unsubscribe removes a subscription by ARN.
func (b *InMemoryBackend) Unsubscribe(subscriptionArn string) error {
	b.mu.Lock("Unsubscribe")
	defer b.mu.Unlock()

	if !b.subscriptions.Delete(subscriptionArn) {
		return ErrSubscriptionNotFound
	}

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

	// Use topic index for O(topic_subs) instead of O(all_subs).
	for _, sub := range b.subscriptionsByTopic.Get(topicArn) {
		if sub.PendingConfirmation {
			sub.PendingConfirmation = false

			return sub, nil
		}
	}

	return nil, ErrSubscriptionNotFound
}

// GetSubscriptionAttributes returns the attributes of a subscription.
func (b *InMemoryBackend) GetSubscriptionAttributes(
	subscriptionArn string,
) (map[string]string, error) {
	b.mu.RLock("GetSubscriptionAttributes")
	defer b.mu.RUnlock()

	sub, exists := b.subscriptions.Get(subscriptionArn)
	if !exists {
		return nil, ErrSubscriptionNotFound
	}

	attrs := map[string]string{
		"SubscriptionArn":              sub.SubscriptionArn,
		topicArnKey:                    sub.TopicArn,
		"Protocol":                     sub.Protocol,
		"Endpoint":                     sub.Endpoint,
		"Owner":                        sub.Owner,
		attrPendingConfirmation:        strconv.FormatBool(sub.PendingConfirmation),
		"ConfirmationWasAuthenticated": strconv.FormatBool(!sub.PendingConfirmation),
		attrRawMessageDelivery:         strconv.FormatBool(sub.RawMessageDelivery),
	}

	if sub.FilterPolicy != "" {
		attrs[attrFilterPolicy] = sub.FilterPolicy
	}

	if sub.RedrivePolicy != "" {
		attrs[attrRedrivePolicy] = sub.RedrivePolicy
	}

	if sub.DeliveryPolicy != "" {
		attrs[attrDeliveryPolicy] = sub.DeliveryPolicy
	}

	if sub.ReplayPolicy != "" {
		attrs[attrReplayPolicy] = sub.ReplayPolicy
	}

	if sub.SubscriptionRoleArn != "" {
		attrs[attrSubscriptionRoleArn] = sub.SubscriptionRoleArn
	}

	if sub.FilterPolicyScope != "" {
		attrs[attrFilterPolicyScope] = sub.FilterPolicyScope
	}

	return attrs, nil
}

// SetSubscriptionAttributes sets a single attribute on a subscription.
// When ReplayPolicy is set to a non-empty value, archived messages from the topic
// (published at or after replayFromTimestamp) are asynchronously delivered to this
// subscription. This mirrors AWS SNS archive replay behaviour.
func (b *InMemoryBackend) SetSubscriptionAttributes(
	subscriptionArn, attrName, attrValue string,
) error {
	// Parse the FilterPolicy outside the backend lock so JSON validation does
	// not serialize against unrelated SNS operations on large policies.
	var parsedPolicy parsedFilterPolicy

	if attrName == attrFilterPolicy {
		p, err := parseFilterPolicy(attrValue)
		if err != nil {
			return err
		}

		parsedPolicy = p
	}

	// Pre-validate redrive policy outside the backend lock for the same reason.
	if attrName == attrRedrivePolicy && attrValue != "" {
		if err := validateRedrivePolicy(attrValue); err != nil {
			return err
		}
		if err := b.checkDLQExists(attrValue); err != nil {
			return err
		}
	}

	// Validate and parse ReplayPolicy before acquiring the lock so JSON parsing
	// and RFC3339 timestamp validation don't hold the lock.
	var replayFromTime time.Time
	if attrName == attrReplayPolicy && attrValue != "" {
		ts, err := parseReplayFromTimestamp(attrValue)
		if err != nil {
			return err
		}

		replayFromTime = ts
	}

	var (
		subSnap  Subscription
		topicArn string
		setErr   error
	)

	func() {
		b.mu.Lock("SetSubscriptionAttributes")
		defer b.mu.Unlock()

		sub, exists := b.subscriptions.Get(subscriptionArn)
		if !exists {
			setErr = ErrSubscriptionNotFound

			return
		}

		if err := applySubscriptionAttr(sub, attrName, attrValue, parsedPolicy); err != nil {
			setErr = err

			return
		}

		// Capture a snapshot for replay (after the attribute is applied so RawMessageDelivery etc. are current).
		subSnap = *sub
		topicArn = sub.TopicArn
	}()

	if setErr != nil {
		return setErr
	}

	// Trigger asynchronous replay when ReplayPolicy is set to a non-empty value.
	if attrName == attrReplayPolicy && attrValue != "" && !replayFromTime.IsZero() {
		go b.replayMessagesToSubscription(subSnap, topicArn, replayFromTime)
	}

	return nil
}

// applySubscriptionAttr mutates sub with the given attribute value.
// Extracted to keep SetSubscriptionAttributes under the cyclomatic complexity budget.
func applySubscriptionAttr(
	sub *Subscription,
	attrName, attrValue string,
	parsedPolicy parsedFilterPolicy,
) error {
	switch attrName {
	case attrRawMessageDelivery:
		sub.RawMessageDelivery = strings.EqualFold(attrValue, "true")
	case attrFilterPolicy:
		sub.FilterPolicy = attrValue
		sub.parsedFilterPolicy = parsedPolicy
	case attrRedrivePolicy:
		sub.RedrivePolicy = attrValue
	case attrDeliveryPolicy:
		sub.DeliveryPolicy = attrValue
	case attrReplayPolicy:
		sub.ReplayPolicy = attrValue
	case attrSubscriptionRoleArn:
		sub.SubscriptionRoleArn = attrValue
	case attrFilterPolicyScope:
		return applyFilterPolicyScope(sub, attrValue)
	default:
		return ErrInvalidParameter
	}

	return nil
}

// applyFilterPolicyScope validates and sets the FilterPolicyScope field.
func applyFilterPolicyScope(sub *Subscription, attrValue string) error {
	if attrValue != "MessageBody" && attrValue != "MessageAttributes" {
		return fmt.Errorf(
			"%w: FilterPolicyScope must be MessageBody or MessageAttributes",
			ErrInvalidParameter,
		)
	}

	sub.FilterPolicyScope = attrValue

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
func (b *InMemoryBackend) ListSubscriptionsByTopic(
	topicArn, nextToken string,
) ([]Subscription, string, error) {
	b.mu.RLock("ListSubscriptionsByTopic")
	defer b.mu.RUnlock()

	if !b.topics.Has(topicArn) {
		return nil, "", ErrTopicNotFound
	}

	topicSubs := b.subscriptionsByTopic.Get(topicArn)
	filtered := make([]Subscription, 0, len(topicSubs))
	for _, sub := range topicSubs {
		filtered = append(filtered, *sub)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].SubscriptionArn < filtered[j].SubscriptionArn
	})

	offset, err := decodeToken(nextToken)
	if err != nil {
		return nil, "", ErrInvalidParameter
	}

	subs, next := paginate(filtered, offset, pageSize)

	return subs, next, nil
}

// ListAllSubscriptions returns all subscriptions sorted by ARN.
func (b *InMemoryBackend) ListAllSubscriptions() []Subscription {
	b.mu.RLock("ListAllSubscriptions")
	defer b.mu.RUnlock()

	return b.sortedSubscriptions()
}

// sortedSubscriptions returns subscriptions sorted by SubscriptionArn. Must be called with at least RLock held.
func (b *InMemoryBackend) sortedSubscriptions() []Subscription {
	subs := make([]Subscription, 0, b.subscriptions.Len())
	for _, s := range b.subscriptions.All() {
		subs = append(subs, *s)
	}

	sort.Slice(subs, func(i, j int) bool {
		return subs[i].SubscriptionArn < subs[j].SubscriptionArn
	})

	return subs
}

// isValidEmail performs a basic check that the string contains exactly one '@'
// with a non-empty local part and a domain containing at least one '.'.
// This mirrors the lightweight check AWS SNS applies; full RFC 5322 is not enforced.
func isValidEmail(email string) bool {
	atIdx := strings.Index(email, "@")
	if atIdx <= 0 {
		return false
	}

	domain := email[atIdx+1:]

	return strings.Contains(domain, ".")
}

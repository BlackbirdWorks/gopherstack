package sns

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// validateSubscribeEndpoint checks that the endpoint is well-formed for the
// given protocol, matching the per-protocol Endpoint constraints the real AWS
// SNS Subscribe API enforces. A malformed endpoint returns InvalidParameter
// (AWS: "Invalid parameter: Endpoint"), matching every other protocol check
// here (SMS/email already validated this way).
func validateSubscribeEndpoint(protocol, endpoint string) error {
	switch protocol {
	case protocolSMS:
		if !isValidE164(endpoint) {
			return invalidEndpointError(protocol, "must be in E.164 format")
		}
	case protocolEmail, protocolEmailJSON:
		if !isValidEmail(endpoint) {
			return invalidEndpointError(protocol, "must be a valid email address")
		}
	case protocolSQS:
		if !arnHasServiceAndResourcePrefix(endpoint, protocolSQS, "") {
			return invalidEndpointError(protocol, "must be a valid SQS queue ARN")
		}
	case protocolLambda:
		if !arnHasServiceAndResourcePrefix(endpoint, protocolLambda, "function:") {
			return invalidEndpointError(protocol, "must be a valid Lambda function ARN")
		}
	case protocolFirehose:
		if !arnHasServiceAndResourcePrefix(endpoint, protocolFirehose, "deliverystream/") {
			return invalidEndpointError(protocol, "must be a valid Firehose delivery stream ARN")
		}
	case protocolApplication:
		if !arnHasServiceAndResourcePrefix(endpoint, "sns", "endpoint/") {
			return invalidEndpointError(protocol, "must be a valid platform endpoint ARN")
		}
	case protocolHTTP, protocolHTTPS:
		if !isValidHTTPEndpoint(endpoint, protocol) {
			return invalidEndpointError(protocol, "must be a URL beginning with "+protocol+"://")
		}
	}

	return nil
}

// invalidEndpointError builds the InvalidParameter error returned when a
// Subscribe endpoint does not comply with the given protocol's constraints.
func invalidEndpointError(protocol, reason string) error {
	return fmt.Errorf(
		"%w: Invalid parameter: Endpoint %s for %s protocol",
		ErrInvalidParameter, reason, protocol,
	)
}

// arnHasServiceAndResourcePrefix reports whether v is a well-formed AWS ARN
// (arn:{partition}:{service}:{region}:{account}:{resource}) for the given
// service, with the resource component starting with resourcePrefix (checked
// only when resourcePrefix is non-empty).
func arnHasServiceAndResourcePrefix(v, service, resourcePrefix string) bool {
	const arnPartsWithResource = 6

	parts := strings.SplitN(v, ":", arnPartsWithResource)
	if len(parts) < arnPartsWithResource || parts[0] != "arn" || parts[2] != service {
		return false
	}

	return resourcePrefix == "" || strings.HasPrefix(parts[5], resourcePrefix)
}

// isValidHTTPEndpoint reports whether endpoint is a syntactically valid URL
// with a non-empty host and a scheme matching protocol ("http" or "https") —
// AWS rejects an "http" protocol Subscribe whose Endpoint is an https:// URL,
// and vice versa.
func isValidHTTPEndpoint(endpoint, protocol string) bool {
	u, err := url.Parse(endpoint)

	return err == nil && u.Host != "" && u.Scheme == protocol
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

	b.mu.Lock(opSubscribe)
	defer b.mu.Unlock()

	topic, exists := b.topics.Get(topicArn)
	if !exists {
		return nil, ErrTopicNotFound
	}

	topicSubs := b.subscriptionsByTopic.Get(topicArn)

	// Dedup: return the existing subscription ARN when protocol+endpoint already
	// has a confirmed subscription on this topic (matches AWS behaviour).
	for _, existing := range topicSubs {
		if !existing.PendingConfirmation &&
			existing.Protocol == protocol &&
			existing.Endpoint == endpoint {
			return existing, nil
		}
	}

	if len(topicSubs) >= b.subscriptionLimitPerTopic {
		return nil, ErrSubscriptionLimitExceeded
	}

	if filterPolicy != "" {
		if err := b.checkFilterPolicyQuotaLocked("", topicSubs); err != nil {
			return nil, err
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

// checkFilterPolicyQuotaLocked enforces the AWS SNS FilterPolicyLimitExceeded
// quotas (maxFilterPoliciesPerTopic, maxFilterPoliciesPerAccount) for a
// subscription that is about to be given a non-empty FilterPolicy. topicSubs
// is the caller's already-fetched index lookup for the subscription's topic
// (reused to avoid a second index scan). excludeArn is the ARN of the
// subscription being created/updated (empty for a brand-new Subscribe call)
// so that updating an existing filter policy in place does not double-count
// against the quota. Must be called with b.mu held.
func (b *InMemoryBackend) checkFilterPolicyQuotaLocked(
	excludeArn string,
	topicSubs []*Subscription,
) error {
	topicCount := 0

	for _, s := range topicSubs {
		if s.FilterPolicy != "" && s.SubscriptionArn != excludeArn {
			topicCount++
		}
	}

	if topicCount+1 > maxFilterPoliciesPerTopic {
		return fmt.Errorf(
			"%w: topic already has %d filter policies (limit %d)",
			ErrFilterPolicyLimitExceeded, topicCount, maxFilterPoliciesPerTopic,
		)
	}

	acctCount := 0

	b.subscriptions.Range(func(s *Subscription) bool {
		if s.FilterPolicy != "" && s.SubscriptionArn != excludeArn {
			acctCount++
		}

		return true
	})

	if acctCount+1 > maxFilterPoliciesPerAccount {
		return fmt.Errorf(
			"%w: account already has %d filter policies (limit %d)",
			ErrFilterPolicyLimitExceeded, acctCount, maxFilterPoliciesPerAccount,
		)
	}

	return nil
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
	// Parse/validate outside the backend lock so JSON parsing, DLQ existence
	// checks, and RFC3339 timestamp validation don't hold the lock.
	parsedPolicy, replayFromTime, err := subscriptionAttrPreValidation(b, attrName, attrValue)
	if err != nil {
		return err
	}

	subSnap, topicArn, err := b.setSubscriptionAttributesLocked(subscriptionArn, attrName, attrValue, parsedPolicy)
	if err != nil {
		return err
	}

	// Trigger asynchronous replay when ReplayPolicy is set to a non-empty value.
	if attrName == attrReplayPolicy && attrValue != "" && !replayFromTime.IsZero() {
		go b.replayMessagesToSubscription(subSnap, topicArn, replayFromTime)
	}

	return nil
}

// subscriptionAttrPreValidation performs the attribute-kind-specific validation
// that SetSubscriptionAttributes must do before acquiring the backend lock:
// FilterPolicy JSON parsing, RedrivePolicy DLQ existence checking, and
// ReplayPolicy timestamp parsing. Extracted to keep SetSubscriptionAttributes
// under the cyclomatic complexity budget.
func subscriptionAttrPreValidation(
	b *InMemoryBackend,
	attrName, attrValue string,
) (parsedFilterPolicy, time.Time, error) {
	var parsedPolicy parsedFilterPolicy

	if attrName == attrFilterPolicy {
		p, err := parseFilterPolicy(attrValue)
		if err != nil {
			return parsedFilterPolicy{}, time.Time{}, err
		}

		parsedPolicy = p
	}

	if attrName == attrRedrivePolicy && attrValue != "" {
		if err := validateRedrivePolicy(attrValue); err != nil {
			return parsedFilterPolicy{}, time.Time{}, err
		}
		if err := b.checkDLQExists(attrValue); err != nil {
			return parsedFilterPolicy{}, time.Time{}, err
		}
	}

	var replayFromTime time.Time

	if attrName == attrReplayPolicy && attrValue != "" {
		ts, err := parseReplayFromTimestamp(attrValue)
		if err != nil {
			return parsedFilterPolicy{}, time.Time{}, err
		}

		replayFromTime = ts
	}

	return parsedPolicy, replayFromTime, nil
}

// setSubscriptionAttributesLocked applies the attribute change under b.mu and
// returns a snapshot of the subscription (and its topic ARN) for the caller's
// post-unlock replay trigger. Extracted from SetSubscriptionAttributes so the
// locked region is a plain method body rather than a function literal.
func (b *InMemoryBackend) setSubscriptionAttributesLocked(
	subscriptionArn, attrName, attrValue string,
	parsedPolicy parsedFilterPolicy,
) (Subscription, string, error) {
	b.mu.Lock("SetSubscriptionAttributes")
	defer b.mu.Unlock()

	sub, exists := b.subscriptions.Get(subscriptionArn)
	if !exists {
		return Subscription{}, "", ErrSubscriptionNotFound
	}

	if attrName == attrFilterPolicy && attrValue != "" {
		topicSubs := b.subscriptionsByTopic.Get(sub.TopicArn)
		if err := b.checkFilterPolicyQuotaLocked(subscriptionArn, topicSubs); err != nil {
			return Subscription{}, "", err
		}
	}

	if attrName == attrReplayPolicy && attrValue != "" {
		if err := b.validateReplayPolicyEligibleLocked(sub); err != nil {
			return Subscription{}, "", err
		}
	}

	if err := applySubscriptionAttr(sub, attrName, attrValue, parsedPolicy); err != nil {
		return Subscription{}, "", err
	}

	// Capture a snapshot for replay (after the attribute is applied so RawMessageDelivery etc. are current).
	return *sub, sub.TopicArn, nil
}

// validateReplayPolicyEligibleLocked enforces that ReplayPolicy may only be
// set on a subscription whose topic is FIFO and whose protocol is one of the
// application-to-application (A2A) protocols SNS message archiving/replay
// supports: sqs, lambda, or firehose. Confirmed against
// docs.aws.amazon.com/sns/latest/dg/message-archiving-and-replay-topic-owner.html
// ("Amazon SNS message archiving and replay is only available for
// application-to-application (A2A) FIFO topics") — standard topics have no
// archive/replay mechanism at all, and A2P protocols (http/https/email/
// email-json/sms/application) are never eligible even on a FIFO topic.
// Must be called with b.mu held.
func (b *InMemoryBackend) validateReplayPolicyEligibleLocked(sub *Subscription) error {
	if sub.Protocol != protocolSQS && sub.Protocol != protocolLambda && sub.Protocol != protocolFirehose {
		return fmt.Errorf(
			"%w: Invalid parameter: ReplayPolicy is only supported for sqs, lambda, "+
				"and firehose subscriptions, got protocol %s",
			ErrInvalidParameter, sub.Protocol,
		)
	}

	topic, exists := b.topics.Get(sub.TopicArn)
	if !exists || topic.Attributes["FifoTopic"] != fifoTopicAttrValue {
		return fmt.Errorf(
			"%w: Invalid parameter: ReplayPolicy is only supported on FIFO topics",
			ErrInvalidParameter,
		)
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

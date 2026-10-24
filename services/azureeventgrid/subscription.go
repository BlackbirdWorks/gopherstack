package azureeventgrid

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"sort"
	"time"
)

const (
	propProvisioning = "provisioningState"
	propTopic        = "topic"
)

func parseSpec(props map[string]any, inputSchema string) (Spec, error) {
	raw, err := json.Marshal(props)
	if err != nil {
		return Spec{}, invalidSpec("properties are not valid JSON")
	}

	var spec Spec
	if err = json.Unmarshal(raw, &spec); err != nil {
		return Spec{}, invalidSpec("properties are malformed: %v", err)
	}

	if err = normalizeSpec(&spec, inputSchema); err != nil {
		return Spec{}, err
	}

	return spec, nil
}

func normalizeSpec(spec *Spec, inputSchema string) error {
	if spec.EventDeliverySchema == "" {
		spec.EventDeliverySchema = inputSchema
	}

	if spec.EventDeliverySchema != inputSchema {
		return invalidSpec(
			"delivery schema %q must match the topic input schema %q",
			spec.EventDeliverySchema,
			inputSchema,
		)
	}

	rp := &spec.RetryPolicy
	if rp.MaxDeliveryAttempts == 0 {
		rp.MaxDeliveryAttempts = DefaultMaxDeliveryAttempts
	}

	if rp.EventTimeToLiveInMinutes == 0 {
		rp.EventTimeToLiveInMinutes = DefaultEventTTLMinutes
	}

	if rp.MaxDeliveryAttempts < 1 || rp.MaxDeliveryAttempts > DefaultMaxDeliveryAttempts ||
		rp.EventTimeToLiveInMinutes < 1 || rp.EventTimeToLiveInMinutes > DefaultEventTTLMinutes {
		return invalidSpec("retryPolicy is out of range")
	}

	if err := validateDestination(spec.Destination); err != nil {
		return err
	}

	return validateDeadLetter(spec.DeadLetterDestination)
}

// specProps renders the ARM properties of a subscription. Webhook secrets in the
// query string are withheld (see FullURL).
func specProps(spec Spec, state, topicRID string) map[string]any {
	raw, _ := json.Marshal(spec)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)

	if dest, ok := out["destination"].(map[string]any); ok && spec.Destination.EndpointType == EndpointWebHook {
		dest["properties"] = webhookProps(spec.Destination)
	}

	out[propProvisioning] = StateSucceeded
	if state == StateAwaitingManual {
		out[propProvisioning] = StateAwaitingManual
	}

	out[propTopic] = topicRID

	return out
}

func webhookProps(d Destination) map[string]any {
	props := map[string]any{}
	maps.Copy(props, d.Properties)

	full := d.str(propEndpointURL)
	if u, err := url.Parse(full); err == nil {
		u.RawQuery, u.Fragment = "", ""
		props["endpointBaseUrl"] = u.String()
	}

	delete(props, propEndpointURL)

	if _, ok := props[propMaxEventsPerBatch]; !ok {
		props[propMaxEventsPerBatch] = defaultMaxEventsBatch
	}

	if _, ok := props[propPreferredBatchKB]; !ok {
		props[propPreferredBatchKB] = defaultBatchKB
	}

	withholdSecrets(props)

	return props
}

// PutSubscription creates or updates an event subscription on a custom topic and
// returns its ARM properties. A WebHook endpoint is validated first.
func (b *InMemoryBackend) PutSubscription(
	ctx context.Context, topicName, name string, props map[string]any,
) (map[string]any, error) {
	return b.PutScopedSubscription(ctx, Scope{Topic: topicName}, name, props)
}

// PutScopedSubscription creates or updates an event subscription on a custom
// topic, a domain or a domain topic. The first subscription of a missing domain
// topic creates it when the domain auto-creates topics.
func (b *InMemoryBackend) PutScopedSubscription(
	ctx context.Context, sc Scope, name string, props map[string]any,
) (map[string]any, error) {
	if !validName(name) {
		return nil, ErrInvalidName
	}

	b.mu.RLock("PutSubscription.read")
	info, err := b.scopeLocked(sc, true)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	spec, err := parseSpec(props, info.schema)
	if err != nil {
		return nil, err
	}

	state, code, err := b.validateEndpoint(ctx, sc, info.rid, name, spec)
	if err != nil {
		return nil, err
	}

	return b.storeSubscription(sc, name, spec, state, code)
}

func (b *InMemoryBackend) storeSubscription(
	sc Scope, name string, spec Spec, state, code string,
) (map[string]any, error) {
	b.mu.Lock("PutSubscription")
	defer b.mu.Unlock()

	if err := b.ensureDomainTopicLocked(sc); err != nil {
		return nil, err
	}

	b.ensureARMScopeLocked(sc)

	info, err := b.scopeLocked(sc, false)
	if err != nil {
		return nil, err
	}

	s, ok := info.subs[name]
	if !ok {
		s = &subscription{Name: name, CreatedAt: b.now().UTC()}
		info.subs[name] = s
	}

	s.Spec, s.State, s.ValidationCode = spec, state, code

	return specProps(spec, s.State, info.rid), nil
}

var patchableMembers = []string{ //nolint:gochecknoglobals // static lookup
	"destination", "filter", "labels", "expirationTimeUtc", "eventDeliverySchema", "retryPolicy",
	"deadLetterDestination",
}

var identityMembers = []string{ //nolint:gochecknoglobals // static lookup
	"deliveryWithResourceIdentity", "deadLetterWithResourceIdentity",
}

// mergeSpecPatch overlays the members of an EventSubscriptionUpdateParameters body
// onto the current spec; a supplied member replaces the stored one, a member that is
// absent or null keeps it.
func mergeSpecPatch(cur Spec, patch map[string]any) (map[string]any, error) {
	for _, k := range identityMembers {
		if patch[k] != nil {
			return nil, invalidSpec("%s is not supported", k)
		}
	}

	raw, err := json.Marshal(cur)
	if err != nil {
		return nil, invalidSpec("current spec is not valid JSON")
	}

	merged := map[string]any{}
	if err = json.Unmarshal(raw, &merged); err != nil {
		return nil, invalidSpec("current spec is not valid JSON")
	}

	for _, k := range patchableMembers {
		if v := patch[k]; v != nil {
			merged[k] = v
		}
	}

	return merged, nil
}

// PatchScopedSubscription applies an EventSubscriptionUpdateParameters body to an
// existing event subscription and returns its ARM properties. Only the supplied
// members change; the merged result is validated exactly as a PUT.
func (b *InMemoryBackend) PatchScopedSubscription(
	ctx context.Context, sc Scope, name string, patch map[string]any,
) (map[string]any, error) {
	b.mu.RLock("PatchSubscription.read")
	info, s, err := b.subAtLocked(sc, name)

	var cur Spec
	if err == nil {
		cur = s.Spec
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	merged, err := mergeSpecPatch(cur, patch)
	if err != nil {
		return nil, err
	}

	spec, err := parseSpec(merged, info.schema)
	if err != nil {
		return nil, err
	}

	state, code, err := b.validateEndpoint(ctx, sc, info.rid, name, spec)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("PatchSubscription")
	defer b.mu.Unlock()

	info, s, err = b.subAtLocked(sc, name)
	if err != nil {
		return nil, err
	}

	s.Spec, s.State, s.ValidationCode = spec, state, code

	return specProps(spec, s.State, info.rid), nil
}

// Subscription returns the ARM properties of a custom topic's event subscription.
func (b *InMemoryBackend) Subscription(topicName, name string) (map[string]any, error) {
	return b.ScopedSubscription(Scope{Topic: topicName}, name)
}

// ScopedSubscription returns the ARM properties of an event subscription.
func (b *InMemoryBackend) ScopedSubscription(sc Scope, name string) (map[string]any, error) {
	b.mu.RLock("Subscription")
	defer b.mu.RUnlock()

	info, s, err := b.subAtLocked(sc, name)
	if err != nil {
		return nil, err
	}

	return specProps(s.Spec, s.State, info.rid), nil
}

func (b *InMemoryBackend) subLocked(topicName, name string) (scopeInfo, *subscription, error) {
	return b.subAtLocked(Scope{Topic: topicName}, name)
}

func (b *InMemoryBackend) subAtLocked(sc Scope, name string) (scopeInfo, *subscription, error) {
	info, err := b.scopeLocked(sc, false)
	if err != nil {
		return scopeInfo{}, nil, err
	}

	s, ok := info.subs[name]
	if !ok {
		return scopeInfo{}, nil, ErrSubscriptionNotFound
	}

	return info, s, nil
}

// SubscriptionCreated returns an event subscription's creation time.
func (b *InMemoryBackend) SubscriptionCreated(topicName, name string) (time.Time, error) {
	b.mu.RLock("SubscriptionCreated")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, name)
	if err != nil {
		return time.Time{}, err
	}

	return s.CreatedAt, nil
}

// FullURL returns the complete webhook endpoint URL of a custom topic's event subscription.
func (b *InMemoryBackend) FullURL(topicName, name string) (string, error) {
	return b.ScopedFullURL(Scope{Topic: topicName}, name)
}

// ScopedFullURL returns the complete webhook endpoint URL of an event subscription.
func (b *InMemoryBackend) ScopedFullURL(sc Scope, name string) (string, error) {
	b.mu.RLock("FullURL")
	defer b.mu.RUnlock()

	_, s, err := b.subAtLocked(sc, name)
	if err != nil {
		return "", err
	}

	if s.Spec.Destination.EndpointType != EndpointWebHook {
		return "", fmt.Errorf("%w: not a WebHook subscription", ErrInvalidSpec)
	}

	return s.Spec.Destination.str(propEndpointURL), nil
}

// DeleteSubscription removes a custom topic's event subscription and its undelivered events.
func (b *InMemoryBackend) DeleteSubscription(topicName, name string) error {
	return b.DeleteScopedSubscription(Scope{Topic: topicName}, name)
}

// DeleteScopedSubscription removes an event subscription and its undelivered
// events; the last subscription of a domain topic also removes the topic when
// the domain auto-deletes topics.
func (b *InMemoryBackend) DeleteScopedSubscription(sc Scope, name string) error {
	b.mu.Lock("DeleteSubscription")
	defer b.mu.Unlock()

	info, _, err := b.subAtLocked(sc, name)
	if err != nil {
		return err
	}

	delete(info.subs, name)
	b.autoDeleteDomainTopicLocked(sc)
	b.dropEmptyARMScopeLocked(sc)

	return nil
}

// ConfirmValidation completes a manual webhook validation started by a validationUrl GET.
func (b *InMemoryBackend) ConfirmValidation(code string) bool {
	if code == "" {
		return false
	}

	b.mu.Lock("ConfirmValidation")
	defer b.mu.Unlock()

	confirmed := false

	b.eachSubsLocked(func(_ Scope, _ string, subs map[string]*subscription) {
		for _, s := range subs {
			if s.ValidationCode == code {
				s.State, s.ValidationCode = StateSucceeded, ""
				confirmed = true
			}
		}
	})

	return confirmed
}

// TopicNames returns the custom topic names sorted.
func (b *InMemoryBackend) TopicNames() []string {
	b.mu.RLock("TopicNames")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.topics))
	for name := range b.topics {
		out = append(out, name)
	}

	sort.Strings(out)

	return out
}

// SubscriptionNames returns a custom topic's event subscription names sorted.
func (b *InMemoryBackend) SubscriptionNames(topicName string) ([]string, error) {
	b.mu.RLock("SubscriptionNames")
	defer b.mu.RUnlock()

	t, err := b.topicLocked(topicName)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(t.Subs))
	for name := range t.Subs {
		out = append(out, name)
	}

	sort.Strings(out)

	return out, nil
}

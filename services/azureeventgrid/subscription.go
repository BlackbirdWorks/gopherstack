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
	if !validName(name) {
		return nil, ErrInvalidName
	}

	b.mu.RLock("PutSubscription.read")
	t, err := b.topicLocked(topicName)

	var schema, rid string
	if err == nil {
		schema, rid = t.InputSchema, t.ResourceID
	}

	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	spec, err := parseSpec(props, schema)
	if err != nil {
		return nil, err
	}

	state, code, err := b.validateEndpoint(ctx, topicName, rid, name, spec)
	if err != nil {
		return nil, err
	}

	return b.storeSubscription(topicName, name, spec, state, code)
}

func (b *InMemoryBackend) storeSubscription(
	topicName, name string, spec Spec, state, code string,
) (map[string]any, error) {
	b.mu.Lock("PutSubscription")
	defer b.mu.Unlock()

	t, err := b.topicLocked(topicName)
	if err != nil {
		return nil, err
	}

	s, ok := t.Subs[name]
	if !ok {
		s = &subscription{Name: name, CreatedAt: b.now().UTC()}
		t.Subs[name] = s
	}

	s.Spec, s.State, s.ValidationCode = spec, state, code

	return specProps(spec, s.State, t.ResourceID), nil
}

// Subscription returns the ARM properties of an event subscription.
func (b *InMemoryBackend) Subscription(topicName, name string) (map[string]any, error) {
	b.mu.RLock("Subscription")
	defer b.mu.RUnlock()

	t, s, err := b.subLocked(topicName, name)
	if err != nil {
		return nil, err
	}

	return specProps(s.Spec, s.State, t.ResourceID), nil
}

func (b *InMemoryBackend) subLocked(topicName, name string) (*topic, *subscription, error) {
	t, err := b.topicLocked(topicName)
	if err != nil {
		return nil, nil, err
	}

	s, ok := t.Subs[name]
	if !ok {
		return nil, nil, ErrSubscriptionNotFound
	}

	return t, s, nil
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

// FullURL returns the complete webhook endpoint URL of an event subscription.
func (b *InMemoryBackend) FullURL(topicName, name string) (string, error) {
	b.mu.RLock("FullURL")
	defer b.mu.RUnlock()

	_, s, err := b.subLocked(topicName, name)
	if err != nil {
		return "", err
	}

	if s.Spec.Destination.EndpointType != EndpointWebHook {
		return "", fmt.Errorf("%w: not a WebHook subscription", ErrInvalidSpec)
	}

	return s.Spec.Destination.str(propEndpointURL), nil
}

// DeleteSubscription removes an event subscription and its undelivered events.
func (b *InMemoryBackend) DeleteSubscription(topicName, name string) error {
	b.mu.Lock("DeleteSubscription")
	defer b.mu.Unlock()

	t, _, err := b.subLocked(topicName, name)
	if err != nil {
		return err
	}

	delete(t.Subs, name)

	return nil
}

// ConfirmValidation completes a manual webhook validation started by a validationUrl GET.
func (b *InMemoryBackend) ConfirmValidation(code string) bool {
	if code == "" {
		return false
	}

	b.mu.Lock("ConfirmValidation")
	defer b.mu.Unlock()

	for _, t := range b.topics {
		for _, s := range t.Subs {
			if s.ValidationCode == code {
				s.State, s.ValidationCode = StateSucceeded, ""

				return true
			}
		}
	}

	return false
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

package sns

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Topics               map[string]*Topic                `json:"topics"`
	Subscriptions        map[string]*Subscription         `json:"subscriptions"`
	TopicTags            map[string]*svcTags.Tags         `json:"topicTags"`
	PlatformApplications map[string]*PlatformApplication  `json:"platformApplications,omitempty"`
	PlatformEndpoints    map[string]*PlatformEndpoint     `json:"platformEndpoints,omitempty"`
	SMSSandbox           map[string]*SandboxPhoneNumber   `json:"smsSandbox,omitempty"`
	OptedOutPhoneNumbers map[string]bool                  `json:"optedOutPhoneNumbers,omitempty"`
	SMSAttributes        map[string]string                `json:"smsAttributes,omitempty"`
	OriginationNumbers   map[string][]XMLOriginationPhone `json:"originationNumbers,omitempty"`
	SMSSandboxEnabled    *bool                            `json:"smsSandboxEnabled,omitempty"`
	AccountID            string                           `json:"accountID"`
	Region               string                           `json:"region"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	sandboxEnabled := b.smsSandboxEnabled
	snap := backendSnapshot{
		Topics:               b.topics,
		Subscriptions:        b.subscriptions,
		TopicTags:            b.topicTags,
		PlatformApplications: b.platformApplications,
		PlatformEndpoints:    b.platformEndpoints,
		SMSSandbox:           b.smsSandbox,
		OptedOutPhoneNumbers: b.optedOutPhoneNumbers,
		SMSAttributes:        b.smsAttributes,
		OriginationNumbers:   b.originationNumbers,
		AccountID:            b.accountID,
		Region:               b.region,
		SMSSandboxEnabled:    &sandboxEnabled,
	}

	return persistence.MarshalSnapshot(ctx, "sns", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The event emitter is not restored — it is re-wired by the CLI after restore.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sns", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Topics == nil {
		snap.Topics = make(map[string]*Topic)
	}

	if snap.Subscriptions == nil {
		snap.Subscriptions = make(map[string]*Subscription)
	}

	if snap.TopicTags == nil {
		snap.TopicTags = make(map[string]*svcTags.Tags)
	}

	if snap.PlatformApplications == nil {
		snap.PlatformApplications = make(map[string]*PlatformApplication)
	}

	if snap.PlatformEndpoints == nil {
		snap.PlatformEndpoints = make(map[string]*PlatformEndpoint)
	}

	if snap.SMSSandbox == nil {
		snap.SMSSandbox = make(map[string]*SandboxPhoneNumber)
	}

	if snap.OptedOutPhoneNumbers == nil {
		snap.OptedOutPhoneNumbers = make(map[string]bool)
	}

	if snap.SMSAttributes == nil {
		snap.SMSAttributes = make(map[string]string)
	}

	if snap.OriginationNumbers == nil {
		snap.OriginationNumbers = make(map[string][]XMLOriginationPhone)
	}

	b.topics = snap.Topics
	b.subscriptions = snap.Subscriptions
	b.topicTags = snap.TopicTags
	b.platformApplications = snap.PlatformApplications
	b.platformEndpoints = snap.PlatformEndpoints
	b.smsSandbox = snap.SMSSandbox
	b.optedOutPhoneNumbers = snap.OptedOutPhoneNumbers
	b.smsAttributes = snap.SMSAttributes
	b.originationNumbers = snap.OriginationNumbers
	b.accountID = snap.AccountID
	b.region = snap.Region
	if snap.SMSSandboxEnabled != nil {
		b.smsSandboxEnabled = *snap.SMSSandboxEnabled
	} else {
		b.smsSandboxEnabled = true // default for old snapshots that lack this field
	}

	// Rebuild the per-topic subscription index and restore the parsed filter-policy
	// cache for each subscription (both are transient and not persisted).
	b.topicSubscriptions = make(map[string]map[string]*Subscription, len(b.topics))
	for topicArn := range b.topics {
		b.topicSubscriptions[topicArn] = make(map[string]*Subscription)
	}

	for _, sub := range b.subscriptions {
		if _, ok := b.topicSubscriptions[sub.TopicArn]; !ok {
			b.topicSubscriptions[sub.TopicArn] = make(map[string]*Subscription)
		}

		b.topicSubscriptions[sub.TopicArn][sub.SubscriptionArn] = sub
		// Restore parsed filter policy; ignore errors so a future stricter validation
		// upgrade does not break loading older snapshots.
		sub.parsedFilterPolicy, _ = parseFilterPolicy(sub.FilterPolicy)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}

package mq

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type backendSnapshot struct {
	Brokers        map[string]*Broker           `json:"brokers"`
	Configurations map[string]*Configuration    `json:"configurations"`
	Tags           map[string]map[string]string `json:"tags"`
	AccountID      string                       `json:"accountID"`
	Region         string                       `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Brokers:        b.brokers,
		Configurations: b.configurations,
		Tags:           b.tags,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "mq: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilTagMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.brokers = snap.Brokers
	b.configurations = snap.Configurations
	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Re-establish the shared-pointer invariant: b.tags[arn] and
	// resource.Tags must point to the same map so that CreateTags and
	// DeleteTags stay consistent without an extra linear scan.
	for _, br := range b.brokers {
		if t, ok := b.tags[br.BrokerArn]; ok {
			br.Tags = t
		} else {
			b.tags[br.BrokerArn] = br.Tags
		}
	}

	for _, cfg := range b.configurations {
		if t, ok := b.tags[cfg.Arn]; ok {
			cfg.Tags = t
		} else {
			b.tags[cfg.Arn] = cfg.Tags
		}
	}

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Brokers == nil {
		snap.Brokers = make(map[string]*Broker)
	}

	if snap.Configurations == nil {
		snap.Configurations = make(map[string]*Configuration)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}
}

// fixNilTagMaps ensures restored resources have non-nil tag maps.
func fixNilTagMaps(snap *backendSnapshot) {
	for _, br := range snap.Brokers {
		if br.Tags == nil {
			br.Tags = make(map[string]string)
		}

		if br.Users == nil {
			br.Users = make(map[string]*User)
		}
	}

	for _, cfg := range snap.Configurations {
		if cfg.Tags == nil {
			cfg.Tags = make(map[string]string)
		}

		if cfg.Data == nil {
			cfg.Data = make(map[int32]string)
		}
	}

	for arn, tagMap := range snap.Tags {
		if tagMap == nil {
			snap.Tags[arn] = make(map[string]string)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

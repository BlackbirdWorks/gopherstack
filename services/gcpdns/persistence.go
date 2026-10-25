package gcpdns

import (
	"context"
	"fmt"

	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot would make an older one unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Zones            map[string]*zoneRec       `json:"zones"`
	Policies         map[string]*dnsapi.Policy `json:"policies"`
	ResponsePolicies map[string]*responseRec   `json:"responsePolicies"`
	Version          int                       `json:"version"`
}

// Snapshot serialises the backend state.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "gcpdns", backendSnapshot{
		Version: snapshotVersion, Zones: b.zones, Policies: b.policies, ResponsePolicies: b.respPols,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpdns", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "gcpdns: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		return nil
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.resetMaps()

	for k, rec := range snap.Zones {
		if rec == nil || rec.Zone == nil {
			return fmt.Errorf("%w: zone %q", ErrSnapshotNull, k)
		}

		if rec.RRSets == nil {
			rec.RRSets = map[string]*dnsapi.ResourceRecordSet{}
		}

		b.zones[k] = rec
	}

	for k, p := range snap.Policies {
		if p != nil {
			b.policies[k] = p
		}
	}

	for k, rec := range snap.ResponsePolicies {
		if rec == nil || rec.Policy == nil {
			return fmt.Errorf("%w: response policy %q", ErrSnapshotNull, k)
		}

		if rec.Rules == nil {
			rec.Rules = map[string]*dnsapi.ResponsePolicyRule{}
		}

		b.respPols[k] = rec
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpdns: restore snapshot: %w", err)
	}

	return nil
}

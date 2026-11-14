package ocidns

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Zones       map[string]*Zone           `json:"zones"`
	Views       map[string]*View           `json:"views"`
	Resolvers   map[string]*Resolver       `json:"resolvers"`
	Endpoints   map[string]*Endpoint       `json:"endpoints"`
	TsigKeys    map[string]*TsigKey        `json:"tsigKeys"`
	Policies    map[string]*SteeringPolicy `json:"steeringPolicies"`
	Attachments map[string]*Attachment     `json:"steeringPolicyAttachments"`
	Retry       map[string]ociretry.Entry  `json:"retryTokens"`
	Auth        []ociauth.KeyRecord        `json:"authKeys"`
	Version     int                        `json:"version"`
	Seq         int64                      `json:"seq"`
}

// Snapshot serialises every resource and the registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocidns", backendSnapshot{
		Version: snapshotVersion, Zones: b.zones, Views: b.views, Resolvers: b.resolvers, Endpoints: b.endpoints,
		TsigKeys: b.tsigs, Policies: b.policies, Attachments: b.attachments, Retry: b.retry.Entries(), Auth: keys,
		Seq: b.seq,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocidns", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocidns: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := snap.normalize(); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.zones, b.views, b.resolvers, b.endpoints = snap.Zones, snap.Views, snap.Resolvers, snap.Endpoints
	b.tsigs, b.policies, b.attachments, b.seq = snap.TsigKeys, snap.Policies, snap.Attachments, snap.Seq
	b.retry.Load(snap.Retry)
	b.Auth.Load(snap.Auth)

	return nil
}

func checkNulls[T any](kind string, m map[string]*T) error {
	for id, v := range m {
		if v == nil {
			return fmt.Errorf("%w: %s %q", ErrSnapshotNull, kind, id)
		}
	}

	return nil
}

func (s *backendSnapshot) normalize() error {
	if s.Zones == nil {
		s.Zones = map[string]*Zone{}
	}

	if s.Views == nil {
		s.Views = map[string]*View{}
	}

	if s.Resolvers == nil {
		s.Resolvers = map[string]*Resolver{}
	}

	if s.Endpoints == nil {
		s.Endpoints = map[string]*Endpoint{}
	}

	if s.TsigKeys == nil {
		s.TsigKeys = map[string]*TsigKey{}
	}

	if s.Policies == nil {
		s.Policies = map[string]*SteeringPolicy{}
	}

	if s.Attachments == nil {
		s.Attachments = map[string]*Attachment{}
	}

	for _, err := range []error{
		checkNulls("zone", s.Zones), checkNulls("view", s.Views), checkNulls("resolver", s.Resolvers),
		checkNulls("endpoint", s.Endpoints), checkNulls("tsig key", s.TsigKeys),
		checkNulls("steering policy", s.Policies), checkNulls("attachment", s.Attachments),
	} {
		if err != nil {
			return err
		}
	}

	for _, z := range s.Zones {
		if z.Records == nil {
			z.Records = map[string]*RRSet{}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ocidns: restore snapshot: %w", err)
	}

	return nil
}

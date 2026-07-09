package sesv2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// sesv2SnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// sesv2SnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const sesv2SnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the SES v2 backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral
// DTO registry is needed here, unlike services/ses. Version guards against
// decoding a snapshot from an incompatible (older or newer) build of this
// backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables                map[string]json.RawMessage   `json:"tables"`
	EmailIdentityPolicies map[string]map[string]string `json:"emailIdentityPolicies"`
	ResourceTags          map[string]map[string]string `json:"resourceTags"`
	MultiRegionEndpoints  map[string]map[string]any    `json:"multiRegionEndpoints"`
	Tenants               map[string]map[string]any    `json:"tenants"`
	TenantResources       map[string][]string          `json:"tenantResources"`
	ResourceTenants       map[string][]string          `json:"resourceTenants"`
	AccountDetails        *AccountDetails              `json:"accountDetails,omitempty"`
	AccountID             string                       `json:"accountID"`
	Region                string                       `json:"region"`
	Emails                []Email                      `json:"emails"`
	Version               int                          `json:"version"`
}

func ensureNonNilMaps(s *backendSnapshot) {
	if s.EmailIdentityPolicies == nil {
		s.EmailIdentityPolicies = make(map[string]map[string]string)
	}

	if s.ResourceTags == nil {
		s.ResourceTags = make(map[string]map[string]string)
	}

	if s.MultiRegionEndpoints == nil {
		s.MultiRegionEndpoints = make(map[string]map[string]any)
	}

	if s.Tenants == nil {
		s.Tenants = make(map[string]map[string]any)
	}

	if s.TenantResources == nil {
		s.TenantResources = make(map[string][]string)
	}

	if s.ResourceTenants == nil {
		s.ResourceTenants = make(map[string][]string)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sesv2: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:               sesv2SnapshotVersion,
		Tables:                tables,
		AccountDetails:        b.accountDetails,
		EmailIdentityPolicies: b.emailIdentityPolicies,
		Emails:                b.emails,
		AccountID:             b.accountID,
		Region:                b.region,
		ResourceTags:          b.resourceTags,
		MultiRegionEndpoints:  b.multiRegionEndpoints,
		Tenants:               b.tenants,
		TenantResources:       b.tenantResources,
		ResourceTenants:       b.resourceTenants,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sesv2: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sesv2", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != sesv2SnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"sesv2: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", sesv2SnapshotVersion)

		b.registry.ResetAll()
		b.emailIdentityPolicies = make(map[string]map[string]string)
		b.resourceTags = make(map[string]map[string]string)
		b.multiRegionEndpoints = make(map[string]map[string]any)
		b.tenants = make(map[string]map[string]any)
		b.tenantResources = make(map[string][]string)
		b.resourceTenants = make(map[string][]string)
		b.accountDetails = nil
		b.emails = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("sesv2: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&snap)

	b.emailIdentityPolicies = snap.EmailIdentityPolicies
	b.accountDetails = snap.AccountDetails
	b.emails = snap.Emails
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.resourceTags = snap.ResourceTags
	b.multiRegionEndpoints = snap.MultiRegionEndpoints
	b.tenants = snap.Tenants
	b.tenantResources = snap.TenantResources
	b.resourceTenants = snap.ResourceTenants

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

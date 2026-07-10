package acm

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// acmSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a DTO type, a registered table's value type,
// or backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus certs' own Reset, not a partial decode)
// any mismatch -- see Restore below. This is the first version: the
// pre-Phase-3.3 snapshot carried no version field at all, so it also fails
// this check and is discarded rather than misread, which is the safe
// behaviour across any snapshot-format change.
const acmSnapshotVersion = 1

// regionalDTO wraps a region-nested resource whose only hidden field is
// region (Certificate) for JSON round-tripping through the ephemeral
// persistence registry below -- the same technique services/codeartifact's
// regionalDTO[V] uses. ID mirrors the resource's own primary-key component
// (its ARN) so the DTO table itself has a stable composite key
// ("region|ARN").
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore for the "dirty" certs table (see store_setup.go's
// registerAllTables doc). It is built fresh on every call (rather than
// reusing b.registry) because the DTO value type (regionalDTO[Certificate])
// differs from the live table value type (Certificate).
func buildPersistenceDTORegistry() (*store.Registry, *store.Table[regionalDTO[Certificate]]) {
	dtoReg := store.NewRegistry()
	certDTOs := store.Register(dtoReg, "certs", store.New(regionalDTOKeyFn[Certificate]))

	return dtoReg, certDTOs
}

// backendSnapshot is the top-level on-disk shape for the ACM backend.
//
// Tables holds one JSON-encoded array under "certs" (the DTO table built
// above for the dirty certs table -- see store_setup.go's registerAllTables
// doc; b.registry itself registers no tables). IdempotencyMap,
// AccountIdempotency, and AccountConfig are non-*T value maps left
// unconverted by Phase 3.3 (see store_setup.go) and are persisted here
// directly, as before. Timers are runtime-only and were never persisted;
// Restore rebuilds them from certificate state (see below).
type backendSnapshot struct {
	Tables             map[string]json.RawMessage                    `json:"tables"`
	IdempotencyMap     map[string]map[string]certIdempotencyEntry    `json:"idempotencyMap,omitempty"`
	AccountIdempotency map[string]map[string]accountIdempotencyEntry `json:"accountIdempotency,omitempty"`
	AccountConfig      map[string]AccountConfig                      `json:"accountConfig"`
	AccountID          string                                        `json:"accountID"`
	Region             string                                        `json:"region"`
	Version            int                                           `json:"version"`
}

type handlerSnapshot struct {
	Tags    map[string]map[string]string `json:"tags"`
	Backend backendSnapshot              `json:"backend"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "acm: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, certDTOs := buildPersistenceDTORegistry()

	for _, c := range b.certs.Snapshot() {
		certDTOs.Put(&regionalDTO[Certificate]{Value: c, Region: c.region, ID: c.ARN})
	}

	dtoTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "acm: snapshot cert table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:            acmSnapshotVersion,
		Tables:             tables,
		IdempotencyMap:     b.idempotencyMap,
		AccountIdempotency: b.accountIdempotency,
		AccountConfig:      b.accountConfig,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	return persistence.MarshalSnapshot(ctx, "acm", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "acm", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != acmSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"acm: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", acmSnapshotVersion)

		b.registry.ResetAll()
		b.certs.Reset()
		b.idempotencyMap = make(map[string]map[string]certIdempotencyEntry)
		b.accountIdempotency = make(map[string]map[string]accountIdempotencyEntry)
		b.accountConfig = make(map[string]AccountConfig)
		b.timers = make(map[string]map[string]*time.Timer)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("acm: restore snapshot tables: %w", err)
	}

	if err := b.restoreCerts(snap.Tables); err != nil {
		return err
	}

	if snap.IdempotencyMap == nil {
		snap.IdempotencyMap = make(map[string]map[string]certIdempotencyEntry)
	}

	if snap.AccountIdempotency == nil {
		snap.AccountIdempotency = make(map[string]map[string]accountIdempotencyEntry)
	}

	if snap.AccountConfig == nil {
		snap.AccountConfig = make(map[string]AccountConfig)
	}

	b.idempotencyMap = snap.IdempotencyMap
	b.accountIdempotency = snap.AccountIdempotency
	b.accountConfig = snap.AccountConfig
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.timers = make(map[string]map[string]*time.Timer)

	// Restart timers for pending validations.
	for _, cert := range b.certs.All() {
		region, arn := cert.region, cert.ARN

		switch {
		case cert.Status == statusPendingValidation:
			t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func(r, a string) func() {
				return func() { b.autoValidate(r, a) }
			}(region, arn))
			b.timersStore(region)[arn] = t
		case cert.RenewalSummary != nil &&
			cert.RenewalSummary.RenewalStatus == renewalStatusPendingValidation:
			t := time.AfterFunc(autoValidateDelayMS*time.Millisecond, func(r, a string) func() {
				return func() { b.autoValidateRenewal(r, a) }
			}(region, arn))
			b.timersStore(region)[arn] = t
		}
	}

	return nil
}

// restoreCerts rebuilds the "dirty" certs store.Table from snap's DTO table,
// factored out of Restore to keep Restore's own cognitive complexity low.
// Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreCerts(tables map[string]json.RawMessage) error {
	dtoReg, certDTOs := buildPersistenceDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("acm: restore snapshot cert table: %w", err)
	}

	b.certs.Reset()

	for _, d := range certDTOs.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.certs.Put(d.Value)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend
// and also capturing the handler's tag state.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	h.tagsMu.RLock("Snapshot")
	tagsMap := make(map[string]map[string]string, h.tags.Len())
	for _, entry := range h.tags.All() {
		tagsMap[entry.ResourceID] = entry.Tags.Clone()
	}
	h.tagsMu.RUnlock()

	snap := handlerSnapshot{
		Tags: tagsMap,
	}
	if err := json.Unmarshal(h.Backend.Snapshot(ctx), &snap.Backend); err != nil {
		return nil
	}

	return persistence.MarshalSnapshot(ctx, "acm", snap)
}

// Restore implements persistence.Persistable by delegating to the backend
// and restoring the handler's tag state.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	var snap handlerSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		// Fall back to backend-only snapshot for backward compatibility.
		return h.Backend.Restore(ctx, data)
	}

	backendData, err := json.Marshal(snap.Backend)
	if err != nil {
		return err
	}

	if restoreErr := h.Backend.Restore(ctx, backendData); restoreErr != nil {
		return restoreErr
	}

	h.tagsMu.Lock("Restore")
	defer h.tagsMu.Unlock()

	for _, entry := range h.tags.All() {
		entry.Tags.Close()
	}
	h.tags.Reset()

	for resourceID, m := range snap.Tags {
		h.tags.Put(&certTagsEntry{
			ResourceID: resourceID,
			Tags:       svcTags.FromMap("acm."+resourceID+".tags", m),
		})
	}

	return nil
}

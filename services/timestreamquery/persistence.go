package timestreamquery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// timestreamquerySnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to backendSnapshot (or a value type held
// by one of the registered store.Tables) would make an older snapshot unsafe
// to decode as the current shape. Restore compares this against the
// persisted value and discards (registry.ResetAll, not a partial decode) any
// mismatch -- see Restore. The pre-Phase-3.3 snapshot format had no version
// field at all, so an old snapshot decodes with Version == 0, which is
// guaranteed to mismatch timestreamquerySnapshotVersion and is discarded the
// same way any other incompatible snapshot is.
const timestreamquerySnapshotVersion = 1

// backendSnapshot is the serialisable form of InMemoryBackend state.
//
// Tables holds one JSON-encoded array per registered store.Table name
// (scheduledQueries, queries -- see store_setup.go), produced by
// b.registry.SnapshotAll(). AccountSettings is still a plain nested map (its
// value type is a plain struct, not *T, so it does not fit store.Table's
// shape -- see store_setup.go) and is persisted directly, as before. Version
// guards against decoding a snapshot from an incompatible (older or newer)
// build of this backend as though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables          map[string]json.RawMessage         `json:"tables"`
	AccountSettings map[string]accountSettingsSnapshot `json:"account_settings"`
	Version         int                                `json:"version"`
}

// accountSettingsSnapshot is the serialisable form of AccountSettings.
type accountSettingsSnapshot struct {
	MaxQueryTCU       *int32                `json:"max_query_tcu,omitempty"`
	QueryCompute      *queryComputeSnapshot `json:"query_compute,omitempty"`
	QueryPricingModel string                `json:"query_pricing_model"`
}

// queryComputeSnapshot is the serialisable form of QueryCompute. Without this,
// an UpdateAccountSettings call that switches an account to PROVISIONED
// compute mode would silently revert to the ON_DEMAND default across a
// Snapshot/Restore round trip (e.g. a gopherstack restart).
type queryComputeSnapshot struct {
	ActiveQueryTCU            *int32                                     `json:"active_query_tcu,omitempty"`
	NotificationConfiguration *accountSettingsNotificationConfigSnapshot `json:"notification_configuration,omitempty"`
	ComputeMode               string                                     `json:"compute_mode,omitempty"`
}

// accountSettingsNotificationConfigSnapshot is the serialisable form of
// AccountSettingsNotificationConfiguration. Without this, a PROVISIONED
// account with a configured capacity-change NotificationConfiguration would
// silently drop it across a Snapshot/Restore round trip.
type accountSettingsNotificationConfigSnapshot struct {
	SnsTopicArn string `json:"sns_topic_arn,omitempty"`
	RoleArn     string `json:"role_arn,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "timestreamquery: failed to snapshot tables", "error", err)

		return nil
	}

	// Snapshot account settings across all regions.
	settingsSnap := make(map[string]accountSettingsSnapshot, len(b.accountSettings))
	for region, s := range b.accountSettings {
		settingsSnap[region] = snapshotAccountSettings(s)
	}

	data, err := json.Marshal(backendSnapshot{
		Version:         timestreamquerySnapshotVersion,
		Tables:          tables,
		AccountSettings: settingsSnap,
	})
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "timestreamquery: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// snapshotAccountSettings converts one region's AccountSettings into its
// serialisable form. Split out of Snapshot to keep that method's cognitive
// complexity low.
func snapshotAccountSettings(s AccountSettings) accountSettingsSnapshot {
	snap := accountSettingsSnapshot{QueryPricingModel: s.QueryPricingModel}
	if s.MaxQueryTCU != nil {
		v := *s.MaxQueryTCU
		snap.MaxQueryTCU = &v
	}

	if s.QueryCompute != nil {
		snap.QueryCompute = snapshotQueryCompute(s.QueryCompute)
	}

	return snap
}

// snapshotQueryCompute converts a QueryCompute into its serialisable form.
func snapshotQueryCompute(qc *QueryCompute) *queryComputeSnapshot {
	out := &queryComputeSnapshot{ComputeMode: qc.ComputeMode}

	pc := qc.ProvisionedCapacity
	if pc == nil {
		return out
	}

	if pc.ActiveQueryTCU != nil {
		v := *pc.ActiveQueryTCU
		out.ActiveQueryTCU = &v
	}

	if nc := pc.NotificationConfiguration; nc != nil {
		ncSnap := &accountSettingsNotificationConfigSnapshot{RoleArn: nc.RoleArn}
		if nc.SnsConfiguration != nil {
			ncSnap.SnsTopicArn = nc.SnsConfiguration.TopicArn
		}
		out.NotificationConfiguration = ncSnap
	}

	return out
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "timestreamquery", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != timestreamquerySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"timestreamquery: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", timestreamquerySnapshotVersion)

		b.registry.ResetAll()
		b.accountSettings = make(map[string]AccountSettings)
		b.clientTokens = newClientTokenCache(queryClientTokenTTL)
		b.scheduledQueryTokens = newClientTokenCache(createScheduledQueryClientTokenTTL)
		b.pageStore = newNextTokenStore()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("timestreamquery: restore snapshot tables: %w", err)
	}

	// Reconstruct per-region account settings.
	b.accountSettings = make(map[string]AccountSettings, len(snap.AccountSettings))
	for region, s := range snap.AccountSettings {
		b.accountSettings[region] = AccountSettings{
			QueryPricingModel: s.QueryPricingModel,
			MaxQueryTCU:       s.MaxQueryTCU,
			QueryCompute:      restoreQueryCompute(s.QueryCompute),
		}
	}

	ensureNonNilMaps(b)

	return nil
}

// ensureNonNilMaps initialises any nil maps in the backend.
// Must be called with the write lock held.
func ensureNonNilMaps(b *InMemoryBackend) {
	if b.accountSettings == nil {
		b.accountSettings = make(map[string]AccountSettings)
	}
}

// restoreQueryCompute reconstructs a QueryCompute from its snapshot form. A
// nil snap (e.g. a snapshot written before QueryCompute was persisted) falls
// back to the ON_DEMAND default rather than leaving QueryCompute nil, since
// DescribeAccountSettings always returns a non-nil QueryCompute for any
// region that has settings on record.
func restoreQueryCompute(snap *queryComputeSnapshot) *QueryCompute {
	if snap == nil {
		return &QueryCompute{ComputeMode: computeModeOnDemand}
	}

	qc := &QueryCompute{ComputeMode: snap.ComputeMode}
	if snap.ActiveQueryTCU != nil {
		v := *snap.ActiveQueryTCU
		qc.ProvisionedCapacity = &ProvisionedCapacity{
			ActiveQueryTCU:            &v,
			LastUpdate:                &LastUpdate{Status: "SUCCEEDED", TargetQueryTCU: &v},
			NotificationConfiguration: restoreAccountSettingsNotificationConfig(snap.NotificationConfiguration),
		}
	}

	return qc
}

// restoreAccountSettingsNotificationConfig reconstructs an
// AccountSettingsNotificationConfiguration from its snapshot form, or nil if
// none was persisted.
func restoreAccountSettingsNotificationConfig(
	snap *accountSettingsNotificationConfigSnapshot,
) *AccountSettingsNotificationConfiguration {
	if snap == nil {
		return nil
	}

	nc := &AccountSettingsNotificationConfiguration{RoleArn: snap.RoleArn}
	if snap.SnsTopicArn != "" {
		nc.SnsConfiguration = &SnsConfiguration{TopicArn: snap.SnsTopicArn}
	}

	return nc
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(ctx, data)
	}

	return nil
}

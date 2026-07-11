package route53resolver

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// route53resolverSnapshotVersion identifies the shape of backendSnapshot's
// Tables blob (i.e. the set/shape of resources registered on b.registry --
// see registerAllTables in store_setup.go). It must be bumped whenever a
// change there would make an older snapshot unsafe to decode as the current
// shape. Restore compares this against the persisted value and discards
// (rather than attempts to partially decode) any mismatch -- see Restore
// below. This mirrors the services/ec2 (commit 12e611a4) and services/sqs
// (commit 0f09d77c) pilots.
const route53resolverSnapshotVersion = 1

type backendSnapshot struct {
	Tables                    map[string]json.RawMessage         `json:"tables"`
	Tags                      map[string]map[string][]svcTags.KV `json:"tags"`
	FirewallRuleGroupPolicies map[string]map[string]string       `json:"firewallRuleGroupPolicies"`
	QueryLogConfigPolicies    map[string]map[string]string       `json:"queryLogConfigPolicies"`
	ResolverRulePolicies      map[string]map[string]string       `json:"resolverRulePolicies"`
	AccountID                 string                             `json:"accountID"`
	Region                    string                             `json:"region"`
	Version                   int                                `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "route53resolver: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:                   route53resolverSnapshotVersion,
		Tables:                    tables,
		Tags:                      b.tags,
		FirewallRuleGroupPolicies: b.firewallRuleGroupPolicies,
		QueryLogConfigPolicies:    b.queryLogConfigPolicies,
		ResolverRulePolicies:      b.resolverRulePolicies,
		AccountID:                 b.accountID,
		Region:                    b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53resolver: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "route53resolver", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != route53resolverSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption. Mirrors the services/ec2 and services/sqs pilots.
		logger.Load(ctx).WarnContext(ctx,
			"route53resolver: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", route53resolverSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string][]svcTags.KV)
		b.firewallRuleGroupPolicies = make(map[string]map[string]string)
		b.queryLogConfigPolicies = make(map[string]map[string]string)
		b.resolverRulePolicies = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("route53resolver: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&snap)

	b.tags = snap.Tags
	b.firewallRuleGroupPolicies = snap.FirewallRuleGroupPolicies
	b.queryLogConfigPolicies = snap.QueryLogConfigPolicies
	b.resolverRulePolicies = snap.ResolverRulePolicies
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string][]svcTags.KV)
	}
	if snap.FirewallRuleGroupPolicies == nil {
		snap.FirewallRuleGroupPolicies = make(map[string]map[string]string)
	}
	if snap.QueryLogConfigPolicies == nil {
		snap.QueryLogConfigPolicies = make(map[string]map[string]string)
	}
	if snap.ResolverRulePolicies == nil {
		snap.ResolverRulePolicies = make(map[string]map[string]string)
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

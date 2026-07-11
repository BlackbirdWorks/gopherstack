package secretsmanager

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// backend's secrets field -- previously a map[string]map[string]*Secret keyed
// outer-first by region -- is replaced by a single flat *store.Table[Secret].
// It follows pkgs/store's package doc and the services/ec2 (commit
// 12e611a4, data-driven registration slice) and services/sqs (commit
// 0f09d77c, DTO-registry) conversions, and mirrors services/cloudwatchlogs's
// region-qualified-table pattern (store_setup.go/region_accessors.go) most
// closely, since secrets are likewise nested by region.
//
// resourcePolicies (map[string]map[string]string) and replicationConfigs
// (map[string]map[string][]ReplicationStatusType) are deliberately NOT
// converted: store.Table requires a *V value with its own identity, but
// these hold a bare string and a bare []ReplicationStatusType respectively,
// neither of which carries a key of its own. They remain plain nested maps,
// unchanged by this refactor (see InMemoryBackend's *Store/*StoreRO helpers
// in backend.go, preserved as-is).
//
// # Region-qualified table
//
// Secret gained an unexported `region` field (see models.go) used to build
// the composite store.Table primary key "region|name" (secretTableKey /
// secretKeyFn below). Being unexported, it never appears in the Secrets
// Manager wire responses (those are always built by hand --
// secretToListEntry, DescribeSecret, etc. -- never by marshaling Secret
// directly), but it also means Secret cannot round-trip through
// Table.Snapshot's own json.Marshal; persistence.go carries it through the
// pre-existing secretSnapshot DTO instead (which already exists for the
// other unexported/json:"-" Secret fields), plus a new Region field.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// secretTableKey returns the store.Table primary key for a secret.
func secretTableKey(region, name string) string {
	return region + "|" + name
}

// secretKeyFn keys a Secret by its region+Name composite key.
func secretKeyFn(s *Secret) string { return secretTableKey(s.region, s.Name) }

// secretRegionIndexKeyFn groups every secret belonging to one region
// (mirrors the old secrets[region] map) for per-region operations such as
// ListSecrets and BatchGetSecretValue's filter-based path.
func secretRegionIndexKeyFn(s *Secret) string { return s.region }

// registerAllTables registers every store.Table on b.registry exactly once.
// It must be called during construction only, immediately after b.registry is
// created -- store.Register panics on a duplicate name, so runtime resets go
// through b.registry.ResetAll() instead (see InMemoryBackend.Reset in
// backend.go), never a second call to this function.
func registerAllTables(b *InMemoryBackend) {
	b.secrets = store.Register(b.registry, "secrets", store.New(secretKeyFn))
	b.secretsByRegion = b.secrets.AddIndex("byRegion", secretRegionIndexKeyFn)
}

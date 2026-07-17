package eventbridge

// Code in this file supports Phase 3.3 of the datalayer refactor: resource
// maps with a pure per-value identity (a Name/ID field of their own) are
// backed by *store.Table[T] instead of a hand-rolled map[string]*T. See
// pkgs/store's package doc and the services/ec2 (commit 12e611a4), services/sqs
// (commit 0f09d77c), and services/ssm+kms (per-parent lazy registration for
// nested maps) conversions this follows.
//
// # Why this looks different from ec2/sqs
//
// ec2 and sqs each model ONE region per InMemoryBackend instance, so a flat
// map[string]*store.Table[T] statically registered once at construction time
// is enough. eventbridge's InMemoryBackend is unusual in two ways: it holds
// EVERY region's state in one instance (region -> value, like ssm), and two
// of its resources nest a level deeper still: rules (region -> event bus ->
// rule) and targets (region -> bus/rule -> target). store.Registry has no
// concept of "region" or "parent" and requires every name registered on it to
// be known and unique up front -- it cannot itself express "one table per
// region (and per parent), regions/parents unknown until first use".
//
// This file bridges the two: each convertible resource keeps its existing
// map-of-maps shape down to (but not including) the leaf collection, which
// becomes a *store.Table[T] created lazily by getOrCreateTable /
// getOrCreateNestedTable / getOrCreateGlobalTable on first access --
// exactly mirroring the lazy map creation the hand-rolled code did before
// conversion (e.g. the old `if b.buses[region] == nil { ... }` guards, now
// folded into the accessors in accessors.go). The first time a table is
// created it is ALSO registered on b.registry under a name that encodes its
// region/parent (e.g. "rules/us-east-1/default") so that persistence.go can
// still drive Snapshot/Restore through one *store.Registry regardless of how
// many regions/parents exist -- see preRegisterSnapshotTables in
// persistence.go.
//
// pipes, registries, and schemas are NOT region-scoped at all -- a single
// InMemoryBackend instance holds one global Pipe/SchemaRegistry/Schema
// catalogue -- and, unlike every resource above, were NEVER part of
// backendSnapshot before this conversion (see persistence.go's history):
// CreatePipe/CreateRegistry/CreateSchema state has always been silently lost
// across a Restore. Preserving that existing (if surprising) behavior
// byte-for-byte means these three must NOT start round-tripping through
// Snapshot/Restore just because they gained a *store.Table. They are
// therefore registered on b.auxRegistry, a second, never-snapshotted
// *store.Registry that exists solely so getOrCreateTable/
// getOrCreateGlobalTable still have somewhere to register them (and so a
// construction-time bug that double-registers one still panics) -- see
// accessors.go's pipesTable/registriesTable/schemasTableFor. b.registry, by
// contrast, is the one persistence.go drives via SnapshotAll/RestoreAll.
//
// # What is NOT converted here, and why
//
//   - archivedEvents (map[string]map[string][]EventEntry, region -> archive
//     name -> events): one-to-many -- there is no single V to key a Table by;
//     the archived events for one archive stay a slice, same as ec2/ssm's
//     history-style slice maps.
//   - busePolicies (map[string]map[string]*EventBusPolicy, region -> bus name
//     -> policy): EventBusPolicy carries no name field of its own naming the
//     bus it belongs to, so there is no pure func(*EventBusPolicy) string to
//     write -- same rationale as ssm's tags/miscResourceTags exclusions.
//   - schemaVersions (map[string][]*SchemaVersion, keyed by
//     "registryName/schemaName"): ordered one-to-many version history, not a
//     single keyed collection.
//   - codeBindings (map[string]*CodeBinding, keyed by
//     "registryName/schemaName/language"): CodeBinding stores Language and
//     SchemaVersion but not the registry/schema names that make up the rest
//     of its map key, so there is no pure func(*CodeBinding) string that
//     reproduces the existing key from the value alone.
import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func eventBusKeyFn(v *EventBus) string                     { return v.Name }
func ruleKeyFn(v *Rule) string                             { return v.Name }
func targetKeyFnV(v *Target) string                        { return v.ID }
func eventSourceKeyFn(v *EventSource) string               { return v.Name }
func replayKeyFn(v *Replay) string                         { return v.ReplayName }
func apiDestinationKeyFn(v *APIDestination) string         { return v.Name }
func archiveKeyFn(v *Archive) string                       { return v.ArchiveName }
func connectionKeyFn(v *Connection) string                 { return v.Name }
func endpointKeyFn(v *Endpoint) string                     { return v.Name }
func partnerEventSourceKeyFn(v *PartnerEventSource) string { return v.Name }
func pipeKeyFn(v *Pipe) string                             { return v.Name }
func schemaRegistryKeyFn(v *SchemaRegistry) string         { return v.RegistryName }
func schemaKeyFn(v *Schema) string                         { return v.SchemaName }

// getOrCreateTable returns the *store.Table[V] for key from m (a backend's
// key->Table field for one resource type), creating and registering it on reg
// (under name+"/"+key) the first time that key is touched. m is a Go map
// (reference type), so inserting into it here mutates the same map the
// caller's field refers to. Used both for per-region resources (key=region,
// reg=b.registry) and per-registry resources (key=registryName,
// reg=b.auxRegistry).
func getOrCreateTable[V any](
	reg *store.Registry,
	m map[string]*store.Table[V],
	name, key string,
	keyFn func(*V) string,
) *store.Table[V] {
	t, ok := m[key]
	if !ok {
		t = store.Register(reg, name+"/"+key, store.New(keyFn))
		m[key] = t
	}

	return t
}

// getOrCreateNestedTable is [getOrCreateTable] for resources nested one level
// deeper than a single per-region/per-registry dimension: rules
// (region -> bus -> Table[Rule]) and targets (region -> bus/rule -> Table[Target]).
func getOrCreateNestedTable[V any](
	reg *store.Registry,
	m map[string]map[string]*store.Table[V],
	name, outerKey, innerKey string,
	keyFn func(*V) string,
) *store.Table[V] {
	inner := m[outerKey]
	if inner == nil {
		inner = make(map[string]*store.Table[V])
		m[outerKey] = inner
	}

	t, ok := inner[innerKey]
	if !ok {
		t = store.Register(reg, name+"/"+outerKey+"/"+innerKey, store.New(keyFn))
		inner[innerKey] = t
	}

	return t
}

// getOrCreateGlobalTable is [getOrCreateTable] for resources with no
// region/parent dimension at all (pipes, registries, schemas): a single
// *store.Table[V] field on the backend, registered lazily on first access.
func getOrCreateGlobalTable[V any](
	reg *store.Registry,
	cur **store.Table[V],
	name string,
	keyFn func(*V) string,
) *store.Table[V] {
	if *cur == nil {
		*cur = store.Register(reg, name, store.New(keyFn))
	}

	return *cur
}

// tableAccessorsByPrefix maps each PERSISTED convertible resource's
// registry-name prefix to a no-op-return accessor call that forces the lazy
// getOrCreate* helpers to register that resource's table for a given
// (region, parent) pair. persistence.go's Restore uses this to pre-register
// every table present in an incoming snapshot's Tables blob BEFORE calling
// b.registry.RestoreAll -- RestoreAll only restores tables already registered
// on b.registry, so a region/bus a fresh backend has never seen must be
// registered first or its data would be silently dropped. rest is the
// portion of the table name after "<prefix>/"; for per-region resources it is
// the region; for rules/targets it is "region/parent" (split again by
// preRegisterSnapshotTables since region never contains "/" but bus/rule
// names may).
//
// pipes, registries, and schemas are deliberately absent: they live on
// b.auxRegistry, not b.registry, and never appear in a Tables blob -- see the
// package doc above.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableAccessorsByPrefix = map[string]func(b *InMemoryBackend, rest string){
	"buses":           func(b *InMemoryBackend, region string) { b.busesTable(region) },
	"eventSources":    func(b *InMemoryBackend, region string) { b.eventSourcesTable(region) },
	"replays":         func(b *InMemoryBackend, region string) { b.replaysTable(region) },
	"apiDestinations": func(b *InMemoryBackend, region string) { b.apiDestinationsTable(region) },
	"archives":        func(b *InMemoryBackend, region string) { b.archivesTable(region) },
	"connections":     func(b *InMemoryBackend, region string) { b.connectionsTable(region) },
	"endpoints":       func(b *InMemoryBackend, region string) { b.endpointsTable(region) },
	"partnerSources":  func(b *InMemoryBackend, region string) { b.partnerSourcesTable(region) },
	"rules": func(b *InMemoryBackend, rest string) {
		// region never contains "/" but bus names may, so only the FIRST "/"
		// may be used to split rest back into (region, busKey).
		if region, busKey, ok := strings.Cut(rest, "/"); ok {
			b.ruleTableFor(region, busKey)
		}
	},
	"targets": func(b *InMemoryBackend, rest string) {
		if region, parentKey, ok := strings.Cut(rest, "/"); ok {
			b.targetTableFor(region, parentKey)
		}
	},
}

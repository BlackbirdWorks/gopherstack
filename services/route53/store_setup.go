package route53

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend whose key is a pure
// function of the stored value's own fields is registered exactly once here
// as a *store.Table[T] on b.registry. See pkgs/store's package doc and the
// services/sqs pilot (commit 0f09d77c) for the pattern this follows.
//
// The following resource fields are deliberately NOT registered here and
// remain plain maps:
//   - trafficPolicies (map[string][]*TrafficPolicy): the value is a SLICE of
//     versions, not a single *TrafficPolicy, so store.Table (which keys one
//     *V per entry) does not fit without inventing a synthetic wrapper type.
//   - vpcAssociations (map[string][]vpcAssociation) and
//     vpcAssocAuthorizations (map[string][]VPCAssociationAuthorization): both
//     are slice-valued AND the element type is a plain value (not a pointer),
//     so neither the "one *V per key" nor the "key derived from *V" shape
//     required by store.Table applies.
//   - tags (map[string]*svcTags.Tags): svcTags.Tags carries no identity field
//     of its own (it is a thin wrapper over a safemap.Map[string,string]); it
//     is keyed externally by resourceID, exactly like EC2's
//     instanceIMDSOptions/verifiedAccessGroupPolicies fields.
//
// zoneData.records (the nested per-zone record-set map) is left as a raw map
// field on zoneData, not promoted to its own top-level store.Table: it is
// scoped entirely to its owning zone and never accessed independent of it,
// the same "nested collection stays inline" call the s3 conversion made for
// bucket objects.
import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// zoneKeyFn is the [store.Table] key function for b.zones: HostedZone.ID is
// set once at CreateHostedZone and never mutated afterward (only Comment is
// updated in place), so it is a stable, pure identity for zoneData.
func zoneKeyFn(v *zoneData) string { return v.zone.ID }

// healthCheckKeyFn is the [store.Table] key function for b.healthChecks.
func healthCheckKeyFn(v *HealthCheck) string { return v.ID }

// keySigningKeyKeyFn is the [store.Table] key function for b.keySigningKeys,
// mirroring the composite key kskKey already built from HostedZoneID+Name.
// Both fields are set at creation and never mutated (only Status changes).
func keySigningKeyKeyFn(v *KeySigningKey) string { return kskKey(v.HostedZoneID, v.Name) }

// keySigningKeyZoneKeyFn is the secondary-index key function grouping key
// signing keys by their owning hosted zone, used for the DeleteHostedZone
// cascade and the DNSSEC status lookups.
func keySigningKeyZoneKeyFn(v *KeySigningKey) string { return v.HostedZoneID }

// cidrCollectionKeyFn is the [store.Table] key function for b.cidrCollections.
func cidrCollectionKeyFn(v *CidrCollection) string { return v.ID }

// queryLoggingConfigKeyFn is the [store.Table] key function for
// b.queryLoggingConfigs.
func queryLoggingConfigKeyFn(v *QueryLoggingConfig) string { return v.ID }

// queryLoggingConfigZoneKeyFn is the secondary-index key function grouping
// query logging configs by their owning hosted zone, used for the
// DeleteHostedZone cascade, the one-config-per-zone existence check in
// CreateQueryLoggingConfig, and the zone filter in ListQueryLoggingConfigs.
func queryLoggingConfigZoneKeyFn(v *QueryLoggingConfig) string { return v.HostedZoneID }

// delegationSetKeyFn is the [store.Table] key function for
// b.reusableDelegationSets.
func delegationSetKeyFn(v *ReusableDelegationSet) string { return v.ID }

// trafficPolicyInstanceKeyFn is the [store.Table] key function for
// b.trafficPolicyInstances.
func trafficPolicyInstanceKeyFn(v *TrafficPolicyInstance) string { return v.ID }

// trafficPolicyInstanceZoneKeyFn is the secondary-index key function grouping
// traffic policy instances by their owning hosted zone. HostedZoneID is set
// at creation and never mutated (unlike TrafficPolicyID, which
// UpdateTrafficPolicyInstance can change in place), so this index never goes
// stale.
func trafficPolicyInstanceZoneKeyFn(v *TrafficPolicyInstance) string { return v.HostedZoneID }

// changeKeyFn is the [store.Table] key function for b.changes. The stored map
// key has always been the bare change ID (e.g. "C1234..."), while
// ChangeInfo.ID carries the full "/change/C1234..." wire form; TrimPrefix
// recovers the former as a pure function of the latter.
func changeKeyFn(v *ChangeInfo) string { return strings.TrimPrefix(v.ID, "/change/") }

// registerTables registers every converted resource map on b.registry exactly
// once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on
// a duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
func (b *InMemoryBackend) registerTables() {
	b.zones = store.Register(b.registry, "zones", store.New(zoneKeyFn))
	b.healthChecks = store.Register(b.registry, "healthChecks", store.New(healthCheckKeyFn))

	b.keySigningKeys = store.Register(b.registry, "keySigningKeys", store.New(keySigningKeyKeyFn))
	b.keySigningKeysByZone = b.keySigningKeys.AddIndex("byZone", keySigningKeyZoneKeyFn)

	b.cidrCollections = store.Register(b.registry, "cidrCollections", store.New(cidrCollectionKeyFn))

	b.queryLoggingConfigs = store.Register(b.registry, "queryLoggingConfigs", store.New(queryLoggingConfigKeyFn))
	b.queryLoggingConfigsByZone = b.queryLoggingConfigs.AddIndex("byZone", queryLoggingConfigZoneKeyFn)

	b.reusableDelegationSets = store.Register(b.registry, "reusableDelegationSets", store.New(delegationSetKeyFn))

	b.trafficPolicyInstances = store.Register(
		b.registry, "trafficPolicyInstances", store.New(trafficPolicyInstanceKeyFn),
	)
	b.trafficPolicyInstancesByZone = b.trafficPolicyInstances.AddIndex("byZone", trafficPolicyInstanceZoneKeyFn)

	b.changes = store.Register(b.registry, "changes", store.New(changeKeyFn))
}

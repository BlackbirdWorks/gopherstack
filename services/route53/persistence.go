package route53

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type zoneDataSnapshot struct {
	Records       map[string]*ResourceRecordSet `json:"records"`
	Zone          HostedZone                    `json:"zone"`
	DNSSECEnabled bool                          `json:"dnssecEnabled,omitempty"`
}

type backendSnapshot struct {
	Zones                  map[string]*zoneDataSnapshot             `json:"zones"`
	HealthChecks           map[string]*HealthCheck                  `json:"healthChecks,omitempty"`
	KeySigningKeys         map[string]*KeySigningKey                `json:"keySigningKeys,omitempty"`
	CidrCollections        map[string]*CidrCollection               `json:"cidrCollections,omitempty"`
	QueryLoggingConfigs    map[string]*QueryLoggingConfig           `json:"queryLoggingConfigs,omitempty"`
	ReusableDelegationSets map[string]*ReusableDelegationSet        `json:"reusableDelegationSets,omitempty"`
	TrafficPolicies        map[string][]*TrafficPolicy              `json:"trafficPolicies,omitempty"`
	TrafficPolicyInstances map[string]*TrafficPolicyInstance        `json:"trafficPolicyInstances,omitempty"`
	VPCAssociations        map[string][]vpcAssociation              `json:"vpcAssociations,omitempty"`
	VPCAssocAuthorizations map[string][]VPCAssociationAuthorization `json:"vpcAssocAuthorizations,omitempty"`
	Changes                map[string]*ChangeInfo                   `json:"changes,omitempty"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Zones:                  make(map[string]*zoneDataSnapshot, len(b.zones)),
		HealthChecks:           make(map[string]*HealthCheck, len(b.healthChecks)),
		KeySigningKeys:         make(map[string]*KeySigningKey, len(b.keySigningKeys)),
		CidrCollections:        make(map[string]*CidrCollection, len(b.cidrCollections)),
		QueryLoggingConfigs:    make(map[string]*QueryLoggingConfig, len(b.queryLoggingConfigs)),
		ReusableDelegationSets: make(map[string]*ReusableDelegationSet, len(b.reusableDelegationSets)),
		TrafficPolicies:        make(map[string][]*TrafficPolicy, len(b.trafficPolicies)),
		TrafficPolicyInstances: make(map[string]*TrafficPolicyInstance, len(b.trafficPolicyInstances)),
		VPCAssociations:        make(map[string][]vpcAssociation, len(b.vpcAssociations)),
		VPCAssocAuthorizations: make(map[string][]VPCAssociationAuthorization, len(b.vpcAssocAuthorizations)),
		Changes:                make(map[string]*ChangeInfo, len(b.changes)),
	}

	for id, zd := range b.zones {
		snap.Zones[id] = &zoneDataSnapshot{
			Zone:          zd.zone,
			Records:       zd.records,
			DNSSECEnabled: zd.dnssecEnabled,
		}
	}

	for id, hc := range b.healthChecks {
		cp := *hc
		snap.HealthChecks[id] = &cp
	}

	for k, ksk := range b.keySigningKeys {
		cp := *ksk
		snap.KeySigningKeys[k] = &cp
	}

	for id, col := range b.cidrCollections {
		cp := *col
		snap.CidrCollections[id] = &cp
	}

	for id, cfg := range b.queryLoggingConfigs {
		cp := *cfg
		snap.QueryLoggingConfigs[id] = &cp
	}

	for id, ds := range b.reusableDelegationSets {
		cp := *ds
		snap.ReusableDelegationSets[id] = &cp
	}

	for id, versions := range b.trafficPolicies {
		versionsCopy := make([]*TrafficPolicy, len(versions))
		for i, tp := range versions {
			cp := *tp
			versionsCopy[i] = &cp
		}

		snap.TrafficPolicies[id] = versionsCopy
	}

	for id, inst := range b.trafficPolicyInstances {
		cp := *inst
		snap.TrafficPolicyInstances[id] = &cp
	}

	for id, assocs := range b.vpcAssociations {
		snap.VPCAssociations[id] = append([]vpcAssociation(nil), assocs...)
	}

	for id, auths := range b.vpcAssocAuthorizations {
		snap.VPCAssocAuthorizations[id] = append([]VPCAssociationAuthorization(nil), auths...)
	}

	for id, ch := range b.changes {
		cp := *ch
		snap.Changes[id] = &cp
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "route53 Snapshot failed", "error", err)

		return nil
	}

	return data
}

// ensureNonNilMaps initialises any nil maps in the snapshot.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Zones == nil {
		snap.Zones = make(map[string]*zoneDataSnapshot)
	}

	if snap.HealthChecks == nil {
		snap.HealthChecks = make(map[string]*HealthCheck)
	}

	if snap.KeySigningKeys == nil {
		snap.KeySigningKeys = make(map[string]*KeySigningKey)
	}

	if snap.CidrCollections == nil {
		snap.CidrCollections = make(map[string]*CidrCollection)
	}

	if snap.QueryLoggingConfigs == nil {
		snap.QueryLoggingConfigs = make(map[string]*QueryLoggingConfig)
	}

	if snap.ReusableDelegationSets == nil {
		snap.ReusableDelegationSets = make(map[string]*ReusableDelegationSet)
	}

	if snap.TrafficPolicies == nil {
		snap.TrafficPolicies = make(map[string][]*TrafficPolicy)
	}

	if snap.TrafficPolicyInstances == nil {
		snap.TrafficPolicyInstances = make(map[string]*TrafficPolicyInstance)
	}

	if snap.VPCAssociations == nil {
		snap.VPCAssociations = make(map[string][]vpcAssociation)
	}

	if snap.VPCAssocAuthorizations == nil {
		snap.VPCAssocAuthorizations = make(map[string][]VPCAssociationAuthorization)
	}

	if snap.Changes == nil {
		snap.Changes = make(map[string]*ChangeInfo)
	}
}

// restoreSimpleMaps restores the simple (non-zone, non-traffic-policy) maps from a snapshot.
func (b *InMemoryBackend) restoreSimpleMaps(snap *backendSnapshot) {
	b.healthChecks = make(map[string]*HealthCheck, len(snap.HealthChecks))

	for id, hc := range snap.HealthChecks {
		cp := *hc
		b.healthChecks[id] = &cp
	}

	b.keySigningKeys = make(map[string]*KeySigningKey, len(snap.KeySigningKeys))

	for k, ksk := range snap.KeySigningKeys {
		cp := *ksk
		b.keySigningKeys[k] = &cp
	}

	b.cidrCollections = make(map[string]*CidrCollection, len(snap.CidrCollections))

	for id, col := range snap.CidrCollections {
		cp := *col
		b.cidrCollections[id] = &cp
	}

	b.queryLoggingConfigs = make(map[string]*QueryLoggingConfig, len(snap.QueryLoggingConfigs))

	for id, cfg := range snap.QueryLoggingConfigs {
		cp := *cfg
		b.queryLoggingConfigs[id] = &cp
	}

	b.reusableDelegationSets = make(map[string]*ReusableDelegationSet, len(snap.ReusableDelegationSets))

	for id, ds := range snap.ReusableDelegationSets {
		cp := *ds
		b.reusableDelegationSets[id] = &cp
	}
}

// restoreAssocMaps restores VPC association, authorization, and change maps from a snapshot.
func (b *InMemoryBackend) restoreAssocMaps(snap *backendSnapshot) {
	b.vpcAssociations = make(map[string][]vpcAssociation, len(snap.VPCAssociations))

	for id, assocs := range snap.VPCAssociations {
		b.vpcAssociations[id] = append([]vpcAssociation(nil), assocs...)
	}

	b.vpcAssocAuthorizations = make(map[string][]VPCAssociationAuthorization, len(snap.VPCAssocAuthorizations))

	for id, auths := range snap.VPCAssocAuthorizations {
		b.vpcAssocAuthorizations[id] = append([]VPCAssociationAuthorization(nil), auths...)
	}

	b.changes = make(map[string]*ChangeInfo, len(snap.Changes))

	for id, ch := range snap.Changes {
		cp := *ch
		b.changes[id] = &cp
	}
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
// The DNS registrar is not restored — it must be re-wired by the caller after restore.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "route53", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.zones = make(map[string]*zoneData, len(snap.Zones))

	for id, zds := range snap.Zones {
		if zds.Records == nil {
			zds.Records = make(map[string]*ResourceRecordSet)
		}

		b.zones[id] = &zoneData{
			zone:          zds.Zone,
			records:       zds.Records,
			dnssecEnabled: zds.DNSSECEnabled,
		}
	}

	b.restoreSimpleMaps(&snap)

	b.trafficPolicies = make(map[string][]*TrafficPolicy, len(snap.TrafficPolicies))

	for id, versions := range snap.TrafficPolicies {
		versionsCopy := make([]*TrafficPolicy, len(versions))
		for i, tp := range versions {
			cp := *tp
			versionsCopy[i] = &cp
		}

		b.trafficPolicies[id] = versionsCopy
	}

	b.trafficPolicyInstances = make(map[string]*TrafficPolicyInstance, len(snap.TrafficPolicyInstances))

	for id, inst := range snap.TrafficPolicyInstances {
		cp := *inst
		b.trafficPolicyInstances[id] = &cp
	}

	b.restoreAssocMaps(&snap)

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

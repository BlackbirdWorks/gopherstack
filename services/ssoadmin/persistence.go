package ssoadmin

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Instances               map[string]*Instance                                    `json:"instances"`
	PermissionSets          map[string]*PermissionSet                               `json:"permissionSets"`
	Assignments             map[string][]*AccountAssignment                         `json:"assignments"`
	CreationStatuses        map[string]*ProvisioningStatus                          `json:"creationStatuses"`
	DeletionStatuses        map[string]*ProvisioningStatus                          `json:"deletionStatuses"`
	ProvisioningStatuses    map[string]*ProvisioningStatus                          `json:"provisioningStatuses"`
	InstanceRegions         map[string][]string                                     `json:"instanceRegions"`
	CustomerManagedPolicies map[string][]CustomerManagedPolicyReference             `json:"customerManagedPolicies"`
	Applications            map[string]*Application                                 `json:"applications"`
	ApplicationAssignments  map[string][]*ApplicationAssignment                     `json:"applicationAssignments"`
	ApplicationScopes       map[string][]string                                     `json:"applicationScopes"`
	ApplicationAuthMethods  map[string][]string                                     `json:"applicationAuthMethods"`
	ApplicationGrants       map[string][]string                                     `json:"applicationGrants"`
	ApplicationAssignConfig map[string]bool                                         `json:"applicationAssignConfig"`
	ApplicationSessions     map[string]string                                       `json:"applicationSessions"`
	InstanceACAs            map[string]*InstanceAccessControlAttributeConfiguration `json:"instanceACAs"`
	TrustedTokenIssuers     map[string]*TrustedTokenIssuer                          `json:"trustedTokenIssuers"`
	PermissionBoundaries    map[string]string                                       `json:"permissionBoundaries"`
	AccountID               string                                                  `json:"accountID"`
	Region                  string                                                  `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:               b.instances,
		PermissionSets:          b.permissionSets,
		Assignments:             b.assignments,
		CreationStatuses:        b.creationStatuses,
		DeletionStatuses:        b.deletionStatuses,
		ProvisioningStatuses:    b.provisioningStatuses,
		InstanceRegions:         b.instanceRegions,
		CustomerManagedPolicies: b.customerManagedPolicies,
		Applications:            b.applications,
		ApplicationAssignments:  b.applicationAssignments,
		ApplicationScopes:       b.applicationScopes,
		ApplicationAuthMethods:  b.applicationAuthMethods,
		ApplicationGrants:       b.applicationGrants,
		ApplicationAssignConfig: b.applicationAssignConfig,
		ApplicationSessions:     b.applicationSessions,
		InstanceACAs:            b.instanceACAs,
		TrustedTokenIssuers:     b.trustedTokenIssuers,
		PermissionBoundaries:    b.permissionBoundaries,
		AccountID:               b.accountID,
		Region:                  b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("ssoadmin: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilTagMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.instances = snap.Instances
	b.permissionSets = snap.PermissionSets
	b.assignments = snap.Assignments
	b.creationStatuses = snap.CreationStatuses
	b.deletionStatuses = snap.DeletionStatuses
	b.provisioningStatuses = snap.ProvisioningStatuses
	b.instanceRegions = snap.InstanceRegions
	b.customerManagedPolicies = snap.CustomerManagedPolicies
	b.applications = snap.Applications
	b.applicationAssignments = snap.ApplicationAssignments
	b.applicationScopes = snap.ApplicationScopes
	b.applicationAuthMethods = snap.ApplicationAuthMethods
	b.applicationGrants = snap.ApplicationGrants
	b.applicationAssignConfig = snap.ApplicationAssignConfig
	b.applicationSessions = snap.ApplicationSessions
	b.instanceACAs = snap.InstanceACAs
	b.trustedTokenIssuers = snap.TrustedTokenIssuers
	b.permissionBoundaries = snap.PermissionBoundaries
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	snap.Instances = ensureMap(snap.Instances)
	snap.PermissionSets = ensureMap(snap.PermissionSets)
	snap.Assignments = ensureMap(snap.Assignments)
	snap.CreationStatuses = ensureMap(snap.CreationStatuses)
	snap.DeletionStatuses = ensureMap(snap.DeletionStatuses)
	snap.ProvisioningStatuses = ensureMap(snap.ProvisioningStatuses)
	snap.InstanceRegions = ensureMap(snap.InstanceRegions)
	snap.CustomerManagedPolicies = ensureMap(snap.CustomerManagedPolicies)
	snap.Applications = ensureMap(snap.Applications)
	snap.ApplicationAssignments = ensureMap(snap.ApplicationAssignments)
	snap.ApplicationScopes = ensureMap(snap.ApplicationScopes)
	snap.ApplicationAuthMethods = ensureMap(snap.ApplicationAuthMethods)
	snap.ApplicationGrants = ensureMap(snap.ApplicationGrants)
	snap.ApplicationAssignConfig = ensureMap(snap.ApplicationAssignConfig)
	snap.ApplicationSessions = ensureMap(snap.ApplicationSessions)
	snap.InstanceACAs = ensureMap(snap.InstanceACAs)
	snap.TrustedTokenIssuers = ensureMap(snap.TrustedTokenIssuers)
	snap.PermissionBoundaries = ensureMap(snap.PermissionBoundaries)
}

func ensureMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return make(map[K]V)
	}

	return m
}

// fixNilTagMaps ensures restored resources have non-nil tag maps.
func fixNilTagMaps(snap *backendSnapshot) {
	for _, inst := range snap.Instances {
		if inst.Tags == nil {
			inst.Tags = make(map[string]string)
		}
	}

	for _, ps := range snap.PermissionSets {
		if ps.Tags == nil {
			ps.Tags = make(map[string]string)
		}
	}

	for _, app := range snap.Applications {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}
	}

	for _, tti := range snap.TrustedTokenIssuers {
		if tti.Tags == nil {
			tti.Tags = make(map[string]string)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}

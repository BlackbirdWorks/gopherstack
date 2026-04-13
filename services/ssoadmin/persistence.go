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
	InstanceACAs            map[string]*InstanceAccessControlAttributeConfiguration `json:"instanceACAs"`
	TrustedTokenIssuers     map[string]*TrustedTokenIssuer                          `json:"trustedTokenIssuers"`
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
		InstanceACAs:            b.instanceACAs,
		TrustedTokenIssuers:     b.trustedTokenIssuers,
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
	b.instanceACAs = snap.InstanceACAs
	b.trustedTokenIssuers = snap.TrustedTokenIssuers
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Instances == nil {
		snap.Instances = make(map[string]*Instance)
	}

	if snap.PermissionSets == nil {
		snap.PermissionSets = make(map[string]*PermissionSet)
	}

	if snap.Assignments == nil {
		snap.Assignments = make(map[string][]*AccountAssignment)
	}

	if snap.CreationStatuses == nil {
		snap.CreationStatuses = make(map[string]*ProvisioningStatus)
	}

	if snap.DeletionStatuses == nil {
		snap.DeletionStatuses = make(map[string]*ProvisioningStatus)
	}

	if snap.ProvisioningStatuses == nil {
		snap.ProvisioningStatuses = make(map[string]*ProvisioningStatus)
	}

	if snap.InstanceRegions == nil {
		snap.InstanceRegions = make(map[string][]string)
	}

	if snap.CustomerManagedPolicies == nil {
		snap.CustomerManagedPolicies = make(map[string][]CustomerManagedPolicyReference)
	}

	if snap.Applications == nil {
		snap.Applications = make(map[string]*Application)
	}

	if snap.ApplicationAssignments == nil {
		snap.ApplicationAssignments = make(map[string][]*ApplicationAssignment)
	}

	if snap.ApplicationScopes == nil {
		snap.ApplicationScopes = make(map[string][]string)
	}

	if snap.ApplicationAuthMethods == nil {
		snap.ApplicationAuthMethods = make(map[string][]string)
	}

	if snap.InstanceACAs == nil {
		snap.InstanceACAs = make(map[string]*InstanceAccessControlAttributeConfiguration)
	}

	if snap.TrustedTokenIssuers == nil {
		snap.TrustedTokenIssuers = make(map[string]*TrustedTokenIssuer)
	}
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
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}

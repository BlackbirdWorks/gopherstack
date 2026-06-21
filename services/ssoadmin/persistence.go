package ssoadmin

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type backendSnapshot struct {
	Instances               map[string]*Instance                        `json:"instances"`
	PermissionSets          map[string]*PermissionSet                   `json:"permissionSets"`
	Assignments             map[string][]*AccountAssignment             `json:"assignments"`
	AssignmentCreationIDs   map[string]string                           `json:"assignmentCreationIDs"`
	CreationStatuses        map[string]*ProvisioningStatus              `json:"creationStatuses"`
	DeletionStatuses        map[string]*ProvisioningStatus              `json:"deletionStatuses"`
	ProvisioningStatuses    map[string]*ProvisioningStatus              `json:"provisioningStatuses"`
	InstanceRegions         map[string][]RegionMetadata                 `json:"instanceRegions"`
	CustomerManagedPolicies map[string][]CustomerManagedPolicyReference `json:"customerManagedPolicies"`
	Applications            map[string]*Application                     `json:"applications"`
	ApplicationAssignments  map[string][]*ApplicationAssignment         `json:"applicationAssignments"`
	ApplicationScopes       map[string][]string                         `json:"applicationScopes"`
	ApplicationAuthMethods  map[string]map[string]json.RawMessage       `json:"applicationAuthMethods"`
	ApplicationGrants       map[string]map[string]json.RawMessage       `json:"applicationGrants"`
	ApplicationAssignConfig map[string]bool                             `json:"applicationAssignConfig"`
	ApplicationSessions     map[string]string                           `json:"applicationSessions"`
	InstanceACAs            map[string]*ABACConfig                      `json:"instanceACAs"`
	TrustedTokenIssuers     map[string]*TrustedTokenIssuer              `json:"trustedTokenIssuers"`
	PermissionBoundaries    map[string]*PermissionsBoundary             `json:"permissionBoundaries"`
	AccountID               string                                      `json:"accountID"`
	Region                  string                                      `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Instances:               b.instances,
		PermissionSets:          b.permissionSets,
		Assignments:             b.assignments,
		AssignmentCreationIDs:   b.assignmentCreationIDs,
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
		logger.Load(ctx).WarnContext(ctx, "ssoadmin: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
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
	b.assignmentCreationIDs = snap.AssignmentCreationIDs
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
	snap.Instances = ensureNonNilMap(snap.Instances)
	snap.PermissionSets = ensureNonNilMap(snap.PermissionSets)
	snap.Assignments = ensureNonNilMap(snap.Assignments)
	snap.AssignmentCreationIDs = ensureNonNilMap(snap.AssignmentCreationIDs)
	snap.CreationStatuses = ensureNonNilMap(snap.CreationStatuses)
	snap.DeletionStatuses = ensureNonNilMap(snap.DeletionStatuses)
	snap.ProvisioningStatuses = ensureNonNilMap(snap.ProvisioningStatuses)
	snap.InstanceRegions = ensureNonNilMap(snap.InstanceRegions)
	snap.CustomerManagedPolicies = ensureNonNilMap(snap.CustomerManagedPolicies)
	snap.Applications = ensureNonNilMap(snap.Applications)
	snap.ApplicationAssignments = ensureNonNilMap(snap.ApplicationAssignments)
	snap.ApplicationScopes = ensureNonNilMap(snap.ApplicationScopes)
	snap.ApplicationAuthMethods = ensureNonNilMap(snap.ApplicationAuthMethods)
	snap.ApplicationGrants = ensureNonNilMap(snap.ApplicationGrants)
	snap.ApplicationAssignConfig = ensureNonNilMap(snap.ApplicationAssignConfig)
	snap.ApplicationSessions = ensureNonNilMap(snap.ApplicationSessions)
	snap.InstanceACAs = ensureNonNilMap(snap.InstanceACAs)
	snap.TrustedTokenIssuers = ensureNonNilMap(snap.TrustedTokenIssuers)
	snap.PermissionBoundaries = ensureNonNilMap(snap.PermissionBoundaries)
}

func ensureNonNilMap[K comparable, V any](m map[K]V) map[K]V {
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
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

package serverlessrepo

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Applications    map[string]*Application                        `json:"applications"`
	AppVersions     map[string]map[string]*ApplicationVersion      `json:"appVersions"`
	CFTemplates     map[string]map[string]*CloudFormationTemplate  `json:"cfTemplates"`
	CFChangeSets    map[string]map[string]*CloudFormationChangeSet `json:"cfChangeSets"`
	AppPolicies     map[string][]*ApplicationPolicyStatement       `json:"appPolicies"`
	AppDependencies map[string]map[string][]*ApplicationDependency `json:"appDependencies"`
	AccountID       string                                         `json:"accountID"`
	Region          string                                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy policy statements to prevent shared-pointer mutations after snapshot.
	appPoliciesCopy := make(map[string][]*ApplicationPolicyStatement, len(b.appPolicies))
	for appName, stmts := range b.appPolicies {
		appPoliciesCopy[appName] = clonePolicyStatements(stmts)
	}

	snap := backendSnapshot{
		Applications:    b.applications,
		AppVersions:     b.appVersions,
		CFTemplates:     b.cfTemplates,
		CFChangeSets:    b.cfChangeSets,
		AppPolicies:     appPoliciesCopy,
		AppDependencies: b.appDependencies,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "serverlessrepo: failed to marshal snapshot", "error", err)
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "serverlessrepo", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureNonNilMaps(&snap)

	b.applications = snap.Applications
	b.appVersions = snap.AppVersions
	b.cfTemplates = snap.CFTemplates
	b.cfChangeSets = snap.CFChangeSets
	b.appPolicies = snap.AppPolicies
	b.appDependencies = snap.AppDependencies
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Applications == nil {
		snap.Applications = make(map[string]*Application)
	}

	ensureNonNilVersionMaps(snap)
	ensureNonNilTemplateMaps(snap)
	ensureNonNilChangeSetMaps(snap)

	if snap.AppPolicies == nil {
		snap.AppPolicies = make(map[string][]*ApplicationPolicyStatement)
	}

	if snap.AppDependencies == nil {
		snap.AppDependencies = make(map[string]map[string][]*ApplicationDependency)
	}
}

func ensureNonNilVersionMaps(snap *backendSnapshot) {
	if snap.AppVersions == nil {
		snap.AppVersions = make(map[string]map[string]*ApplicationVersion)
	}

	for appName, versions := range snap.AppVersions {
		if versions == nil {
			snap.AppVersions[appName] = make(map[string]*ApplicationVersion)
		}

		for _, v := range versions {
			if v != nil && v.ParameterDefinitions == nil {
				v.ParameterDefinitions = []ParameterDefinition{}
			}

			if v != nil && v.RequiredCapabilities == nil {
				v.RequiredCapabilities = []string{}
			}
		}
	}
}

func ensureNonNilTemplateMaps(snap *backendSnapshot) {
	if snap.CFTemplates == nil {
		snap.CFTemplates = make(map[string]map[string]*CloudFormationTemplate)
	}

	for appName, templates := range snap.CFTemplates {
		if templates == nil {
			snap.CFTemplates[appName] = make(map[string]*CloudFormationTemplate)
		}
	}
}

func ensureNonNilChangeSetMaps(snap *backendSnapshot) {
	if snap.CFChangeSets == nil {
		snap.CFChangeSets = make(map[string]map[string]*CloudFormationChangeSet)
	}

	for appName, changeSets := range snap.CFChangeSets {
		if changeSets == nil {
			snap.CFChangeSets[appName] = make(map[string]*CloudFormationChangeSet)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

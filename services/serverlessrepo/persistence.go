package serverlessrepo

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Applications map[string]*Application                        `json:"applications"`
	AppVersions  map[string]map[string]*ApplicationVersion      `json:"appVersions"`
	CFTemplates  map[string]map[string]*CloudFormationTemplate  `json:"cfTemplates"`
	CFChangeSets map[string]map[string]*CloudFormationChangeSet `json:"cfChangeSets"`
	AppPolicies  map[string][]*ApplicationPolicyStatement       `json:"appPolicies"`
	AccountID    string                                         `json:"accountID"`
	Region       string                                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Applications: b.applications,
		AppVersions:  b.appVersions,
		CFTemplates:  b.cfTemplates,
		CFChangeSets: b.cfChangeSets,
		AppPolicies:  b.appPolicies,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("serverlessrepo: failed to marshal snapshot", "error", err)
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
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
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Applications == nil {
		snap.Applications = make(map[string]*Application)
	}

	if snap.AppVersions == nil {
		snap.AppVersions = make(map[string]map[string]*ApplicationVersion)
	}

	if snap.CFTemplates == nil {
		snap.CFTemplates = make(map[string]map[string]*CloudFormationTemplate)
	}

	if snap.CFChangeSets == nil {
		snap.CFChangeSets = make(map[string]map[string]*CloudFormationChangeSet)
	}

	if snap.AppPolicies == nil {
		snap.AppPolicies = make(map[string][]*ApplicationPolicyStatement)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }

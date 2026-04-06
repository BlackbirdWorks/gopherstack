package codedeploy

import "encoding/json"

// backendSnapshot is the JSON-serialisable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Applications        map[string]*Application                `json:"applications"`
	DeploymentGroups    map[string]map[string]*DeploymentGroup `json:"deploymentGroups"`
	Deployments         map[string]*Deployment                 `json:"deployments"`
	OnPremisesInstances map[string]*OnPremisesInstance         `json:"onPremisesInstances"`
	DeploymentConfigs   map[string]*DeploymentConfig           `json:"deploymentConfigs"`
	AccountID           string                                 `json:"accountID"`
	Region              string                                 `json:"region"`
}

// ensureNonNil initialises any nil maps so callers do not need to guard after Restore.
func (s *backendSnapshot) ensureNonNil() {
	if s.Applications == nil {
		s.Applications = make(map[string]*Application)
	}

	if s.DeploymentGroups == nil {
		s.DeploymentGroups = make(map[string]map[string]*DeploymentGroup)
	}

	if s.Deployments == nil {
		s.Deployments = make(map[string]*Deployment)
	}

	if s.OnPremisesInstances == nil {
		s.OnPremisesInstances = make(map[string]*OnPremisesInstance)
	}

	if s.DeploymentConfigs == nil {
		s.DeploymentConfigs = make(map[string]*DeploymentConfig)
	}
}

// Snapshot serialises the backend state to JSON. Returns nil on marshal failure.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Applications:        b.applications,
		DeploymentGroups:    b.deploymentGroups,
		Deployments:         b.deployments,
		OnPremisesInstances: b.onPremisesInstances,
		DeploymentConfigs:   b.deploymentConfigs,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.applications = snap.Applications
	b.deploymentGroups = snap.DeploymentGroups
	b.deployments = snap.Deployments
	b.onPremisesInstances = snap.OnPremisesInstances
	b.deploymentConfigs = snap.DeploymentConfigs
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }

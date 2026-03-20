package elasticbeanstalk

import "encoding/json"

type backendSnapshot struct {
	Applications map[string]*Application        `json:"applications"`
	Environments map[string]*Environment        `json:"environments"`
	AppVersions  map[string]*ApplicationVersion `json:"appVersions"`
	AccountID    string                         `json:"accountID"`
	Region       string                         `json:"region"`
	EnvCounter   int                            `json:"envCounter"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Applications: b.applications,
		Environments: b.environments,
		AppVersions:  b.appVersions,
		AccountID:    b.accountID,
		Region:       b.region,
		EnvCounter:   b.envCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Applications == nil {
		snap.Applications = make(map[string]*Application)
	}

	if snap.Environments == nil {
		snap.Environments = make(map[string]*Environment)
	}

	if snap.AppVersions == nil {
		snap.AppVersions = make(map[string]*ApplicationVersion)
	}

	b.applications = snap.Applications
	b.environments = snap.Environments
	b.appVersions = snap.AppVersions
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.envCounter = snap.EnvCounter

	b.appARNIndex = make(map[string]string, len(b.applications))
	b.envARNIndex = make(map[string]string, len(b.environments))
	b.verARNIndex = make(map[string]string, len(b.appVersions))

	for name, app := range b.applications {
		b.appARNIndex[app.ApplicationARN] = name
	}

	for key, env := range b.environments {
		b.envARNIndex[env.EnvironmentARN] = key
	}

	for key, ver := range b.appVersions {
		b.verARNIndex[ver.ApplicationVersionARN] = key
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}

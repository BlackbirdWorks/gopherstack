package elasticbeanstalk

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Applications         map[string]map[string]*Application            `json:"applications"`
	Environments         map[string]map[string]*Environment            `json:"environments"`
	AppVersions          map[string]map[string]*ApplicationVersion     `json:"appVersions"`
	ConfigTemplates      map[string]map[string]*ConfigurationTemplate  `json:"configTemplates"`
	PlatformVersions     map[string]map[string]*PlatformVersion        `json:"platformVersions"`
	ManagedActionHistory map[string]map[string][]*ManagedActionHistory `json:"managedActionHistory,omitempty"`
	Events               map[string][]*EventRecord                     `json:"events,omitempty"`
	EnvCounters          map[string]int                                `json:"envCounters,omitempty"`
	AccountID            string                                        `json:"accountID"`
	Region               string                                        `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Applications:         b.applications,
		Environments:         b.environments,
		AppVersions:          b.appVersions,
		ConfigTemplates:      b.configTemplates,
		PlatformVersions:     b.platformVersions,
		ManagedActionHistory: b.managedActionHistory,
		Events:               b.events,
		EnvCounters:          b.envCounters,
		AccountID:            b.accountID,
		Region:               b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("elasticbeanstalk: failed to snapshot backend state", "error", err)

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
		snap.Applications = make(map[string]map[string]*Application)
	}

	if snap.Environments == nil {
		snap.Environments = make(map[string]map[string]*Environment)
	}

	if snap.AppVersions == nil {
		snap.AppVersions = make(map[string]map[string]*ApplicationVersion)
	}

	if snap.ConfigTemplates == nil {
		snap.ConfigTemplates = make(map[string]map[string]*ConfigurationTemplate)
	}

	if snap.PlatformVersions == nil {
		snap.PlatformVersions = make(map[string]map[string]*PlatformVersion)
	}

	if snap.ManagedActionHistory == nil {
		snap.ManagedActionHistory = make(map[string]map[string][]*ManagedActionHistory)
	}

	if snap.Events == nil {
		snap.Events = make(map[string][]*EventRecord)
	}

	if snap.EnvCounters == nil {
		snap.EnvCounters = make(map[string]int)
	}

	b.applications = snap.Applications
	b.environments = snap.Environments
	b.appVersions = snap.AppVersions
	b.configTemplates = snap.ConfigTemplates
	b.platformVersions = snap.PlatformVersions
	b.managedActionHistory = snap.ManagedActionHistory
	b.events = snap.Events
	b.envCounters = snap.EnvCounters
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildARNIndexes()

	return nil
}

// rebuildARNIndexes reconstructs the in-memory ARN lookup indexes from restored data.
func (b *InMemoryBackend) rebuildARNIndexes() {
	b.appARNIndex = make(map[string]map[string]string)
	b.envARNIndex = make(map[string]map[string]string)
	b.verARNIndex = make(map[string]map[string]string)

	for region, apps := range b.applications {
		b.appARNIndex[region] = make(map[string]string, len(apps))
		for name, app := range apps {
			b.appARNIndex[region][app.ApplicationARN] = name
		}
	}

	for region, envs := range b.environments {
		b.envARNIndex[region] = make(map[string]string, len(envs))
		for key, env := range envs {
			b.envARNIndex[region][env.EnvironmentARN] = key
		}
	}

	for region, vers := range b.appVersions {
		b.verARNIndex[region] = make(map[string]string, len(vers))
		for key, ver := range vers {
			b.verARNIndex[region][ver.ApplicationVersionARN] = key
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

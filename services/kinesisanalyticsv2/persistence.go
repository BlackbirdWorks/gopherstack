package kinesisanalyticsv2

import (
	"encoding/json"
	"log/slog"
	"maps"
	"time"
)

// persistedApplication is a persistence-safe representation of Application,
// including fields that use json:"-" on the live struct.
type persistedApplication struct {
	CreatedAt                       time.Time                        `json:"created_at"`
	ApplicationARN                  string                           `json:"ApplicationARN"`
	ApplicationName                 string                           `json:"ApplicationName"`
	ApplicationStatus               string                           `json:"ApplicationStatus"`
	RuntimeEnvironment              string                           `json:"RuntimeEnvironment"`
	ServiceExecutionRole            string                           `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription          string                           `json:"ApplicationDescription,omitempty"`
	ApplicationMode                 string                           `json:"ApplicationMode,omitempty"`
	Tags                            []Tag                            `json:"Tags"`
	CloudWatchLoggingOptionDescs    []CloudWatchLoggingOptionDesc    `json:"CloudWatchLoggingOptionDescs"`
	InputDescriptions               []InputDescription               `json:"InputDescriptions"`
	OutputDescriptions              []OutputDescription              `json:"OutputDescriptions"`
	ReferenceDataSourceDescriptions []ReferenceDataSourceDescription `json:"ReferenceDataSourceDescriptions"`
	VpcConfigurationDescriptions    []VpcConfigurationDescription    `json:"VpcConfigurationDescriptions"`
	ApplicationVersionID            int64                            `json:"ApplicationVersionId"`
}

// persistedSnapshot is a persistence-safe representation of Snapshot.
type persistedSnapshot struct {
	SnapshotCreation   time.Time `json:"snapshot_creation"`
	ApplicationARN     string    `json:"ApplicationARN"`
	SnapshotName       string    `json:"SnapshotName"`
	SnapshotStatus     string    `json:"SnapshotStatus"`
	ApplicationVersion int64     `json:"ApplicationVersionId"`
}

func toPersistedApp(app *Application) persistedApplication {
	return persistedApplication{
		CreatedAt:                       app.CreatedAt,
		ApplicationARN:                  app.ApplicationARN,
		ApplicationName:                 app.ApplicationName,
		ApplicationStatus:               app.ApplicationStatus,
		RuntimeEnvironment:              app.RuntimeEnvironment,
		ServiceExecutionRole:            app.ServiceExecutionRole,
		ApplicationDescription:          app.ApplicationDescription,
		ApplicationMode:                 app.ApplicationMode,
		Tags:                            cloneTags(app.Tags),
		CloudWatchLoggingOptionDescs:    copyCWLOptions(app.CloudWatchLoggingOptionDescs),
		InputDescriptions:               copyInputDescs(app.InputDescriptions),
		OutputDescriptions:              copyOutputDescs(app.OutputDescriptions),
		ReferenceDataSourceDescriptions: copyRefDataSources(app.ReferenceDataSourceDescriptions),
		VpcConfigurationDescriptions:    copyVpcConfigs(app.VpcConfigurationDescriptions),
		ApplicationVersionID:            app.ApplicationVersionID,
	}
}

// ensureNonNil returns the slice if non-nil, otherwise returns an empty slice of the same type.
func ensureNonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func fromPersistedApp(p persistedApplication) *Application {
	return &Application{
		CreatedAt:                       p.CreatedAt,
		ApplicationARN:                  p.ApplicationARN,
		ApplicationName:                 p.ApplicationName,
		ApplicationStatus:               p.ApplicationStatus,
		RuntimeEnvironment:              p.RuntimeEnvironment,
		ServiceExecutionRole:            p.ServiceExecutionRole,
		ApplicationDescription:          p.ApplicationDescription,
		ApplicationMode:                 p.ApplicationMode,
		ApplicationVersionID:            p.ApplicationVersionID,
		Tags:                            ensureNonNil(p.Tags),
		CloudWatchLoggingOptionDescs:    ensureNonNil(p.CloudWatchLoggingOptionDescs),
		InputDescriptions:               ensureNonNil(p.InputDescriptions),
		OutputDescriptions:              ensureNonNil(p.OutputDescriptions),
		ReferenceDataSourceDescriptions: ensureNonNil(p.ReferenceDataSourceDescriptions),
		VpcConfigurationDescriptions:    ensureNonNil(p.VpcConfigurationDescriptions),
	}
}

func toPersistedSnap(s *Snapshot) persistedSnapshot {
	return persistedSnapshot{
		SnapshotCreation:   s.SnapshotCreation,
		ApplicationARN:     s.ApplicationARN,
		SnapshotName:       s.SnapshotName,
		SnapshotStatus:     s.SnapshotStatus,
		ApplicationVersion: s.ApplicationVersion,
	}
}

func fromPersistedSnap(p persistedSnapshot) *Snapshot {
	return &Snapshot{
		SnapshotCreation:   p.SnapshotCreation,
		ApplicationARN:     p.ApplicationARN,
		SnapshotName:       p.SnapshotName,
		SnapshotStatus:     p.SnapshotStatus,
		ApplicationVersion: p.ApplicationVersion,
	}
}

// backendSnapshot is the persisted form of the backend state. All resource maps are
// nested by region (outer key = region) to mirror the in-memory layout and keep
// same-named resources in different regions fully isolated across restarts.
type backendSnapshot struct {
	Applications    map[string]map[string]persistedApplication `json:"applications"`
	ApplicationARNs map[string]map[string]string               `json:"application_arns"`
	Snapshots       map[string]map[string][]persistedSnapshot  `json:"snapshots"`
	NextID          int64                                      `json:"next_id"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	appsCopy := make(map[string]map[string]persistedApplication, len(b.applications))
	for region, regionApps := range b.applications {
		regionCopy := make(map[string]persistedApplication, len(regionApps))
		for k, v := range regionApps {
			regionCopy[k] = toPersistedApp(v)
		}
		appsCopy[region] = regionCopy
	}

	arnCopy := make(map[string]map[string]string, len(b.applicationARNs))
	for region, regionARNs := range b.applicationARNs {
		regionCopy := make(map[string]string, len(regionARNs))
		maps.Copy(regionCopy, regionARNs)
		arnCopy[region] = regionCopy
	}

	snapsCopy := make(map[string]map[string][]persistedSnapshot, len(b.snapshots))
	for region, regionSnaps := range b.snapshots {
		regionCopy := make(map[string][]persistedSnapshot, len(regionSnaps))
		for k, v := range regionSnaps {
			sl := make([]persistedSnapshot, len(v))
			for i, s := range v {
				sl[i] = toPersistedSnap(s)
			}
			regionCopy[k] = sl
		}
		snapsCopy[region] = regionCopy
	}

	snap := backendSnapshot{
		Applications:    appsCopy,
		ApplicationARNs: arnCopy,
		Snapshots:       snapsCopy,
		NextID:          b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("kinesisanalyticsv2: failed to marshal snapshot", "error", err)

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

	apps := make(map[string]map[string]*Application, len(snap.Applications))
	for region, regionApps := range snap.Applications {
		regionLive := make(map[string]*Application, len(regionApps))
		for k, v := range regionApps {
			regionLive[k] = fromPersistedApp(v)
		}
		apps[region] = regionLive
	}

	snapshots := make(map[string]map[string][]*Snapshot, len(snap.Snapshots))
	for region, regionSnaps := range snap.Snapshots {
		regionLive := make(map[string][]*Snapshot, len(regionSnaps))
		for k, v := range regionSnaps {
			sl := make([]*Snapshot, len(v))
			for i, s := range v {
				sl[i] = fromPersistedSnap(s)
			}
			regionLive[k] = sl
		}
		snapshots[region] = regionLive
	}

	arnIndex := snap.ApplicationARNs
	if arnIndex == nil {
		arnIndex = make(map[string]map[string]string)
	}

	b.applications = apps
	b.applicationARNs = arnIndex
	b.snapshots = snapshots
	b.nextID = snap.NextID

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	if sb, ok := h.Backend.(*InMemoryBackend); ok {
		return sb.Restore(data)
	}

	return nil
}

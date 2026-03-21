package codebuild

import "encoding/json"

type backendSnapshot struct {
	Projects        map[string]*Project            `json:"projects"`
	Builds          map[string]*Build              `json:"builds"`
	BuildsByProject map[string]map[string]struct{} `json:"buildsByProject"`
	ProjectARNIndex map[string]string              `json:"projectArnIndex"`
	BuildARNIndex   map[string]string              `json:"buildArnIndex"`
	AccountID       string                         `json:"accountID"`
	Region          string                         `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Projects:        b.projects,
		Builds:          b.builds,
		BuildsByProject: b.buildsByProject,
		ProjectARNIndex: b.projectARNIndex,
		BuildARNIndex:   b.buildARNIndex,
		AccountID:       b.accountID,
		Region:          b.region,
	}

	data, _ := json.Marshal(snap)

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

	if snap.Projects == nil {
		snap.Projects = make(map[string]*Project)
	}

	if snap.Builds == nil {
		snap.Builds = make(map[string]*Build)
	}

	if snap.BuildsByProject == nil {
		snap.BuildsByProject = make(map[string]map[string]struct{})
	}

	if snap.ProjectARNIndex == nil {
		snap.ProjectARNIndex = make(map[string]string)
	}

	if snap.BuildARNIndex == nil {
		snap.BuildARNIndex = make(map[string]string)
	}

	b.projects = snap.Projects
	b.builds = snap.Builds
	b.buildsByProject = snap.BuildsByProject
	b.projectARNIndex = snap.ProjectARNIndex
	b.buildARNIndex = snap.BuildARNIndex
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }

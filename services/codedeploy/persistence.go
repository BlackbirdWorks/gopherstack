package codedeploy

import (
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// snapshotApplication is the JSON-serializable form of Application (with tags map).
type snapshotApplication struct {
	Application
	TagsMap map[string]string `json:"tagsMap"`
}

// snapshotDeploymentGroup is the JSON-serializable form of DeploymentGroup (with tags map).
type snapshotDeploymentGroup struct {
	DeploymentGroup
	TagsMap map[string]string `json:"tagsMap"`
}

// snapshotOnPremisesInstance is the JSON-serializable form of OnPremisesInstance (with tags map).
type snapshotOnPremisesInstance struct {
	OnPremisesInstance
	TagsMap map[string]string `json:"tagsMap"`
}

// backendSnapshot is the JSON-serialisable snapshot of InMemoryBackend state.
type backendSnapshot struct {
	Applications        map[string]*snapshotApplication                        `json:"applications"`
	DeploymentGroups    map[string]map[string]*snapshotDeploymentGroup         `json:"deploymentGroups"`
	Deployments         map[string]*Deployment                                 `json:"deployments"`
	OnPremisesInstances map[string]*snapshotOnPremisesInstance                 `json:"onPremisesInstances"`
	DeploymentConfigs   map[string]*DeploymentConfig                           `json:"deploymentConfigs"`
	GitHubTokens        []string                                               `json:"githubTokens,omitempty"`
	AccountID           string                                                 `json:"accountID"`
	Region              string                                                 `json:"region"`
}

// ensureNonNil initialises any nil maps so callers do not need to guard after Restore.
func (s *backendSnapshot) ensureNonNil() {
	if s.Applications == nil {
		s.Applications = make(map[string]*snapshotApplication)
	}

	if s.DeploymentGroups == nil {
		s.DeploymentGroups = make(map[string]map[string]*snapshotDeploymentGroup)
	}

	if s.Deployments == nil {
		s.Deployments = make(map[string]*Deployment)
	}

	if s.OnPremisesInstances == nil {
		s.OnPremisesInstances = make(map[string]*snapshotOnPremisesInstance)
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
		AccountID:           b.accountID,
		Region:              b.region,
		Deployments:         b.deployments,
		DeploymentConfigs:   b.deploymentConfigs,
		Applications:        make(map[string]*snapshotApplication, len(b.applications)),
		DeploymentGroups:    make(map[string]map[string]*snapshotDeploymentGroup, len(b.deploymentGroups)),
		OnPremisesInstances: make(map[string]*snapshotOnPremisesInstance, len(b.onPremisesInstances)),
	}

	for name, app := range b.applications {
		var tagsMap map[string]string
		if app.Tags != nil {
			tagsMap = app.Tags.Clone()
		}

		snap.Applications[name] = &snapshotApplication{
			Application: *app,
			TagsMap:     tagsMap,
		}
	}

	for appName, dgs := range b.deploymentGroups {
		snap.DeploymentGroups[appName] = make(map[string]*snapshotDeploymentGroup, len(dgs))
		for dgName, dg := range dgs {
			var tagsMap map[string]string
			if dg.Tags != nil {
				tagsMap = dg.Tags.Clone()
			}

			snap.DeploymentGroups[appName][dgName] = &snapshotDeploymentGroup{
				DeploymentGroup: *dg,
				TagsMap:         tagsMap,
			}
		}
	}

	for name, inst := range b.onPremisesInstances {
		var tagsMap map[string]string
		if inst.Tags != nil {
			tagsMap = inst.Tags.Clone()
		}

		snap.OnPremisesInstances[name] = &snapshotOnPremisesInstance{
			OnPremisesInstance: *inst,
			TagsMap:            tagsMap,
		}
	}

	for name := range b.githubTokens {
		snap.GitHubTokens = append(snap.GitHubTokens, name)
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

	// Rebuild applications with live tags.Tags from snapshot tag maps.
	apps := make(map[string]*Application, len(snap.Applications))
	for name, sa := range snap.Applications {
		app := sa.Application
		app.Tags = tags.New("codedeploy.application." + name + ".tags")
		if len(sa.TagsMap) > 0 {
			app.Tags.Merge(sa.TagsMap)
		}
		apps[name] = &app
	}

	// Rebuild deployment groups with live tags.Tags.
	dgs := make(map[string]map[string]*DeploymentGroup, len(snap.DeploymentGroups))
	for appName, dgMap := range snap.DeploymentGroups {
		dgs[appName] = make(map[string]*DeploymentGroup, len(dgMap))
		for dgName, sdg := range dgMap {
			dg := sdg.DeploymentGroup
			dg.Tags = tags.New("codedeploy.dg." + appName + "." + dgName + ".tags")
			if len(sdg.TagsMap) > 0 {
				dg.Tags.Merge(sdg.TagsMap)
			}
			dgs[appName][dgName] = &dg
		}
	}

	// Rebuild on-premises instances with live tags.Tags.
	insts := make(map[string]*OnPremisesInstance, len(snap.OnPremisesInstances))
	for name, si := range snap.OnPremisesInstances {
		inst := si.OnPremisesInstance
		inst.Tags = tags.New("codedeploy.onprem." + name + ".tags")
		if len(si.TagsMap) > 0 {
			inst.Tags.Merge(si.TagsMap)
		}
		insts[name] = &inst
	}

	// Rebuild GitHub tokens set.
	githubTokens := make(map[string]struct{}, len(snap.GitHubTokens))
	for _, name := range snap.GitHubTokens {
		githubTokens[name] = struct{}{}
	}

	b.applications = apps
	b.deploymentGroups = dgs
	b.deployments = snap.Deployments
	b.onPremisesInstances = insts
	b.deploymentConfigs = snap.DeploymentConfigs
	b.githubTokens = githubTokens
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }

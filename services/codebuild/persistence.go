package codebuild

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Projects            map[string]*Project            `json:"projects"`
	Builds              map[string]*Build              `json:"builds"`
	BuildsByProject     map[string]map[string]struct{} `json:"buildsByProject"`
	ProjectARNIndex     map[string]string              `json:"projectArnIndex"`
	BuildARNIndex       map[string]string              `json:"buildArnIndex"`
	Fleets              map[string]*Fleet              `json:"fleets"`
	FleetARNIndex       map[string]string              `json:"fleetArnIndex"`
	ReportGroups        map[string]*ReportGroup        `json:"reportGroups"`
	ReportGroupARNIndex map[string]string              `json:"reportGroupArnIndex"`
	Reports             map[string]*Report             `json:"reports"`
	BuildBatches        map[string]*BuildBatch         `json:"buildBatches"`
	CommandExecutions   map[string]*CommandExecution   `json:"commandExecutions"`
	Sandboxes           map[string]*Sandbox            `json:"sandboxes"`
	Webhooks            map[string]*Webhook            `json:"webhooks"`
	ResourcePolicies    map[string]string              `json:"resourcePolicies"`
	SourceCredentials   map[string]*SourceCredentials  `json:"sourceCredentials"`
	SandboxesByProject  map[string]map[string]struct{} `json:"sandboxesByProject"`
	BatchesByProject    map[string]map[string]struct{} `json:"batchesByProject"`
	CommandsBySandbox   map[string]map[string]struct{} `json:"commandsBySandbox"`
	ReportsByGroup      map[string]map[string]struct{} `json:"reportsByGroup"`
	AccountID           string                         `json:"accountID"`
	Region              string                         `json:"region"`
}

// ensureNonNil replaces nil maps in the snapshot with empty initialized maps
// so callers don't need to guard against nil after a Restore.
func (s *backendSnapshot) ensureNonNil() {
	s.ensureNonNilCore()
	s.ensureNonNilExt()
}

func (s *backendSnapshot) ensureNonNilCore() {
	if s.Projects == nil {
		s.Projects = make(map[string]*Project)
	}
	if s.Builds == nil {
		s.Builds = make(map[string]*Build)
	}
	if s.BuildsByProject == nil {
		s.BuildsByProject = make(map[string]map[string]struct{})
	}
	if s.ProjectARNIndex == nil {
		s.ProjectARNIndex = make(map[string]string)
	}
	if s.BuildARNIndex == nil {
		s.BuildARNIndex = make(map[string]string)
	}
	if s.Fleets == nil {
		s.Fleets = make(map[string]*Fleet)
	}
	if s.FleetARNIndex == nil {
		s.FleetARNIndex = make(map[string]string)
	}
	if s.ReportGroups == nil {
		s.ReportGroups = make(map[string]*ReportGroup)
	}
	if s.ReportGroupARNIndex == nil {
		s.ReportGroupARNIndex = make(map[string]string)
	}
	if s.Reports == nil {
		s.Reports = make(map[string]*Report)
	}
}

func (s *backendSnapshot) ensureNonNilExt() {
	if s.BuildBatches == nil {
		s.BuildBatches = make(map[string]*BuildBatch)
	}
	if s.CommandExecutions == nil {
		s.CommandExecutions = make(map[string]*CommandExecution)
	}
	if s.Sandboxes == nil {
		s.Sandboxes = make(map[string]*Sandbox)
	}
	if s.Webhooks == nil {
		s.Webhooks = make(map[string]*Webhook)
	}
	if s.ResourcePolicies == nil {
		s.ResourcePolicies = make(map[string]string)
	}
	if s.SourceCredentials == nil {
		s.SourceCredentials = make(map[string]*SourceCredentials)
	}
	if s.SandboxesByProject == nil {
		s.SandboxesByProject = make(map[string]map[string]struct{})
	}
	if s.BatchesByProject == nil {
		s.BatchesByProject = make(map[string]map[string]struct{})
	}
	if s.CommandsBySandbox == nil {
		s.CommandsBySandbox = make(map[string]map[string]struct{})
	}
	if s.ReportsByGroup == nil {
		s.ReportsByGroup = make(map[string]map[string]struct{})
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Projects:            b.projects,
		Builds:              b.builds,
		BuildsByProject:     b.buildsByProject,
		ProjectARNIndex:     b.projectARNIndex,
		BuildARNIndex:       b.buildARNIndex,
		Fleets:              b.fleets,
		FleetARNIndex:       b.fleetARNIndex,
		ReportGroups:        b.reportGroups,
		ReportGroupARNIndex: b.reportGroupARNIndex,
		Reports:             b.reports,
		BuildBatches:        b.buildBatches,
		CommandExecutions:   b.commandExecutions,
		Sandboxes:           b.sandboxes,
		Webhooks:            b.webhooks,
		ResourcePolicies:    b.resourcePolicies,
		SourceCredentials:   b.sourceCredentials,
		SandboxesByProject:  b.sandboxesByProject,
		BatchesByProject:    b.batchesByProject,
		CommandsBySandbox:   b.commandsBySandbox,
		ReportsByGroup:      b.reportsByGroup,
		AccountID:           b.accountID,
		Region:              b.region,
	}

	return persistence.MarshalSnapshot(ctx, "codebuild", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codebuild", data, &snap); err != nil {
		return err
	}

	snap.ensureNonNil()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.projects = snap.Projects
	b.builds = snap.Builds
	b.buildsByProject = snap.BuildsByProject
	b.projectARNIndex = snap.ProjectARNIndex
	b.buildARNIndex = snap.BuildARNIndex
	b.fleets = snap.Fleets
	b.fleetARNIndex = snap.FleetARNIndex
	b.reportGroups = snap.ReportGroups
	b.reportGroupARNIndex = snap.ReportGroupARNIndex
	b.reports = snap.Reports
	b.buildBatches = snap.BuildBatches
	b.commandExecutions = snap.CommandExecutions
	b.sandboxes = snap.Sandboxes
	b.webhooks = snap.Webhooks
	b.resourcePolicies = snap.ResourcePolicies
	b.sourceCredentials = snap.SourceCredentials
	b.sandboxesByProject = snap.SandboxesByProject
	b.batchesByProject = snap.BatchesByProject
	b.commandsBySandbox = snap.CommandsBySandbox
	b.reportsByGroup = snap.ReportsByGroup
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

package codebuild

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildBuildARN(projectName, buildID string) string {
	return arn.Build("codebuild", b.region, b.accountID, "build/"+projectName+":"+buildID)
}

// StartBuildConfig holds override parameters for a StartBuild call.
type StartBuildConfig struct {
	BuildspecOverride        string
	ComputeTypeOverride      string
	ImageOverride            string
	ServiceRoleOverride      string
	SourceVersion            string
	EnvVarsOverride          []EnvironmentVariable
	TimeoutInMinutesOverride int32
	DebugSessionEnabled      bool
}

// applyBuildOverrides applies a StartBuildConfig to copies of the project's env/source and returns
// the resulting environment, source, service role, and timeout for the new build.
func applyBuildOverrides(proj *Project, cfg StartBuildConfig) (ProjectEnvironment, ProjectSource, string, int32) {
	env := proj.Environment
	src := proj.Source

	if len(cfg.EnvVarsOverride) > 0 {
		merged := make([]EnvironmentVariable, 0, len(env.EnvironmentVariables)+len(cfg.EnvVarsOverride))
		merged = append(merged, env.EnvironmentVariables...)

		for _, ov := range cfg.EnvVarsOverride {
			replaced := false

			for i, ev := range merged {
				if ev.Name == ov.Name {
					merged[i] = ov
					replaced = true

					break
				}
			}

			if !replaced {
				merged = append(merged, ov)
			}
		}

		env.EnvironmentVariables = merged
	}

	if cfg.ComputeTypeOverride != "" {
		env.ComputeType = cfg.ComputeTypeOverride
	}

	if cfg.ImageOverride != "" {
		env.Image = cfg.ImageOverride
	}

	if cfg.BuildspecOverride != "" {
		src.Buildspec = cfg.BuildspecOverride
	}

	if cfg.SourceVersion != "" {
		src.Location = cfg.SourceVersion
	}

	serviceRole := proj.ServiceRole
	if cfg.ServiceRoleOverride != "" {
		serviceRole = cfg.ServiceRoleOverride
	}

	timeoutInMinutes := proj.TimeoutInMinutes
	if cfg.TimeoutInMinutesOverride > 0 {
		timeoutInMinutes = cfg.TimeoutInMinutesOverride
	}

	return env, src, serviceRole, timeoutInMinutes
}

// StartBuild creates a new build for the given project.
// Env var overrides follow real AWS merge semantics: same-name vars are replaced, new ones appended.
func (b *InMemoryBackend) StartBuild(projectName string, cfg StartBuildConfig) (*Build, error) {
	b.mu.Lock("StartBuild")
	defer b.mu.Unlock()

	proj, ok := b.projects.Get(projectName)
	if !ok {
		return nil, ErrNotFound
	}

	buildID := randomID()
	fullID := projectName + ":" + buildID
	now := float64(time.Now().Unix())

	env, src, serviceRole, timeoutInMinutes := applyBuildOverrides(proj, cfg)
	artifacts := proj.Artifacts

	build := &Build{
		ID:                      fullID,
		Arn:                     b.buildBuildARN(projectName, buildID),
		ProjectName:             projectName,
		BuildStatus:             buildStatusInProgress,
		StartTime:               now,
		CurrentPhase:            phaseSubmitted,
		ServiceRole:             serviceRole,
		EncryptionKey:           proj.EncryptionKey,
		TimeoutInMinutes:        timeoutInMinutes,
		QueuedTimeoutInMinutes:  proj.QueuedTimeoutInMinutes,
		Environment:             &env,
		Source:                  &src,
		Artifacts:               &artifacts,
		Cache:                   proj.Cache,
		VpcConfig:               proj.VpcConfig,
		FileSystemLocations:     proj.FileSystemLocations,
		SecondaryArtifacts:      proj.SecondaryArtifacts,
		SecondarySources:        proj.SecondarySources,
		SecondarySourceVersions: proj.SecondarySourceVersions,
		Phases: []BuildPhase{
			{PhaseType: phaseSubmitted, PhaseStatus: "SUCCEEDED", StartTime: now, EndTime: now, DurationInSeconds: 0},
		},
	}
	b.builds.Put(build)

	out := *build

	return &out, nil
}

// BatchGetBuilds returns builds by ID or ARN. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuilds(ids []string) ([]*Build, []string) {
	b.mu.RLock("BatchGetBuilds")
	defer b.mu.RUnlock()

	found := make([]*Build, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		build, ok := b.builds.Get(id)
		if !ok {
			if matches := b.buildsByARN.Get(id); len(matches) > 0 {
				build, ok = matches[0], true
			}
		}

		if ok {
			out := *build
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// StopBuild marks a build as STOPPED.
func (b *InMemoryBackend) StopBuild(id string) (*Build, error) {
	b.mu.Lock("StopBuild")
	defer b.mu.Unlock()

	build, ok := b.builds.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	build.BuildStatus = buildStatusStopped
	build.EndTime = float64(time.Now().Unix())
	build.CurrentPhase = phaseCompleted
	build.BuildComplete = true

	out := *build

	return &out, nil
}

// ListBuilds returns all build IDs in the backend in sorted order.
func (b *InMemoryBackend) ListBuilds() []string {
	b.mu.RLock("ListBuilds")
	defer b.mu.RUnlock()

	items := b.builds.Snapshot()
	ids := make([]string, len(items))

	for i, bd := range items {
		ids[i] = bd.ID
	}

	return ids
}

// BatchDeleteBuilds deletes builds by ID and returns the IDs that were deleted.
func (b *InMemoryBackend) BatchDeleteBuilds(ids []string) []string {
	b.mu.Lock("BatchDeleteBuilds")
	defer b.mu.Unlock()

	deleted := make([]string, 0, len(ids))

	for _, id := range ids {
		if b.builds.Delete(id) {
			deleted = append(deleted, id)
		}
	}

	return deleted
}

// RetryBuild creates a new build for the same project, inheriting configuration from the
// existing build (environment, source, artifacts, role, timeouts) matching real AWS semantics.
func (b *InMemoryBackend) RetryBuild(id string) (*Build, error) {
	b.mu.Lock("RetryBuild")
	defer b.mu.Unlock()

	existing, ok := b.builds.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	projectName := existing.ProjectName
	if !b.projects.Has(projectName) {
		return nil, fmt.Errorf("%w: project %s not found", ErrNotFound, projectName)
	}

	buildID := randomID()
	fullID := projectName + ":" + buildID
	now := float64(time.Now().Unix())

	build := &Build{
		ID:                      fullID,
		Arn:                     b.buildBuildARN(projectName, buildID),
		ProjectName:             projectName,
		BuildStatus:             buildStatusInProgress,
		StartTime:               now,
		CurrentPhase:            phaseSubmitted,
		ServiceRole:             existing.ServiceRole,
		EncryptionKey:           existing.EncryptionKey,
		TimeoutInMinutes:        existing.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  existing.QueuedTimeoutInMinutes,
		Environment:             existing.Environment,
		Source:                  existing.Source,
		Artifacts:               existing.Artifacts,
		Cache:                   existing.Cache,
		VpcConfig:               existing.VpcConfig,
		FileSystemLocations:     existing.FileSystemLocations,
		SecondaryArtifacts:      existing.SecondaryArtifacts,
		SecondarySources:        existing.SecondarySources,
		SecondarySourceVersions: existing.SecondarySourceVersions,
		Phases: []BuildPhase{
			{PhaseType: phaseSubmitted, PhaseStatus: "SUCCEEDED", StartTime: now, EndTime: now},
		},
	}
	b.builds.Put(build)

	out := *build

	return &out, nil
}

// ListBuildsForProject returns all build IDs for a given project in sorted order.
func (b *InMemoryBackend) ListBuildsForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListBuildsForProject")
	defer b.mu.RUnlock()

	if !b.projects.Has(projectName) {
		return nil, ErrNotFound
	}

	group := b.buildsByProject.Get(projectName)
	ids := make([]string, len(group))

	for i, bd := range group {
		ids[i] = bd.ID
	}

	sort.Strings(ids)

	return ids, nil
}

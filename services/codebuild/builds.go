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

// StartBuildConfig holds override parameters for a StartBuild call, mirroring
// aws-sdk-go-v2/service/codebuild@v1.72.4/api_op_StartBuild.go's
// StartBuildInput. IdempotencyToken and LogsConfigOverride are intentionally
// not modeled: this emulator does not deduplicate build submissions, and
// neither field has an observable effect through any real read op (Build has
// no logsConfig field of its own -- LogsConfigOverride only affects where a
// real build's logs are delivered, which this emulator's Build.Logs, a
// distinct always-nil field pending real log delivery, does not simulate).
type StartBuildConfig struct {
	ArtifactsOverride                *ProjectArtifacts
	CacheOverride                    *ProjectCache
	RegistryCredentialOverride       *RegistryCredential
	FleetOverride                    *ProjectFleet
	SourceAuthOverride               *SourceAuth
	BuildStatusConfigOverride        *BuildStatusConfig
	GitSubmodulesConfigOverride      *GitSubmodulesConfig
	InsecureSslOverride              *bool
	ReportBuildStatusOverride        *bool
	PrivilegedModeOverride           *bool
	GitCloneDepthOverride            *int32
	AutoRetryLimitOverride           *int32
	ServiceRoleOverride              string
	HostKernelOverride               string
	ComputeTypeOverride              string
	SourceVersion                    string
	SourceTypeOverride               string
	SourceLocationOverride           string
	EnvironmentTypeOverride          string
	CertificateOverride              string
	ImagePullCredentialsTypeOverride string
	ImageOverride                    string
	EncryptionKeyOverride            string
	BuildspecOverride                string
	SecondaryArtifactsOverride       []ProjectArtifacts
	SecondarySourcesOverride         []ProjectSource
	SecondarySourcesVersionOverride  []ProjectSourceVersion
	EnvVarsOverride                  []EnvironmentVariable
	TimeoutInMinutesOverride         int32
	QueuedTimeoutInMinutesOverride   int32
	DebugSessionEnabled              bool
}

// mergeEnvVarOverrides applies real AWS's StartBuild env-var merge semantics:
// same-name vars are replaced in place, new ones appended.
func mergeEnvVarOverrides(existing, overrides []EnvironmentVariable) []EnvironmentVariable {
	merged := make([]EnvironmentVariable, 0, len(existing)+len(overrides))
	merged = append(merged, existing...)

	for _, ov := range overrides {
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

	return merged
}

// applyEnvironmentScalarOverrides applies a StartBuildConfig's simple
// (string/bool/pointer-object) environment field overrides to a copy of the
// project's environment.
func applyEnvironmentScalarOverrides(env ProjectEnvironment, cfg StartBuildConfig) ProjectEnvironment {
	if cfg.ComputeTypeOverride != "" {
		env.ComputeType = cfg.ComputeTypeOverride
	}

	if cfg.ImageOverride != "" {
		env.Image = cfg.ImageOverride
	}

	if cfg.EnvironmentTypeOverride != "" {
		env.Type = cfg.EnvironmentTypeOverride
	}

	if cfg.CertificateOverride != "" {
		env.Certificate = cfg.CertificateOverride
	}

	if cfg.ImagePullCredentialsTypeOverride != "" {
		env.ImagePullCredentialsType = cfg.ImagePullCredentialsTypeOverride
	}

	if cfg.HostKernelOverride != "" {
		env.HostKernel = cfg.HostKernelOverride
	}

	if cfg.RegistryCredentialOverride != nil {
		env.RegistryCredential = cfg.RegistryCredentialOverride
	}

	if cfg.FleetOverride != nil {
		env.Fleet = cfg.FleetOverride
	}

	if cfg.PrivilegedModeOverride != nil {
		env.PrivilegedMode = *cfg.PrivilegedModeOverride
	}

	return env
}

// applyEnvironmentOverrides applies a StartBuildConfig's environment-related
// overrides to a copy of the project's environment.
func applyEnvironmentOverrides(env ProjectEnvironment, cfg StartBuildConfig) ProjectEnvironment {
	if len(cfg.EnvVarsOverride) > 0 {
		env.EnvironmentVariables = mergeEnvVarOverrides(env.EnvironmentVariables, cfg.EnvVarsOverride)
	}

	return applyEnvironmentScalarOverrides(env, cfg)
}

// applySourceOverrides applies a StartBuildConfig's source-related overrides
// to a copy of the project's primary source. It never touches src.Location
// via cfg.SourceVersion -- SourceVersion selects which commit/branch/tag of
// the existing source to build, a distinct concept from SourceLocationOverride
// (aws-sdk-go-v2/service/codebuild@v1.72.4/types.Build has separate
// SourceVersion and Source.Location fields).
func applySourceOverrides(src ProjectSource, cfg StartBuildConfig) ProjectSource {
	if cfg.BuildspecOverride != "" {
		src.Buildspec = cfg.BuildspecOverride
	}

	if cfg.SourceTypeOverride != "" {
		src.Type = cfg.SourceTypeOverride
	}

	if cfg.SourceLocationOverride != "" {
		src.Location = cfg.SourceLocationOverride
	}

	if cfg.SourceAuthOverride != nil {
		src.Auth = *cfg.SourceAuthOverride
	}

	if cfg.InsecureSslOverride != nil {
		src.InsecureSsl = *cfg.InsecureSslOverride
	}

	if cfg.GitCloneDepthOverride != nil {
		src.GitCloneDepth = *cfg.GitCloneDepthOverride
	}

	if cfg.ReportBuildStatusOverride != nil {
		src.ReportBuildStatus = *cfg.ReportBuildStatusOverride
	}

	if cfg.BuildStatusConfigOverride != nil {
		src.BuildStatusConfig = cfg.BuildStatusConfigOverride
	}

	if cfg.GitSubmodulesConfigOverride != nil {
		src.GitSubmodulesConfig = cfg.GitSubmodulesConfigOverride
	}

	return src
}

// buildOverrideResult carries the resolved build fields after applying a
// StartBuildConfig on top of a project's defaults.
type buildOverrideResult struct {
	Cache                   *ProjectCache
	ServiceRole             string
	EncryptionKey           string
	Artifacts               ProjectArtifacts
	Source                  ProjectSource
	SecondaryArtifacts      []ProjectArtifacts
	SecondarySources        []ProjectSource
	SecondarySourceVersions []ProjectSourceVersion
	Environment             ProjectEnvironment
	TimeoutInMinutes        int32
	QueuedTimeoutInMinutes  int32
}

// applyBuildOverrides applies a StartBuildConfig to a project's defaults and
// returns the resolved fields for the new build.
func applyBuildOverrides(proj *Project, cfg StartBuildConfig) buildOverrideResult {
	out := buildOverrideResult{
		Environment:             applyEnvironmentOverrides(proj.Environment, cfg),
		Source:                  applySourceOverrides(proj.Source, cfg),
		Artifacts:               proj.Artifacts,
		Cache:                   proj.Cache,
		SecondaryArtifacts:      proj.SecondaryArtifacts,
		SecondarySources:        proj.SecondarySources,
		SecondarySourceVersions: proj.SecondarySourceVersions,
		ServiceRole:             proj.ServiceRole,
		EncryptionKey:           proj.EncryptionKey,
		TimeoutInMinutes:        proj.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  proj.QueuedTimeoutInMinutes,
	}

	if cfg.ArtifactsOverride != nil {
		out.Artifacts = *cfg.ArtifactsOverride
	}

	if cfg.CacheOverride != nil {
		out.Cache = cfg.CacheOverride
	}

	if cfg.SecondaryArtifactsOverride != nil {
		out.SecondaryArtifacts = cfg.SecondaryArtifactsOverride
	}

	if cfg.SecondarySourcesOverride != nil {
		out.SecondarySources = cfg.SecondarySourcesOverride
	}

	if cfg.SecondarySourcesVersionOverride != nil {
		out.SecondarySourceVersions = cfg.SecondarySourcesVersionOverride
	}

	if cfg.ServiceRoleOverride != "" {
		out.ServiceRole = cfg.ServiceRoleOverride
	}

	if cfg.EncryptionKeyOverride != "" {
		out.EncryptionKey = cfg.EncryptionKeyOverride
	}

	if cfg.TimeoutInMinutesOverride > 0 {
		out.TimeoutInMinutes = cfg.TimeoutInMinutesOverride
	}

	if cfg.QueuedTimeoutInMinutesOverride > 0 {
		out.QueuedTimeoutInMinutes = cfg.QueuedTimeoutInMinutesOverride
	}

	return out
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

	ov := applyBuildOverrides(proj, cfg)

	autoRetryLimit := proj.AutoRetryLimit
	if cfg.AutoRetryLimitOverride != nil {
		autoRetryLimit = *cfg.AutoRetryLimitOverride
	}

	sourceVersion := proj.SourceVersion
	if cfg.SourceVersion != "" {
		sourceVersion = cfg.SourceVersion
	}

	build := &Build{
		ID:                      fullID,
		Arn:                     b.buildBuildARN(projectName, buildID),
		ProjectName:             projectName,
		BuildStatus:             buildStatusInProgress,
		StartTime:               now,
		CurrentPhase:            phaseSubmitted,
		ServiceRole:             ov.ServiceRole,
		EncryptionKey:           ov.EncryptionKey,
		TimeoutInMinutes:        ov.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  ov.QueuedTimeoutInMinutes,
		SourceVersion:           sourceVersion,
		ResolvedSourceVersion:   sourceVersion,
		Environment:             &ov.Environment,
		Source:                  &ov.Source,
		Artifacts:               &ov.Artifacts,
		Cache:                   ov.Cache,
		VpcConfig:               proj.VpcConfig,
		FileSystemLocations:     proj.FileSystemLocations,
		SecondaryArtifacts:      ov.SecondaryArtifacts,
		SecondarySources:        ov.SecondarySources,
		SecondarySourceVersions: ov.SecondarySourceVersions,
		AutoRetryConfig:         &AutoRetryConfig{AutoRetryLimit: autoRetryLimit},
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
// The auto-retry chain (AutoRetryConfig.AutoRetryNumber/PreviousAutoRetry/NextAutoRetry) links
// the new build back to the one it retried, matching aws-sdk-go-v2/service/codebuild@v1.72.4's
// types.AutoRetryConfig.
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
	buildArn := b.buildBuildARN(projectName, buildID)
	now := float64(time.Now().Unix())

	var autoRetryLimit, autoRetryNumber int32
	if existing.AutoRetryConfig != nil {
		autoRetryLimit = existing.AutoRetryConfig.AutoRetryLimit
		autoRetryNumber = existing.AutoRetryConfig.AutoRetryNumber + 1
	}

	build := &Build{
		ID:                      fullID,
		Arn:                     buildArn,
		ProjectName:             projectName,
		BuildStatus:             buildStatusInProgress,
		StartTime:               now,
		CurrentPhase:            phaseSubmitted,
		ServiceRole:             existing.ServiceRole,
		EncryptionKey:           existing.EncryptionKey,
		TimeoutInMinutes:        existing.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  existing.QueuedTimeoutInMinutes,
		SourceVersion:           existing.SourceVersion,
		ResolvedSourceVersion:   existing.ResolvedSourceVersion,
		Environment:             existing.Environment,
		Source:                  existing.Source,
		Artifacts:               existing.Artifacts,
		Cache:                   existing.Cache,
		VpcConfig:               existing.VpcConfig,
		FileSystemLocations:     existing.FileSystemLocations,
		SecondaryArtifacts:      existing.SecondaryArtifacts,
		SecondarySources:        existing.SecondarySources,
		SecondarySourceVersions: existing.SecondarySourceVersions,
		AutoRetryConfig: &AutoRetryConfig{
			AutoRetryLimit:    autoRetryLimit,
			AutoRetryNumber:   autoRetryNumber,
			PreviousAutoRetry: existing.Arn,
		},
		Phases: []BuildPhase{
			{PhaseType: phaseSubmitted, PhaseStatus: "SUCCEEDED", StartTime: now, EndTime: now},
		},
	}
	b.builds.Put(build)

	if existing.AutoRetryConfig == nil {
		existing.AutoRetryConfig = &AutoRetryConfig{}
	}

	existing.AutoRetryConfig.NextAutoRetry = buildArn

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

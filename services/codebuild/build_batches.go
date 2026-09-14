// Package codebuild implements an in-memory AWS CodeBuild backend.
//
// Build batch design: a BuildBatch models the AWS shape (types.BuildBatch): its own
// Environment/Source/Artifacts/Cache default to the project's and can
// diverge via StartBuildBatch's *Override fields, exactly like StartBuild's
// overrides (see StartBuildConfig, builds.go). Its BuildGroups come from the
// buildspec's `batch:` section (batchspec.go): build-list nodes have no
// dependencies and all start immediately; build-graph nodes start once their
// DependsOn groups reach a terminal, non-blocking state. Each started group
// is a real Build (BuildBatchArn set) created through startBatchChildBuild,
// sharing the batch's resolved environment layered with the node's own `env:`
// override.
//
// Dependency ordering rides the same mechanism that already advances plain
// builds: Builds in this emulator complete only when the Janitor ticks (see
// janitor.go), not synchronously inside StartBuild. reconcileBatch is called
// both at StartBuildBatch (to launch initially-eligible groups) and by the
// Janitor's tick (to advance dependents once their dependencies go
// terminal) -- BuildGroups themselves carry enough state (Identifier,
// DependsOn, IgnoreFailure, CurrentBuildSummary) to resume this purely from
// persisted data, so no additional backend-only scheduling state is needed.
package codebuild

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

const buildStatusFailed = "FAILED"

func (b *InMemoryBackend) buildBatchARN(projectName, batchID string) string {
	return arn.Build("codebuild", b.region, b.accountID, "build-batch/"+projectName+":"+batchID)
}

// AddBuildBatchInternal seeds a BuildBatch directly into the backend (test helper).
func (b *InMemoryBackend) AddBuildBatchInternal(bb *BuildBatch) {
	b.mu.Lock("AddBuildBatchInternal")
	defer b.mu.Unlock()

	b.buildBatches.Put(bb)
}

// BatchGetBuildBatches returns build batches by ID. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuildBatches(ids []string) ([]*BuildBatch, []string) {
	b.mu.RLock("BatchGetBuildBatches")
	defer b.mu.RUnlock()

	found := make([]*BuildBatch, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if bb, ok := b.buildBatches.Get(id); ok {
			out := *bb
			out.BuildGroups = cloneBuildGroups(bb.BuildGroups)
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// ListBuildBatches returns all build batch IDs in sorted order, optionally
// filtered by status (empty statusFilter returns every batch).
func (b *InMemoryBackend) ListBuildBatches(statusFilter string) []string {
	b.mu.RLock("ListBuildBatches")
	defer b.mu.RUnlock()

	items := b.buildBatches.Snapshot()
	ids := make([]string, 0, len(items))

	for _, bb := range items {
		if statusFilter != "" && bb.BuildBatchStatus != statusFilter {
			continue
		}

		ids = append(ids, bb.ID)
	}

	return ids
}

// StartBuildBatchConfig holds override parameters for a StartBuildBatch call,
// mirroring aws-sdk-go-v2/service/codebuild@v1.72.4's api_op_StartBuildBatch.go
// StartBuildBatchInput. IdempotencyToken and LogsConfigOverride are skipped
// for the same reasons StartBuildConfig skips them (see builds.go).
// StartBuildBatchInput has no FleetOverride, HostKernelOverride, or
// AutoRetryLimitOverride -- api_op_StartBuildBatch.go simply doesn't declare
// them (unlike StartBuildInput).
type StartBuildBatchConfig struct {
	ArtifactsOverride                *ProjectArtifacts
	BuildBatchConfigOverride         *BuildBatchConfig
	CacheOverride                    *ProjectCache
	RegistryCredentialOverride       *RegistryCredential
	SourceAuthOverride               *SourceAuth
	GitSubmodulesConfigOverride      *GitSubmodulesConfig
	InsecureSslOverride              *bool
	ReportBuildBatchStatusOverride   *bool
	PrivilegedModeOverride           *bool
	GitCloneDepthOverride            *int32
	BuildTimeoutInMinutesOverride    *int32
	QueuedTimeoutInMinutesOverride   *int32
	ServiceRoleOverride              string
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
	DebugSessionEnabled              bool
}

func applyBatchEnvironmentOverrides(env ProjectEnvironment, cfg StartBuildBatchConfig) ProjectEnvironment {
	if len(cfg.EnvVarsOverride) > 0 {
		env.EnvironmentVariables = mergeEnvVarOverrides(env.EnvironmentVariables, cfg.EnvVarsOverride)
	}

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

	if cfg.RegistryCredentialOverride != nil {
		env.RegistryCredential = cfg.RegistryCredentialOverride
	}

	if cfg.PrivilegedModeOverride != nil {
		env.PrivilegedMode = *cfg.PrivilegedModeOverride
	}

	return env
}

func applyBatchSourceOverrides(src ProjectSource, cfg StartBuildBatchConfig) ProjectSource {
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

	if cfg.ReportBuildBatchStatusOverride != nil {
		src.ReportBuildStatus = *cfg.ReportBuildBatchStatusOverride
	}

	if cfg.GitSubmodulesConfigOverride != nil {
		src.GitSubmodulesConfig = cfg.GitSubmodulesConfigOverride
	}

	return src
}

// batchOverrideResult carries the resolved batch-level fields after applying
// a StartBuildBatchConfig on top of a project's defaults (or, for
// RetryBuildBatch, an existing batch's already-resolved fields).
type batchOverrideResult struct {
	Cache                   *ProjectCache
	Config                  *BuildBatchConfig
	VpcConfig               *VpcConfig
	ServiceRole             string
	EncryptionKey           string
	Artifacts               ProjectArtifacts
	Source                  ProjectSource
	SecondaryArtifacts      []ProjectArtifacts
	SecondarySources        []ProjectSource
	SecondarySourceVersions []ProjectSourceVersion
	FileSystemLocations     []FileSystemLocation
	Environment             ProjectEnvironment
	BuildTimeoutInMinutes   int32
	QueuedTimeoutInMinutes  int32
}

// applyBatchOverrides applies a StartBuildBatchConfig to a project's
// defaults and returns the resolved fields for the new batch. A project's
// BuildBatchConfig.ServiceRole/TimeoutInMins (aws-sdk-go-v2/service/codebuild/
// types.ProjectBuildBatchConfig, types/types.go:1707) take precedence over
// the project's own ServiceRole/TimeoutInMinutes before StartBuildBatch's own
// overrides are applied, matching real AWS's batch-specific config layer.
func applyBatchOverrides(proj *Project, cfg StartBuildBatchConfig) batchOverrideResult {
	out := batchOverrideResult{
		Environment:             applyBatchEnvironmentOverrides(proj.Environment, cfg),
		Source:                  applyBatchSourceOverrides(proj.Source, cfg),
		Artifacts:               proj.Artifacts,
		Cache:                   proj.Cache,
		VpcConfig:               proj.VpcConfig,
		FileSystemLocations:     proj.FileSystemLocations,
		SecondaryArtifacts:      proj.SecondaryArtifacts,
		SecondarySources:        proj.SecondarySources,
		SecondarySourceVersions: proj.SecondarySourceVersions,
		ServiceRole:             proj.ServiceRole,
		EncryptionKey:           proj.EncryptionKey,
		BuildTimeoutInMinutes:   proj.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  proj.QueuedTimeoutInMinutes,
		Config:                  proj.BuildBatchConfig,
	}

	if proj.BuildBatchConfig != nil {
		if proj.BuildBatchConfig.ServiceRole != "" {
			out.ServiceRole = proj.BuildBatchConfig.ServiceRole
		}

		if proj.BuildBatchConfig.TimeoutInMins > 0 {
			out.BuildTimeoutInMinutes = proj.BuildBatchConfig.TimeoutInMins
		}
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

	if cfg.BuildTimeoutInMinutesOverride != nil {
		out.BuildTimeoutInMinutes = *cfg.BuildTimeoutInMinutesOverride
	}

	if cfg.QueuedTimeoutInMinutesOverride != nil {
		out.QueuedTimeoutInMinutes = *cfg.QueuedTimeoutInMinutesOverride
	}

	if cfg.BuildBatchConfigOverride != nil {
		out.Config = cfg.BuildBatchConfigOverride
	}

	return out
}

// checkMaximumBuildsAllowed enforces BuildBatchConfig.Restrictions.
// MaximumBuildsAllowed (aws-sdk-go-v2/service/codebuild/types.
// BatchRestrictions, types/types.go:34): a batch definition with more nodes
// than allowed fails to start, matching real AWS.
func checkMaximumBuildsAllowed(cfg *BuildBatchConfig, numBuilds int) error {
	if cfg == nil || cfg.Restrictions.MaximumBuildsAllowed <= 0 {
		return nil
	}

	//nolint:gosec // numBuilds is a batch node count, never near int32 overflow
	if int32(numBuilds) <= cfg.Restrictions.MaximumBuildsAllowed {
		return nil
	}

	return fmt.Errorf(
		"%w: batch definition has %d builds, exceeding the project's maximum of %d",
		ErrValidation, numBuilds, cfg.Restrictions.MaximumBuildsAllowed,
	)
}

// newBuildGroups creates one BuildGroup per definition node, in definition
// order, with no CurrentBuildSummary yet -- reconcileBatch starts the
// initially-eligible ones.
func newBuildGroups(def batchDefinition) []BuildGroup {
	groups := make([]BuildGroup, 0, len(def.nodes))
	for _, n := range def.nodes {
		groups = append(groups, BuildGroup{
			Identifier:    n.Identifier,
			DependsOn:     n.DependsOn,
			IgnoreFailure: n.IgnoreFailure,
		})
	}

	return groups
}

// cloneBuildGroups deep-copies groups so a caller can't mutate backend-owned
// state through a returned BuildBatch -- BuildGroups holds nested pointers
// (CurrentBuildSummary) a shallow struct copy would still alias.
func cloneBuildGroups(groups []BuildGroup) []BuildGroup {
	out := make([]BuildGroup, len(groups))

	for i, g := range groups {
		out[i] = g

		if g.CurrentBuildSummary != nil {
			summary := *g.CurrentBuildSummary
			out[i].CurrentBuildSummary = &summary
		}

		if g.DependsOn != nil {
			out[i].DependsOn = append([]string(nil), g.DependsOn...)
		}

		if g.PriorBuildSummaryList != nil {
			out[i].PriorBuildSummaryList = append([]BuildSummary(nil), g.PriorBuildSummaryList...)
		}
	}

	return out
}

// newBuildBatchRecord constructs and reconciles a new BuildBatch from
// already-resolved fields, shared by StartBuildBatch and RetryBuildBatch.
func (b *InMemoryBackend) newBuildBatchRecord(
	projectName string, ov batchOverrideResult, sourceVersion string, debugSessionEnabled bool,
	def batchDefinition, now float64,
) *BuildBatch {
	batchID := uuid.NewString()
	id := projectName + ":" + batchID

	b.buildBatchNumbers[projectName]++

	bb := &BuildBatch{
		ID:                      id,
		Arn:                     b.buildBatchARN(projectName, batchID),
		ProjectName:             projectName,
		BuildBatchNumber:        b.buildBatchNumbers[projectName],
		BuildBatchStatus:        buildStatusInProgress,
		CurrentPhase:            phaseSubmitted,
		StartTime:               now,
		SourceVersion:           sourceVersion,
		ResolvedSourceVersion:   sourceVersion,
		ServiceRole:             ov.ServiceRole,
		EncryptionKey:           ov.EncryptionKey,
		BuildTimeoutInMinutes:   ov.BuildTimeoutInMinutes,
		QueuedTimeoutInMinutes:  ov.QueuedTimeoutInMinutes,
		DebugSessionEnabled:     debugSessionEnabled,
		Environment:             &ov.Environment,
		Source:                  &ov.Source,
		Artifacts:               &ov.Artifacts,
		Cache:                   ov.Cache,
		VpcConfig:               ov.VpcConfig,
		FileSystemLocations:     ov.FileSystemLocations,
		SecondaryArtifacts:      ov.SecondaryArtifacts,
		SecondarySources:        ov.SecondarySources,
		SecondarySourceVersions: ov.SecondarySourceVersions,
		BuildBatchConfig:        ov.Config,
		BuildGroups:             newBuildGroups(def),
		Phases: []BuildBatchPhase{
			{PhaseType: phaseSubmitted, PhaseStatus: buildStatusSucceeded, StartTime: now, EndTime: now},
		},
	}

	b.reconcileBatch(bb, def, now)

	return bb
}

// StartBuildBatch creates a new build batch for a project. The project's
// buildspec (or cfg.BuildspecOverride) must declare a `batch:` section
// (build-list or build-graph); otherwise this returns the real
// InvalidInputException (errNoBatchConfig, batchspec.go).
func (b *InMemoryBackend) StartBuildBatch(projectName string, cfg StartBuildBatchConfig) (*BuildBatch, error) {
	b.mu.Lock("StartBuildBatch")
	defer b.mu.Unlock()

	proj, ok := b.projects.Get(projectName)
	if !ok {
		return nil, ErrNotFound
	}

	ov := applyBatchOverrides(proj, cfg)

	def, err := parseBatchDefinition(ov.Source.Buildspec)
	if err != nil {
		return nil, err
	}

	if err = checkMaximumBuildsAllowed(ov.Config, len(def.nodes)); err != nil {
		return nil, err
	}

	sourceVersion := proj.SourceVersion
	if cfg.SourceVersion != "" {
		sourceVersion = cfg.SourceVersion
	}

	now := float64(time.Now().Unix())
	bb := b.newBuildBatchRecord(projectName, ov, sourceVersion, cfg.DebugSessionEnabled, def, now)
	b.buildBatches.Put(bb)

	out := *bb
	out.BuildGroups = cloneBuildGroups(bb.BuildGroups)

	return &out, nil
}

// DeleteBuildBatch removes a build batch by ID. Idempotent: real AWS's
// DeleteBuildBatch declares no ResourceNotFoundException (botocore
// codebuild/2016-10-06/service-2.json operations.DeleteBuildBatch.errors:
// only InvalidInputException), so deleting an already-gone batch is not an
// error.
func (b *InMemoryBackend) DeleteBuildBatch(id string) error {
	b.mu.Lock("DeleteBuildBatch")
	defer b.mu.Unlock()

	b.buildBatches.Delete(id)

	return nil
}

func derefEnvironment(p *ProjectEnvironment) ProjectEnvironment {
	if p == nil {
		return ProjectEnvironment{}
	}

	return *p
}

func derefSource(p *ProjectSource) ProjectSource {
	if p == nil {
		return ProjectSource{}
	}

	return *p
}

func derefArtifacts(p *ProjectArtifacts) ProjectArtifacts {
	if p == nil {
		return ProjectArtifacts{}
	}

	return *p
}

func sourceBuildspec(src *ProjectSource) string {
	if src == nil {
		return ""
	}

	return src.Buildspec
}

// RetryBuildBatch restarts a batch build, reusing the resolved
// environment/source/artifacts/config of the batch being retried and running
// its buildspec's batch definition again from scratch.
//
// Disclosed gap: real AWS only allows retrying a FAILED batch, and RetryType
// (RETRY_ALL_BUILDS vs RETRY_FAILED_BUILDS) selects whether every group or
// only the failed ones re-run (api_op_RetryBuildBatch.go). Neither is
// enforced/implemented here -- see PARITY.md.
func (b *InMemoryBackend) RetryBuildBatch(id string) (*BuildBatch, error) {
	b.mu.Lock("RetryBuildBatch")
	defer b.mu.Unlock()

	existing, ok := b.buildBatches.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	def, err := parseBatchDefinition(sourceBuildspec(existing.Source))
	if err != nil {
		return nil, err
	}

	ov := batchOverrideResult{
		Environment:             derefEnvironment(existing.Environment),
		Source:                  derefSource(existing.Source),
		Artifacts:               derefArtifacts(existing.Artifacts),
		Cache:                   existing.Cache,
		VpcConfig:               existing.VpcConfig,
		FileSystemLocations:     existing.FileSystemLocations,
		SecondaryArtifacts:      existing.SecondaryArtifacts,
		SecondarySources:        existing.SecondarySources,
		SecondarySourceVersions: existing.SecondarySourceVersions,
		ServiceRole:             existing.ServiceRole,
		EncryptionKey:           existing.EncryptionKey,
		BuildTimeoutInMinutes:   existing.BuildTimeoutInMinutes,
		QueuedTimeoutInMinutes:  existing.QueuedTimeoutInMinutes,
		Config:                  existing.BuildBatchConfig,
	}

	now := float64(time.Now().Unix())
	bb := b.newBuildBatchRecord(
		existing.ProjectName, ov, existing.SourceVersion, existing.DebugSessionEnabled, def, now,
	)
	b.buildBatches.Put(bb)

	out := *bb
	out.BuildGroups = cloneBuildGroups(bb.BuildGroups)

	return &out, nil
}

// startBatchChildBuild starts a real Build for one BuildGroup node, sharing
// bb's resolved environment/source/artifacts with the node's own `env:`/
// buildspec layered on top. BuildBatchArn is set so BatchGetBuilds/
// ListBuildsForProject expose the child as batch-linked (aws-sdk-go-v2/
// service/codebuild/types.Build.BuildBatchArn, types/types.go:67).
func (b *InMemoryBackend) startBatchChildBuild(bb *BuildBatch, node batchNode, now float64) *Build {
	env := applyNodeEnvOverrides(derefEnvironment(bb.Environment), node)

	src := derefSource(bb.Source)
	if node.Buildspec != "" {
		src.Buildspec = node.Buildspec
	}

	buildID := randomID()
	fullID := bb.ProjectName + ":" + buildID

	build := &Build{
		ID:                      fullID,
		Arn:                     b.buildBuildARN(bb.ProjectName, buildID),
		ProjectName:             bb.ProjectName,
		BuildBatchArn:           bb.Arn,
		BuildStatus:             buildStatusInProgress,
		StartTime:               now,
		CurrentPhase:            phaseSubmitted,
		ServiceRole:             bb.ServiceRole,
		EncryptionKey:           bb.EncryptionKey,
		TimeoutInMinutes:        bb.BuildTimeoutInMinutes,
		QueuedTimeoutInMinutes:  bb.QueuedTimeoutInMinutes,
		SourceVersion:           bb.SourceVersion,
		ResolvedSourceVersion:   bb.ResolvedSourceVersion,
		Environment:             &env,
		Source:                  &src,
		Artifacts:               bb.Artifacts,
		Cache:                   bb.Cache,
		VpcConfig:               bb.VpcConfig,
		FileSystemLocations:     bb.FileSystemLocations,
		SecondaryArtifacts:      bb.SecondaryArtifacts,
		SecondarySources:        bb.SecondarySources,
		SecondarySourceVersions: bb.SecondarySourceVersions,
		Phases: []BuildPhase{
			{PhaseType: phaseSubmitted, PhaseStatus: buildStatusSucceeded, StartTime: now, EndTime: now},
		},
	}
	b.builds.Put(build)

	return build
}

// indexGroups snapshots groups by Identifier for dependency lookups.
func indexGroups(groups []BuildGroup) map[string]BuildGroup {
	byID := make(map[string]BuildGroup, len(groups))
	for _, g := range groups {
		byID[g.Identifier] = g
	}

	return byID
}

// groupReadyToStart reports whether every one of g's dependencies has
// reached a state that no longer blocks g: SUCCEEDED, or a failure the
// dependency itself ignores (IgnoreFailure).
func groupReadyToStart(byID map[string]BuildGroup, g BuildGroup) bool {
	for _, dep := range g.DependsOn {
		depGroup, ok := byID[dep]
		if !ok || depGroup.CurrentBuildSummary == nil {
			return false
		}

		status := depGroup.CurrentBuildSummary.BuildStatus
		if status == buildStatusInProgress {
			return false
		}

		if isFailureStatus(status) && !depGroup.IgnoreFailure {
			return false
		}
	}

	return true
}

// refreshGroupSummary syncs a started group's CurrentBuildSummary with its
// live child Build's current status.
func (b *InMemoryBackend) refreshGroupSummary(g *BuildGroup) {
	if g.CurrentBuildSummary == nil {
		return
	}

	matches := b.buildsByARN.Get(g.CurrentBuildSummary.Arn)
	if len(matches) == 0 {
		return
	}

	g.CurrentBuildSummary.BuildStatus = matches[0].BuildStatus
}

// reconcileBatch starts any not-yet-started BuildGroup whose dependencies
// are satisfied, refreshes already-started groups' CurrentBuildSummary from
// their live child Build, and recomputes the batch's derived status. Called
// with the backend's write lock already held (StartBuildBatch,
// RetryBuildBatch, or the Janitor tick).
func (b *InMemoryBackend) reconcileBatch(bb *BuildBatch, def batchDefinition, now float64) {
	if bb.BuildBatchStatus != buildStatusInProgress {
		return
	}

	for i := range bb.BuildGroups {
		b.refreshGroupSummary(&bb.BuildGroups[i])
	}

	byID := indexGroups(bb.BuildGroups)

	for i := range bb.BuildGroups {
		g := &bb.BuildGroups[i]
		if g.CurrentBuildSummary != nil || !groupReadyToStart(byID, *g) {
			continue
		}

		child := b.startBatchChildBuild(bb, def.node(g.Identifier), now)
		g.CurrentBuildSummary = &BuildSummary{Arn: child.Arn, BuildStatus: child.BuildStatus, RequestedOn: now}
		byID[g.Identifier] = *g
	}

	bb.BuildBatchStatus = deriveBatchStatus(bb.BuildGroups)
	if bb.BuildBatchStatus != buildStatusInProgress {
		bb.EndTime = now
		bb.Complete = true
		bb.CurrentPhase = phaseCompleted
	}
}

type groupOutcome int

const (
	outcomePending groupOutcome = iota
	outcomeBlocked
	outcomeSucceeded
	outcomeFailed
)

func isFailureStatus(status string) bool {
	return status == buildStatusFailed || status == "FAULT" || status == "TIMED_OUT" || status == buildStatusStopped
}

func outcomeFromStatus(status string, ignoreFailure bool) groupOutcome {
	switch {
	case status == buildStatusSucceeded:
		return outcomeSucceeded
	case isFailureStatus(status) && ignoreFailure:
		return outcomeSucceeded
	case isFailureStatus(status):
		return outcomeFailed
	default:
		return outcomePending
	}
}

// resolveGroupOutcome determines one group's contribution to the batch's
// derived status, recursing into its dependencies when it hasn't started
// yet. A dependency that failed (and isn't ignored) permanently blocks a
// group that depends on it -- that group will never get a CurrentBuildSummary,
// so it must still count as a failure for the batch as a whole.
// checkBatchDependencyCycles already rejects real cycles at parse time; memo
// still doubles as a defensive cycle guard.
func resolveGroupOutcome(byID map[string]BuildGroup, id string, memo map[string]groupOutcome) groupOutcome {
	if outcome, ok := memo[id]; ok {
		return outcome
	}

	memo[id] = outcomePending

	g := byID[id]
	if g.CurrentBuildSummary != nil {
		outcome := outcomeFromStatus(g.CurrentBuildSummary.BuildStatus, g.IgnoreFailure)
		memo[id] = outcome

		return outcome
	}

	for _, dep := range g.DependsOn {
		switch resolveGroupOutcome(byID, dep, memo) {
		case outcomeFailed, outcomeBlocked:
			memo[id] = outcomeBlocked

			return outcomeBlocked
		case outcomePending:
			return outcomePending
		case outcomeSucceeded:
			continue
		}
	}

	return outcomePending
}

// deriveBatchStatus derives a batch's overall status from its groups:
// IN_PROGRESS while any group is neither terminal nor permanently blocked,
// SUCCEEDED once every group succeeded (or ignored its own failure),
// otherwise FAILED.
func deriveBatchStatus(groups []BuildGroup) string {
	byID := indexGroups(groups)
	memo := make(map[string]groupOutcome, len(groups))
	anyFailed := false

	for _, g := range groups {
		switch resolveGroupOutcome(byID, g.Identifier, memo) {
		case outcomePending:
			return buildStatusInProgress
		case outcomeBlocked, outcomeFailed:
			anyFailed = true
		case outcomeSucceeded:
			continue
		}
	}

	if anyFailed {
		return buildStatusFailed
	}

	return buildStatusSucceeded
}

// stopChildIfInProgress stops one child Build (by ARN) if it's still
// IN_PROGRESS, mirroring StopBuild's transition.
func (b *InMemoryBackend) stopChildIfInProgress(buildArn string, now float64) (*Build, bool) {
	matches := b.buildsByARN.Get(buildArn)
	if len(matches) == 0 {
		return nil, false
	}

	child := matches[0]
	if child.BuildStatus != buildStatusInProgress {
		return child, false
	}

	child.BuildStatus = buildStatusStopped
	child.EndTime = now
	child.CurrentPhase = phaseCompleted
	child.BuildComplete = true

	return child, true
}

// StopBuildBatch stops every in-progress child build and marks the batch
// STOPPED.
func (b *InMemoryBackend) StopBuildBatch(id string) (*BuildBatch, error) {
	b.mu.Lock("StopBuildBatch")
	defer b.mu.Unlock()

	bb, ok := b.buildBatches.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	now := float64(time.Now().Unix())

	for i := range bb.BuildGroups {
		g := &bb.BuildGroups[i]
		if g.CurrentBuildSummary == nil {
			continue
		}

		if child, stopped := b.stopChildIfInProgress(g.CurrentBuildSummary.Arn, now); stopped {
			g.CurrentBuildSummary.BuildStatus = child.BuildStatus
		}
	}

	bb.BuildBatchStatus = buildStatusStopped
	bb.EndTime = now
	bb.Complete = true
	bb.CurrentPhase = phaseCompleted

	out := *bb
	out.BuildGroups = cloneBuildGroups(bb.BuildGroups)

	return &out, nil
}

// ListBuildBatchesForProject returns all batch IDs for a project in sorted
// order, optionally filtered by status (empty statusFilter returns every
// batch for the project).
func (b *InMemoryBackend) ListBuildBatchesForProject(projectName, statusFilter string) ([]string, error) {
	b.mu.RLock("ListBuildBatchesForProject")
	defer b.mu.RUnlock()

	if !b.projects.Has(projectName) {
		return nil, ErrNotFound
	}

	group := b.buildBatchesByProject.Get(projectName)
	ids := make([]string, 0, len(group))

	for _, bb := range group {
		if statusFilter != "" && bb.BuildBatchStatus != statusFilter {
			continue
		}

		ids = append(ids, bb.ID)
	}

	sort.Strings(ids)

	return ids, nil
}

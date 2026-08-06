package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// Shared NetworkMigrationExecution/NetworkMigrationJob bookkeeping engine for
// family N (networkmigrationjobs.go) and family M (networkmigration.go).
//
// No op in this SDK surface CREATES a NetworkMigrationExecutionID -- the five
// Start* ops all REQUIRE one as input, and ListNetworkMigrationExecutions only
// lists. resolveOrCreateExecutionLocked auto-vivifies a NetworkMigrationExecution
// the first time any Start* op references an unseen (DefinitionID, ExecutionID)
// pair -- an explicit, documented gopherstack convention, never presented as
// derived AWS behavior.
//
// ListNetworkMigrationAnalysisResults, ListNetworkMigrationCodeGenerationSegments,
// and ListNetworkMigrationDeployedStacks always return an empty Items list, even
// after their parent job SUCCEEDS: analyzing topology, generating code, and
// deploying stacks all require engines this repo does not have, and fabricating
// that content would misrepresent what the emulator did (PARITY.md). The
// state-bookkeeping shell -- job status PENDING -> STARTED -> SUCCEEDED,
// executions tracking Stage/Activity/Status -- is the honest half this file provides.

// nmActivityToStage maps an ExecutionStageActivity to its ExecutionStage --
// identical for 5 of 6 values; MAPPING_UPDATE (Activity-only, no Stage
// counterpart exists in the 5-value ExecutionStage enum) maps to the
// MAPPING stage, since a mapping update is still, at the Stage level, part
// of the mapping phase (documented judgment call, not SDK-specified).
func nmActivityToStage(activity string) string {
	if activity == StageMappingUpdate {
		return StageMapping
	}

	return activity
}

// resolveNMDefinitionLocked resolves definitionID to its stored
// NetworkMigrationDefinition. Callers must hold b.mu.
func (b *InMemoryBackend) resolveNMDefinitionLocked(definitionID string) (*NetworkMigrationDefinition, bool) {
	return b.nmDefinitions.Get(definitionID)
}

// resolveOrCreateExecutionLocked resolves (definitionID, executionID) to its
// NetworkMigrationExecution, auto-vivifying one if this pair has never been
// seen before -- see this file's doc comment. Returns
// ResourceNotFoundException if definitionID itself does not exist (an
// execution can never be auto-vivified under a nonexistent definition).
// Callers must hold b.mu.
func (b *InMemoryBackend) resolveOrCreateExecutionLocked(
	definitionID, executionID, activity string,
) (*NetworkMigrationExecution, error) {
	if _, ok := b.resolveNMDefinitionLocked(definitionID); !ok {
		return nil, notFoundError(resourceNMDefinition, definitionID)
	}

	if executionID == "" {
		return nil, validationError("networkMigrationExecutionID is required")
	}

	key := nmExecutionKey(definitionID, executionID)
	if e, ok := b.nmExecutions.Get(key); ok {
		now := nowUTC()
		e.Activity = activity
		e.Stage = nmActivityToStage(activity)
		e.Status = NMStatusStarted
		e.UpdatedAt = now

		return e, nil
	}

	now := nowUTC()
	t := tags.New("mgn.nmexecution." + executionID + ".tags")

	e := &NetworkMigrationExecution{
		NetworkMigrationExecutionID:  executionID,
		NetworkMigrationDefinitionID: definitionID,
		Activity:                     activity,
		Stage:                        nmActivityToStage(activity),
		Status:                       NMStatusStarted,
		Tags:                         t,
		CreatedAt:                    now,
		UpdatedAt:                    now,
	}
	b.nmExecutions.Put(e)

	return e, nil
}

// createAndScheduleNMJobLocked creates a NetworkMigrationJob for
// (definitionID, executionID, activity), auto-vivifying the execution if
// needed, and schedules its PENDING -> STARTED -> SUCCEEDED progression
// (mirroring the execution's own Status once the job completes). Callers
// must hold b.mu.
func (b *InMemoryBackend) createAndScheduleNMJobLocked(definitionID, executionID, activity string) (string, error) {
	if _, err := b.resolveOrCreateExecutionLocked(definitionID, executionID, activity); err != nil {
		return "", err
	}

	id := newNMJobID()
	now := nowUTC()

	job := &NetworkMigrationJob{
		JobID:                        id,
		NetworkMigrationDefinitionID: definitionID,
		NetworkMigrationExecutionID:  executionID,
		Activity:                     activity,
		Status:                       NMStatusPending,
		CreatedAt:                    now,
	}
	b.nmJobs.Put(job)

	b.scheduleNMJobLocked(id, definitionID, executionID)

	return id, nil
}

// scheduleNMJobLocked walks jobID PENDING -> STARTED -> SUCCEEDED over 2
// asyncTransitionDelay ticks, mirroring the parent execution's Status once
// the job completes.
func (b *InMemoryBackend) scheduleNMJobLocked(jobID, definitionID, executionID string) {
	b.work.After("NMJobStarted", asyncTransitionDelay, func() {
		b.mu.Lock("NMJobStarted-async")
		if j, ok := b.nmJobs.Get(jobID); ok && j.Status == NMStatusPending {
			j.Status = NMStatusStarted
		}
		b.mu.Unlock()

		b.work.After("NMJobSucceeded", asyncTransitionDelay, func() {
			b.mu.Lock("NMJobSucceeded-async")
			defer b.mu.Unlock()

			j, ok := b.nmJobs.Get(jobID)
			if !ok || j.Status != NMStatusStarted {
				return
			}

			j.Status = NMStatusSucceeded
			j.EndedAt = nowUTC()

			if e, found := b.nmExecutions.Get(nmExecutionKey(definitionID, executionID)); found {
				e.Status = NMStatusSucceeded
				e.UpdatedAt = j.EndedAt
			}
		})
	})
}

// nmJobsForExecution returns every NetworkMigrationJob for (definitionID,
// executionID) matching activity, in Snapshot (deterministic) order.
// Callers must hold b.mu (either lock).
func (b *InMemoryBackend) nmJobsForExecutionLocked(definitionID, executionID, activity string) []*NetworkMigrationJob {
	items := b.nmJobsByExecution.Get(nmExecutionKey(definitionID, executionID))
	out := make([]*NetworkMigrationJob, 0, len(items))

	for _, j := range items {
		if j.Activity == activity {
			out = append(out, j.clone())
		}
	}

	return out
}

// StartNetworkMigrationAnalysis starts a new analysis job.
func (b *InMemoryBackend) StartNetworkMigrationAnalysis(definitionID, executionID string) (string, error) {
	b.mu.Lock("StartNetworkMigrationAnalysis")
	defer b.mu.Unlock()

	return b.createAndScheduleNMJobLocked(definitionID, executionID, StageAnalyze)
}

// ListNetworkMigrationAnalyses returns a page of analysis job details.
func (b *InMemoryBackend) ListNetworkMigrationAnalyses(
	definitionID, executionID, token string,
	limit int,
) (page.Page[*NetworkMigrationJob], error) {
	return b.listNMJobs(definitionID, executionID, StageAnalyze, token, limit)
}

// ListNetworkMigrationAnalysisResults always returns an empty list -- see
// this file's doc comment on why analysis CONTENT is never fabricated.
func (b *InMemoryBackend) ListNetworkMigrationAnalysisResults(definitionID, executionID string) error {
	return b.requireNMScopeExists(definitionID, executionID)
}

// StartNetworkMigrationCodeGeneration starts a new code-generation job.
func (b *InMemoryBackend) StartNetworkMigrationCodeGeneration(definitionID, executionID string) (string, error) {
	b.mu.Lock("StartNetworkMigrationCodeGeneration")
	defer b.mu.Unlock()

	return b.createAndScheduleNMJobLocked(definitionID, executionID, StageCodeGeneration)
}

// ListNetworkMigrationCodeGenerations returns a page of code-generation job
// details.
func (b *InMemoryBackend) ListNetworkMigrationCodeGenerations(
	definitionID, executionID, token string,
	limit int,
) (page.Page[*NetworkMigrationJob], error) {
	return b.listNMJobs(definitionID, executionID, StageCodeGeneration, token, limit)
}

// ListNetworkMigrationCodeGenerationSegments always returns an empty list --
// see this file's doc comment.
func (b *InMemoryBackend) ListNetworkMigrationCodeGenerationSegments(definitionID, executionID string) error {
	return b.requireNMScopeExists(definitionID, executionID)
}

// StartNetworkMigrationDeployment starts a new deployment job.
func (b *InMemoryBackend) StartNetworkMigrationDeployment(definitionID, executionID string) (string, error) {
	b.mu.Lock("StartNetworkMigrationDeployment")
	defer b.mu.Unlock()

	return b.createAndScheduleNMJobLocked(definitionID, executionID, StageDeploy)
}

// ListNetworkMigrationDeployments returns a page of deployment job details.
func (b *InMemoryBackend) ListNetworkMigrationDeployments(
	definitionID, executionID, token string,
	limit int,
) (page.Page[*NetworkMigrationJob], error) {
	return b.listNMJobs(definitionID, executionID, StageDeploy, token, limit)
}

// ListNetworkMigrationDeployedStacks always returns an empty list -- no real
// CloudFormation-equivalent deployment engine exists (see this file's doc
// comment).
func (b *InMemoryBackend) ListNetworkMigrationDeployedStacks(definitionID, executionID string) error {
	return b.requireNMScopeExists(definitionID, executionID)
}

// ListNetworkMigrationExecutionsFilters mirrors
// types.ListNetworkMigrationExecutionRequestFilters.
type ListNetworkMigrationExecutionsFilters struct {
	NetworkMigrationExecutionIDs      []string
	NetworkMigrationExecutionStatuses []string
}

func matchesNMExecutionFilter(e *NetworkMigrationExecution, f ListNetworkMigrationExecutionsFilters) bool {
	if len(f.NetworkMigrationExecutionIDs) > 0 &&
		!containsStr(f.NetworkMigrationExecutionIDs, e.NetworkMigrationExecutionID) {
		return false
	}

	if len(f.NetworkMigrationExecutionStatuses) > 0 && !containsStr(f.NetworkMigrationExecutionStatuses, e.Status) {
		return false
	}

	return true
}

// ListNetworkMigrationExecutions returns a page of NetworkMigrationExecutions
// for definitionID matching f -- the only List op in family N scoped by
// definitionID alone (no ExecutionID scoping key, since it lists the
// executions themselves).
func (b *InMemoryBackend) ListNetworkMigrationExecutions(
	definitionID string,
	f ListNetworkMigrationExecutionsFilters,
	token string,
	limit int,
) (page.Page[*NetworkMigrationExecution], error) {
	b.mu.RLock("ListNetworkMigrationExecutions")
	defer b.mu.RUnlock()

	if _, ok := b.resolveNMDefinitionLocked(definitionID); !ok {
		return page.Page[*NetworkMigrationExecution]{}, notFoundError(resourceNMDefinition, definitionID)
	}

	items := b.nmExecutionsByDef.Get(definitionID)
	filtered := make([]*NetworkMigrationExecution, 0, len(items))

	for _, e := range items {
		if matchesNMExecutionFilter(e, f) {
			filtered = append(filtered, e.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// listNMJobs is the shared paged-list helper backing every family-N List*
// job-details op (Analyses/CodeGenerations/Deployments) plus family M's
// ListNetworkMigrationMappings/MappingUpdates (networkmigration.go).
func (b *InMemoryBackend) listNMJobs(
	definitionID, executionID, activity, token string,
	limit int,
) (page.Page[*NetworkMigrationJob], error) {
	b.mu.RLock("listNMJobs:" + activity)
	defer b.mu.RUnlock()

	if err := b.requireNMScopeExistsLocked(definitionID, executionID); err != nil {
		return page.Page[*NetworkMigrationJob]{}, err
	}

	return page.New(
		b.nmJobsForExecutionLocked(definitionID, executionID, activity),
		token,
		limit,
		defaultPageLimit,
	), nil
}

// requireNMScopeExists validates (definitionID, executionID) both exist,
// taking the read lock itself -- used by the family-N ops whose real SDK
// Output has no Items field to page at all (they only ever return an empty
// wire list -- see this file's doc comment), so there is nothing to build a
// page.Page from.
func (b *InMemoryBackend) requireNMScopeExists(definitionID, executionID string) error {
	b.mu.RLock("requireNMScopeExists")
	defer b.mu.RUnlock()

	return b.requireNMScopeExistsLocked(definitionID, executionID)
}

// requireNMScopeExistsLocked validates (definitionID, executionID) both
// exist. Callers must hold b.mu (either lock).
func (b *InMemoryBackend) requireNMScopeExistsLocked(definitionID, executionID string) error {
	if _, ok := b.resolveNMDefinitionLocked(definitionID); !ok {
		return notFoundError(resourceNMDefinition, definitionID)
	}

	if _, ok := b.nmExecutions.Get(nmExecutionKey(definitionID, executionID)); !ok {
		return notFoundError(resourceNMExecution, executionID)
	}

	return nil
}

package mgn

import (
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family A (16 ops): DescribeSourceServers, UpdateSourceServer,
// UpdateSourceServerReplicationType, DeleteSourceServer,
// ChangeServerLifeCycleState, DisconnectFromService, FinalizeCutover,
// MarkAsArchived, StartTest, StartCutover, StartReplication, StopReplication,
// PauseReplication, ResumeReplication, RetryDataReplication,
// TerminateTargetInstances.
//
// # The hard design problem: how a SourceServer comes to exist at all
//
// No public operation in this 95-op SDK surface creates a SourceServer. Real
// AWS creates one only when the MGN Replication Agent, installed on the
// actual source machine, calls an internal, non-public control-plane API to
// register itself -- that call is not part of this SDK. The only PUBLIC
// creation path is StartImport's bulk metadata load (see exportimport.go),
// which this emulator implements honestly: it never reads real S3 object
// content (no schema for that content exists anywhere in this SDK to derive
// one from -- inventing one would be fabrication), so it always creates zero
// records.
//
// This package's chosen resolution -- an explicit, documented emulator
// decision, never presented as derived AWS behavior -- is SeedSourceServer
// below: an EXPORTED Go function reachable only by calling into this package
// directly (never through any SDK wire operation, never routed by
// handler.go), for exactly the purpose PARITY.md's gaps section describes:
// "an implementer needs a deliberate, explicitly-documented decision for how
// SourceServer records get seeded in this emulator." This package's own
// round-trip tests use it to seed servers before exercising the 16
// operations below, which is the entire, real 70-op replication surface this
// service exists to emulate -- leaving it permanently unreachable (the
// "leave genuinely empty" option) was rejected as far less useful than a
// clearly-labeled, non-AWS seam.
//
// # LifeCycleState transition table (documented, SDK-inferred -- NOT
// independently confirmed against AWS's real, unpublished state machine)
//
//	PENDING_INSTALLATION -> NOT_READY   once the (simulated) replication agent
//	                                    "reports in" -- this backend skips
//	                                    PENDING_INSTALLATION/DISCOVERED
//	                                    entirely and seeds directly into
//	                                    NOT_READY, since there is no real
//	                                    agent-discovery event to model.
//	NOT_READY            -> READY_FOR_TEST   once DataReplicationState reaches
//	                                          CONTINUOUS (scheduleReplication).
//	READY_FOR_TEST       -> TESTING          via StartTest.
//	TESTING              -> READY_FOR_CUTOVER  once the Job completes.
//	READY_FOR_CUTOVER    -> CUTTING_OVER     via StartCutover.
//	CUTTING_OVER         -> CUTOVER          via FinalizeCutover (does NOT
//	                                          happen automatically on Job
//	                                          completion -- FinalizeCutover is
//	                                          a distinct, separate call).
//	CUTOVER              -> DISCONNECTED     via DisconnectFromService, the
//	                                          terminal, expected end state.
//
// ChangeServerLifeCycleState is the one caller-driven escape hatch: it can
// force State directly to READY_FOR_TEST/READY_FOR_CUTOVER/CUTOVER (the only
// 3 values types.ChangeServerLifeCycleStateSourceServerLifecycleState
// exposes), bypassing the table above -- exactly matching real AWS's own
// documented purpose for this call (a manual override for operators).

// resolveSourceServerLocked resolves sourceServerID to its stored
// SourceServer. Callers must hold b.mu.
func (b *InMemoryBackend) resolveSourceServerLocked(sourceServerID string) (*SourceServer, bool) {
	return b.sourceServers.Get(sourceServerID)
}

// SeedSourceServerOptions configures SeedSourceServer. Every field is
// optional; zero values fall back to a documented, invented default (see
// each field's own comment) rather than an AWS-confirmed one, since no real
// source machine exists to derive defaults from.
type SeedSourceServerOptions struct {
	Tags              map[string]string
	SourceServerID    string
	ApplicationID     string
	UserProvidedID    string
	ReplicationType   string
	TotalStorageBytes int64
}

// SeedSourceServer creates a new SourceServer directly in this backend --
// see this file's package-level doc comment for why this exported, non-SDK
// helper exists and why it is never routed as an SDK operation. The new
// server starts in LifeCycleState NOT_READY / DataReplicationState
// INITIATING and progresses on a deterministic timer toward READY_FOR_TEST /
// CONTINUOUS (see scheduleReplicationLocked) -- exactly the same honest,
// time-based progression a real StartImport-created server would need,
// just reachable without inventing an S3 file-format schema this SDK never
// documents.
func (b *InMemoryBackend) SeedSourceServer(opts SeedSourceServerOptions) *SourceServer {
	b.mu.Lock("SeedSourceServer")
	defer b.mu.Unlock()

	id := opts.SourceServerID
	if id == "" {
		id = newSourceServerID()
	}

	replicationType := opts.ReplicationType
	if replicationType == "" {
		replicationType = ReplicationTypeAgentBased
	}

	totalBytes := opts.TotalStorageBytes
	if totalBytes <= 0 {
		totalBytes = defaultTotalStorageBytes
	}

	now := nowRFC3339()
	t := tags.New("mgn.sourceserver." + id + ".tags")
	t.Merge(opts.Tags)

	s := &SourceServer{
		SourceServerID:  id,
		Arn:             b.sourceServerARN(id),
		ApplicationID:   opts.ApplicationID,
		UserProvidedID:  opts.UserProvidedID,
		ReplicationType: replicationType,
		Tags:            t,
		LifeCycle: &LifeCycle{
			State:                     LifeCycleStateNotReady,
			AddedToServiceDateTime:    now,
			LastSeenByServiceDateTime: now,
		},
		DataReplicationInfo: &DataReplicationInfo{
			DataReplicationState: DataReplicationStateInitiating,
			DataReplicationInitiation: &DataReplicationInitiation{
				StartDateTime: now,
				Steps:         seedInitiationSteps(),
			},
			ReplicatedDisks: []DataReplicationInfoReplicatedDisk{
				{DeviceName: "/dev/sda1", TotalStorageBytes: totalBytes},
			},
		},
	}

	b.sourceServers.Put(s)

	b.launchConfigs.Put(&LaunchConfiguration{
		SourceServerID:    id,
		Name:              "Dr " + id,
		BootMode:          BootModeUseSource,
		LaunchDisposition: LaunchDispositionStarted,
	})
	b.replicationConfigs.Put(&ReplicationConfiguration{
		SourceServerID:                id,
		Name:                          "Replication Configuration " + id,
		DataPlaneRouting:              DataPlaneRoutingPrivateIP,
		DefaultLargeStagingDiskType:   StagingDiskTypeGp3,
		EbsEncryption:                 EbsEncryptionDefault,
		AssociateDefaultSecurityGroup: true,
		ReplicationServerInstanceType: "t3.small",
	})

	b.scheduleReplicationLocked(id)

	return s.clone()
}

// seedInitiationSteps returns the fixed 12-step DataReplicationInitiation
// walk, all NOT_STARTED except the first (WAIT), which begins IN_PROGRESS.
func seedInitiationSteps() []DataReplicationInitiationStep {
	names := dataReplicationInitiationStepNames()
	steps := make([]DataReplicationInitiationStep, len(names))

	for i, name := range names {
		status := StepStatusNotStarted
		if i == 0 {
			status = StepStatusInProgress
		}

		steps[i] = DataReplicationInitiationStep{Name: name, Status: status}
	}

	return steps
}

// scheduleReplicationLocked walks a newly seeded/restarted SourceServer's
// DataReplicationInfo through INITIATING -> INITIAL_SYNC -> BACKLOG ->
// CONTINUOUS over 3 asyncTransitionDelay ticks, monotonically increasing
// ReplicatedStorageBytes toward TotalStorageBytes -- a deterministic,
// time-based progression (PARITY.md's explicit guidance), never a
// fabricated realistic-looking bandwidth/lag figure. All 12
// DataReplicationInitiationStep entries are marked SUCCEEDED together at the
// first tick rather than walked one real timer-tick per step: the SDK
// documents 12 discrete steps, but per-step ticks would need ~1.2s of real
// wall-clock time per seeded server purely for test-timing reasons with no
// additional honesty gained (PARITY.md explicitly sanctions "returning them
// all at once ... if explicitly documented" as a defensible simpler pass).
// Every tick re-checks the server still exists and is still in the state
// this scheduler put it in before mutating, so a later
// Stop/Pause/DisconnectFromService call correctly halts further progression
// without needing to cancel the underlying timer. Each tick's own mutation
// is a separate named tickReplicationXLocked method purely to keep this
// function's own cognitive complexity low (decomposition, not suppression
// -- see .claude/memories/parity-principles.md's ban on cyclop/funlen/
// gocyclo/gocognit suppressions).
func (b *InMemoryBackend) scheduleReplicationLocked(sourceServerID string) {
	b.work.After("ReplicationInitiated", asyncTransitionDelay, func() {
		b.tickReplicationInitiatedLocked(sourceServerID)

		b.work.After("ReplicationBacklog", asyncTransitionDelay, func() {
			b.tickReplicationBacklogLocked(sourceServerID)

			b.work.After("ReplicationContinuous", asyncTransitionDelay, func() {
				b.tickReplicationContinuousLocked(sourceServerID)
			})
		})
	})
}

// tickReplicationInitiatedLocked marks every DataReplicationInitiation step
// SUCCEEDED and moves DataReplicationState INITIATING -> INITIAL_SYNC.
func (b *InMemoryBackend) tickReplicationInitiatedLocked(sourceServerID string) {
	b.mu.Lock("ReplicationInitiated-async")
	defer b.mu.Unlock()

	s, ok := b.sourceServers.Get(sourceServerID)
	if !ok || s.DataReplicationInfo == nil ||
		s.DataReplicationInfo.DataReplicationState != DataReplicationStateInitiating {
		return
	}

	for i := range s.DataReplicationInfo.DataReplicationInitiation.Steps {
		s.DataReplicationInfo.DataReplicationInitiation.Steps[i].Status = StepStatusSucceeded
	}

	s.DataReplicationInfo.DataReplicationState = DataReplicationStateInitialSync
}

// tickReplicationBacklogLocked moves DataReplicationState INITIAL_SYNC ->
// BACKLOG, bringing each replicated disk halfway to TotalStorageBytes.
func (b *InMemoryBackend) tickReplicationBacklogLocked(sourceServerID string) {
	const halfway = 2

	b.mu.Lock("ReplicationBacklog-async")
	defer b.mu.Unlock()

	s, ok := b.sourceServers.Get(sourceServerID)
	if !ok || s.DataReplicationInfo == nil ||
		s.DataReplicationInfo.DataReplicationState != DataReplicationStateInitialSync {
		return
	}

	s.DataReplicationInfo.DataReplicationState = DataReplicationStateBacklog

	for i := range s.DataReplicationInfo.ReplicatedDisks {
		d := &s.DataReplicationInfo.ReplicatedDisks[i]
		d.ReplicatedStorageBytes = d.TotalStorageBytes / halfway
		d.BackloggedStorageBytes = d.TotalStorageBytes - d.ReplicatedStorageBytes
	}
}

// tickReplicationContinuousLocked moves DataReplicationState BACKLOG ->
// CONTINUOUS, bringing each replicated disk fully caught up, and flips
// LifeCycleState NOT_READY -> READY_FOR_TEST.
func (b *InMemoryBackend) tickReplicationContinuousLocked(sourceServerID string) {
	b.mu.Lock("ReplicationContinuous-async")
	defer b.mu.Unlock()

	s, ok := b.sourceServers.Get(sourceServerID)
	if !ok || s.DataReplicationInfo == nil ||
		s.DataReplicationInfo.DataReplicationState != DataReplicationStateBacklog {
		return
	}

	s.DataReplicationInfo.DataReplicationState = DataReplicationStateContinuous
	s.DataReplicationInfo.LastSnapshotDateTime = nowRFC3339()

	for i := range s.DataReplicationInfo.ReplicatedDisks {
		d := &s.DataReplicationInfo.ReplicatedDisks[i]
		d.ReplicatedStorageBytes = d.TotalStorageBytes
		d.BackloggedStorageBytes = 0
	}

	if s.LifeCycle != nil && s.LifeCycle.State == LifeCycleStateNotReady {
		s.LifeCycle.State = LifeCycleStateReadyForTest
	}
}

// DescribeSourceServersFilters mirrors types.DescribeSourceServersRequestFilters.
type DescribeSourceServersFilters struct {
	ApplicationIDs   []string
	IsArchived       *bool
	LifeCycleStates  []string
	ReplicationTypes []string
	SourceServerIDs  []string
}

func matchesSourceServerFilter(s *SourceServer, f DescribeSourceServersFilters) bool {
	if len(f.SourceServerIDs) > 0 && !containsStr(f.SourceServerIDs, s.SourceServerID) {
		return false
	}

	if len(f.ApplicationIDs) > 0 && !containsStr(f.ApplicationIDs, s.ApplicationID) {
		return false
	}

	if f.IsArchived != nil && s.IsArchived != *f.IsArchived {
		return false
	}

	if len(f.ReplicationTypes) > 0 && !containsStr(f.ReplicationTypes, s.ReplicationType) {
		return false
	}

	if len(f.LifeCycleStates) > 0 {
		state := ""
		if s.LifeCycle != nil {
			state = s.LifeCycle.State
		}

		if !containsStr(f.LifeCycleStates, state) {
			return false
		}
	}

	return true
}

// DescribeSourceServers returns a page of SourceServers matching f.
func (b *InMemoryBackend) DescribeSourceServers(
	f DescribeSourceServersFilters,
	token string,
	limit int,
) (page.Page[*SourceServer], error) {
	b.mu.RLock("DescribeSourceServers")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*SourceServer]{}, err
	}

	all := b.sourceServers.Snapshot()
	filtered := make([]*SourceServer, 0, len(all))

	for _, s := range all {
		if matchesSourceServerFilter(s, f) {
			filtered = append(filtered, s.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// UpdateSourceServer applies a ConnectorAction to sourceServerID and returns
// the flattened SourceServer (PARITY.md wire-trap #1).
func (b *InMemoryBackend) UpdateSourceServer(
	sourceServerID string,
	action *SourceServerConnectorAction,
) (*SourceServer, error) {
	b.mu.Lock("UpdateSourceServer")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	s.ConnectorAction = action

	return s.clone(), nil
}

// UpdateSourceServerReplicationType changes sourceServerID's ReplicationType.
func (b *InMemoryBackend) UpdateSourceServerReplicationType(
	sourceServerID, replicationType string,
) (*SourceServer, error) {
	b.mu.Lock("UpdateSourceServerReplicationType")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if replicationType != ReplicationTypeAgentBased && replicationType != ReplicationTypeSnapshotShipping {
		return nil, validationError("replicationType must be AGENT_BASED or SNAPSHOT_SHIPPING")
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	s.ReplicationType = replicationType

	return s.clone(), nil
}

// DeleteSourceServer deletes sourceServerID and its per-server launch/
// replication configuration and post-launch actions.
func (b *InMemoryBackend) DeleteSourceServer(sourceServerID string) error {
	b.mu.Lock("DeleteSourceServer")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.Tags != nil {
		s.Tags.Close()
	}

	b.sourceServers.Delete(sourceServerID)
	b.launchConfigs.Delete(sourceServerID)
	b.replicationConfigs.Delete(sourceServerID)

	for _, a := range b.sourceServerActionsByServer.Get(sourceServerID) {
		b.sourceServerActions.Delete(actionKey(a.SourceServerID, a.ActionID))
	}

	return nil
}

// ChangeServerLifeCycleState forces sourceServerID's LifeCycleState to one
// of the 3 caller-settable targets (see this file's doc comment).
func (b *InMemoryBackend) ChangeServerLifeCycleState(sourceServerID, targetState string) (*SourceServer, error) {
	b.mu.Lock("ChangeServerLifeCycleState")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	switch targetState {
	case ChangeLifecycleReadyForTest, ChangeLifecycleReadyForCutover, ChangeLifecycleCutover:
	default:
		return nil, validationError("lifeCycle.state must be READY_FOR_TEST, READY_FOR_CUTOVER, or CUTOVER")
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.LifeCycle == nil {
		s.LifeCycle = &LifeCycle{}
	}

	s.LifeCycle.State = targetState

	return s.clone(), nil
}

// DisconnectFromService moves sourceServerID to its terminal DISCONNECTED
// state (both LifeCycleState and DataReplicationState).
func (b *InMemoryBackend) DisconnectFromService(sourceServerID string) (*SourceServer, error) {
	b.mu.Lock("DisconnectFromService")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.LifeCycle == nil {
		s.LifeCycle = &LifeCycle{}
	}

	s.LifeCycle.State = LifeCycleStateDisconnected

	if s.DataReplicationInfo != nil {
		s.DataReplicationInfo.DataReplicationState = DataReplicationStateDisconnected
	}

	return s.clone(), nil
}

// FinalizeCutover completes a cutover already started by StartCutover --
// see this file's doc comment: CUTTING_OVER -> CUTOVER is the one
// LifeCycleState transition that happens ONLY here, not automatically on
// Job completion.
func (b *InMemoryBackend) FinalizeCutover(sourceServerID string) (*SourceServer, error) {
	b.mu.Lock("FinalizeCutover")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.LifeCycle == nil || s.LifeCycle.State != LifeCycleStateCuttingOver {
		return nil, conflictErrorWithResource(
			resourceSourceServer, sourceServerID,
			"source server is not in a cutting-over state: "+sourceServerID,
		)
	}

	s.LifeCycle.State = LifeCycleStateCutover

	if s.LifeCycle.LastCutover == nil {
		s.LifeCycle.LastCutover = &LifeCycleLastCutover{}
	}

	s.LifeCycle.LastCutover.Finalized = &timestamped{APICallDateTime: nowRFC3339()}

	return s.clone(), nil
}

// MarkAsArchived sets sourceServerID's IsArchived flag -- a visibility/
// cleanup flag orthogonal to LifeCycleState (an archived server can be in
// any lifecycle state -- see this file's doc comment).
func (b *InMemoryBackend) MarkAsArchived(sourceServerID string) (*SourceServer, error) {
	b.mu.Lock("MarkAsArchived")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	s.IsArchived = true

	return s.clone(), nil
}

// StartReplication restarts data replication for a stopped/disconnected
// source server -- a void-result op (StartReplicationOutput genuinely has
// no fields beyond ResultMetadata, confirmed by direct SDK read, matching
// PARITY.md's note that this asymmetry with its Stop/Pause/Resume siblings
// is real, not an oversight).
func (b *InMemoryBackend) StartReplication(sourceServerID string) error {
	b.mu.Lock("StartReplication")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.DataReplicationInfo == nil {
		s.DataReplicationInfo = &DataReplicationInfo{}
	}

	s.DataReplicationInfo.DataReplicationState = DataReplicationStateInitiating
	s.DataReplicationInfo.DataReplicationInitiation = &DataReplicationInitiation{
		StartDateTime: nowRFC3339(),
		Steps:         seedInitiationSteps(),
	}

	b.scheduleReplicationLocked(sourceServerID)

	return nil
}

// StopReplication halts data replication. Any in-flight
// scheduleReplicationLocked tick becomes a no-op once it observes the state
// is no longer what it expects (see that function's own doc comment).
func (b *InMemoryBackend) StopReplication(sourceServerID string) (*SourceServer, error) {
	return b.setReplicationState(sourceServerID, DataReplicationStateStopped)
}

// PauseReplication pauses data replication.
func (b *InMemoryBackend) PauseReplication(sourceServerID string) (*SourceServer, error) {
	return b.setReplicationState(sourceServerID, DataReplicationStatePaused)
}

// ResumeReplication resumes a paused replication back to CONTINUOUS --
// simplified: this emulator has no real backlog to re-drain, so resuming
// goes directly back to CONTINUOUS rather than re-walking INITIAL_SYNC/
// BACKLOG, a documented simplification (not an SDK-specified behavior).
func (b *InMemoryBackend) ResumeReplication(sourceServerID string) (*SourceServer, error) {
	b.mu.Lock("ResumeReplication")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.DataReplicationInfo == nil || s.DataReplicationInfo.DataReplicationState != DataReplicationStatePaused {
		return nil, conflictErrorWithResource(
			resourceSourceServer,
			sourceServerID,
			"source server is not paused: "+sourceServerID,
		)
	}

	s.DataReplicationInfo.DataReplicationState = DataReplicationStateContinuous

	return s.clone(), nil
}

// RetryDataReplication restarts a stalled/errored replication. This emulator
// never fabricates a failure condition (no DataReplicationErrorString value
// is ever set -- PARITY.md), so there is no real "stalled" state to retry
// out of; this always succeeds by restarting the same deterministic
// progression StartReplication uses, documented as a simplification.
func (b *InMemoryBackend) RetryDataReplication(sourceServerID string) (*SourceServer, error) {
	b.mu.Lock("RetryDataReplication")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	s.DataReplicationInfo = &DataReplicationInfo{
		DataReplicationState: DataReplicationStateInitiating,
		DataReplicationInitiation: &DataReplicationInitiation{
			StartDateTime: nowRFC3339(),
			Steps:         seedInitiationSteps(),
		},
		ReplicatedDisks: s.DataReplicationInfo.ReplicatedDisks,
	}

	b.scheduleReplicationLocked(sourceServerID)

	return s.clone(), nil
}

// setReplicationState is the shared helper backing Stop/PauseReplication --
// both simply pin DataReplicationState to a caller-requested terminal-ish
// value.
func (b *InMemoryBackend) setReplicationState(sourceServerID, state string) (*SourceServer, error) {
	b.mu.Lock("setReplicationState:" + state)
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	s, ok := b.resolveSourceServerLocked(sourceServerID)
	if !ok {
		return nil, notFoundError(resourceSourceServer, sourceServerID)
	}

	if s.DataReplicationInfo == nil {
		s.DataReplicationInfo = &DataReplicationInfo{}
	}

	s.DataReplicationInfo.DataReplicationState = state

	return s.clone(), nil
}

// StartTest starts a batch test Job across sourceServerIDs. Real AWS
// requires READY_FOR_TEST; StartTest's own error set (Conflict,
// UninitializedAccount, Validation -- no ResourceNotFound, confirmed by
// direct SDK read) means an unknown SourceServerID cannot 404 here, so this
// backend folds it into ValidationException instead (a documented
// necessity, not a guess -- there is no other error shape this op can use
// for that condition).
func (b *InMemoryBackend) StartTest(sourceServerIDs []string, jobTags map[string]string) (*Job, error) {
	return b.startBatchJob(sourceServerIDs, jobTags, JobTypeLaunch, InitiatedByStartTest)
}

// StartCutover starts a batch cutover Job across sourceServerIDs. Same
// error-shape constraint as StartTest -- see its doc comment.
func (b *InMemoryBackend) StartCutover(sourceServerIDs []string, jobTags map[string]string) (*Job, error) {
	return b.startBatchJob(sourceServerIDs, jobTags, JobTypeLaunch, InitiatedByStartCutover)
}

// TerminateTargetInstances starts a batch terminate Job across
// sourceServerIDs. Same error-shape constraint as StartTest -- see its doc
// comment.
func (b *InMemoryBackend) TerminateTargetInstances(sourceServerIDs []string, jobTags map[string]string) (*Job, error) {
	return b.startBatchJob(sourceServerIDs, jobTags, JobTypeTerminate, InitiatedByTerminate)
}

// startBatchJob validates sourceServerIDs and each one's LifeCycleState
// precondition for kind, then delegates Job creation/scheduling to
// jobs.go's createAndScheduleJobLocked. Preconditions
// (StartTest requires READY_FOR_TEST, StartCutover requires
// READY_FOR_CUTOVER, TerminateTargetInstances requires none beyond
// existing) are this package's own inference from field/enum semantics, not
// independently SDK-confirmed (PARITY.md).
func (b *InMemoryBackend) startBatchJob(
	sourceServerIDs []string,
	jobTags map[string]string,
	jobType, initiatedBy string,
) (*Job, error) {
	b.mu.Lock("startBatchJob:" + initiatedBy)
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if len(sourceServerIDs) == 0 {
		return nil, validationError("sourceServerIDs must not be empty")
	}

	servers := make([]*SourceServer, 0, len(sourceServerIDs))

	for _, id := range sourceServerIDs {
		s, ok := b.resolveSourceServerLocked(id)
		if !ok {
			return nil, validationError("unknown source server: " + id)
		}

		if err := requireLifecyclePrecondition(s, initiatedBy); err != nil {
			return nil, err
		}

		servers = append(servers, s)
	}

	return b.createAndScheduleJobLocked(servers, jobTags, jobType, initiatedBy), nil
}

// requireLifecyclePrecondition enforces the (documented, SDK-inferred)
// legal precondition for starting a batch job of the given kind on s.
func requireLifecyclePrecondition(s *SourceServer, initiatedBy string) error {
	state := ""
	if s.LifeCycle != nil {
		state = s.LifeCycle.State
	}

	switch initiatedBy {
	case InitiatedByStartTest:
		if state != LifeCycleStateReadyForTest {
			return conflictErrorWithResource(
				resourceSourceServer, s.SourceServerID,
				"source server is not ready for test: "+s.SourceServerID,
			)
		}
	case InitiatedByStartCutover:
		if state != LifeCycleStateReadyForCutover {
			return conflictErrorWithResource(
				resourceSourceServer, s.SourceServerID,
				"source server is not ready for cutover: "+s.SourceServerID,
			)
		}
	}

	return nil
}

// containsStr reports whether ss contains s.
func containsStr(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

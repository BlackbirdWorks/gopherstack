package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family B (3 ops): DescribeJobs, DescribeJobLogItems,
// DeleteJob -- plus createAndScheduleJobLocked, the shared Job-creation/
// progression engine sourceservers.go's StartTest/StartCutover/
// TerminateTargetInstances all delegate to.
//
// # Job progression (an honest async walk, per PARITY.md's guidance)
//
// PENDING -> STARTED -> [per participating server: SNAPSHOT_START/END ->
// CONVERSION_START/END -> LAUNCH_START] -> JOB_END -> COMPLETED, over 4
// asyncTransitionDelay ticks. This is a deterministic simulation that always
// succeeds (no JobLogEvent value describing failure/skip/cancel --
// SERVER_SKIPPED, CLEANUP_*, LAUNCH_FAILED, JOB_CANCEL -- is ever emitted,
// since no real launch engine exists to fail): JobStatus itself has no
// FAILED value at all (only PENDING/STARTED/COMPLETED, confirmed by direct
// SDK read), so this backend's own rollup rule -- "COMPLETED once every
// participating server's LaunchStatus is terminal" -- is a documented
// implementation choice (not SDK-specified) that this simulation never
// actually needs to test the FAILED-adjacent half of, since nothing here
// ever fails.

// createAndScheduleJobLocked creates a new Job spanning servers, applies the
// (documented, SDK-inferred) LifeCycleState transition each initiatedBy
// kind causes, and schedules its async progression. Callers must hold
// b.mu.
func (b *InMemoryBackend) createAndScheduleJobLocked(
	servers []*SourceServer,
	jobTags map[string]string,
	jobType, initiatedBy string,
) *Job {
	id := newJobID()
	now := nowRFC3339()

	t := tags.New("mgn.job." + id + ".tags")
	t.Merge(jobTags)

	participants := make([]*ParticipatingServer, len(servers))
	for i, s := range servers {
		participants[i] = &ParticipatingServer{SourceServerID: s.SourceServerID, LaunchStatus: LaunchStatusPending}
		applyJobStartLifecycle(s, initiatedBy, id, now)
	}

	job := &Job{
		JobID:                id,
		Arn:                  b.jobARN(id),
		CreationDateTime:     now,
		InitiatedBy:          initiatedBy,
		Status:               JobStatusPending,
		Type:                 jobType,
		Tags:                 t,
		ParticipatingServers: participants,
	}
	b.jobs.Put(job)

	b.scheduleJobLocked(id, initiatedBy)

	return job.clone()
}

// applyJobStartLifecycle applies the LifeCycleState transition StartTest/
// StartCutover cause the instant the Job is created (TerminateTargetInstances
// causes none at creation time -- see this file's doc comment).
func applyJobStartLifecycle(s *SourceServer, initiatedBy, jobID, now string) {
	if s.LifeCycle == nil {
		s.LifeCycle = &LifeCycle{}
	}

	switch initiatedBy {
	case InitiatedByStartTest:
		s.LifeCycle.State = LifeCycleStateTesting

		if s.LifeCycle.LastTest == nil {
			s.LifeCycle.LastTest = &LifeCycleLastTest{}
		}

		s.LifeCycle.LastTest.Initiated = &timestampedJobRef{APICallDateTime: now, JobID: jobID}
	case InitiatedByStartCutover:
		s.LifeCycle.State = LifeCycleStateCuttingOver

		if s.LifeCycle.LastCutover == nil {
			s.LifeCycle.LastCutover = &LifeCycleLastCutover{}
		}

		s.LifeCycle.LastCutover.Initiated = &timestampedJobRef{APICallDateTime: now, JobID: jobID}
	}
}

// addJobLogLocked appends one JobLog entry for jobID. Callers must hold
// b.mu.
func (b *InMemoryBackend) addJobLogLocked(jobID, event string, data *JobLogEventData) {
	b.jobLogs.Put(&JobLog{
		LogID:       jobID + "#" + randomHexID(),
		JobID:       jobID,
		Event:       event,
		LogDateTime: nowRFC3339(),
		EventData:   data,
	})
}

// scheduleJobLocked walks jobID through its 4-tick progression (see this
// file's doc comment), guarding every tick on the Job's still being in the
// state this scheduler expects -- matching sourceservers.go's
// scheduleReplicationLocked idiom, so a DeleteJob mid-flight simply makes
// later ticks harmless no-ops. Each tick's own mutation is a separate named
// tickXLocked method purely to keep this function's own cognitive
// complexity low (decomposition, not suppression -- see
// .claude/memories/parity-principles.md's ban on cyclop/funlen/gocyclo/
// gocognit suppressions). Callers must hold b.mu (this method itself only
// schedules; it does not mutate synchronously).
func (b *InMemoryBackend) scheduleJobLocked(jobID, initiatedBy string) {
	b.work.After("JobStarted", asyncTransitionDelay, func() {
		b.tickJobStartedLocked(jobID)

		b.work.After("JobConverting", asyncTransitionDelay, func() {
			b.tickJobConvertingLocked(jobID)

			b.work.After("JobLaunching", asyncTransitionDelay, func() {
				b.tickJobLaunchingLocked(jobID, initiatedBy)

				b.work.After("JobEnd", asyncTransitionDelay, func() {
					b.mu.Lock("JobEnd-async")
					defer b.mu.Unlock()

					b.finishJobLocked(jobID, initiatedBy)
				})
			})
		})
	})
}

// tickJobStartedLocked moves jobID PENDING -> STARTED, writing JOB_START and
// per-participant SNAPSHOT_START log entries.
func (b *InMemoryBackend) tickJobStartedLocked(jobID string) {
	b.mu.Lock("JobStarted-async")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobID)
	if !ok || job.Status != JobStatusPending {
		return
	}

	job.Status = JobStatusStarted
	b.addJobLogLocked(jobID, JobLogEventJobStart, nil)

	for _, p := range job.ParticipatingServers {
		p.LaunchStatus = LaunchStatusInProgress
		b.addJobLogLocked(jobID, JobLogEventSnapshotStart, &JobLogEventData{SourceServerID: p.SourceServerID})
	}
}

// tickJobConvertingLocked writes per-participant SNAPSHOT_END/
// CONVERSION_START log entries.
func (b *InMemoryBackend) tickJobConvertingLocked(jobID string) {
	b.mu.Lock("JobConverting-async")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobID)
	if !ok || job.Status != JobStatusStarted {
		return
	}

	for _, p := range job.ParticipatingServers {
		b.addJobLogLocked(jobID, JobLogEventSnapshotEnd, &JobLogEventData{SourceServerID: p.SourceServerID})
		b.addJobLogLocked(jobID, JobLogEventConversionStart, &JobLogEventData{SourceServerID: p.SourceServerID})
	}
}

// tickJobLaunchingLocked writes per-participant CONVERSION_END/LAUNCH_START
// log entries and mints each non-terminate participant's synthetic EC2
// instance ID.
func (b *InMemoryBackend) tickJobLaunchingLocked(jobID, initiatedBy string) {
	b.mu.Lock("JobLaunching-async")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobID)
	if !ok || job.Status != JobStatusStarted {
		return
	}

	for _, p := range job.ParticipatingServers {
		b.addJobLogLocked(jobID, JobLogEventConversionEnd, &JobLogEventData{SourceServerID: p.SourceServerID})
		b.addJobLogLocked(jobID, JobLogEventLaunchStart, &JobLogEventData{SourceServerID: p.SourceServerID})

		if initiatedBy != InitiatedByTerminate {
			p.LaunchedEc2InstanceID = newSyntheticInstanceID()
		}
	}
}

// finishJobLocked marks jobID COMPLETED, writes the final JOB_END log entry,
// and applies each participating server's terminal LifeCycleState/
// LaunchedInstance mutation for initiatedBy. Callers must hold b.mu.
func (b *InMemoryBackend) finishJobLocked(jobID, initiatedBy string) {
	job, ok := b.jobs.Get(jobID)
	if !ok || job.Status != JobStatusStarted {
		return
	}

	job.Status = JobStatusCompleted
	job.EndDateTime = nowRFC3339()
	b.addJobLogLocked(jobID, JobLogEventJobEnd, nil)

	for _, p := range job.ParticipatingServers {
		s, found := b.sourceServers.Get(p.SourceServerID)
		if !found {
			continue
		}

		switch initiatedBy {
		case InitiatedByStartTest:
			p.LaunchStatus = LaunchStatusLaunched
			s.LaunchedInstance = &LaunchedInstance{Ec2InstanceID: p.LaunchedEc2InstanceID, JobID: jobID}

			if s.LifeCycle != nil && s.LifeCycle.State == LifeCycleStateTesting {
				s.LifeCycle.State = LifeCycleStateReadyForCutover
			}
		case InitiatedByStartCutover:
			p.LaunchStatus = LaunchStatusLaunched
			s.LaunchedInstance = &LaunchedInstance{Ec2InstanceID: p.LaunchedEc2InstanceID, JobID: jobID}
		case InitiatedByTerminate:
			p.LaunchStatus = LaunchStatusTerminated
			s.LaunchedInstance = nil
		}
	}
}

// newSyntheticInstanceID mints a gopherstack-format EC2 instance ID
// ("i-" + 17 hex chars, matching real EC2's own ID shape) for a Job's
// LaunchedInstance.Ec2InstanceID. This is a bookkeeping-only synthetic ID,
// NOT cross-checked against a real services/ec2 instance -- see
// models.go's LaunchedInstance doc comment and PARITY.md's cross-service
// wiring section (real EC2 launch on cutover is a documented follow-on, out
// of this pass's scope).
func newSyntheticInstanceID() string { return "i-" + randomHexID() + randomHexID()[:2] }

// DescribeJobsFilters mirrors types.DescribeJobsRequestFilters. FromDate/
// ToDate filtering is NOT implemented (this backend's Job.CreationDateTime
// is an opaque RFC3339 string per models.go's convention, and filtering by
// date range is not exercised by this pass's round-trip tests) -- JobIDs
// filtering is the one implemented, real filter.
type DescribeJobsFilters struct {
	JobIDs []string
}

func matchesJobFilter(j *Job, f DescribeJobsFilters) bool {
	return len(f.JobIDs) == 0 || containsStr(f.JobIDs, j.JobID)
}

// DescribeJobs returns a page of Jobs matching f.
func (b *InMemoryBackend) DescribeJobs(f DescribeJobsFilters, token string, limit int) (page.Page[*Job], error) {
	b.mu.RLock("DescribeJobs")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*Job]{}, err
	}

	all := b.jobs.Snapshot()
	filtered := make([]*Job, 0, len(all))

	for _, j := range all {
		if matchesJobFilter(j, f) {
			filtered = append(filtered, j.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// DescribeJobLogItems returns a page of JobLog entries for jobID, in
// creation order.
func (b *InMemoryBackend) DescribeJobLogItems(jobID, token string, limit int) (page.Page[*JobLog], error) {
	b.mu.RLock("DescribeJobLogItems")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*JobLog]{}, err
	}

	items := b.jobLogsByJob.Get(jobID)
	cloned := make([]*JobLog, len(items))

	for i, l := range items {
		cloned[i] = l.clone()
	}

	return page.New(cloned, token, limit, defaultPageLimit), nil
}

// DeleteJob deletes jobID and its log entries. Rejected (ConflictException)
// while the Job is still PENDING/STARTED.
func (b *InMemoryBackend) DeleteJob(jobID string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return err
	}

	job, ok := b.jobs.Get(jobID)
	if !ok {
		return notFoundError(resourceJob, jobID)
	}

	if job.Status == JobStatusPending || job.Status == JobStatusStarted {
		return conflictErrorWithResource(resourceJob, jobID, "job is still running: "+jobID)
	}

	if job.Tags != nil {
		job.Tags.Close()
	}

	b.jobs.Delete(jobID)

	for _, l := range b.jobLogsByJob.Get(jobID) {
		b.jobLogs.Delete(l.LogID)
	}

	return nil
}

package amplify

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	// defaultAmplifyJanitorInterval is short relative to other services'
	// janitors (e.g. CodeBuild/Batch default to a minute) because Amplify
	// deployment jobs and domain verification are the kind of thing a caller
	// commonly polls right after starting, and there is no real build fleet
	// behind this emulator to wait on.
	defaultAmplifyJanitorInterval = 5 * time.Second

	amplifyWorkerServiceName = "amplify"
	jobAdvancerComponent     = "InProgressJobAdvancer"
	domainAdvancerComponent  = "PendingDomainAdvancer"
)

// isTerminalJobStatus reports whether a job status is terminal, i.e. no
// further backend action will ever change it.
func isTerminalJobStatus(status JobStatus) bool {
	return status == JobStatusSucceed || status == JobStatusFailed || status == JobStatusCancelled
}

// isTerminalDomainStatus reports whether a domain association status is
// terminal, i.e. no further backend action will ever change it.
func isTerminalDomainStatus(status DomainStatus) bool {
	return status == DomainStatusAvailable || status == DomainStatusFailed
}

// Janitor is the Amplify background worker that advances jobs and domain
// associations out of their transient starting states.
//
// StartJob/StartDeployment only ever set a job's Status to RUNNING, and
// CreateDomainAssociation only ever sets a domain's DomainStatus to
// PENDING_VERIFICATION -- real Amplify eventually finishes the build /
// verifies the certificate on its own, but nothing else in this backend ever
// mutates those fields again (StopJob/DeleteDomainAssociation require an
// explicit caller action). Without this janitor a job or domain association
// would sit in its transient state forever, and any client polling
// GetJob/ListJobs or GetDomainAssociation/ListDomainAssociations to wait for
// completion would spin indefinitely.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new Amplify Janitor for the given backend. A zero
// interval falls back to defaultAmplifyJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultAmplifyJanitorInterval
	}

	return &Janitor{Backend: backend, Interval: interval}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, amplifyWorkerServiceName)
	g.Ticker(
		jobAdvancerComponent,
		j.Interval,
		j.TaskTimeout,
		j.tick,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single janitor pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.tick(ctx)
}

// tick performs one full janitor pass: advance RUNNING jobs to SUCCEED, then
// advance PENDING_VERIFICATION (or otherwise non-terminal) domain
// associations to AVAILABLE.
func (j *Janitor) tick(ctx context.Context) {
	j.advanceJobs(ctx)
	j.advanceDomains(ctx)
}

// advanceJobs transitions jobs still in RUNNING to a terminal SUCCEED state.
func (j *Janitor) advanceJobs(ctx context.Context) {
	var jobIDs []string

	j.Backend.mu.RLock("AmplifyJanitorAdvanceJobsRead")

	for _, job := range j.Backend.jobs.All() {
		if !isTerminalJobStatus(job.Status) {
			jobIDs = append(jobIDs, jobKey(job.AppID, job.BranchName, job.JobID))
		}
	}

	j.Backend.mu.RUnlock()

	if len(jobIDs) == 0 {
		return
	}

	now := time.Now().UTC()

	j.Backend.mu.Lock("AmplifyJanitorAdvanceJobs")

	for _, key := range jobIDs {
		if job, ok := j.Backend.jobs.Get(key); ok && !isTerminalJobStatus(job.Status) {
			job.Status = JobStatusSucceed
			job.EndTime = now
		}
	}

	j.Backend.mu.Unlock()

	count := len(jobIDs)

	telemetry.RecordWorkerTask(amplifyWorkerServiceName, jobAdvancerComponent, "success")
	telemetry.RecordWorkerItems(amplifyWorkerServiceName, jobAdvancerComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Amplify janitor: jobs advanced to SUCCEED", "count", count)
}

// advanceDomains transitions domain associations that are not yet terminal to
// AVAILABLE, marking every configured subdomain verified in the process (real
// Amplify verifies each subdomain's DNS record before the association as a
// whole becomes AVAILABLE).
func (j *Janitor) advanceDomains(ctx context.Context) {
	var domainKeys []string

	j.Backend.mu.RLock("AmplifyJanitorAdvanceDomainsRead")

	for _, domain := range j.Backend.domains.All() {
		if !isTerminalDomainStatus(domain.DomainStatus) {
			domainKeys = append(domainKeys, domainKey(domain.AppID, domain.DomainName))
		}
	}

	j.Backend.mu.RUnlock()

	if len(domainKeys) == 0 {
		return
	}

	j.Backend.mu.Lock("AmplifyJanitorAdvanceDomains")

	for _, key := range domainKeys {
		domain, ok := j.Backend.domains.Get(key)
		if !ok || isTerminalDomainStatus(domain.DomainStatus) {
			continue
		}

		domain.DomainStatus = DomainStatusAvailable

		for i := range domain.SubDomains {
			domain.SubDomains[i].Verified = true
		}
	}

	j.Backend.mu.Unlock()

	count := len(domainKeys)

	telemetry.RecordWorkerTask(amplifyWorkerServiceName, domainAdvancerComponent, "success")
	telemetry.RecordWorkerItems(amplifyWorkerServiceName, domainAdvancerComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Amplify janitor: domain associations advanced to AVAILABLE", "count", count)
}

package batch

import (
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	defaultBatchLogGroup = "/aws/batch/job"
	logGroupOption       = "awslogs-group"
	logStreamPrefixOpt   = "awslogs-stream-prefix"
	envKeyValueParts     = 2
)

// execState holds container-execution wiring and in-flight attempts. All
// fields are guarded by the backend lock except ctx/wg, which are immutable
// or internally synchronised.
type execState struct {
	runner   ContainerRunner
	sink     JobLogSink
	issuer   JobRoleCredentialIssuer
	running  map[string]*runState
	ctx      context.Context
	cancel   context.CancelFunc
	endpoint string
	wg       sync.WaitGroup
}

// runState is one in-flight job attempt.
type runState struct {
	ctx        context.Context
	cancel     context.CancelFunc
	timer      *time.Timer
	arrayIndex *int32
	nodes      []*nodePlan
	logGroup   string
	jobRoleArn string
	region     string
	jobID      string
	queueName  string
	ceName     string
	ceKey      string
	key        string
	logStream  string
	spec       ContainerSpec
	timeout    time.Duration
	vcpus      float64
	attempt    int32
	mainNode   int32
	numNodes   int32
	timedOut   atomic.Bool
}

type attemptOutcome struct {
	exitCode     *int32
	reason       string
	statusReason string
	success      bool
}

func newExecState() *execState {
	ctx, cancel := context.WithCancel(context.Background())

	return &execState{ctx: ctx, cancel: cancel, running: make(map[string]*runState)}
}

// SetContainerRunner enables real container execution of submitted jobs.
func (b *InMemoryBackend) SetContainerRunner(r ContainerRunner) {
	b.mu.Lock("SetContainerRunner")
	defer b.mu.Unlock()

	b.exec.runner = r
}

// SetJobLogSink wires the destination for job container output.
func (b *InMemoryBackend) SetJobLogSink(s JobLogSink) {
	b.mu.Lock("SetJobLogSink")
	defer b.mu.Unlock()

	b.exec.sink = s
}

// SetJobRoleCredentialIssuer wires job-role credential issuance.
func (b *InMemoryBackend) SetJobRoleCredentialIssuer(i JobRoleCredentialIssuer) {
	b.mu.Lock("SetJobRoleCredentialIssuer")
	defer b.mu.Unlock()

	b.exec.issuer = i
}

// SetExecutionEndpoint sets the emulator endpoint injected as AWS_ENDPOINT_URL.
func (b *InMemoryBackend) SetExecutionEndpoint(url string) {
	b.mu.Lock("SetExecutionEndpoint")
	defer b.mu.Unlock()

	b.exec.endpoint = url
}

// ExecutionEnabled reports whether submitted jobs run in containers.
func (b *InMemoryBackend) ExecutionEnabled() bool {
	b.mu.RLock("ExecutionEnabled")
	defer b.mu.RUnlock()

	return b.exec.runner != nil
}

// StopExecution cancels every in-flight container and waits for them to exit.
func (b *InMemoryBackend) StopExecution() {
	b.mu.Lock("StopExecution")
	b.exec.cancel()
	b.mu.Unlock()

	b.exec.wg.Wait()

	b.mu.RLock("StopExecution")
	runner := b.exec.runner
	b.mu.RUnlock()

	if c, ok := runner.(io.Closer); ok {
		_ = c.Close()
	}
}

// DispatchJobs advances every executable job as far as dependencies and capacity allow.
func (b *InMemoryBackend) DispatchJobs() {
	b.mu.Lock("BatchDispatch")
	defer b.mu.Unlock()

	b.dispatchLocked()
}

func (b *InMemoryBackend) kickDispatchLocked() {
	if b.exec.runner != nil && b.exec.ctx.Err() == nil {
		b.exec.wg.Go(b.DispatchJobs)
	}
}

// execOwnsLocked reports whether the executor, not the janitor, drives job.
func (b *InMemoryBackend) execOwnsLocked(job *Job) bool {
	if b.exec.runner == nil || isArrayParent(job) {
		return false
	}

	jd, ok := b.jobDefinitions.Get(regionKey(job.region, job.JobDefinition))

	if ok && jd.NodeProperties != nil {
		return b.nodesExecutableLocked(job)
	}

	return ok && jd.ContainerProperties != nil && jd.ContainerProperties.Image != ""
}

type dispatchCandidate struct {
	job      *Job
	priority int32
}

func (b *InMemoryBackend) dispatchLocked() {
	if b.exec.runner == nil || b.exec.ctx.Err() != nil {
		return
	}

	now := time.Now().UnixMilli()

	var cands []dispatchCandidate

	var parents []*Job

	for _, job := range b.jobs.All() {
		switch {
		case isArrayParent(job):
			parents = append(parents, job)
		case !b.execOwnsLocked(job):
		default:
			cands = b.collectCandidateLocked(cands, job, now)
		}
	}

	sort.SliceStable(cands, func(i, j int) bool {
		a, c := cands[i], cands[j]
		if a.priority != c.priority {
			return a.priority > c.priority
		}

		if a.job.CreatedAt != c.job.CreatedAt {
			return a.job.CreatedAt < c.job.CreatedAt
		}

		return a.job.JobID < c.job.JobID
	})

	for _, c := range cands {
		b.advanceCandidateLocked(c.job, now)
	}

	for _, p := range parents {
		b.syncArrayParentLocked(p, now)
	}
}

func (b *InMemoryBackend) collectCandidateLocked(
	cands []dispatchCandidate, job *Job, now int64,
) []dispatchCandidate {
	switch job.Status {
	case jobStatusSubmitted, jobStatusPending, jobStatusRunnable:
		return append(cands, dispatchCandidate{job: job, priority: b.queuePriorityLocked(job)})
	case jobStatusStarting, jobStatusRunning:
		if _, active := b.exec.running[regionKey(job.region, job.JobID)]; !active && b.failHostLostLocked(job, now) {
			return append(cands, dispatchCandidate{job: job, priority: b.queuePriorityLocked(job)})
		}
	}

	return cands
}

func (b *InMemoryBackend) queuePriorityLocked(job *Job) int32 {
	if jq, ok := b.lookupJQByNameOrARN(job.region, job.JobQueue); ok {
		return jq.Priority
	}

	return 0
}

// failHostLostLocked closes the attempt of a job whose container is gone
// (host lost, e.g. across a restart) and reports whether it was requeued.
// A host-loss attempt has no exit code or container reason.
func (b *InMemoryBackend) failHostLostLocked(job *Job, now int64) bool {
	o := attemptOutcome{statusReason: reasonHostLost}

	b.closeAttemptLocked(job, &runState{}, o, now)

	if b.shouldRetryLocked(job, o) {
		job.Status = jobStatusRunnable
		job.StatusReason = ""
		job.StartedAt = nil

		return true
	}

	job.Status = jobStatusFailed
	job.StatusReason = reasonHostLost
	job.StoppedAt = &now

	return false
}

func (b *InMemoryBackend) advanceCandidateLocked(job *Job, now int64) {
	switch (&Janitor{Backend: b}).dependencyStatus(job) {
	case dependencyFailed:
		job.Status = jobStatusFailed
		job.StatusReason = "dependency failed"
		job.StoppedAt = &now

		return
	case dependencyPending:
		job.Status = jobStatusPending

		return
	case dependencySatisfied:
	}

	job.Status = jobStatusRunnable

	b.tryStartLocked(job)
}

func (b *InMemoryBackend) tryStartLocked(job *Job) {
	jd, ok := b.jobDefinitions.Get(regionKey(job.region, job.JobDefinition))
	if !ok {
		return
	}

	jq, ok := b.lookupJQByNameOrARN(job.region, job.JobQueue)
	if !ok {
		return
	}

	if jd.NodeProperties != nil {
		b.tryStartMultiNodeLocked(job, jd, jq)

		return
	}

	detail := b.buildJobContainerDetail(job.region, job)
	if detail == nil {
		return
	}

	vcpus, memMiB := containerResources(detail)

	ceName, ceKey, ok := b.pickCELocked(jq, max(vcpus, 1))
	if !ok {
		return
	}

	rs := b.newRunStateLocked(job, jd, jq, detail)
	rs.ceName, rs.ceKey = ceName, ceKey
	rs.vcpus = max(vcpus, 1)
	rs.spec = ContainerSpec{
		Image:          detail.Image,
		User:           detail.User,
		Command:        expandCommand(detail.Command, jd.Parameters, job.Parameters, detail.Environment),
		MemoryMiB:      memMiB,
		VCPUs:          vcpus,
		Privileged:     detail.Privileged,
		ReadonlyRootFS: detail.ReadonlyRootFilesystem,
		Labels:         map[string]string{"gopherstack.batch.job": job.JobID},
		Env:            envStrings(detail.Environment),
	}

	b.exec.running[rs.key] = rs
	job.Status = jobStatusStarting
	job.StatusReason = ""

	runner, sink, issuer, endpoint := b.exec.runner, b.exec.sink, b.exec.issuer, b.exec.endpoint

	b.exec.wg.Go(func() { b.executeAttempt(rs, runner, sink, issuer, endpoint) })
}

func (b *InMemoryBackend) newRunStateLocked(
	job *Job, jd *JobDefinition, jq *JobQueue, detail *ContainerDetail,
) *runState {
	ctx, cancel := context.WithCancel(b.exec.ctx)

	rs := &runState{
		ctx:        ctx,
		cancel:     cancel,
		key:        regionKey(job.region, job.JobID),
		region:     job.region,
		jobID:      job.JobID,
		queueName:  jq.JobQueueName,
		jobRoleArn: detail.JobRoleArn,
		attempt:    attemptsMade(job) + 1,
		timeout:    time.Duration(b.effectiveTimeoutSeconds(job)) * time.Second,
	}

	rs.logGroup, rs.logStream = logTarget(detail, jd.JobDefinitionName)

	if job.ArrayParentID != "" && job.ArrayProperties != nil {
		idx := job.ArrayProperties.Index
		rs.arrayIndex = &idx
	}

	return rs
}

// logTarget resolves the CloudWatch log group and a fresh stream name for a container.
func logTarget(detail *ContainerDetail, defName string) (string, string) {
	group, prefix := defaultBatchLogGroup, defName

	if lc := detail.LogConfiguration; lc != nil {
		if g := lc.Options[logGroupOption]; g != "" {
			group = g
		}

		if p := lc.Options[logStreamPrefixOpt]; p != "" {
			prefix = p
		}
	}

	return group, fmt.Sprintf("%s/default/%s", prefix, newTaskID())
}

// pickCELocked returns the first queue-ordered compute environment that is
// enabled and has vcpus free. A queue without compute environments is unbounded.
func (b *InMemoryBackend) pickCELocked(jq *JobQueue, vcpus float64) (string, string, bool) {
	if len(jq.ComputeEnvironmentOrder) == 0 {
		return "", "", true
	}

	order := slices.Clone(jq.ComputeEnvironmentOrder)
	sort.SliceStable(order, func(i, j int) bool { return order[i].Order < order[j].Order })

	for _, o := range order {
		ce, found := b.lookupCEByNameOrARN(jq.region, o.ComputeEnvironment)
		if !found || ce.State != stateEnabled || ce.Status != statusValid {
			continue
		}

		ceKey := regionKey(ce.region, ce.ComputeEnvironmentArn)
		if ce.ComputeResources != nil && ce.ComputeResources.MaxvCpus > 0 &&
			b.usedVcpusLocked(ceKey)+vcpus > float64(ce.ComputeResources.MaxvCpus) {
			continue
		}

		return ce.ComputeEnvironmentName, ceKey, true
	}

	return "", "", false
}

func (b *InMemoryBackend) usedVcpusLocked(ceKey string) float64 {
	var used float64

	for _, rs := range b.exec.running {
		if rs.ceKey == ceKey {
			used += rs.vcpus
		}
	}

	return used
}

func (b *InMemoryBackend) executeAttempt(
	rs *runState, runner ContainerRunner, sink JobLogSink, issuer JobRoleCredentialIssuer, endpoint string,
) {
	defer rs.cancel()

	env, err := runtimeEnv(rs, issuer, endpoint)
	if err != nil {
		b.finishAttempt(rs, 0, fmt.Errorf("%s: job role credentials: %w", reasonStartFailed, err))

		return
	}

	rs.spec.Env = append(env, rs.spec.Env...)
	rs.spec.Env = append(rs.spec.Env, batchEnv(rs)...)

	hooks := RunHooks{OnStarted: func() { b.markRunning(rs) }}

	if sink != nil {
		if ensureErr := sink.EnsureLogGroupAndStream(rs.region, rs.logGroup, rs.logStream); ensureErr != nil {
			logger.Load(rs.ctx).WarnContext(rs.ctx, "batch: ensure log stream", "error", ensureErr)
		}

		hooks.OnLog = func(lines []string) {
			_ = sink.PutLogLines(rs.region, rs.logGroup, rs.logStream, lines)
		}
	}

	code, runErr := runner.RunContainer(rs.ctx, rs.spec, hooks)
	b.finishAttempt(rs, code, runErr)
}

func (b *InMemoryBackend) markRunning(rs *runState) {
	b.mu.Lock("BatchJobRunning")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(rs.key)
	if !ok || job.Status != jobStatusStarting {
		return
	}

	now := time.Now().UnixMilli()
	job.Status = jobStatusRunning
	job.StartedAt = &now
	job.StatusReason = ""
	job.Attempts = append(job.Attempts, JobAttempt{
		StartedAt: &now,
		Container: &JobAttemptContainer{LogStreamName: rs.logStream},
	})

	if rs.timeout > 0 {
		rs.timer = time.AfterFunc(rs.timeout, func() {
			rs.timedOut.Store(true)
			rs.cancel()
		})
	}
}

func (b *InMemoryBackend) finishAttempt(rs *runState, code int, runErr error) {
	b.mu.Lock("BatchJobFinished")
	defer b.mu.Unlock()

	if rs.timer != nil {
		rs.timer.Stop()
	}

	delete(b.exec.running, rs.key)

	defer b.dispatchLocked()

	job, ok := b.jobs.Get(rs.key)
	if !ok || isTerminalJobStatus(job.Status) || b.exec.ctx.Err() != nil {
		return
	}

	now := time.Now().UnixMilli()
	outcome := classifyOutcome(rs, code, runErr)

	b.closeAttemptLocked(job, rs, outcome, now)

	switch {
	case outcome.success:
		job.Status = jobStatusSucceeded
		job.StatusReason = ""
		job.StoppedAt = &now
	case b.shouldRetryLocked(job, outcome):
		job.Status = jobStatusRunnable
		job.StatusReason = ""
		job.StartedAt = nil
	default:
		job.Status = jobStatusFailed
		job.StatusReason = outcome.statusReason
		job.StoppedAt = &now
	}
}

func classifyOutcome(rs *runState, code int, runErr error) attemptOutcome {
	switch {
	case rs.timedOut.Load():
		return attemptOutcome{reason: reasonTimeout, statusReason: reasonTimeout}
	case runErr != nil:
		msg := truncate(runErr.Error(), reasonMaxLen)

		return attemptOutcome{reason: msg, statusReason: msg}
	case code != 0:
		c := int32(code) //nolint:gosec // container exit codes fit int32

		return attemptOutcome{exitCode: &c, reason: reasonEssentialEnd, statusReason: reasonEssentialEnd}
	}

	zero := int32(0)

	return attemptOutcome{exitCode: &zero, success: true}
}

func (b *InMemoryBackend) closeAttemptLocked(job *Job, rs *runState, o attemptOutcome, now int64) {
	container := &JobAttemptContainer{LogStreamName: rs.logStream, ExitCode: o.exitCode, Reason: o.reason}

	if n := len(job.Attempts); n > 0 && job.Attempts[n-1].StoppedAt == nil {
		job.Attempts[n-1].StoppedAt = &now
		job.Attempts[n-1].StatusReason = o.statusReason
		job.Attempts[n-1].Container = container

		return
	}

	job.Attempts = append(job.Attempts, JobAttempt{
		StoppedAt: &now, StatusReason: o.statusReason, Container: container,
	})
}

func (b *InMemoryBackend) effectiveRetryStrategy(job *Job) *RetryStrategy {
	if job.RetryStrategy != nil && job.RetryStrategy.Attempts > 0 {
		return job.RetryStrategy
	}

	if jd, ok := b.jobDefinitions.Get(regionKey(job.region, job.JobDefinition)); ok {
		return jd.RetryStrategy
	}

	return nil
}

// shouldRetryLocked applies attempts and evaluateOnExit: the first matching
// rule decides, and with no match the job is retried.
func (b *InMemoryBackend) shouldRetryLocked(job *Job, o attemptOutcome) bool {
	if attemptsMade(job) >= b.effectiveRetryAttempts(job) {
		return false
	}

	rs := b.effectiveRetryStrategy(job)
	if rs == nil {
		return true
	}

	for _, rule := range rs.EvaluateOnExit {
		if rule.matches(o) {
			return strings.EqualFold(rule.Action, "RETRY")
		}
	}

	return true
}

func (r EvaluateOnExit) matches(o attemptOutcome) bool {
	if r.OnExitCode != "" && (o.exitCode == nil || !globMatch(r.OnExitCode, strconv.Itoa(int(*o.exitCode)))) {
		return false
	}

	if r.OnReason != "" && !globMatch(r.OnReason, o.reason) {
		return false
	}

	return r.OnStatusReason == "" || globMatch(r.OnStatusReason, o.statusReason)
}

func globMatch(pattern, s string) bool {
	if prefix, ok := strings.CutSuffix(pattern, "*"); ok {
		return strings.HasPrefix(s, prefix)
	}

	return pattern == s
}

// terminateRunsLocked stops the containers of job and its array children and closes their open attempts.
func (b *InMemoryBackend) terminateRunsLocked(job *Job, reason string, now int64) {
	if b.exec.runner == nil {
		return
	}

	stop := func(j *Job) {
		if rs, ok := b.exec.running[regionKey(j.region, j.JobID)]; ok {
			rs.cancel()
		}

		if n := len(j.Attempts); n > 0 && j.Attempts[n-1].StoppedAt == nil {
			j.Attempts[n-1].StoppedAt = &now
			j.Attempts[n-1].StatusReason = reason

			if c := j.Attempts[n-1].Container; c != nil {
				c.Reason = reason
			}
		}
	}

	stop(job)

	if isArrayParent(job) {
		for _, c := range b.arrayChildrenLocked(job) {
			stop(c)
		}
	}
}

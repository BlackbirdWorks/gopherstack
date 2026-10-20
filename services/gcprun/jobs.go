package gcprun

import (
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultTaskTimeout = 600 * time.Second
	maxTaskTimeout     = 168 * time.Hour
	defaultMaxRetries  = 3
	maxMaxRetries      = 10
)

func jobMutable() []string {
	return []string{
		"labels",
		"annotations",
		"client",
		"client_version",
		"launch_stage",
		"binary_authorization",
		"template",
	}
}

func validateJob(j *runpb.Job) error {
	t := j.GetTemplate().GetTemplate()
	if j.GetTemplate() == nil || t == nil {
		return errInvalid("template.template is required.")
	}

	if err := validateContainers(t.GetContainers(), false); err != nil {
		return err
	}

	if j.GetTemplate().GetTaskCount() < 0 || j.GetTemplate().GetParallelism() < 0 {
		return errInvalid("task_count and parallelism must not be negative.")
	}

	if d := t.GetTimeout(); d != nil && (d.AsDuration() < time.Second || d.AsDuration() > maxTaskTimeout) {
		return errInvalid("Task timeout must be between 1s and 604800s.")
	}

	if t.GetMaxRetries() < 0 || t.GetMaxRetries() > maxMaxRetries {
		return errInvalid("max_retries must be between 0 and %d.", maxMaxRetries)
	}

	return nil
}

// applyJobDefaults fills the documented defaults of an execution template.
func applyJobDefaults(j *runpb.Job) {
	et := j.GetTemplate()
	if et.GetTaskCount() == 0 {
		et.TaskCount = 1
	}

	tt := et.GetTemplate()
	if tt.GetTimeout() == nil {
		tt.Timeout = durationpb.New(defaultTaskTimeout)
	}

	if tt.GetRetries() == nil {
		tt.Retries = &runpb.TaskTemplate_MaxRetries{MaxRetries: defaultMaxRetries}
	}
}

func (b *InMemoryBackend) buildJobLocked(n resName, req *runpb.Job) *runpb.Job {
	now := b.ts()
	pb := clone(req)
	pb.Name = n.parent() + "/jobs/" + n.ids[0]
	pb.Uid, pb.Generation, pb.CreateTime, pb.UpdateTime = newUID(), 1, now, now
	pb.DeleteTime, pb.ExpireTime, pb.LatestCreatedExecution, pb.ExecutionCount = nil, nil, nil, 0
	applyJobDefaults(pb)
	b.refreshJobLocked(pb)

	return pb
}

func (b *InMemoryBackend) refreshJobLocked(pb *runpb.Job) {
	at := b.ts()
	ready := newCondition(condReady, runpb.Condition_CONDITION_SUCCEEDED, "", at)
	pb.TerminalCondition, pb.Conditions = ready, []*runpb.Condition{ready}
	pb.ObservedGeneration, pb.Reconciling = pb.GetGeneration(), false
	pb.Etag = etag(pb.GetUid(), pb.GetGeneration(), at)
}

// CreateJob stores a job; it is ready immediately.
func (b *InMemoryBackend) CreateJob(in *runpb.CreateJobRequest) (*longrunningpb.Operation, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if err = validateID(in.GetJobId(), "job_id"); err != nil {
		return nil, err
	}

	if err = validateJob(in.GetJob()); err != nil {
		return nil, err
	}

	n, err := parseName(p.parent()+"/jobs/"+in.GetJobId(), "jobs")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if _, exists := b.jobs[p.parent()+"/jobs/"+n.ids[0]]; exists {
		return nil, errExists("Resource '%s' already exists.", n.ids[0])
	}

	pb := b.buildJobLocked(n, in.GetJob())
	if !in.GetValidateOnly() {
		b.jobs[pb.GetName()] = pb
	}

	return b.doneOp(opName(n.parent()), clone(pb), clone(pb))
}

// GetJob returns the named job.
func (b *InMemoryBackend) GetJob(name string) (*runpb.Job, error) {
	n, err := parseName(name, "jobs")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[name]
	if !ok {
		return nil, errNotFound("JOB", n.ids[0], n.location, n.project)
	}

	return clone(j), nil
}

// ListJobs lists the jobs under a location ordered by name.
func (b *InMemoryBackend) ListJobs(in *runpb.ListJobsRequest) ([]*runpb.Job, string, error) {
	p, err := parseParent(in.GetParent())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListJobs")

	all := collect(b.jobs, p.parent()+"/jobs/", clone[*runpb.Job])
	b.mu.RUnlock()

	return paginate(all, in.GetPageToken(), in.GetPageSize())
}

// UpdateJob applies the masked fields and bumps the job's generation.
func (b *InMemoryBackend) UpdateJob(in *runpb.UpdateJobRequest) (*longrunningpb.Operation, error) {
	name := in.GetJob().GetName()

	n, err := parseName(name, "jobs")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateJob")

	cur, ok := b.jobs[name]
	if !ok {
		b.mu.Unlock()

		if in.GetAllowMissing() {
			return b.CreateJob(&runpb.CreateJobRequest{
				Parent: n.parent(), JobId: n.ids[0], Job: in.GetJob(), ValidateOnly: in.GetValidateOnly(),
			})
		}

		return nil, errNotFound("JOB", n.ids[0], n.location, n.project)
	}

	defer b.mu.Unlock()

	work := clone(cur)
	if err = applyMask(work, in.GetJob(), nil, jobMutable()); err != nil {
		return nil, err
	}

	if err = validateJob(work); err != nil {
		return nil, err
	}

	applyJobDefaults(work)
	work.Generation++
	b.refreshJobLocked(work)

	if !in.GetValidateOnly() {
		b.jobs[name] = work
	}

	return b.doneOp(opName(n.parent()), clone(work), clone(work))
}

// DeleteJob removes the job with its executions and tasks, cancelling any that still run.
func (b *InMemoryBackend) DeleteJob(in *runpb.DeleteJobRequest) (*longrunningpb.Operation, error) {
	n, err := parseName(in.GetName(), "jobs")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteJob")

	j, ok := b.jobs[in.GetName()]
	if !ok {
		b.mu.Unlock()

		return nil, errNotFound("JOB", n.ids[0], n.location, n.project)
	}

	if in.GetEtag() != "" && in.GetEtag() != j.GetEtag() {
		b.mu.Unlock()

		return nil, errPrecondition("The etag %q does not match the job's current etag.", in.GetEtag())
	}

	view := clone(j)
	view.DeleteTime = b.ts()

	if in.GetValidateOnly() {
		b.mu.Unlock()

		return b.doneOp(opName(n.parent()), view, view)
	}

	delete(b.jobs, in.GetName())

	var waits []chan struct{}

	for _, name := range sortedKeys(b.executions, in.GetName()+"/executions/") {
		e := b.executions[name]
		e.cancel()
		waits = append(waits, e.done)
	}
	b.mu.Unlock()

	for _, w := range waits {
		<-w
	}

	b.mu.Lock("DeleteJob.purge")
	b.purgeExecutionsLocked(in.GetName() + "/executions/")
	b.mu.Unlock()

	return b.doneOp(opName(n.parent()), view, view)
}

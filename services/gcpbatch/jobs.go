package gcpbatch

import (
	"strings"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	maxTaskCount = 100000
	idHexLen     = 8
	zeroUUID     = "00000000-0000-0000-0000-000000000000"
)

func (r *jobRec) setPhase(to int32, at time.Time) {
	from := r.Phase
	r.Phase = to
	r.Events = append(r.Events, eventRec{
		Time: at, Type: eventStatusChanged,
		Desc: "Job state is set from " + stateName(from) + " to " + stateName(to) + " for job " + r.pb.GetName() + ".",
	})

	switch to {
	case jobRunning:
		r.StartedAt = at
	case jobSucceeded, jobFailed, jobCancelled, jobCancelling, jobDeleting:
		if r.EndedAt.IsZero() && !r.StartedAt.IsZero() {
			r.EndedAt = at
		}
	}
}

// setTask moves a task to a new state unless it already finished.
func (r *jobRec) setTask(i int, to int32, at time.Time, exit *int32, note string) {
	t := r.Tasks[i]
	if t.terminal() {
		return
	}

	desc := "Task state is updated from " + taskStateName(t.State) + " to " + taskStateName(to)
	if note != "" {
		desc += ": " + note
	}

	t.State = to
	t.Events = append(t.Events, eventRec{Time: at, Type: eventStatusChanged, Desc: desc, Exit: exit})
}

// interrupt ends every unfinished task because the job was cancelled or deleted.
func (r *jobRec) interrupt(at time.Time) {
	for i, t := range r.Tasks {
		switch t.State {
		case taskPending, taskAssigned:
			r.setTask(i, taskUnexecuted, at, nil, "job "+strings.ToLower(stateName(r.Phase)))
		case taskRunning:
			r.setTask(i, taskFailed, at, nil, "task stopped because the job was cancelled or deleted")
		}
	}
}

func (r *jobRec) parallelism() int {
	p := r.Parallel
	if p <= 0 || p > int64(len(r.Tasks)) {
		p = int64(len(r.Tasks))
	}

	return int(max(p, 1))
}

// advanceStateOnly applies the planned timeline of a job that runs no containers.
func (r *jobRec) advanceStateOnly(now time.Time) {
	if r.Phase == jobQueued && !now.Before(r.ScheduledAt) {
		r.setPhase(jobScheduled, r.ScheduledAt)
	}

	if r.Phase == jobScheduled && !now.Before(r.RunningAt) {
		r.setPhase(jobRunning, r.RunningAt)
	}

	if r.Phase != jobRunning {
		return
	}

	par, run := r.parallelism(), time.Duration(r.TaskRun)
	last, done := r.RunningAt, 0

	for i := range r.Tasks {
		start := r.RunningAt.Add(time.Duration(i/par) * run)
		end := start.Add(run)

		if !now.Before(start) {
			r.setTask(i, taskAssigned, start, nil, "")
			r.setTask(i, taskRunning, start, nil, "")
		}

		if !now.Before(end) {
			zero := int32(0)
			r.setTask(i, taskSucceeded, end, &zero, "")

			last = end
			done++
		}
	}

	if done == len(r.Tasks) {
		r.setPhase(jobSucceeded, last)
	}
}

// advanceLocked settles cancel, delete and state-only progress on the clock.
func (b *InMemoryBackend) advanceLocked() {
	now := b.now()

	for name, r := range b.jobs {
		switch {
		case r.Deleting:
			if !now.Before(r.DeleteDone) {
				delete(b.jobs, name)
			}
		case r.Cancelled:
			if r.Phase != jobCancelled && !now.Before(r.CancelDone) {
				r.setPhase(jobCancelled, r.CancelDone)
			}
		case !r.Engine && !r.finished():
			r.advanceStateOnly(now)
		}
	}
}

func (b *InMemoryBackend) liveJobLocked(name string) (*jobRec, error) {
	if _, _, err := parseJobName(name); err != nil {
		return nil, err
	}

	r, ok := b.jobs[name]
	if !ok {
		return nil, errNotFound("Job %q not found.", name)
	}

	return r, nil
}

func newJobID() string { return awsid.NewN("job-", idHexLen) }

func (b *InMemoryBackend) buildJobLocked(name string, src *batchpb.Job) *jobRec {
	pb := clone(src)
	now := b.ts()
	pb.Name, pb.CreateTime, pb.UpdateTime, pb.Status = name, now, now, nil
	pb.Uid = name[strings.LastIndex(name, "/")+1:] + "-" + uuid.NewString()

	g := pb.GetTaskGroups()[0]
	g.Name = groupFullName(name)

	if n := len(g.GetTaskEnvironments()); n > 0 {
		g.TaskCount = int64(n)
	}

	if g.GetTaskCount() == 0 {
		g.TaskCount = 1
	}

	if g.GetSchedulingPolicy() == batchpb.TaskGroup_IN_ORDER {
		g.Parallelism = 1
	} else if g.GetParallelism() == 0 {
		g.Parallelism = g.GetTaskCount()
	}

	g.Parallelism = min(g.GetParallelism(), g.GetTaskCount())

	created := b.now()
	r := &jobRec{
		pb: pb, Phase: jobQueued, Parallel: g.GetParallelism(), Engine: b.rt != nil,
		Tasks:       make([]*taskRec, g.GetTaskCount()),
		ScheduledAt: created.Add(b.timing.Schedule),
		TaskRun:     int64(b.timing.TaskRun),
	}
	r.RunningAt = r.ScheduledAt.Add(b.timing.Start)

	for i := range r.Tasks {
		r.Tasks[i] = &taskRec{State: taskPending}
	}

	return r
}

// validateCreate checks a CreateJobRequest and returns the project, location and (possibly generated) id.
func validateCreate(in *batchpb.CreateJobRequest) (string, string, string, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return "", "", "", err
	}

	if location == anyLocation {
		return "", "", "", errInvalid("Invalid parent %q: a concrete location is required.", in.GetParent())
	}

	id := in.GetJobId()
	if id == "" {
		id = newJobID()
	} else if !jobIDRe.MatchString(id) {
		return "", "", "", errInvalid("Invalid job_id %q: must match %s.", id, jobIDRe.String())
	}

	if rid := in.GetRequestId(); rid != "" && (!requestRe.MatchString(rid) || rid == zeroUUID) {
		return "", "", "", errInvalid("request_id must be a valid non-zero UUID.")
	}

	if err = validateJob(in.GetJob()); err != nil {
		return "", "", "", err
	}

	if in.GetJob().GetTaskGroups()[0].GetTaskCount() > maxTaskCount {
		return "", "", "", errInvalid("task_count must be at most %d.", maxTaskCount)
	}

	return project, location, id, nil
}

// CreateJob validates and records a job. It returns the job without a long-running operation, as the API does.
func (b *InMemoryBackend) CreateJob(in *batchpb.CreateJobRequest) (*batchpb.Job, error) {
	project, location, id, err := validateCreate(in)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	b.advanceLocked()

	name := jobName(project, location, id)
	reqKey := in.GetParent() + "|" + in.GetRequestId()

	if in.GetRequestId() != "" {
		if r, live := b.jobs[b.requests[reqKey]]; live {
			return r.view(b.now()), nil
		}
	}

	if _, ok := b.jobs[name]; ok {
		return nil, errExists("Job %q already exists.", name)
	}

	r := b.buildJobLocked(name, in.GetJob())
	b.jobs[name] = r

	if in.GetRequestId() != "" {
		b.requests[reqKey] = name
	}

	if r.Engine {
		b.startRunnerLocked(r)
	}

	b.advanceLocked()

	return r.view(b.now()), nil
}

// GetJob returns one job.
func (b *InMemoryBackend) GetJob(name string) (*batchpb.Job, error) {
	b.mu.Lock("GetJob")
	defer b.mu.Unlock()

	b.advanceLocked()

	r, err := b.liveJobLocked(name)
	if err != nil {
		return nil, err
	}

	return r.view(b.now()), nil
}

// DeleteJob starts deletion: the job reads DELETION_IN_PROGRESS until the operation ends, then is gone.
func (b *InMemoryBackend) DeleteJob(in *batchpb.DeleteJobRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	b.advanceLocked()

	r, err := b.liveJobLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if r.Deleting {
		return nil, errPrecondition("Job %q is already being deleted.", in.GetName())
	}

	now := b.now()
	b.stopRunnerLocked(in.GetName())
	r.interrupt(now)
	r.Deleting = true
	r.DeleteDone = now.Add(b.timing.Delete)
	r.setPhase(jobDeleting, now)

	project, location, _ := parseJobName(in.GetName())

	return b.newOpLocked(opSpec{
		project: project, location: location, target: in.GetName(), verb: verbDelete,
		end: r.DeleteDone, resp: &emptypb.Empty{},
	})
}

// CancelJob starts cancellation: the job reads CANCELLATION_IN_PROGRESS, then CANCELLED.
func (b *InMemoryBackend) CancelJob(in *batchpb.CancelJobRequest) (*longrunningpb.Operation, error) {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	b.advanceLocked()

	r, err := b.liveJobLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if r.finished() || r.interrupted() {
		return nil, errPrecondition("Job %q is in state %s and cannot be cancelled.", in.GetName(), stateName(r.Phase))
	}

	now := b.now()
	b.stopRunnerLocked(in.GetName())
	r.interrupt(now)
	r.Cancelled = true
	r.CancelDone = now.Add(b.timing.Cancel)
	r.setPhase(jobCancelling, now)

	project, location, _ := parseJobName(in.GetName())

	return b.newOpLocked(opSpec{
		project: project, location: location, target: in.GetName(), verb: verbCancel,
		end: r.CancelDone, resp: &batchpb.CancelJobResponse{},
	})
}

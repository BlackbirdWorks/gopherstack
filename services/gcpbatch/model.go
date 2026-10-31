package gcpbatch

import (
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	jobQueued     = int32(batchpb.JobStatus_QUEUED)
	jobScheduled  = int32(batchpb.JobStatus_SCHEDULED)
	jobRunning    = int32(batchpb.JobStatus_RUNNING)
	jobSucceeded  = int32(batchpb.JobStatus_SUCCEEDED)
	jobFailed     = int32(batchpb.JobStatus_FAILED)
	jobDeleting   = int32(batchpb.JobStatus_DELETION_IN_PROGRESS)
	jobCancelling = int32(batchpb.JobStatus_CANCELLATION_IN_PROGRESS)
	jobCancelled  = int32(batchpb.JobStatus_CANCELLED)

	taskPending    = int32(batchpb.TaskStatus_PENDING)
	taskAssigned   = int32(batchpb.TaskStatus_ASSIGNED)
	taskRunning    = int32(batchpb.TaskStatus_RUNNING)
	taskFailed     = int32(batchpb.TaskStatus_FAILED)
	taskSucceeded  = int32(batchpb.TaskStatus_SUCCEEDED)
	taskUnexecuted = int32(batchpb.TaskStatus_UNEXECUTED)

	eventStatusChanged = "STATUS_CHANGED"
	eventRetry         = "TASK_RETRY"
)

// eventRec is one status event of a job or task.
type eventRec struct {
	Time time.Time `json:"time"`
	Exit *int32    `json:"exit,omitempty"`
	Type string    `json:"type"`
	Desc string    `json:"desc"`
}

type taskRec struct {
	Events   []eventRec `json:"events,omitempty"`
	State    int32      `json:"state"`
	Attempts int32      `json:"attempts,omitempty"`
}

func (t *taskRec) terminal() bool {
	return t.State == taskFailed || t.State == taskSucceeded || t.State == taskUnexecuted
}

// jobRec is a job with its materialised status. Planned times drive state-only progression.
type jobRec struct {
	EndedAt     time.Time `json:"endedAt"`
	CancelDone  time.Time `json:"cancelDone"`
	DeleteDone  time.Time `json:"deleteDone"`
	ScheduledAt time.Time `json:"scheduledAt"`
	RunningAt   time.Time `json:"runningAt"`
	StartedAt   time.Time `json:"startedAt"`
	pb          *batchpb.Job
	Events      []eventRec `json:"events"`
	Tasks       []*taskRec `json:"tasks"`
	TaskRun     int64      `json:"taskRun"`
	Parallel    int64      `json:"parallel"`
	Phase       int32      `json:"phase"`
	Engine      bool       `json:"engine"`
	Cancelled   bool       `json:"cancelled"`
	Deleting    bool       `json:"deleting"`
}

func (r *jobRec) interrupted() bool { return r.Cancelled || r.Deleting }

func (r *jobRec) finished() bool {
	return r.Phase == jobSucceeded || r.Phase == jobFailed || r.Phase == jobCancelled
}

func (r *jobRec) group() *batchpb.TaskGroup { return r.pb.GetTaskGroups()[0] }

func stateName(s int32) string { return batchpb.JobStatus_State(s).String() }

func taskStateName(s int32) string { return batchpb.TaskStatus_State(s).String() }

func eventPB(e eventRec, withExec bool) *batchpb.StatusEvent {
	out := &batchpb.StatusEvent{Type: e.Type, Description: e.Desc, EventTime: timestamppb.New(e.Time)}
	if withExec && e.Exit != nil {
		out.TaskExecution = &batchpb.TaskExecution{ExitCode: *e.Exit}
	}

	return out
}

func eventsPB(es []eventRec, withExec bool) []*batchpb.StatusEvent {
	out := make([]*batchpb.StatusEvent, 0, len(es))
	for _, e := range es {
		out = append(out, eventPB(e, withExec))
	}

	return out
}

func (r *jobRec) counts() map[string]int64 {
	c := map[string]int64{}
	for _, t := range r.Tasks {
		c[taskStateName(t.State)]++
	}

	return c
}

// view renders the job as the API returns it.
func (r *jobRec) view(now time.Time) *batchpb.Job {
	out := clone(r.pb)
	st := &batchpb.JobStatus{
		State:        batchpb.JobStatus_State(r.Phase),
		StatusEvents: eventsPB(r.Events, false),
		TaskGroups: map[string]*batchpb.JobStatus_TaskGroupStatus{
			groupName: {Counts: r.counts()},
		},
	}

	if !r.StartedAt.IsZero() {
		end := now
		if !r.EndedAt.IsZero() {
			end = r.EndedAt
		}

		st.RunDuration = durationpb.New(max(end.Sub(r.StartedAt), 0))
	}

	out.Status = st

	if n := len(r.Events); n > 0 {
		out.UpdateTime = timestamppb.New(r.Events[n-1].Time)
	}

	return out
}

func (r *jobRec) taskView(i int) *batchpb.Task {
	t := r.Tasks[i]

	return &batchpb.Task{
		Name: taskName(r.pb.GetName(), i),
		Status: &batchpb.TaskStatus{
			State: batchpb.TaskStatus_State(t.State), StatusEvents: eventsPB(t.Events, true),
		},
	}
}

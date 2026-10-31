package gcpbatch_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

func TestCreateJobValidation(t *testing.T) {
	t.Parallel()

	mod := func(f func(j *batchpb.Job)) *batchpb.Job {
		j := basicJob(1)
		f(j)

		return j
	}

	tests := []struct {
		job   *batchpb.Job
		name  string
		id    string
		extra string
		want  codes.Code
	}{
		{name: "valid", job: basicJob(2), id: "ok-1", want: codes.OK},
		{name: "no task group", job: &batchpb.Job{}, id: "a", want: codes.InvalidArgument},
		{name: "two task groups", job: mod(func(j *batchpb.Job) {
			j.TaskGroups = append(j.GetTaskGroups(), j.GetTaskGroups()[0])
		}), id: "a", want: codes.InvalidArgument, extra: "Only one TaskGroup"},
		{name: "no runnables", job: mod(func(j *batchpb.Job) {
			j.TaskGroups[0].TaskSpec.Runnables = nil
		}), id: "a", want: codes.InvalidArgument},
		{
			name: "container without image",
			job:  basicJob(1, containerRunnable("")),
			id:   "a",
			want: codes.InvalidArgument,
		},
		{name: "empty script", job: basicJob(1, &batchpb.Runnable{Executable: &batchpb.Runnable_Script_{
			Script: &batchpb.Runnable_Script{},
		}}), id: "a", want: codes.InvalidArgument},
		{name: "retry out of range", job: mod(func(j *batchpb.Job) {
			j.TaskGroups[0].TaskSpec.MaxRetryCount = 11
		}), id: "a", want: codes.InvalidArgument},
		{name: "in order parallelism", job: mod(func(j *batchpb.Job) {
			j.TaskGroups[0].SchedulingPolicy = batchpb.TaskGroup_IN_ORDER
			j.TaskGroups[0].Parallelism = 2
		}), id: "a", want: codes.InvalidArgument},
		{name: "bad secret ref", job: mod(func(j *batchpb.Job) {
			j.TaskGroups[0].TaskSpec.Environment = &batchpb.Environment{SecretVariables: map[string]string{"A": "nope"}}
		}), id: "a", want: codes.InvalidArgument},
		{name: "bad policy", job: mod(func(j *batchpb.Job) {
			j.TaskGroups[0].TaskSpec.LifecyclePolicies = []*batchpb.LifecyclePolicy{{
				Action: batchpb.LifecyclePolicy_RETRY_TASK,
			}}
		}), id: "a", want: codes.InvalidArgument},
		{name: "priority", job: mod(func(j *batchpb.Job) { j.Priority = 100 }), id: "a", want: codes.InvalidArgument},
		{name: "reserved label", job: mod(func(j *batchpb.Job) {
			j.Labels = map[string]string{"goog-x": "y"}
		}), id: "a", want: codes.InvalidArgument},
		{name: "path logs without path", job: mod(func(j *batchpb.Job) {
			j.LogsPolicy = &batchpb.LogsPolicy{Destination: batchpb.LogsPolicy_PATH}
		}), id: "a", want: codes.InvalidArgument},
		{name: "bad allocation location", job: mod(func(j *batchpb.Job) {
			j.AllocationPolicy = &batchpb.AllocationPolicy{Location: &batchpb.AllocationPolicy_LocationPolicy{
				AllowedLocations: []string{"us-central1"},
			}}
		}), id: "a", want: codes.InvalidArgument},
		{name: "bad id", job: basicJob(1), id: "Bad_ID", want: codes.InvalidArgument},
		{name: "id ends with dash", job: basicJob(1), id: "abc-", want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			_, err := e.client.CreateJob(
				t.Context(),
				&batchpb.CreateJobRequest{Parent: parent, JobId: tt.id, Job: tt.job},
			)
			assert.Equal(t, tt.want, status.Code(err))

			if tt.extra != "" {
				assert.Contains(t, err.Error(), tt.extra)
			}
		})
	}
}

func TestCreateJobDefaultsAndIdentity(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withTiming(gcpbatch.Timing{Schedule: time.Minute}))
	job := basicJob(4)
	job.Labels = map[string]string{"team": "data"}
	job.Name, job.Uid = "ignored", "ignored"
	got := e.create(t, "defaults", job)

	assert.Equal(t, parent+"/jobs/defaults", got.GetName())
	assert.True(t, strings.HasPrefix(got.GetUid(), "defaults-"))
	assert.Equal(t, parent+"/jobs/defaults/taskGroups/group0", got.GetTaskGroups()[0].GetName())
	assert.Equal(t, int64(4), got.GetTaskGroups()[0].GetParallelism())
	assert.Equal(t, batchpb.JobStatus_QUEUED, got.GetStatus().GetState())
	assert.Equal(t, map[string]int64{"PENDING": 4}, got.GetStatus().GetTaskGroups()["group0"].GetCounts())
	assert.NotNil(t, got.GetCreateTime())

	gen, err := e.client.CreateJob(t.Context(), &batchpb.CreateJobRequest{Parent: parent, Job: basicJob(1)})
	require.NoError(t, err)
	assert.Contains(t, gen.GetName(), parent+"/jobs/job-")

	_, err = e.client.CreateJob(
		t.Context(),
		&batchpb.CreateJobRequest{Parent: parent, JobId: "defaults", Job: basicJob(1)},
	)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	_, err = e.client.GetJob(t.Context(), &batchpb.GetJobRequest{Name: parent + "/jobs/missing"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.GetJob(t.Context(), &batchpb.GetJobRequest{Name: "bogus"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestCreateJobRequestIDIdempotent(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	req := &batchpb.CreateJobRequest{
		Parent: parent, JobId: "idem", Job: basicJob(1), RequestId: "6f1c1d6e-3b0e-4f5e-8f55-0c6d3b2b1a10",
	}

	first, err := e.client.CreateJob(t.Context(), req)
	require.NoError(t, err)

	second, err := e.client.CreateJob(t.Context(), req)
	require.NoError(t, err)
	assert.Equal(t, first.GetUid(), second.GetUid())

	req.RequestId = "not-a-uuid"
	_, err = e.client.CreateJob(t.Context(), req)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestStateOnlyProgression(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withTiming(gcpbatch.Timing{
		Schedule: 10 * time.Second, Start: 20 * time.Second, TaskRun: 30 * time.Second,
	}))
	job := basicJob(3)
	job.TaskGroups[0].Parallelism = 2
	e.create(t, "prog", job)

	counts := func() map[string]int64 {
		return e.get(t, "prog").GetStatus().GetTaskGroups()["group0"].GetCounts()
	}

	steps := []struct {
		counts map[string]int64
		state  batchpb.JobStatus_State
		after  time.Duration
	}{
		{state: batchpb.JobStatus_QUEUED, counts: map[string]int64{"PENDING": 3}},
		{state: batchpb.JobStatus_SCHEDULED, after: 10 * time.Second, counts: map[string]int64{"PENDING": 3}},
		{
			state:  batchpb.JobStatus_RUNNING,
			after:  20 * time.Second,
			counts: map[string]int64{"RUNNING": 2, "PENDING": 1},
		},
		{
			state:  batchpb.JobStatus_RUNNING,
			after:  30 * time.Second,
			counts: map[string]int64{"SUCCEEDED": 2, "RUNNING": 1},
		},
		{state: batchpb.JobStatus_SUCCEEDED, after: 30 * time.Second, counts: map[string]int64{"SUCCEEDED": 3}},
	}

	for _, s := range steps {
		e.clock.Advance(s.after)

		got := e.get(t, "prog")
		assert.Equal(t, s.state, got.GetStatus().GetState(), s.state.String())
		assert.Equal(t, s.counts, counts(), s.state.String())
	}

	final := e.get(t, "prog")
	assert.Equal(t, 60*time.Second, final.GetStatus().GetRunDuration().AsDuration())

	changes := make([]string, 0, len(final.GetStatus().GetStatusEvents()))
	for _, ev := range final.GetStatus().GetStatusEvents() {
		assert.Equal(t, "STATUS_CHANGED", ev.GetType())
		changes = append(changes, ev.GetDescription())
	}

	assert.Equal(t, []string{
		"Job state is set from QUEUED to SCHEDULED for job " + parent + "/jobs/prog.",
		"Job state is set from SCHEDULED to RUNNING for job " + parent + "/jobs/prog.",
		"Job state is set from RUNNING to SUCCEEDED for job " + parent + "/jobs/prog.",
	}, changes)

	for _, task := range e.tasks(t, "prog") {
		assert.Equal(t, batchpb.TaskStatus_SUCCEEDED, task.GetStatus().GetState())
		last := task.GetStatus().GetStatusEvents()
		assert.Equal(t, int32(0), last[len(last)-1].GetTaskExecution().GetExitCode())
	}
}

func TestTasksListAndGet(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withTiming(gcpbatch.Timing{Start: time.Second, TaskRun: time.Minute}))
	job := basicJob(3)
	job.TaskGroups[0].Parallelism = 1
	e.create(t, "tl", job)
	e.clock.Advance(time.Second)

	group := parent + "/jobs/tl/taskGroups/group0"

	tests := []struct {
		name   string
		filter string
		want   []string
		code   codes.Code
	}{
		{name: "all", want: []string{"RUNNING", "PENDING", "PENDING"}},
		{name: "running", filter: "State=RUNNING", want: []string{"RUNNING"}},
		{name: "pending", filter: "State=PENDING", want: []string{"PENDING", "PENDING"}},
		{name: "bad filter", filter: "foo", code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListTasks(t.Context(), &batchpb.ListTasksRequest{Parent: group, Filter: tt.filter})

			var got []string

			var err error

			for {
				var task *batchpb.Task

				task, err = it.Next()
				if err != nil {
					break
				}

				got = append(got, task.GetStatus().GetState().String())
			}

			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}

	task, err := e.client.GetTask(t.Context(), &batchpb.GetTaskRequest{Name: group + "/tasks/0"})
	require.NoError(t, err)
	assert.Equal(t, batchpb.TaskStatus_RUNNING, task.GetStatus().GetState())

	missing := []string{group + "/tasks/9", group + "/tasks/x", parent + "/jobs/none/taskGroups/group0/tasks/0"}

	for _, name := range missing {
		_, err = e.client.GetTask(t.Context(), &batchpb.GetTaskRequest{Name: name})
		assert.Equal(t, codes.NotFound, status.Code(err), name)
	}

	it := e.client.ListTasks(t.Context(), &batchpb.ListTasksRequest{Parent: parent + "/jobs/tl/taskGroups/other"})
	_, err = it.Next()
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestListJobsFilterOrderPage(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withTiming(gcpbatch.Timing{Schedule: time.Hour}))

	for _, id := range []string{"c-job", "a-job", "b-job"} {
		j := basicJob(1)
		j.Labels = map[string]string{"env": "prod"}

		if id == "b-job" {
			j.Labels["env"] = "dev"
		}

		e.create(t, id, j)
		e.clock.Advance(time.Second)
	}

	list := func(req *batchpb.ListJobsRequest) ([]string, error) {
		req.Parent = parent
		it := e.client.ListJobs(t.Context(), req)

		var names []string

		for {
			j, err := it.Next()
			if err != nil {
				if err.Error() != "no more items in iterator" {
					return names, err
				}

				return names, nil
			}

			names = append(names, j.GetName()[len(parent)+len("/jobs/"):])
		}
	}

	tests := []struct {
		name string
		req  *batchpb.ListJobsRequest
		want []string
		code codes.Code
	}{
		{name: "by name", req: &batchpb.ListJobsRequest{}, want: []string{"a-job", "b-job", "c-job"}},
		{
			name: "create time desc",
			req:  &batchpb.ListJobsRequest{OrderBy: "create_time desc"},
			want: []string{"b-job", "a-job", "c-job"},
		},
		{
			name: "label filter",
			req:  &batchpb.ListJobsRequest{Filter: "labels.env=prod"},
			want: []string{"a-job", "c-job"},
		},
		{
			name: "state filter",
			req:  &batchpb.ListJobsRequest{Filter: `status.state="QUEUED" AND labels.env=dev`},
			want: []string{"b-job"},
		},
		{name: "no match", req: &batchpb.ListJobsRequest{Filter: `status.state="RUNNING"`}, want: nil},
		{name: "bad filter", req: &batchpb.ListJobsRequest{Filter: "bogus"}, code: codes.InvalidArgument},
		{name: "bad state", req: &batchpb.ListJobsRequest{Filter: "status.state=NOPE"}, code: codes.InvalidArgument},
		{name: "bad order", req: &batchpb.ListJobsRequest{OrderBy: "color"}, code: codes.InvalidArgument},
		{name: "page size", req: &batchpb.ListJobsRequest{PageSize: 1}, want: []string{"a-job", "b-job", "c-job"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := list(tt.req)
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	every := e.client.ListJobs(t.Context(), &batchpb.ListJobsRequest{Parent: "projects/" + project + "/locations/-"})
	n := 0

	for {
		if _, err := every.Next(); err != nil {
			break
		}

		n++
	}

	assert.Equal(t, 3, n)
}

func TestCancelJob(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withTiming(gcpbatch.Timing{
		Schedule: time.Second, Start: time.Second, TaskRun: time.Minute, Cancel: 5 * time.Second,
	}))
	e.create(t, "cx", basicJob(2))
	e.clock.Advance(2 * time.Second)

	op, err := e.client.CancelJob(t.Context(), &batchpb.CancelJobRequest{Name: parent + "/jobs/cx"})
	require.NoError(t, err)
	assert.False(t, op.Done())
	assert.Equal(t, batchpb.JobStatus_CANCELLATION_IN_PROGRESS, e.get(t, "cx").GetStatus().GetState())

	_, err = e.client.CancelJob(t.Context(), &batchpb.CancelJobRequest{Name: parent + "/jobs/cx"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	e.clock.Advance(5 * time.Second)

	got := e.get(t, "cx")
	assert.Equal(t, batchpb.JobStatus_CANCELLED, got.GetStatus().GetState())

	for _, task := range e.tasks(t, "cx") {
		assert.Contains(
			t,
			[]batchpb.TaskStatus_State{batchpb.TaskStatus_FAILED, batchpb.TaskStatus_UNEXECUTED},
			task.GetStatus().GetState(),
		)
	}

	resp, err := op.Wait(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, resp)

	_, err = e.client.CancelJob(t.Context(), &batchpb.CancelJobRequest{Name: parent + "/jobs/cx"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestDeleteJob(t *testing.T) {
	t.Parallel()

	e := newEnv(t, withTiming(gcpbatch.Timing{Delete: 10 * time.Second, Schedule: time.Hour}))
	e.create(t, "del", basicJob(1))

	op, err := e.client.DeleteJob(t.Context(), &batchpb.DeleteJobRequest{Name: parent + "/jobs/del"})
	require.NoError(t, err)
	assert.False(t, op.Done())
	assert.Equal(t, batchpb.JobStatus_DELETION_IN_PROGRESS, e.get(t, "del").GetStatus().GetState())

	_, err = e.client.DeleteJob(t.Context(), &batchpb.DeleteJobRequest{Name: parent + "/jobs/del"})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))

	e.clock.Advance(10 * time.Second)
	require.NoError(t, op.Wait(t.Context()))

	_, err = e.client.GetJob(t.Context(), &batchpb.GetJobRequest{Name: parent + "/jobs/del"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.DeleteJob(t.Context(), &batchpb.DeleteJobRequest{Name: parent + "/jobs/del"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	e.create(t, "del", basicJob(1))
}

func TestStoredSpecRoundTrip(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	job := basicJob(1, &batchpb.Runnable{
		DisplayName: "scr", IgnoreExitStatus: true,
		Executable: &batchpb.Runnable_Script_{Script: &batchpb.Runnable_Script{
			Command: &batchpb.Runnable_Script_Text{Text: "echo hi"},
		}},
	})
	spec := job.GetTaskGroups()[0].GetTaskSpec()
	spec.MaxRunDuration = durationpb.New(time.Hour)
	spec.ComputeResource = &batchpb.ComputeResource{CpuMilli: 500, MemoryMib: 256}
	spec.MaxRetryCount = 3
	job.AllocationPolicy = &batchpb.AllocationPolicy{
		ServiceAccount: &batchpb.ServiceAccount{Email: "sa@p.iam.gserviceaccount.com"},
		Instances: []*batchpb.AllocationPolicy_InstancePolicyOrTemplate{{
			PolicyTemplate: &batchpb.AllocationPolicy_InstancePolicyOrTemplate_Policy{
				Policy: &batchpb.AllocationPolicy_InstancePolicy{MachineType: "e2-standard-4"},
			},
		}},
	}
	e.create(t, "rt", job)

	got := e.get(t, "rt")
	gs := got.GetTaskGroups()[0].GetTaskSpec()
	assert.Equal(t, "echo hi", gs.GetRunnables()[0].GetScript().GetText())
	assert.Equal(t, time.Hour, gs.GetMaxRunDuration().AsDuration())
	assert.Equal(t, int64(500), gs.GetComputeResource().GetCpuMilli())
	assert.Equal(t, "e2-standard-4", got.GetAllocationPolicy().GetInstances()[0].GetPolicy().GetMachineType())
	assert.Equal(t, "sa@p.iam.gserviceaccount.com", got.GetAllocationPolicy().GetServiceAccount().GetEmail())
}

package gcpbatch_test

import (
	"context"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

func (e *env) finish(t *testing.T, id string) *batchpb.Job {
	t.Helper()
	e.backend.WaitIdle()

	return e.get(t, id)
}

func taskStates(tasks []*batchpb.Task) []string {
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		out = append(out, task.GetStatus().GetState().String())
	}

	return out
}

func TestEngineTaskEnvironment(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{"ok:1": exitsOK()})
	e := newEnv(t, withRuntime(rt))

	job := basicJob(3)
	g := job.GetTaskGroups()[0]
	g.TaskSpec.Environment = &batchpb.Environment{Variables: map[string]string{"A": "spec", "B": "spec"}}
	g.TaskEnvironments = []*batchpb.Environment{
		{Variables: map[string]string{"B": "t0"}}, nil, {Variables: map[string]string{"B": "t2"}},
	}
	g.TaskCount = 99
	g.TaskSpec.Runnables[0].Environment = &batchpb.Environment{Variables: map[string]string{"C": "run"}}
	g.TaskSpec.Runnables[0].Executable = &batchpb.Runnable_Container_{Container: &batchpb.Runnable_Container{
		ImageUri: "ok:1", Entrypoint: "/bin/run", Commands: []string{"a", "b"}, Volumes: []string{"/h:/c:ro"},
		BlockExternalNetwork: true,
	}}
	g.RequireHostsFile = true

	created := e.create(t, "envs", job)
	assert.Equal(t, int64(3), created.GetTaskGroups()[0].GetTaskCount())

	got := e.finish(t, "envs")
	assert.Equal(t, batchpb.JobStatus_SUCCEEDED, got.GetStatus().GetState())

	specs := rt.specsFor("ok:1")
	require.Len(t, specs, 3)

	byIndex := map[string]container.Spec{}
	for _, s := range specs {
		byIndex[envOf(s, "BATCH_TASK_INDEX")] = s
	}

	require.Len(t, byIndex, 3)

	for idx, wantB := range map[string]string{"0": "t0", "1": "spec", "2": "t2"} {
		s := byIndex[idx]
		assert.Equal(t, "3", envOf(s, "BATCH_TASK_COUNT"), idx)
		assert.Equal(t, wantB, envOf(s, "B"), idx)
		assert.Equal(t, "spec", envOf(s, "A"), idx)
		assert.Equal(t, "run", envOf(s, "C"), idx)
		assert.Equal(t, gcpbatch.HostsFilePath, envOf(s, "BATCH_HOSTS_FILE"), idx)
		assert.Equal(t, []string{"/bin/run"}, s.Entrypoint)
		assert.Equal(t, []string{"a", "b"}, s.Cmd)
		assert.Equal(t, "none", s.Network)
		assert.Contains(t, s.Mounts, "/h:/c:ro")
	}
}

func TestEngineSecretVariables(t *testing.T) {
	t.Parallel()

	const ref = "projects/p/secrets/db/versions/1"

	tests := []struct {
		secrets fakeSecrets
		name    string
		want    string
		state   batchpb.JobStatus_State
	}{
		{name: "resolved", secrets: fakeSecrets{ref: "hunter2"}, want: "hunter2", state: batchpb.JobStatus_SUCCEEDED},
		{name: "missing secret", secrets: fakeSecrets{}, state: batchpb.JobStatus_FAILED},
		{name: "no secret manager", state: batchpb.JobStatus_FAILED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime(map[string]behavior{"ok:1": exitsOK()})
			svc := gcpbatch.Services{}

			if tt.secrets != nil {
				svc.Secrets = tt.secrets
			}

			e := newEnv(t, withRuntime(rt), withServices(svc))
			job := basicJob(1)
			job.TaskGroups[0].TaskSpec.Environment = &batchpb.Environment{
				SecretVariables: map[string]string{"DB_PASSWORD": ref},
			}
			e.create(t, "sec", job)

			got := e.finish(t, "sec")
			assert.Equal(t, tt.state, got.GetStatus().GetState())

			if tt.want != "" {
				assert.Equal(t, tt.want, envOf(rt.specsFor("ok:1")[0], "DB_PASSWORD"))
			}

			for _, task := range e.tasks(t, "sec") {
				for _, ev := range task.GetStatus().GetStatusEvents() {
					assert.NotContains(t, ev.GetDescription(), "hunter2")
				}
			}
		})
	}
}

func TestEngineRetriesAndLifecyclePolicies(t *testing.T) {
	t.Parallel()

	policy := func(a batchpb.LifecyclePolicy_Action, codes ...int32) []*batchpb.LifecyclePolicy {
		return []*batchpb.LifecyclePolicy{{
			Action: a, ActionCondition: &batchpb.LifecyclePolicy_ActionCondition{ExitCodes: codes},
		}}
	}

	tests := []struct {
		name     string
		policies []*batchpb.LifecyclePolicy
		exit     int
		failFor  int32
		retries  int32
		wantRuns int32
		want     batchpb.TaskStatus_State
	}{
		{name: "no retries", exit: 3, failFor: 99, wantRuns: 1, want: batchpb.TaskStatus_FAILED},
		{name: "retry then succeed", exit: 3, failFor: 2, retries: 3, wantRuns: 3, want: batchpb.TaskStatus_SUCCEEDED},
		{name: "retries exhausted", exit: 3, failFor: 99, retries: 2, wantRuns: 3, want: batchpb.TaskStatus_FAILED},
		{
			name: "retry matching code", exit: 50001, failFor: 1, retries: 2, wantRuns: 2,
			want:     batchpb.TaskStatus_SUCCEEDED,
			policies: policy(batchpb.LifecyclePolicy_RETRY_TASK, 50001),
		},
		{
			name: "retry policy ignores other code", exit: 3, failFor: 99, retries: 5, wantRuns: 1,
			want:     batchpb.TaskStatus_FAILED,
			policies: policy(batchpb.LifecyclePolicy_RETRY_TASK, 50001),
		},
		{
			name: "fail on matching code", exit: 3, failFor: 99, retries: 5, wantRuns: 1,
			want:     batchpb.TaskStatus_FAILED,
			policies: policy(batchpb.LifecyclePolicy_FAIL_TASK, 3),
		},
		{
			name: "fail policy retries other codes", exit: 4, failFor: 1, retries: 5, wantRuns: 2,
			want:     batchpb.TaskStatus_SUCCEEDED,
			policies: policy(batchpb.LifecyclePolicy_FAIL_TASK, 3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var runs atomic.Int32

			rt := newFakeRuntime(map[string]behavior{
				"flaky:1": func(context.Context, container.Spec, io.Writer) int {
					if runs.Add(1) <= tt.failFor {
						return tt.exit
					}

					return 0
				},
			})
			e := newEnv(t, withRuntime(rt))
			job := basicJob(1, containerRunnable("flaky:1"))
			job.TaskGroups[0].TaskSpec.MaxRetryCount = tt.retries
			job.TaskGroups[0].TaskSpec.LifecyclePolicies = tt.policies
			e.create(t, "retry", job)

			got := e.finish(t, "retry")
			assert.Equal(t, tt.wantRuns, runs.Load())

			task := e.tasks(t, "retry")[0]
			assert.Equal(t, tt.want, task.GetStatus().GetState())

			wantJob := batchpb.JobStatus_SUCCEEDED
			if tt.want == batchpb.TaskStatus_FAILED {
				wantJob = batchpb.JobStatus_FAILED
			}

			assert.Equal(t, wantJob, got.GetStatus().GetState())

			evs := task.GetStatus().GetStatusEvents()
			last := evs[len(evs)-1]
			assert.Equal(t, tt.want.String() != "SUCCEEDED", last.GetTaskExecution().GetExitCode() != 0)
		})
	}
}

func TestEngineRunnableSemantics(t *testing.T) {
	t.Parallel()

	var order atomic.Int32

	seen := map[string]int32{}
	record := func(name string, code int) behavior {
		return func(context.Context, container.Spec, io.Writer) int {
			seen[name] = order.Add(1)

			return code
		}
	}

	rt := newFakeRuntime(map[string]behavior{
		"bad:1": record("bad", 7), "after:1": record("after", 0), "always:1": record("always", 0),
		"ignored:1": record("ignored", 9), "bgsvc:1": blocks(),
	})
	e := newEnv(t, withRuntime(rt))

	bg := containerRunnable("bgsvc:1")
	bg.Background = true

	ignored := containerRunnable("ignored:1")
	ignored.IgnoreExitStatus = true

	always := containerRunnable("always:1")
	always.AlwaysRun = true

	e.create(t, "sem", basicJob(1, bg, ignored, containerRunnable("bad:1"), containerRunnable("after:1"), always))

	got := e.finish(t, "sem")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())

	mu := seen
	assert.Contains(t, mu, "ignored")
	assert.Contains(t, mu, "bad")
	assert.Contains(t, mu, "always")
	assert.NotContains(t, mu, "after")
	assert.Greater(t, mu["always"], mu["bad"])

	evs := e.tasks(t, "sem")[0].GetStatus().GetStatusEvents()
	assert.Equal(t, int32(7), evs[len(evs)-1].GetTaskExecution().GetExitCode())
}

func TestEngineBackgroundRunnableDoesNotFailTask(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{"bg:1": blocks(), "ok:1": exitsOK()})
	e := newEnv(t, withRuntime(rt))

	bg := containerRunnable("bg:1")
	bg.Background = true
	e.create(t, "bg", basicJob(2, bg, containerRunnable("ok:1")))

	got := e.finish(t, "bg")
	assert.Equal(t, batchpb.JobStatus_SUCCEEDED, got.GetStatus().GetState())
	assert.Equal(t, []string{"SUCCEEDED", "SUCCEEDED"}, taskStates(e.tasks(t, "bg")))
}

func TestEngineScriptRunnables(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{"script:img": exitsOK()})
	e := newEnv(t, withRuntime(rt))

	text := &batchpb.Runnable{Executable: &batchpb.Runnable_Script_{Script: &batchpb.Runnable_Script{
		Command: &batchpb.Runnable_Script_Text{Text: "echo $BATCH_TASK_INDEX"},
	}}}
	path := &batchpb.Runnable{Executable: &batchpb.Runnable_Script_{Script: &batchpb.Runnable_Script{
		Command: &batchpb.Runnable_Script_Path{Path: "/opt/run.sh"},
	}}}
	e.create(t, "scripts", basicJob(1, text, path))

	assert.Equal(t, batchpb.JobStatus_SUCCEEDED, e.finish(t, "scripts").GetStatus().GetState())

	specs := rt.specsFor("script:img")
	require.Len(t, specs, 2)
	assert.Equal(t, "echo $BATCH_TASK_INDEX", specs[0].Entrypoint[len(specs[0].Entrypoint)-1])
	assert.Equal(t, []string{"/opt/run.sh"}, specs[1].Entrypoint)
}

func TestEngineInOrderStopsAfterFailure(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{
		"x:1": func(_ context.Context, s container.Spec, _ io.Writer) int {
			if envOf(s, "BATCH_TASK_INDEX") == "1" {
				return 2
			}

			return 0
		},
	})
	e := newEnv(t, withRuntime(rt))
	job := basicJob(4, containerRunnable("x:1"))
	job.TaskGroups[0].SchedulingPolicy = batchpb.TaskGroup_IN_ORDER
	e.create(t, "ordered", job)

	got := e.finish(t, "ordered")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())
	assert.Equal(t, []string{"SUCCEEDED", "FAILED", "UNEXECUTED", "UNEXECUTED"}, taskStates(e.tasks(t, "ordered")))
}

func TestEngineParallelFailureLetsOthersFinish(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{
		"x:1": func(_ context.Context, s container.Spec, _ io.Writer) int {
			n, _ := strconv.Atoi(envOf(s, "BATCH_TASK_INDEX"))

			return n % 2
		},
	})
	e := newEnv(t, withRuntime(rt))
	e.create(t, "par", basicJob(4, containerRunnable("x:1")))

	got := e.finish(t, "par")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())
	assert.Equal(t, []string{"SUCCEEDED", "FAILED", "SUCCEEDED", "FAILED"}, taskStates(e.tasks(t, "par")))
	assert.Equal(
		t,
		map[string]int64{"SUCCEEDED": 2, "FAILED": 2},
		got.GetStatus().GetTaskGroups()["group0"].GetCounts(),
	)
}

func TestEngineMaxRunDuration(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{"slow:1": blocks()})
	e := newEnv(t, withRuntime(rt), withTimer(func(time.Duration) (<-chan time.Time, func() bool) {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}

		return ch, func() bool { return true }
	}))
	job := basicJob(1, containerRunnable("slow:1"))
	job.TaskGroups[0].TaskSpec.MaxRunDuration = durationpb.New(time.Minute)
	e.create(t, "slow", job)

	got := e.finish(t, "slow")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())

	evs := e.tasks(t, "slow")[0].GetStatus().GetStatusEvents()
	assert.Equal(t, int32(50005), evs[len(evs)-1].GetTaskExecution().GetExitCode())
}

func TestEngineRunnableTimeout(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{"slow:1": blocks(), "ok:1": exitsOK()})
	e := newEnv(t, withRuntime(rt), withTimer(func(time.Duration) (<-chan time.Time, func() bool) {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}

		return ch, func() bool { return true }
	}))
	slow := containerRunnable("slow:1")
	slow.Timeout = durationpb.New(time.Second)
	e.create(t, "rto", basicJob(1, slow, containerRunnable("ok:1")))

	got := e.finish(t, "rto")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())
	assert.Empty(t, rt.specsFor("ok:1"))

	evs := e.tasks(t, "rto")[0].GetStatus().GetStatusEvents()
	assert.Equal(t, int32(50005), evs[len(evs)-1].GetTaskExecution().GetExitCode())
}

func TestEngineStartFailureFailsTask(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(nil)
	e := newEnv(t, withRuntime(rt))
	e.create(t, "nostart", basicJob(1, containerRunnable("missing:1")))

	got := e.finish(t, "nostart")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())
}

func TestEngineCancelStopsRunningTasks(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 1)
	rt := newFakeRuntime(map[string]behavior{
		"hang:1": func(ctx context.Context, _ container.Spec, _ io.Writer) int {
			started <- struct{}{}
			<-ctx.Done()

			return 137
		},
	})
	e := newEnv(t, withRuntime(rt))
	e.create(t, "stop", basicJob(2, containerRunnable("hang:1")))
	<-started

	_, err := e.client.CancelJob(t.Context(), &batchpb.CancelJobRequest{Name: parent + "/jobs/stop"})
	require.NoError(t, err)

	got := e.finish(t, "stop")
	assert.Equal(t, batchpb.JobStatus_CANCELLED, got.GetStatus().GetState())

	for _, s := range taskStates(e.tasks(t, "stop")) {
		assert.Contains(t, []string{"FAILED", "UNEXECUTED"}, s)
	}
}

func TestEngineCloudLogging(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime(map[string]behavior{
		"say:1": func(_ context.Context, s container.Spec, out io.Writer) int {
			_, _ = io.WriteString(out, "hello "+envOf(s, "BATCH_TASK_INDEX")+"\n")

			return 0
		},
	})

	tests := []struct {
		name    string
		logs    *batchpb.LogsPolicy
		wantLog []string
	}{
		{
			name:    "cloud logging",
			logs:    &batchpb.LogsPolicy{Destination: batchpb.LogsPolicy_CLOUD_LOGGING},
			wantLog: []string{"hello 0", "hello 1"},
		},
		{name: "unspecified", logs: nil},
		{name: "path", logs: &batchpb.LogsPolicy{Destination: batchpb.LogsPolicy_PATH, LogsPath: "/tmp/x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sink := &fakeLogs{}
			e := newEnv(t, withRuntime(rt), withServices(gcpbatch.Services{Logs: sink}))
			job := basicJob(2, containerRunnable("say:1"))
			job.LogsPolicy = tt.logs
			e.create(t, "logs", job)
			e.finish(t, "logs")

			assert.ElementsMatch(t, tt.wantLog, sink.texts())

			for _, en := range sink.entries {
				assert.Equal(t, "projects/"+project+"/logs/batch_task_logs", en.GetLogName())
			}
		})
	}
}

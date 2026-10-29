package batch_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/batch"
)

var errFakeIssue = errors.New("role not assumable")

type fakeResult struct {
	err   error
	after <-chan struct{}
	logs  []string
	exit  int
	block bool
}

type fakeRunner struct {
	behave    func(call int, spec batch.ContainerSpec) fakeResult
	gate      chan struct{}
	specs     []batch.ContainerSpec
	cancelled atomic.Int32
	active    atomic.Int32
	maxActive atomic.Int32
	calls     atomic.Int32
	mu        sync.Mutex
}

func (f *fakeRunner) RunContainer(
	ctx context.Context, spec batch.ContainerSpec, hooks batch.RunHooks,
) (int, error) {
	call := int(f.calls.Add(1))

	f.mu.Lock()
	f.specs = append(f.specs, spec)
	f.mu.Unlock()

	n := f.active.Add(1)
	defer f.active.Add(-1)

	for {
		m := f.maxActive.Load()
		if n <= m || f.maxActive.CompareAndSwap(m, n) {
			break
		}
	}

	res := fakeResult{}
	if f.behave != nil {
		res = f.behave(call, spec)
	}

	if res.err != nil {
		return 0, res.err
	}

	if hooks.OnAddress != nil {
		hooks.OnAddress("10.1.0.2")
	}

	hooks.OnStarted()

	if len(res.logs) > 0 {
		hooks.OnLog(res.logs)
	}

	if res.after != nil {
		select {
		case <-ctx.Done():
			f.cancelled.Add(1)

			return 0, ctx.Err()
		case <-res.after:
		}
	}

	if res.block || f.gate != nil {
		var gate <-chan struct{}
		if !res.block {
			gate = f.gate
		}

		select {
		case <-ctx.Done():
			f.cancelled.Add(1)

			return 0, ctx.Err()
		case <-gate:
		}
	}

	return res.exit, nil
}

func (f *fakeRunner) specAt(i int) batch.ContainerSpec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.specs[i]
}

type fakeSink struct {
	lines   map[string][]string
	ensured []string
	mu      sync.Mutex
}

func (s *fakeSink) EnsureLogGroupAndStream(region, group, stream string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensured = append(s.ensured, region+"|"+group+"|"+stream)

	return nil
}

func (s *fakeSink) PutLogLines(region, group, stream string, msgs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lines == nil {
		s.lines = map[string][]string{}
	}

	k := region + "|" + group + "|" + stream
	s.lines[k] = append(s.lines[k], msgs...)

	return nil
}

func (s *fakeSink) all() map[string][]string {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := map[string][]string{}
	for k, v := range s.lines {
		out[k] = append([]string(nil), v...)
	}

	return out
}

type fakeIssuer struct {
	err   error
	roles []string
	mu    sync.Mutex
}

func (i *fakeIssuer) IssueJobCredentials(roleArn, _ string) (batch.JobCredentials, error) {
	i.mu.Lock()
	i.roles = append(i.roles, roleArn)
	i.mu.Unlock()

	if i.err != nil {
		return batch.JobCredentials{}, i.err
	}

	return batch.JobCredentials{AccessKeyID: "AKIAJOB", SecretAccessKey: "secret", SessionToken: "tok"}, nil
}

type execEnv struct {
	b      *batch.InMemoryBackend
	runner *fakeRunner
	sink   *fakeSink
	queue  string
	jd     string
}

type execOpts struct {
	props    *batch.ContainerProperties
	retry    *batch.RetryStrategy
	maxVcpus int32
}

func newExecEnv(t *testing.T, runner *fakeRunner, o execOpts) *execEnv {
	t.Helper()

	ctx := context.Background()
	b := batch.NewInMemoryBackend("000000000000", "us-east-1")
	sink := &fakeSink{}

	b.SetContainerRunner(runner)
	b.SetJobLogSink(sink)
	b.SetExecutionEndpoint("http://host.docker.internal:4566")
	t.Cleanup(b.StopExecution)

	ce, err := b.CreateComputeEnvironment(ctx, "ce", "MANAGED", "ENABLED", nil, "", &batch.ComputeResources{
		Type: "EC2", MaxvCpus: o.maxVcpus,
	}, nil, nil, nil)
	require.NoError(t, err)

	queue, err := b.CreateJobQueue(ctx, "q", 1, "ENABLED",
		[]batch.ComputeEnvironmentOrder{{ComputeEnvironment: ce.ComputeEnvironmentArn, Order: 1}},
		nil, "", nil, "", nil)
	require.NoError(t, err)

	props := o.props
	if props == nil {
		props = &batch.ContainerProperties{Image: "busybox", Vcpus: 1, Memory: 128}
	}

	_, err = b.RegisterJobDefinition(ctx, "jd", "container", nil, nil, 0, 0, props, nil, nil, nil, nil,
		map[string]string{"greeting": "hello"}, false, o.retry)
	require.NoError(t, err)

	return &execEnv{b: b, runner: runner, sink: sink, queue: queue.JobQueueName, jd: "jd"}
}

func (e *execEnv) submit(t *testing.T, opts submitOpts) *batch.Job {
	t.Helper()

	job, err := e.b.SubmitJob(context.Background(), "job", e.queue, e.jd, nil, opts.params, opts.deps,
		opts.retry, opts.timeout, opts.array, opts.overrides, nil, "", 0, false)
	require.NoError(t, err)

	return job
}

type submitOpts struct {
	params    map[string]string
	retry     *batch.RetryStrategy
	timeout   *batch.JobTimeout
	array     *batch.ArrayProperties
	overrides *batch.ContainerOverrides
	deps      []batch.JobDependency
}

func (e *execEnv) describe(t *testing.T, id string) *batch.Job {
	t.Helper()

	jobs := e.b.DescribeJobs(context.Background(), []string{id})
	require.Len(t, jobs, 1)

	return jobs[0]
}

func (e *execEnv) waitStatus(t *testing.T, id, want string) *batch.Job {
	t.Helper()

	require.Eventually(t, func() bool { return e.describe(t, id).Status == want }, 10*time.Second, 5*time.Millisecond,
		"job %s never reached %s (now %s)", id, want, e.describe(t, id).Status)

	return e.describe(t, id)
}

func TestExecJobStateAndExitCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantReason string
		exit       int
	}{
		{name: "exit zero", exit: 0, wantStatus: "SUCCEEDED"},
		{name: "exit three", exit: 3, wantStatus: "FAILED", wantReason: "Essential container in task exited"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{
				gate: make(chan struct{}),
				behave: func(int, batch.ContainerSpec) fakeResult {
					return fakeResult{exit: tt.exit, logs: []string{"line one", "line two"}}
				},
			}
			e := newExecEnv(t, runner, execOpts{})
			job := e.submit(t, submitOpts{})

			running := e.waitStatus(t, job.JobID, "RUNNING")
			require.NotNil(t, running.StartedAt)
			require.NotNil(t, running.Container)
			assert.True(t, strings.HasPrefix(running.Container.LogStreamName, "jd/default/"))
			require.Len(t, running.Attempts, 1)

			close(runner.gate)

			done := e.waitStatus(t, job.JobID, tt.wantStatus)
			assert.Equal(t, tt.wantReason, done.StatusReason)
			require.NotNil(t, done.Container.ExitCode)
			assert.Equal(t, int32(tt.exit), *done.Container.ExitCode)
			assert.Equal(t, running.Container.LogStreamName, done.Container.LogStreamName)
			require.Len(t, done.Attempts, 1)
			assert.Equal(t, done.Container.LogStreamName, done.Attempts[0].Container.LogStreamName)
			assert.NotNil(t, done.Attempts[0].StoppedAt)

			raw, err := json.Marshal(done.Attempts[0].Container)
			require.NoError(t, err)
			assert.Contains(t, string(raw), `"exitCode"`)

			key := "us-east-1|/aws/batch/job|" + done.Container.LogStreamName
			assert.Equal(t, []string{"line one", "line two"}, e.sink.all()[key])
		})
	}
}

func TestExecContainerSpecAndEnvironment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		props    *batch.ContainerProperties
		sub      submitOpts
		issuer   *fakeIssuer
		wantCmd  []string
		wantEnv  []string
		wantMem  int64
		wantCPU  float64
		wantRole bool
	}{
		{
			name: "definition and ref params",
			props: &batch.ContainerProperties{
				Image:       "busybox",
				Vcpus:       1,
				Memory:      256,
				Command:     []string{"echo", "Ref::greeting", "Ref::who", "$(NAME)"},
				Environment: []batch.KeyValuePair{{Name: "NAME", Value: "dev"}},
			},
			sub:     submitOpts{params: map[string]string{"who": "world"}},
			wantCmd: []string{"echo", "hello", "world", "dev"},
			wantEnv: []string{
				"NAME=dev", "AWS_REGION=us-east-1", "AWS_ENDPOINT_URL=http://host.docker.internal:4566",
				"AWS_BATCH_JQ_NAME=q", "AWS_BATCH_JOB_ATTEMPT=1", "AWS_BATCH_CE_NAME=ce",
			},
			wantMem: 256,
			wantCPU: 1,
		},
		{
			name:  "overrides win",
			props: &batch.ContainerProperties{Image: "busybox", Vcpus: 1, Memory: 256, Command: []string{"old"}},
			sub: submitOpts{overrides: &batch.ContainerOverrides{
				Command:     []string{"new", "Ref::greeting"},
				Environment: []batch.KeyValuePair{{Name: "X", Value: "1"}},
				ResourceRequirements: []batch.ResourceRequirement{
					{Type: "VCPU", Value: "2"}, {Type: "MEMORY", Value: "512"},
				},
			}},
			wantCmd: []string{"new", "hello"},
			wantEnv: []string{"X=1"},
			wantMem: 512,
			wantCPU: 2,
		},
		{
			name: "job role credentials",
			props: &batch.ContainerProperties{
				Image: "busybox", Vcpus: 1, Memory: 128, JobRoleArn: "arn:aws:iam::000000000000:role/job",
			},
			issuer: &fakeIssuer{},
			wantEnv: []string{
				"AWS_ACCESS_KEY_ID=AKIAJOB", "AWS_SECRET_ACCESS_KEY=secret", "AWS_SESSION_TOKEN=tok",
			},
			wantMem:  128,
			wantCPU:  1,
			wantRole: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{}
			e := newExecEnv(t, runner, execOpts{props: tt.props})

			if tt.issuer != nil {
				e.b.SetJobRoleCredentialIssuer(tt.issuer)
			}

			job := e.submit(t, tt.sub)
			e.waitStatus(t, job.JobID, "SUCCEEDED")

			spec := runner.specAt(0)
			assert.Equal(t, "busybox", spec.Image)
			assert.Equal(t, tt.wantCmd, spec.Command)
			assert.Equal(t, tt.wantMem, spec.MemoryMiB)
			assert.InDelta(t, tt.wantCPU, spec.VCPUs, 0.001)
			assert.Contains(t, spec.Env, "AWS_BATCH_JOB_ID="+job.JobID)

			for _, want := range tt.wantEnv {
				assert.Contains(t, spec.Env, want)
			}

			if tt.wantRole {
				assert.Equal(t, []string{"arn:aws:iam::000000000000:role/job"}, tt.issuer.roles)
			}
		})
	}
}

func TestExecJobRoleCredentialFailureFailsJob(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{}
	e := newExecEnv(t, runner, execOpts{props: &batch.ContainerProperties{
		Image: "busybox", Vcpus: 1, Memory: 128, JobRoleArn: "arn:aws:iam::000000000000:role/missing",
	}})
	e.b.SetJobRoleCredentialIssuer(&fakeIssuer{err: errFakeIssue})

	job := e.submit(t, submitOpts{})
	done := e.waitStatus(t, job.JobID, "FAILED")

	assert.Contains(t, done.StatusReason, "CannotStartContainerError")
	assert.Equal(t, int32(0), runner.calls.Load())
}

func TestExecRetryStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		retry        *batch.RetryStrategy
		name         string
		wantStatus   string
		exits        []int
		wantAttempts int
	}{
		{
			name:         "retries until success",
			retry:        &batch.RetryStrategy{Attempts: 3},
			exits:        []int{1, 1, 0},
			wantStatus:   "SUCCEEDED",
			wantAttempts: 3,
		},
		{
			name:         "attempts exhausted",
			retry:        &batch.RetryStrategy{Attempts: 2},
			exits:        []int{1, 1, 0},
			wantStatus:   "FAILED",
			wantAttempts: 2,
		},
		{
			name: "exit rule stops retry",
			retry: &batch.RetryStrategy{Attempts: 3, EvaluateOnExit: []batch.EvaluateOnExit{
				{Action: "EXIT", OnExitCode: "3"}, {Action: "RETRY", OnExitCode: "*"},
			}},
			exits:        []int{3, 0},
			wantStatus:   "FAILED",
			wantAttempts: 1,
		},
		{
			name: "retry rule matches reason glob",
			retry: &batch.RetryStrategy{Attempts: 3, EvaluateOnExit: []batch.EvaluateOnExit{
				{Action: "RETRY", OnReason: "Essential*"},
			}},
			exits:        []int{2, 0},
			wantStatus:   "SUCCEEDED",
			wantAttempts: 2,
		},
		{
			name: "no matching rule retries",
			retry: &batch.RetryStrategy{Attempts: 2, EvaluateOnExit: []batch.EvaluateOnExit{
				{Action: "EXIT", OnExitCode: "9"},
			}},
			exits:        []int{1, 0},
			wantStatus:   "SUCCEEDED",
			wantAttempts: 2,
		},
		{
			name:         "no retry strategy runs once",
			exits:        []int{1, 0},
			wantStatus:   "FAILED",
			wantAttempts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{behave: func(call int, _ batch.ContainerSpec) fakeResult {
				return fakeResult{exit: tt.exits[call-1]}
			}}
			e := newExecEnv(t, runner, execOpts{})
			job := e.submit(t, submitOpts{retry: tt.retry})

			done := e.waitStatus(t, job.JobID, tt.wantStatus)
			assert.Len(t, done.Attempts, tt.wantAttempts)
			assert.Equal(t, tt.wantAttempts, int(runner.calls.Load()))

			for i := range tt.wantAttempts {
				assert.Contains(t, runner.specAt(i).Env, "AWS_BATCH_JOB_ATTEMPT="+string(rune('1'+i)))
			}
		})
	}
}

func TestExecAttemptTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		attempts     int32
		wantAttempts int
	}{
		{name: "single attempt", attempts: 1, wantAttempts: 1},
		{name: "retried then failed", attempts: 2, wantAttempts: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				runner := &fakeRunner{behave: func(int, batch.ContainerSpec) fakeResult {
					return fakeResult{block: true}
				}}
				e := newExecEnv(t, runner, execOpts{})
				job := e.submit(t, submitOpts{
					timeout: &batch.JobTimeout{AttemptDurationSeconds: 60},
					retry:   &batch.RetryStrategy{Attempts: tt.attempts},
				})

				for range tt.wantAttempts {
					synctest.Wait()
					time.Sleep(61 * time.Second)
					synctest.Wait()
				}

				done := e.describe(t, job.JobID)
				assert.Equal(t, "FAILED", done.Status)
				assert.Equal(t, "Job attempt duration exceeded timeout", done.StatusReason)
				assert.Len(t, done.Attempts, tt.wantAttempts)
				assert.Equal(t, int32(tt.wantAttempts), runner.cancelled.Load())
				assert.Nil(t, done.Container.ExitCode)
			})
		})
	}
}

func TestExecTerminateStopsContainer(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{behave: func(int, batch.ContainerSpec) fakeResult { return fakeResult{block: true} }}
	e := newExecEnv(t, runner, execOpts{})
	job := e.submit(t, submitOpts{})
	e.waitStatus(t, job.JobID, "RUNNING")

	require.NoError(t, e.b.TerminateJob(context.Background(), job.JobID, "stop it"))

	require.Eventually(t, func() bool { return runner.cancelled.Load() == 1 }, 10*time.Second, 5*time.Millisecond)

	done := e.describe(t, job.JobID)
	assert.Equal(t, "FAILED", done.Status)
	assert.Equal(t, "stop it", done.StatusReason)
	require.Len(t, done.Attempts, 1)
	assert.Equal(t, "stop it", done.Attempts[0].StatusReason)

	e.b.StopExecution()
	assert.Equal(t, "FAILED", e.describe(t, job.JobID).Status)
	assert.Equal(t, "stop it", e.describe(t, job.JobID).StatusReason)
}

func TestExecArrayJobConcurrencyAndIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		maxVcpus  int32
		wantMax   int32
		arraySize int32
	}{
		{name: "capacity two", maxVcpus: 2, wantMax: 2, arraySize: 5},
		{name: "capacity one", maxVcpus: 1, wantMax: 1, arraySize: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{gate: make(chan struct{})}
			e := newExecEnv(t, runner, execOpts{maxVcpus: tt.maxVcpus})
			parent := e.submit(t, submitOpts{array: &batch.ArrayProperties{Size: tt.arraySize}})

			require.Eventually(
				t,
				func() bool { return runner.active.Load() == tt.wantMax },
				10*time.Second,
				time.Millisecond,
			)

			done := make(chan struct{})

			go func() {
				defer close(done)

				for range tt.arraySize {
					runner.gate <- struct{}{}
				}
			}()

			e.waitStatus(t, parent.JobID, "SUCCEEDED")
			<-done
			assert.Equal(t, tt.wantMax, runner.maxActive.Load())

			seen := map[string]bool{}
			for i := range int(tt.arraySize) {
				for _, kv := range runner.specAt(i).Env {
					if strings.HasPrefix(kv, "AWS_BATCH_JOB_ARRAY_INDEX=") {
						seen[kv] = true
					}
				}
			}

			assert.Len(t, seen, int(tt.arraySize))
		})
	}
}

func TestExecDependsOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantReason string
		firstExit  int
		wantRuns   int32
	}{
		{name: "dependency succeeds", firstExit: 0, wantStatus: "SUCCEEDED", wantRuns: 2},
		{name: "dependency fails", firstExit: 1, wantStatus: "FAILED", wantReason: "dependency failed", wantRuns: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{gate: make(chan struct{}), behave: func(call int, _ batch.ContainerSpec) fakeResult {
				if call == 1 {
					return fakeResult{exit: tt.firstExit}
				}

				return fakeResult{}
			}}
			e := newExecEnv(t, runner, execOpts{})
			first := e.submit(t, submitOpts{})
			second := e.submit(t, submitOpts{deps: []batch.JobDependency{{JobID: first.JobID}}})

			e.waitStatus(t, first.JobID, "RUNNING")
			e.waitStatus(t, second.JobID, "PENDING")
			assert.Equal(t, int32(1), runner.calls.Load())

			runner.gate <- struct{}{}

			if tt.wantRuns == 2 {
				e.waitStatus(t, second.JobID, "RUNNING")
				runner.gate <- struct{}{}
			}

			done := e.waitStatus(t, second.JobID, tt.wantStatus)
			assert.Equal(t, tt.wantReason, done.StatusReason)
			assert.Equal(t, tt.wantRuns, runner.calls.Load())
		})
	}
}

func TestExecSequentialArrayRunsInOrder(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{}
	e := newExecEnv(t, runner, execOpts{maxVcpus: 8})
	parent := e.submit(t, submitOpts{
		array: &batch.ArrayProperties{Size: 4},
		deps:  []batch.JobDependency{{Type: "SEQUENTIAL"}},
	})

	e.waitStatus(t, parent.JobID, "SUCCEEDED")
	assert.Equal(t, int32(1), runner.maxActive.Load())

	for i := range 4 {
		assert.Contains(t, runner.specAt(i).Env, "AWS_BATCH_JOB_ARRAY_INDEX="+string(rune('0'+i)))
	}
}

func TestExecCapacityQueuesRunnableAndHonoursPriority(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{gate: make(chan struct{})}
	e := newExecEnv(t, runner, execOpts{maxVcpus: 1})

	_, err := e.b.CreateJobQueue(context.Background(), "high", 100, "ENABLED",
		[]batch.ComputeEnvironmentOrder{{ComputeEnvironment: "ce", Order: 1}}, nil, "", nil, "", nil)
	require.NoError(t, err)

	first := e.submit(t, submitOpts{})
	e.waitStatus(t, first.JobID, "RUNNING")

	low := e.submit(t, submitOpts{})
	highJob, err := e.b.SubmitJob(
		context.Background(),
		"hi",
		"high",
		e.jd,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		"",
		0,
		false,
	)
	require.NoError(t, err)

	e.waitStatus(t, low.JobID, "RUNNABLE")
	e.waitStatus(t, highJob.JobID, "RUNNABLE")
	assert.Equal(t, int32(1), runner.calls.Load())

	runner.gate <- struct{}{}
	e.waitStatus(t, highJob.JobID, "RUNNING")
	assert.Equal(t, "RUNNABLE", e.describe(t, low.JobID).Status)

	runner.gate <- struct{}{}
	e.waitStatus(t, low.JobID, "RUNNING")
	runner.gate <- struct{}{}
	e.waitStatus(t, low.JobID, "SUCCEEDED")
	assert.Equal(t, int32(1), runner.maxActive.Load())
}

func TestExecDisabledKeepsSimulatedBehaviour(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := batch.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateJobQueue(ctx, "q", 1, "ENABLED", nil, nil, "", nil, "", nil)
	require.NoError(t, err)
	_, err = b.RegisterJobDefinition(ctx, "jd", "container", nil, nil, 0, 0,
		&batch.ContainerProperties{Image: "busybox", Vcpus: 1, Memory: 128}, nil, nil, nil, nil, nil, false, nil)
	require.NoError(t, err)

	job, err := b.SubmitJob(ctx, "j", "q", "jd", nil, nil, nil, nil, nil, nil, nil, nil, "", 0, false)
	require.NoError(t, err)

	assert.False(t, b.ExecutionEnabled())

	j := batch.NewJanitor(b, time.Minute, time.Hour, time.Hour)
	j.SweepOnce(ctx)
	assert.Equal(t, "RUNNING", b.DescribeJobs(ctx, []string{job.JobID})[0].Status)
	j.SweepOnce(ctx)
	assert.Equal(t, "SUCCEEDED", b.DescribeJobs(ctx, []string{job.JobID})[0].Status)
}

func TestBatchProviderExecuteJobsSetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		on   bool
	}{
		{name: "default off", on: false},
		{name: "enabled", on: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &batch.Provider{}
			svc, err := p.Init(&service.AppContext{
				Config: batchConfigProvider{batch.Settings{ExecuteJobs: tt.on}}, Logger: slog.Default(),
			})
			require.NoError(t, err)

			h, ok := svc.(*batch.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.on, h.Backend.ExecutionEnabled())
			h.Backend.StopExecution()
		})
	}
}

func TestExecJanitorDoesNotSimulateRunningContainers(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{behave: func(int, batch.ContainerSpec) fakeResult { return fakeResult{block: true} }}
	e := newExecEnv(t, runner, execOpts{})
	job := e.submit(t, submitOpts{})
	e.waitStatus(t, job.JobID, "RUNNING")

	j := batch.NewJanitor(e.b, time.Minute, time.Hour, time.Hour)
	j.SweepOnce(context.Background())
	j.SweepOnce(context.Background())

	assert.Equal(t, "RUNNING", e.describe(t, job.JobID).Status)
	assert.Equal(t, int32(0), runner.cancelled.Load())
}

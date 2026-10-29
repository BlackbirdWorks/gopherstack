package batch_test

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func mnpProps(numNodes, mainNode int32, ranges ...string) *batch.NodeProperties {
	np := &batch.NodeProperties{NumNodes: numNodes, MainNode: mainNode}

	for _, r := range ranges {
		np.NodeRangeProperties = append(np.NodeRangeProperties, batch.NodeRangeProperty{
			TargetNodes:         r,
			ContainerProperties: &batch.ContainerProperties{Image: "busybox", Vcpus: 1, Memory: 64},
		})
	}

	return np
}

func (e *execEnv) registerMNP(t *testing.T, np *batch.NodeProperties, retry *batch.RetryStrategy) {
	t.Helper()

	_, err := e.b.RegisterJobDefinition(context.Background(), "mnp", "multinode", nil, nil, 0, 0, nil, np,
		nil, nil, nil, nil, false, retry)
	require.NoError(t, err)
}

func (e *execEnv) submitMNP(t *testing.T) *batch.Job {
	t.Helper()

	job, err := e.b.SubmitJob(context.Background(), "job", e.queue, "mnp", nil, nil, nil, nil, nil, nil, nil, nil,
		"", 0, false)
	require.NoError(t, err)

	return job
}

func specEnv(spec batch.ContainerSpec) map[string]string {
	out := map[string]string{}

	for _, kv := range spec.Env {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}

	return out
}

func nodeIndexOf(spec batch.ContainerSpec) string { return specEnv(spec)["AWS_BATCH_JOB_NODE_INDEX"] }

func (f *fakeRunner) specCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.specs)
}

func (e *execEnv) waitCalls(t *testing.T, n int) {
	t.Helper()

	require.Eventually(t, func() bool { return e.runner.specCount() >= n }, 10*time.Second, 5*time.Millisecond)
}

func TestMNPRegisterValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		np      *batch.NodeProperties
		name    string
		defType string
		wantErr bool
	}{
		{name: "valid open range", defType: "multinode", np: mnpProps(4, 0, "0:")},
		{name: "valid two groups", defType: "multinode", np: mnpProps(4, 1, "0:1", "2:3")},
		{name: "valid nested", defType: "multinode", np: mnpProps(4, 0, "0:3", "1:2")},
		{name: "multinode needs props", defType: "multinode", wantErr: true},
		{name: "container with props", defType: "container", np: mnpProps(2, 0, "0:"), wantErr: true},
		{name: "main out of range", defType: "multinode", np: mnpProps(2, 2, "0:"), wantErr: true},
		{name: "negative main", defType: "multinode", np: mnpProps(2, -1, "0:"), wantErr: true},
		{name: "zero nodes", defType: "multinode", np: mnpProps(0, 0, "0:"), wantErr: true},
		{name: "gap in coverage", defType: "multinode", np: mnpProps(4, 0, "0:1", "3:3"), wantErr: true},
		{name: "range past numNodes", defType: "multinode", np: mnpProps(2, 0, "0:5"), wantErr: true},
		{name: "reversed range", defType: "multinode", np: mnpProps(4, 0, "3:1", "0:"), wantErr: true},
		{name: "bad target", defType: "multinode", np: mnpProps(2, 0, "a:b"), wantErr: true},
		{name: "no ranges", defType: "multinode", np: mnpProps(2, 0), wantErr: true},
		{name: "six groups", defType: "multinode", np: mnpProps(6, 0, "0", "1", "2", "3", "4", "5"), wantErr: true},
		{
			name: "range without container", defType: "multinode", wantErr: true,
			np: &batch.NodeProperties{NumNodes: 1, NodeRangeProperties: []batch.NodeRangeProperty{{TargetNodes: "0:"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := batch.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.RegisterJobDefinition(context.Background(), "mnp", tt.defType, nil, nil, 0, 0, nil, tt.np,
				nil, nil, nil, nil, false, nil)

			if tt.wantErr {
				require.ErrorIs(t, err, batch.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestMNPEnvAndMainFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		main int32
	}{
		{name: "main zero", main: 0},
		{name: "main middle", main: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{gate: make(chan struct{})}
			mainStr := strconv.Itoa(int(tt.main))
			runner.behave = func(_ int, spec batch.ContainerSpec) fakeResult {
				if nodeIndexOf(spec) == mainStr {
					return fakeResult{}
				}

				return fakeResult{block: true}
			}

			e := newExecEnv(t, runner, execOpts{})
			e.registerMNP(t, mnpProps(3, tt.main, "0:"), nil)
			job := e.submitMNP(t)

			e.waitCalls(t, 3)
			e.waitStatus(t, job.JobID, "RUNNING")

			assert.Equal(t, mainStr, nodeIndexOf(runner.specAt(0)), "main starts first")

			seen := map[string]bool{}

			for i := range 3 {
				env := specEnv(runner.specAt(i))
				idx := env["AWS_BATCH_JOB_NODE_INDEX"]
				seen[idx] = true

				assert.Equal(t, job.JobID+"#"+idx, env["AWS_BATCH_JOB_ID"])
				assert.Equal(t, "3", env["AWS_BATCH_JOB_NUM_NODES"])
				assert.Equal(t, mainStr, env["AWS_BATCH_JOB_MAIN_NODE_INDEX"])

				ip, hasIP := env["AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS"]
				if idx == mainStr {
					assert.False(t, hasIP, "main node has no main-node address")
				} else {
					assert.Equal(t, "10.1.0.2", ip)
				}
			}

			assert.Len(t, seen, 3)
			close(runner.gate)
			e.waitStatus(t, job.JobID, "SUCCEEDED")
		})
	}
}

func TestMNPMainExitStopsChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		mainExit   int
	}{
		{name: "main succeeds", mainExit: 0, wantStatus: "SUCCEEDED"},
		{name: "main fails", mainExit: 3, wantStatus: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{gate: make(chan struct{})}
			runner.behave = func(_ int, spec batch.ContainerSpec) fakeResult {
				if nodeIndexOf(spec) == "0" {
					return fakeResult{exit: tt.mainExit}
				}

				return fakeResult{block: true}
			}

			e := newExecEnv(t, runner, execOpts{})
			e.registerMNP(t, mnpProps(3, 0, "0:"), nil)
			job := e.submitMNP(t)

			e.waitCalls(t, 3)
			e.waitStatus(t, job.JobID, "RUNNING")
			close(runner.gate)

			done := e.waitStatus(t, job.JobID, tt.wantStatus)
			require.NotNil(t, done.StoppedAt)
			assert.Equal(t, int32(2), runner.cancelled.Load(), "both children stopped")

			require.NotNil(t, done.NodeProperties)
			assert.Equal(t, int32(0), done.NodeProperties.MainNode)

			for _, idx := range []string{"#1", "#2"} {
				child := e.waitStatus(t, job.JobID+idx, tt.wantStatus)
				assert.Equal(t, "Terminated because the main node exited", child.StatusReason)
				assert.Nil(t, e.describe(t, job.JobID+idx).Container.ExitCode)
			}

			main := e.describe(t, job.JobID+"#0")
			require.NotNil(t, main.Container.ExitCode)
			assert.Equal(t, int32(tt.mainExit), *main.Container.ExitCode)
		})
	}
}

func TestMNPChildFailureDoesNotFailJob(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	runner := &fakeRunner{}
	runner.behave = func(_ int, spec batch.ContainerSpec) fakeResult {
		if nodeIndexOf(spec) == "1" {
			return fakeResult{exit: 2}
		}

		return fakeResult{after: release}
	}

	e := newExecEnv(t, runner, execOpts{})
	e.registerMNP(t, mnpProps(2, 0, "0:"), &batch.RetryStrategy{Attempts: 3})
	job := e.submitMNP(t)

	e.waitCalls(t, 2)
	e.waitStatus(t, job.JobID+"#1", "FAILED")
	close(release)

	done := e.waitStatus(t, job.JobID, "SUCCEEDED")
	assert.Len(t, done.Attempts, 1, "child failure does not trigger a retry")

	child := e.describe(t, job.JobID+"#1")
	require.NotNil(t, child.Container.ExitCode)
	assert.Equal(t, int32(2), *child.Container.ExitCode)
}

func TestMNPRetryAppliesToWholeJob(t *testing.T) {
	t.Parallel()

	var once [3]sync.Once

	childUp := [3]chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	runner := &fakeRunner{}
	runner.behave = func(_ int, spec batch.ContainerSpec) fakeResult {
		attempt := 1
		if specEnv(spec)["AWS_BATCH_JOB_ATTEMPT"] == "2" {
			attempt = 2
		}

		if nodeIndexOf(spec) == "1" {
			once[attempt].Do(func() { close(childUp[attempt]) })

			return fakeResult{block: true}
		}

		if attempt == 1 {
			return fakeResult{exit: 1, after: childUp[attempt]}
		}

		return fakeResult{after: childUp[attempt]}
	}

	e := newExecEnv(t, runner, execOpts{})
	e.registerMNP(t, mnpProps(2, 0, "0:"), &batch.RetryStrategy{Attempts: 2})
	job := e.submitMNP(t)

	done := e.waitStatus(t, job.JobID, "SUCCEEDED")
	require.Len(t, done.Attempts, 2)
	assert.Equal(t, int32(1), *done.Attempts[0].Container.ExitCode)

	child := e.describe(t, job.JobID+"#1")
	assert.Len(t, child.Attempts, 2, "each parent attempt updates the child's attempts")
	assert.Equal(t, 4, runner.specCount())
}

func TestMNPNodeFailsToStart(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{}
	runner.behave = func(int, batch.ContainerSpec) fakeResult { return fakeResult{err: errFakeIssue} }

	e := newExecEnv(t, runner, execOpts{})
	e.registerMNP(t, mnpProps(3, 0, "0:"), nil)
	job := e.submitMNP(t)

	e.waitStatus(t, job.JobID, "FAILED")

	for _, idx := range []string{"#0", "#1", "#2"} {
		assert.Equal(t, "FAILED", e.waitStatus(t, job.JobID+idx, "FAILED").Status)
	}

	assert.Equal(t, 1, runner.specCount(), "children never start when the main node cannot")
}

func TestMNPSDKTerminateAndListing(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)
	multiNodeDef(t, env)

	runner := &fakeRunner{gate: make(chan struct{})}
	runner.behave = func(int, batch.ContainerSpec) fakeResult { return fakeResult{block: true} }
	env.bk.SetContainerRunner(runner)
	t.Cleanup(env.bk.StopExecution)

	id, err := env.submit(t, &batchsdk.SubmitJobInput{JobDefinition: aws.String("mnp")})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return runner.specCount() == 3 }, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		return env.describe(t, id).Status == types.JobStatusRunning &&
			env.describe(t, id+"#2").Status == types.JobStatusRunning
	}, 10*time.Second, 5*time.Millisecond)

	parent := env.describe(t, id)
	require.NotNil(t, parent.NodeDetails)
	assert.Equal(t, int32(0), aws.ToInt32(parent.NodeDetails.NodeIndex))
	assert.True(t, aws.ToBool(parent.NodeDetails.IsMainNode))
	assert.Nil(t, parent.Container)

	nodes, err := env.client.ListJobs(t.Context(), &batchsdk.ListJobsInput{
		MultiNodeJobId: aws.String(id), JobStatus: types.JobStatusRunning,
	})
	require.NoError(t, err)
	assert.Len(t, nodes.JobSummaryList, 3)

	child := env.describe(t, id+"#2")
	require.NotNil(t, child.NodeDetails)
	assert.Equal(t, int32(2), aws.ToInt32(child.NodeDetails.NodeIndex))
	assert.False(t, aws.ToBool(child.NodeDetails.IsMainNode))

	_, err = env.client.TerminateJob(t.Context(), &batchsdk.TerminateJobInput{
		JobId: aws.String(id), Reason: aws.String("stop"),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return runner.cancelled.Load() == 3 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, types.JobStatusFailed, env.describe(t, id).Status)

	require.Eventually(t, func() bool {
		return env.describe(t, id+"#1").Status == types.JobStatusFailed &&
			env.describe(t, id+"#0").Status == types.JobStatusFailed
	}, 10*time.Second, 5*time.Millisecond)
}

func TestMNPNoRunnerReportsNodeDetails(t *testing.T) {
	t.Parallel()

	env := newJobEnv(t)
	multiNodeDef(t, env)

	id, err := env.submit(t, &batchsdk.SubmitJobInput{JobDefinition: aws.String("mnp")})
	require.NoError(t, err)

	parent := env.describe(t, id)
	require.NotNil(t, parent.NodeDetails)
	assert.True(t, aws.ToBool(parent.NodeDetails.IsMainNode))

	child := env.describe(t, id+"#1")
	assert.False(t, aws.ToBool(child.NodeDetails.IsMainNode))
	assert.Empty(t, child.Attempts, "no fabricated attempts without an executor")
}

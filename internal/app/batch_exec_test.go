package app

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

type batchEnvRunner struct {
	env []string
	mu  sync.Mutex
}

func (r *batchEnvRunner) RunContainer(
	_ context.Context, spec batchbackend.ContainerSpec, hooks batchbackend.RunHooks,
) (int, error) {
	r.mu.Lock()
	r.env = append([]string(nil), spec.Env...)
	r.mu.Unlock()

	hooks.OnStarted()
	hooks.OnLog([]string{"hello from batch"})

	return 0, nil
}

func (r *batchEnvRunner) envMap() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := map[string]string{}

	for _, kv := range r.env {
		k, v, _ := strings.Cut(kv, "=")
		out[k] = v
	}

	return out
}

func TestBatchExecWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principal  string
		wantStatus string
	}{
		{name: "trusted_role", principal: "ecs-tasks.amazonaws.com", wantStatus: "SUCCEEDED"},
		{name: "untrusted_role", principal: "lambda.amazonaws.com", wantStatus: "FAILED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000", Region: "us-east-1", Port: ":8125"}
			cli.Lambda.DockerHost = "172.17.0.1"
			byName := newWiredServices(t, cli)

			iamBk := byName["IAM"].(*iambackend.Handler).Backend.(*iambackend.InMemoryBackend)
			stsBk := byName["STS"].(*stsbackend.Handler).Backend.(*stsbackend.InMemoryBackend)
			cwBk := byName["CloudWatchLogs"].(*cwlogsbackend.Handler).Backend.(*cwlogsbackend.InMemoryBackend)
			bk := byName["Batch"].(*batchbackend.Handler).Backend

			trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"Service":"` + tt.principal + `"},"Action":"sts:AssumeRole"}]}`
			_, err := iamBk.CreateRole("job-role", "/", trust, "")
			require.NoError(t, err)

			runner := &batchEnvRunner{}
			bk.SetContainerRunner(runner)
			t.Cleanup(bk.StopExecution)

			ctx := context.Background()
			_, err = bk.CreateJobQueue(ctx, "q", 1, "ENABLED", nil, nil, "", nil, "", nil)
			require.NoError(t, err)

			_, err = bk.RegisterJobDefinition(ctx, "jd", "container", nil, nil, 0, 0, &batchbackend.ContainerProperties{
				Image: "busybox", Vcpus: 1, Memory: 128, JobRoleArn: "arn:aws:iam::000000000000:role/job-role",
			}, nil, nil, nil, nil, nil, false, nil)
			require.NoError(t, err)

			job, err := bk.SubmitJob(ctx, "j", "q", "jd", nil, nil, nil, nil, nil, nil, nil, nil, "", 0, false)
			require.NoError(t, err)

			var done *batchbackend.Job

			require.Eventually(t, func() bool {
				done = bk.DescribeJobs(ctx, []string{job.JobID})[0]

				return done.Status == tt.wantStatus
			}, 10*time.Second, 5*time.Millisecond)

			if tt.wantStatus == "FAILED" {
				assert.Contains(t, done.StatusReason, "CannotStartContainerError")

				return
			}

			env := runner.envMap()
			assert.Equal(t, "http://172.17.0.1:8125", env["AWS_ENDPOINT_URL"])

			id, idErr := stsBk.GetCallerIdentity(env["AWS_ACCESS_KEY_ID"], env["AWS_SESSION_TOKEN"])
			require.NoError(t, idErr)
			assert.Contains(t, id.GetCallerIdentityResult.Arn, "assumed-role/job-role/AWSBatch-")

			events, _, _, getErr := cwBk.GetLogEvents(ctx, "/aws/batch/job", done.Container.LogStreamName,
				nil, nil, 10, "", true)
			require.NoError(t, getErr)
			require.Len(t, events, 1)
			assert.Equal(t, "hello from batch", events[0].Message)
		})
	}
}

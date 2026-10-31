package gcpbatch_test

import (
	"log/slog"
	"testing"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

const dockerImage = "alpine:3.20"

func dockerRuntime(t *testing.T) container.Runtime {
	t.Helper()

	if testing.Short() {
		t.Skip("docker-backed Batch tests are skipped in short mode")
	}

	rt, err := container.NewRuntime(container.Config{Logger: slog.Default()})
	if err != nil {
		t.Skipf("no container runtime: %v", err)
	}

	t.Cleanup(func() { _ = rt.Close() })

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container runtime is not reachable: %v", err)
	}

	if err = rt.PullImage(t.Context(), dockerImage); err != nil {
		t.Skipf("cannot pull %s: %v", dockerImage, err)
	}

	return rt
}

func TestDockerTwoTaskJob(t *testing.T) {
	t.Parallel()

	rt := dockerRuntime(t)
	sink := &fakeLogs{}
	e := newEnv(t, func(b *gcpbatch.InMemoryBackend) { b.SetRuntime(rt, dockerImage) },
		withServices(gcpbatch.Services{Logs: sink, Secrets: fakeSecrets{"projects/p/secrets/s/versions/1": "s3cret"}}))

	script := &batchpb.Runnable{Executable: &batchpb.Runnable_Script_{Script: &batchpb.Runnable_Script{
		Command: &batchpb.Runnable_Script_Text{
			Text: "#!/bin/sh\necho \"script $BATCH_TASK_INDEX/$BATCH_TASK_COUNT secret=${SECRET}\"\n",
		},
	}}}
	script.Environment = &batchpb.Environment{
		SecretVariables: map[string]string{"SECRET": "projects/p/secrets/s/versions/1"},
	}

	job := basicJob(2,
		containerRunnable(dockerImage, "sh", "-c", `echo "task $BATCH_TASK_INDEX of $BATCH_TASK_COUNT"`),
		script,
	)
	e.create(t, "docker", job)

	got := e.finish(t, "docker")
	require.Equal(t, batchpb.JobStatus_SUCCEEDED, got.GetStatus().GetState())
	assert.Equal(t, []string{"SUCCEEDED", "SUCCEEDED"}, taskStates(e.tasks(t, "docker")))

	assert.Equal(t, []string{
		"script 0/2 secret=s3cret", "script 1/2 secret=s3cret", "task 0 of 2", "task 1 of 2",
	}, sink.texts())
}

func TestDockerFailingTaskRetries(t *testing.T) {
	t.Parallel()

	rt := dockerRuntime(t)
	e := newEnv(t, func(b *gcpbatch.InMemoryBackend) { b.SetRuntime(rt, dockerImage) })

	job := basicJob(1, containerRunnable(dockerImage, "sh", "-c", "exit 7"))
	job.TaskGroups[0].TaskSpec.MaxRetryCount = 1
	e.create(t, "dockerfail", job)

	got := e.finish(t, "dockerfail")
	assert.Equal(t, batchpb.JobStatus_FAILED, got.GetStatus().GetState())

	evs := e.tasks(t, "dockerfail")[0].GetStatus().GetStatusEvents()
	assert.Equal(t, int32(7), evs[len(evs)-1].GetTaskExecution().GetExitCode())

	retries := 0

	for _, ev := range evs {
		if ev.GetType() == "TASK_RETRY" {
			retries++
		}
	}

	assert.Equal(t, 1, retries)
}

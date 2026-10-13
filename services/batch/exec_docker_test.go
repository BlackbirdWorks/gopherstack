package batch_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

const dockerTestImage = "alpine:3.21"

func TestExecRealDockerExitCodes(t *testing.T) {
	t.Parallel()

	probe, err := batch.NewDockerContainerRunner()
	require.NoError(t, err)

	t.Cleanup(func() { _ = probe.(io.Closer).Close() })

	probeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, probeErr := probe.RunContainer(probeCtx, batch.ContainerSpec{
		Image: dockerTestImage, Command: []string{"true"},
	}, batch.RunHooks{}); probeErr != nil {
		t.Skipf("docker or %s unavailable: %v", dockerTestImage, probeErr)
	}

	tests := []struct {
		name       string
		script     string
		wantStatus string
		wantLog    string
		wantExit   int32
	}{
		{
			name:       "exit zero",
			script:     "echo hello-from-job; exit 0",
			wantStatus: "SUCCEEDED",
			wantExit:   0,
			wantLog:    "hello-from-job",
		},
		{name: "exit three", script: "echo failing >&2; exit 3", wantStatus: "FAILED", wantExit: 3, wantLog: "failing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newExecEnv(t, &fakeRunner{}, execOpts{props: &batch.ContainerProperties{
				Image: dockerTestImage, Vcpus: 1, Memory: 64, Command: []string{"sh", "-c", tt.script},
			}})

			runner, runnerErr := batch.NewDockerContainerRunner()
			require.NoError(t, runnerErr)
			e.b.SetContainerRunner(runner)

			job := e.submit(t, submitOpts{})
			done := e.waitStatus(t, job.JobID, tt.wantStatus)

			require.NotNil(t, done.Container.ExitCode)
			assert.Equal(t, tt.wantExit, *done.Container.ExitCode)

			key := "us-east-1|/aws/batch/job|" + done.Container.LogStreamName
			assert.Eventually(t, func() bool {
				return len(e.sink.all()[key]) == 1 && e.sink.all()[key][0] == tt.wantLog
			}, 10*time.Second, 10*time.Millisecond)
		})
	}
}

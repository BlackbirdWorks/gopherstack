package batch_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestMNPRealDockerChildReachesMain(t *testing.T) {
	t.Parallel()

	runner, err := batch.NewDockerContainerRunner()
	require.NoError(t, err)

	t.Cleanup(func() { _ = runner.(io.Closer).Close() })

	probeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, probeErr := runner.RunContainer(probeCtx, batch.ContainerSpec{
		Image: dockerTestImage, Command: []string{"true"},
	}, batch.RunHooks{}); probeErr != nil {
		t.Skipf("docker or %s unavailable: %v", dockerTestImage, probeErr)
	}

	mainCmd := "nc -l -p 9000"
	childCmd := `for i in $(seq 1 60); do ` +
		`echo "ping-from-child-$AWS_BATCH_JOB_NODE_INDEX" | ` +
		`nc -w 2 "$AWS_BATCH_JOB_MAIN_NODE_PRIVATE_IPV4_ADDRESS" 9000 && exit 0; ` +
		`sleep 1; done; exit 1`

	np := &batch.NodeProperties{NumNodes: 2, MainNode: 0, NodeRangeProperties: []batch.NodeRangeProperty{
		{TargetNodes: "0:0", ContainerProperties: &batch.ContainerProperties{
			Image: dockerTestImage, Vcpus: 1, Memory: 64, Command: []string{"sh", "-c", mainCmd},
		}},
		{TargetNodes: "1:1", ContainerProperties: &batch.ContainerProperties{
			Image: dockerTestImage, Vcpus: 1, Memory: 64, Command: []string{"sh", "-c", childCmd},
		}},
	}}

	e := newExecEnv(t, &fakeRunner{}, execOpts{})
	e.b.SetContainerRunner(runner)
	e.registerMNP(t, np, nil)

	job := e.submitMNP(t)
	done := e.waitStatus(t, job.JobID, "SUCCEEDED")
	require.Len(t, done.Attempts, 1)

	main := e.describe(t, job.JobID+"#0")
	key := "us-east-1|/aws/batch/job|" + main.Container.LogStreamName

	assert.Eventually(t, func() bool {
		return strings.Contains(strings.Join(e.sink.all()[key], "\n"), "ping-from-child-1")
	}, 10*time.Second, 10*time.Millisecond, "main node received the child's message over the job network")
}

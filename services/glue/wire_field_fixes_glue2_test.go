package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestBatchStopJobRun_ReportsSuccessfulSubmissions drives BatchStopJobRun
// through the real client with one stoppable run and one already-finished
// run. BatchStopJobRunOutput.SuccessfulSubmissions
// (glue@v1.152.0 api_op_BatchStopJobRun.go) is the designated place to report
// which run IDs were actually accepted for stopping; before the fix the wire
// output type had no such field at all, so a client had no way to tell which
// of its requested run IDs actually stopped versus merely not-erroring on
// the other one, even though the Errors half of the same response worked.
func TestBatchStopJobRun_ReportsSuccessfulSubmissions(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("job1"),
		Role:    aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	runOut, err := client.StartJobRun(ctx, &gluesdk.StartJobRunInput{JobName: aws.String("job1")})
	require.NoError(t, err)
	runID := *runOut.JobRunId

	out, err := client.BatchStopJobRun(ctx, &gluesdk.BatchStopJobRunInput{
		JobName:   aws.String("job1"),
		JobRunIds: []string{runID, "no-such-run"},
	})
	require.NoError(t, err)

	require.Len(t, out.SuccessfulSubmissions, 1, "the real run should be reported as successfully submitted to stop")
	require.Equal(t, runID, *out.SuccessfulSubmissions[0].JobRunId)
	require.Equal(t, "job1", *out.SuccessfulSubmissions[0].JobName)

	require.Len(t, out.Errors, 1, "the unknown run should still be reported as an error")
	require.Equal(t, "no-such-run", *out.Errors[0].JobRunId)
}

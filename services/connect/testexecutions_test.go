package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *contactEnv) publishedTestCase(t *testing.T, name string) string {
	t.Helper()

	return aws.ToString(e.newTestCase(t, name, func(in *sdk.CreateTestCaseInput) {
		in.Status = types.TestCaseStatusPublished
	}).TestCaseId)
}

func TestTestCaseExecutions(t *testing.T) {
	t.Parallel()

	e := newContactEnv(t)
	inst := aws.String(e.inst)
	tc := e.publishedTestCase(t, "login")
	saved := aws.ToString(e.newTestCase(t, "draft", nil).TestCaseId)

	_, err := e.f.client.StartTestCaseExecution(
		t.Context(), &sdk.StartTestCaseExecutionInput{InstanceId: inst, TestCaseId: aws.String(saved)})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "unpublished")

	_, err = e.f.client.StartTestCaseExecution(
		t.Context(), &sdk.StartTestCaseExecutionInput{InstanceId: inst, TestCaseId: aws.String("nope")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	started, err := e.f.client.StartTestCaseExecution(
		t.Context(), &sdk.StartTestCaseExecutionInput{InstanceId: inst, TestCaseId: aws.String(tc)})
	require.NoError(t, err)
	assert.Equal(t, types.TestCaseExecutionStatusInitiated, started.Status)
	assert.Equal(t, tc, aws.ToString(started.TestCaseId))
	exec := started.TestCaseExecutionId

	e.f.clock.advance(60e9)

	list, err := e.f.client.ListTestCaseExecutions(t.Context(), &sdk.ListTestCaseExecutionsInput{
		InstanceId: inst, TestCaseName: aws.String("LOGIN"),
	})
	require.NoError(t, err)
	require.Len(t, list.TestCaseExecutions, 1)
	assert.Equal(t, map[string]string{"suite": "smoke"}, list.TestCaseExecutions[0].Tags)

	other, err := e.f.client.ListTestCaseExecutions(t.Context(), &sdk.ListTestCaseExecutionsInput{
		InstanceId: inst, Status: types.TestCaseExecutionStatusPassed,
	})
	require.NoError(t, err)
	assert.Empty(t, other.TestCaseExecutions)

	ref := func() (*string, *string) { return aws.String(tc), exec }
	tid, eid := ref()

	_, err = e.f.client.StopTestCaseExecution(t.Context(), &sdk.StopTestCaseExecutionInput{
		InstanceId: inst, TestCaseId: aws.String(saved), TestCaseExecutionId: eid,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "wrong test case")

	_, err = e.f.client.StopTestCaseExecution(t.Context(), &sdk.StopTestCaseExecutionInput{
		InstanceId: inst, TestCaseId: tid, TestCaseExecutionId: eid,
	})
	require.NoError(t, err)

	_, err = e.f.client.StopTestCaseExecution(t.Context(), &sdk.StopTestCaseExecutionInput{
		InstanceId: inst, TestCaseId: tid, TestCaseExecutionId: eid,
	})
	assert.Equal(t, "InvalidRequestException", apiCode(err), "already stopped")

	sum, err := e.f.client.GetTestCaseExecutionSummary(t.Context(), &sdk.GetTestCaseExecutionSummaryInput{
		InstanceId: inst, TestCaseId: tid, TestCaseExecutionId: eid,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TestCaseExecutionStatusStopped, sum.Status)
	assert.InDelta(t, 60.0, sum.EndTime.Sub(*sum.StartTime).Seconds(), 0.001)

	recs, err := e.f.client.ListTestCaseExecutionRecords(t.Context(), &sdk.ListTestCaseExecutionRecordsInput{
		InstanceId: inst, TestCaseId: tid, TestCaseExecutionId: eid,
	})
	require.NoError(t, err)
	require.Len(t, recs.ExecutionRecords, 1)
	assert.Equal(t, types.ExecutionRecordStatusStopped, recs.ExecutionRecords[0].Status)

	_, err = e.f.client.GetTestCaseExecutionSummary(t.Context(), &sdk.GetTestCaseExecutionSummaryInput{
		InstanceId: inst, TestCaseId: tid, TestCaseExecutionId: aws.String("nope"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

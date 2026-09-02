package swf_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestListOpenWorkflowExecutions_ReverseOrder_SDKRoundTrip proves
// ListOpenWorkflowExecutionsInput.ReverseOrder actually governs result
// order through the real SDK client. Confirmed against
// aws-sdk-go-v2/service/swf@v1.37.4's api_op_ListOpenWorkflowExecutions.go
// doc comment on ReverseOrder: "By default the results are returned in
// descending order of the start time of the executions" -- before this fix,
// handleListOpenWorkflowExecutionsInput had no ReverseOrder field at all
// (silently dropped off the wire) and results came back in whatever order
// the backend's secondary index happened to hold them (insertion order),
// not sorted by start time in either direction.
func TestListOpenWorkflowExecutions_ReverseOrder_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	client := newTestSWFSDKClient(t, swf.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
		Name:                                   aws.String("list-open-order-dom"),
		WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:                              aws.String("list-open-order-dom"),
		Name:                                aws.String("orderWF"),
		Version:                             aws.String("1.0"),
		DefaultTaskList:                     &swftypes.TaskList{Name: aws.String("tl")},
		DefaultTaskStartToCloseTimeout:      aws.String("NONE"),
		DefaultExecutionStartToCloseTimeout: aws.String("3600"),
		DefaultChildPolicy:                  swftypes.ChildPolicyTerminate,
	})
	require.NoError(t, err)

	workflowIDs := []string{"wf-open-a", "wf-open-b", "wf-open-c"}
	for _, id := range workflowIDs {
		_, startErr := client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
			Domain:       aws.String("list-open-order-dom"),
			WorkflowId:   aws.String(id),
			WorkflowType: &swftypes.WorkflowType{Name: aws.String("orderWF"), Version: aws.String("1.0")},
		})
		require.NoError(t, startErr)
		// StartTimestamp has millisecond resolution (models.go milliDivisor);
		// separate calls so each execution gets a distinct, orderable value.
		time.Sleep(3 * time.Millisecond)
	}

	oldest := time.Now().Add(-time.Hour)
	descending, err := client.ListOpenWorkflowExecutions(ctx, &swfsdk.ListOpenWorkflowExecutionsInput{
		Domain:          aws.String("list-open-order-dom"),
		StartTimeFilter: &swftypes.ExecutionTimeFilter{OldestDate: aws.Time(oldest)},
	})
	require.NoError(t, err)
	require.Len(t, descending.ExecutionInfos, 3)
	require.Equal(
		t,
		[]string{"wf-open-c", "wf-open-b", "wf-open-a"},
		workflowIDsOf(descending.ExecutionInfos),
		"default order must be descending start time (most recently started first)",
	)

	ascending, err := client.ListOpenWorkflowExecutions(ctx, &swfsdk.ListOpenWorkflowExecutionsInput{
		Domain:          aws.String("list-open-order-dom"),
		StartTimeFilter: &swftypes.ExecutionTimeFilter{OldestDate: aws.Time(oldest)},
		ReverseOrder:    true,
	})
	require.NoError(t, err)
	require.Len(t, ascending.ExecutionInfos, 3)
	require.Equal(
		t,
		[]string{"wf-open-a", "wf-open-b", "wf-open-c"},
		workflowIDsOf(ascending.ExecutionInfos),
		"reverseOrder=true must flip to ascending start time (oldest first)",
	)
}

// TestListClosedWorkflowExecutions_ReverseOrder_SDKRoundTrip proves the same
// ReverseOrder wiring for ListClosedWorkflowExecutions, ordered by close
// time when closeTimeFilter (not startTimeFilter) selects the results --
// confirmed against api_op_ListClosedWorkflowExecutions.go's ReverseOrder
// doc comment ("descending order of the start or the close time") and
// CloseTimeFilter's own doc ("the returned results are ordered by their
// close times").
func TestListClosedWorkflowExecutions_ReverseOrder_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	client := newTestSWFSDKClient(t, swf.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
		Name:                                   aws.String("list-closed-order-dom"),
		WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:                              aws.String("list-closed-order-dom"),
		Name:                                aws.String("orderWF"),
		Version:                             aws.String("1.0"),
		DefaultTaskList:                     &swftypes.TaskList{Name: aws.String("tl")},
		DefaultTaskStartToCloseTimeout:      aws.String("NONE"),
		DefaultExecutionStartToCloseTimeout: aws.String("3600"),
		DefaultChildPolicy:                  swftypes.ChildPolicyTerminate,
	})
	require.NoError(t, err)

	workflowIDs := []string{"wf-closed-a", "wf-closed-b", "wf-closed-c"}
	for _, id := range workflowIDs {
		_, startErr := client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
			Domain:       aws.String("list-closed-order-dom"),
			WorkflowId:   aws.String(id),
			WorkflowType: &swftypes.WorkflowType{Name: aws.String("orderWF"), Version: aws.String("1.0")},
		})
		require.NoError(t, startErr)
	}

	// Close them out of start order (b, then a, then c) so a test that
	// accidentally sorted by StartTimestamp instead of CloseTimestamp would
	// produce a different, detectably wrong sequence.
	for _, id := range []string{"wf-closed-b", "wf-closed-a", "wf-closed-c"} {
		_, termErr := client.TerminateWorkflowExecution(ctx, &swfsdk.TerminateWorkflowExecutionInput{
			Domain:     aws.String("list-closed-order-dom"),
			WorkflowId: aws.String(id),
		})
		require.NoError(t, termErr)
		time.Sleep(3 * time.Millisecond)
	}

	oldest := time.Now().Add(-time.Hour)
	descending, err := client.ListClosedWorkflowExecutions(ctx, &swfsdk.ListClosedWorkflowExecutionsInput{
		Domain:          aws.String("list-closed-order-dom"),
		CloseTimeFilter: &swftypes.ExecutionTimeFilter{OldestDate: aws.Time(oldest)},
	})
	require.NoError(t, err)
	require.Len(t, descending.ExecutionInfos, 3)
	require.Equal(
		t,
		[]string{"wf-closed-c", "wf-closed-a", "wf-closed-b"},
		workflowIDsOf(descending.ExecutionInfos),
		"default order must be descending close time (most recently closed first)",
	)

	ascending, err := client.ListClosedWorkflowExecutions(ctx, &swfsdk.ListClosedWorkflowExecutionsInput{
		Domain:          aws.String("list-closed-order-dom"),
		CloseTimeFilter: &swftypes.ExecutionTimeFilter{OldestDate: aws.Time(oldest)},
		ReverseOrder:    true,
	})
	require.NoError(t, err)
	require.Len(t, ascending.ExecutionInfos, 3)
	require.Equal(
		t,
		[]string{"wf-closed-b", "wf-closed-a", "wf-closed-c"},
		workflowIDsOf(ascending.ExecutionInfos),
		"reverseOrder=true must flip to ascending close time (oldest-closed first)",
	)
}

func workflowIDsOf(infos []swftypes.WorkflowExecutionInfo) []string {
	ids := make([]string, len(infos))
	for i, info := range infos {
		ids[i] = aws.ToString(info.Execution.WorkflowId)
	}

	return ids
}

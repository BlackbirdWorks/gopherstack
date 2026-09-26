package swf_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for swf's five flagged List ops: ListDomains,
// ListActivityTypes, ListWorkflowTypes, ListOpenWorkflowExecutions and
// ListClosedWorkflowExecutions all already emitted exactly their real
// Info-summary member sets (verified via cmd/structfielddiff against
// swf@v1.37.4) -- no leaks, no gaps. ListOpenWorkflowExecutions and
// ListClosedWorkflowExecutions share one executionInfoOutput builder, which
// matches types.WorkflowExecutionInfo (the same type real AWS's own
// ListOpenWorkflowExecutionsOutput/ListClosedWorkflowExecutionsOutput both
// use for ExecutionInfos).
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("domains exact", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("d1"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		out, err := client.ListDomains(ctx, &swfsdk.ListDomainsInput{
			RegistrationStatus: swftypes.RegistrationStatusRegistered,
		})
		require.NoError(t, err)
		require.Len(t, out.DomainInfos, 1)
		d := out.DomainInfos[0]
		assert.Equal(t, "d1", aws.ToString(d.Name))
		assert.Equal(t, swftypes.RegistrationStatusRegistered, d.Status)
	})

	t.Run("activity types exact", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("d1"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		_, err = client.RegisterActivityType(ctx, &swfsdk.RegisterActivityTypeInput{
			Domain:  aws.String("d1"),
			Name:    aws.String("act1"),
			Version: aws.String("1.0"),
		})
		require.NoError(t, err)

		out, err := client.ListActivityTypes(ctx, &swfsdk.ListActivityTypesInput{
			Domain:             aws.String("d1"),
			RegistrationStatus: swftypes.RegistrationStatusRegistered,
		})
		require.NoError(t, err)
		require.Len(t, out.TypeInfos, 1)
		info := out.TypeInfos[0]
		require.NotNil(t, info.ActivityType)
		assert.Equal(t, "act1", aws.ToString(info.ActivityType.Name))
		assert.Equal(t, "1.0", aws.ToString(info.ActivityType.Version))
		assert.NotNil(t, info.CreationDate)
	})

	t.Run("workflow types exact", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("d1"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
			Domain:  aws.String("d1"),
			Name:    aws.String("wf1"),
			Version: aws.String("1.0"),
		})
		require.NoError(t, err)

		out, err := client.ListWorkflowTypes(ctx, &swfsdk.ListWorkflowTypesInput{
			Domain:             aws.String("d1"),
			RegistrationStatus: swftypes.RegistrationStatusRegistered,
		})
		require.NoError(t, err)
		require.Len(t, out.TypeInfos, 1)
		info := out.TypeInfos[0]
		require.NotNil(t, info.WorkflowType)
		assert.Equal(t, "wf1", aws.ToString(info.WorkflowType.Name))
		assert.NotNil(t, info.CreationDate)
	})

	t.Run("open and closed workflow executions exact", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("d1"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
			Domain:                              aws.String("d1"),
			Name:                                aws.String("wf1"),
			Version:                             aws.String("1.0"),
			DefaultTaskList:                     &swftypes.TaskList{Name: aws.String("tl")},
			DefaultTaskStartToCloseTimeout:      aws.String("NONE"),
			DefaultExecutionStartToCloseTimeout: aws.String("60"),
			DefaultChildPolicy:                  swftypes.ChildPolicyTerminate,
		})
		require.NoError(t, err)

		_, err = client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
			Domain:       aws.String("d1"),
			WorkflowId:   aws.String("wf-exec-1"),
			WorkflowType: &swftypes.WorkflowType{Name: aws.String("wf1"), Version: aws.String("1.0")},
			TagList:      []string{"tag1"},
		})
		require.NoError(t, err)

		openOut, err := client.ListOpenWorkflowExecutions(ctx, &swfsdk.ListOpenWorkflowExecutionsInput{
			Domain:          aws.String("d1"),
			StartTimeFilter: &swftypes.ExecutionTimeFilter{OldestDate: aws.Time(time.Now().Add(-time.Hour))},
		})
		require.NoError(t, err)
		require.Len(t, openOut.ExecutionInfos, 1)
		oe := openOut.ExecutionInfos[0]
		assert.Equal(t, swftypes.ExecutionStatusOpen, oe.ExecutionStatus)
		require.NotNil(t, oe.Execution)
		assert.Equal(t, "wf-exec-1", aws.ToString(oe.Execution.WorkflowId))
		assert.Contains(t, oe.TagList, "tag1")

		_, err = client.TerminateWorkflowExecution(ctx, &swfsdk.TerminateWorkflowExecutionInput{
			Domain:     aws.String("d1"),
			WorkflowId: aws.String("wf-exec-1"),
		})
		require.NoError(t, err)

		closedOut, err := client.ListClosedWorkflowExecutions(ctx, &swfsdk.ListClosedWorkflowExecutionsInput{
			Domain:          aws.String("d1"),
			StartTimeFilter: &swftypes.ExecutionTimeFilter{OldestDate: aws.Time(time.Now().Add(-time.Hour))},
		})
		require.NoError(t, err)
		require.Len(t, closedOut.ExecutionInfos, 1)
		ce := closedOut.ExecutionInfos[0]
		assert.Equal(t, swftypes.ExecutionStatusClosed, ce.ExecutionStatus)
		assert.Equal(t, swftypes.CloseStatusTerminated, ce.CloseStatus)
		assert.NotNil(t, ce.CloseTimestamp)
	})
}

package swf_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/swf"
)

const swfRTRegion = "us-east-1"

// newTestSWFSDKClient stands up the real aws-sdk-go-v2 swf client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production -- so a fix is verified by
// the real client's own deserializer, not gopherstack's own JSON tags.
func newTestSWFSDKClient(t *testing.T, h *swf.Handler) *swfsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(swfRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return swfsdk.NewFromConfig(cfg, func(o *swfsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// findHistoryEvent returns the first event of the given type, or nil.
func findHistoryEvent(events []swftypes.HistoryEvent, eventType swftypes.EventType) *swftypes.HistoryEvent {
	for i := range events {
		if events[i].EventType == eventType {
			return &events[i]
		}
	}

	return nil
}

// TestRequestCancelWorkflowExecution_CancelRequestedCause_SDKRoundTrip proves
// a direct, operator-initiated RequestCancelWorkflowExecution call leaves
// WorkflowExecutionCancelRequestedEventAttributes.Cause unset through the
// real SDK client. Confirmed against aws-sdk-go-v2/service/swf@v1.37.4's
// types.WorkflowExecutionCancelRequestedCause enum (types/enums.go), which
// defines exactly one value -- CHILD_POLICY_APPLIED, reserved for the
// automatic child-policy cascade -- not "OPERATOR_INITIATED", which
// gopherstack previously stamped on this event for a direct call. Since
// Cause is a bare string type (not smithy-enum-validated on decode), the
// old value silently decoded as a nonexistent enum value rather than
// failing the call -- this test pins the fix by asserting the real client
// sees no cause at all, matching real AWS's behavior for a direct call.
func TestRequestCancelWorkflowExecution_CancelRequestedCause_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	client := newTestSWFSDKClient(t, swf.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
		Name:                                   aws.String("cancel-cause-dom"),
		WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:                              aws.String("cancel-cause-dom"),
		Name:                                aws.String("cancelCauseWF"),
		Version:                             aws.String("1.0"),
		DefaultTaskList:                     &swftypes.TaskList{Name: aws.String("tl")},
		DefaultTaskStartToCloseTimeout:      aws.String("NONE"),
		DefaultExecutionStartToCloseTimeout: aws.String("60"),
		DefaultChildPolicy:                  swftypes.ChildPolicyTerminate,
	})
	require.NoError(t, err)

	_, err = client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
		Domain:       aws.String("cancel-cause-dom"),
		WorkflowId:   aws.String("wf-cancel-cause"),
		WorkflowType: &swftypes.WorkflowType{Name: aws.String("cancelCauseWF"), Version: aws.String("1.0")},
	})
	require.NoError(t, err)

	_, err = client.RequestCancelWorkflowExecution(ctx, &swfsdk.RequestCancelWorkflowExecutionInput{
		Domain:     aws.String("cancel-cause-dom"),
		WorkflowId: aws.String("wf-cancel-cause"),
	})
	require.NoError(t, err)

	hist, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
		Domain: aws.String("cancel-cause-dom"),
		Execution: &swftypes.WorkflowExecution{
			WorkflowId: aws.String("wf-cancel-cause"),
			RunId:      aws.String(""),
		},
	})
	require.NoError(t, err)

	ev := findHistoryEvent(hist.Events, swftypes.EventTypeWorkflowExecutionCancelRequested)
	require.NotNil(t, ev, "expected a WorkflowExecutionCancelRequested history event")
	require.NotNil(t, ev.WorkflowExecutionCancelRequestedEventAttributes)
	assert.Empty(
		t,
		ev.WorkflowExecutionCancelRequestedEventAttributes.Cause,
		"a direct call must leave Cause unset -- OPERATOR_INITIATED is not a value the real enum defines",
	)
}

// TestChildWorkflowExecutionTimedOut_TimeoutType_SDKRoundTrip proves
// ChildWorkflowExecutionTimedOutEventAttributes.TimeoutType decodes through
// the real SDK client on the parent's history when a child execution times
// out. Confirmed against aws-sdk-go-v2/service/swf@v1.37.4's
// types.ChildWorkflowExecutionTimedOutEventAttributes (types/types.go),
// which declares TimeoutType a required member -- before the fix,
// propagateChildClosureLocked's base attrs carried no timeoutType at all for
// this event (timeout_sweep.go passed nil extra), so a real typed client
// would see a zero-value "" TimeoutType instead of "START_TO_CLOSE".
//
// The child's own ExecutionStartToCloseTimeout is registered as "0" so it is
// already expired the instant it starts (deadline == StartTimestamp); no
// sleep or fabricated clock is needed since the very next lock-holding
// backend call (GetWorkflowExecutionHistory on the parent) lazily sweeps
// every execution, including the child, with the real wall clock.
func TestChildWorkflowExecutionTimedOut_TimeoutType_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	client := newTestSWFSDKClient(t, swf.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
		Name:                                   aws.String("child-timeout-dom"),
		WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:                              aws.String("child-timeout-dom"),
		Name:                                aws.String("parentWF"),
		Version:                             aws.String("1.0"),
		DefaultTaskList:                     &swftypes.TaskList{Name: aws.String("parent-tl")},
		DefaultTaskStartToCloseTimeout:      aws.String("NONE"),
		DefaultExecutionStartToCloseTimeout: aws.String("3600"),
		DefaultChildPolicy:                  swftypes.ChildPolicyTerminate,
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:                              aws.String("child-timeout-dom"),
		Name:                                aws.String("childWF"),
		Version:                             aws.String("1.0"),
		DefaultTaskList:                     &swftypes.TaskList{Name: aws.String("child-tl")},
		DefaultTaskStartToCloseTimeout:      aws.String("NONE"),
		DefaultExecutionStartToCloseTimeout: aws.String("0"),
		DefaultChildPolicy:                  swftypes.ChildPolicyTerminate,
	})
	require.NoError(t, err)

	_, err = client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
		Domain:       aws.String("child-timeout-dom"),
		WorkflowId:   aws.String("wf-parent"),
		WorkflowType: &swftypes.WorkflowType{Name: aws.String("parentWF"), Version: aws.String("1.0")},
	})
	require.NoError(t, err)

	parentTask, err := client.PollForDecisionTask(ctx, &swfsdk.PollForDecisionTaskInput{
		Domain:   aws.String("child-timeout-dom"),
		TaskList: &swftypes.TaskList{Name: aws.String("parent-tl")},
	})
	require.NoError(t, err)
	require.NotNil(t, parentTask.TaskToken)

	_, err = client.RespondDecisionTaskCompleted(ctx, &swfsdk.RespondDecisionTaskCompletedInput{
		TaskToken: parentTask.TaskToken,
		Decisions: []swftypes.Decision{{
			DecisionType: swftypes.DecisionTypeStartChildWorkflowExecution,
			StartChildWorkflowExecutionDecisionAttributes: &swftypes.StartChildWorkflowExecutionDecisionAttributes{
				WorkflowId:   aws.String("wf-child"),
				WorkflowType: &swftypes.WorkflowType{Name: aws.String("childWF"), Version: aws.String("1.0")},
			},
		}},
	})
	require.NoError(t, err)

	// The child now exists with an already-elapsed deadline; this call's own
	// lazy sweep (over ALL executions, not just the parent's) closes it as
	// TIMED_OUT and propagates ChildWorkflowExecutionTimedOut onto the
	// parent's history before returning it.
	hist, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
		Domain: aws.String("child-timeout-dom"),
		Execution: &swftypes.WorkflowExecution{
			WorkflowId: aws.String("wf-parent"),
			RunId:      aws.String(""),
		},
	})
	require.NoError(t, err)

	ev := findHistoryEvent(hist.Events, swftypes.EventTypeChildWorkflowExecutionTimedOut)
	require.NotNil(t, ev, "expected a ChildWorkflowExecutionTimedOut history event on the parent")
	require.NotNil(t, ev.ChildWorkflowExecutionTimedOutEventAttributes)
	assert.Equal(
		t,
		swftypes.WorkflowExecutionTimeoutTypeStartToClose,
		ev.ChildWorkflowExecutionTimedOutEventAttributes.TimeoutType,
		"TimeoutType is a required member of ChildWorkflowExecutionTimedOutEventAttributes",
	)
}

// TestDeprecationDate_SDKRoundTrip proves ActivityTypeInfo/WorkflowTypeInfo's
// DeprecationDate is nil while REGISTERED and populated with a real epoch
// timestamp once DeprecateActivityType/DeprecateWorkflowType is called.
// Confirmed against aws-sdk-go-v2/service/swf@v1.37.4's types.go
// ("If DEPRECATED, the date and time Deprecate* was called") and
// deserializers.go's "deprecationDate" epoch-seconds case for both types --
// before this fix, ActivityType/WorkflowType had no field to hold this at
// all, so a real client always saw nil regardless of deprecation status.
func TestDeprecationDate_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("activity_type", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("deprecation-date-dom"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		_, err = client.RegisterActivityType(ctx, &swfsdk.RegisterActivityTypeInput{
			Domain:  aws.String("deprecation-date-dom"),
			Name:    aws.String("depActivity"),
			Version: aws.String("1.0"),
		})
		require.NoError(t, err)

		before, err := client.DescribeActivityType(ctx, &swfsdk.DescribeActivityTypeInput{
			Domain:       aws.String("deprecation-date-dom"),
			ActivityType: &swftypes.ActivityType{Name: aws.String("depActivity"), Version: aws.String("1.0")},
		})
		require.NoError(t, err)
		assert.Nil(t, before.TypeInfo.DeprecationDate, "must be unset while REGISTERED")

		_, err = client.DeprecateActivityType(ctx, &swfsdk.DeprecateActivityTypeInput{
			Domain:       aws.String("deprecation-date-dom"),
			ActivityType: &swftypes.ActivityType{Name: aws.String("depActivity"), Version: aws.String("1.0")},
		})
		require.NoError(t, err)

		after, err := client.DescribeActivityType(ctx, &swfsdk.DescribeActivityTypeInput{
			Domain:       aws.String("deprecation-date-dom"),
			ActivityType: &swftypes.ActivityType{Name: aws.String("depActivity"), Version: aws.String("1.0")},
		})
		require.NoError(t, err)
		require.NotNil(t, after.TypeInfo.DeprecationDate, "must be set once DEPRECATED")
		assert.False(t, after.TypeInfo.DeprecationDate.IsZero())
	})

	t.Run("workflow_type", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("deprecation-date-wf-dom"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
			Domain:  aws.String("deprecation-date-wf-dom"),
			Name:    aws.String("depWorkflow"),
			Version: aws.String("1.0"),
		})
		require.NoError(t, err)

		before, err := client.DescribeWorkflowType(ctx, &swfsdk.DescribeWorkflowTypeInput{
			Domain:       aws.String("deprecation-date-wf-dom"),
			WorkflowType: &swftypes.WorkflowType{Name: aws.String("depWorkflow"), Version: aws.String("1.0")},
		})
		require.NoError(t, err)
		assert.Nil(t, before.TypeInfo.DeprecationDate, "must be unset while REGISTERED")

		_, err = client.DeprecateWorkflowType(ctx, &swfsdk.DeprecateWorkflowTypeInput{
			Domain:       aws.String("deprecation-date-wf-dom"),
			WorkflowType: &swftypes.WorkflowType{Name: aws.String("depWorkflow"), Version: aws.String("1.0")},
		})
		require.NoError(t, err)

		after, err := client.DescribeWorkflowType(ctx, &swfsdk.DescribeWorkflowTypeInput{
			Domain:       aws.String("deprecation-date-wf-dom"),
			WorkflowType: &swftypes.WorkflowType{Name: aws.String("depWorkflow"), Version: aws.String("1.0")},
		})
		require.NoError(t, err)
		require.NotNil(t, after.TypeInfo.DeprecationDate, "must be set once DEPRECATED")
		assert.False(t, after.TypeInfo.DeprecationDate.IsZero())
	})
}

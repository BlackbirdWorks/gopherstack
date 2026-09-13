package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestReqFieldSlice1_SSM_RealClient proves the gopherstack-xhu2t slice 1
// fixes for ssm's tier-1 reqfielddiff findings: DescribeAutomationStepExecutions.
// ReverseOrder, DescribeParameters.Shared, GetDeployablePatchSnapshotForInstance.
// UseS3DualStackEndpoint, and RegisterTaskWithMaintenanceWindow/
// UpdateMaintenanceWindowTask.CutoffBehavior. Each subtest drives the real
// aws-sdk-go-v2 client and asserts an observable effect.
//
// The other 45 of ssm's 52 tier-1 findings are false positives: reqfielddiff
// cannot resolve the decode target through this service's jsonOp[I, O]
// generic dispatch shape (handler.go: every op is registered as
// jsonOp(h.Backend.<Op>), so the wire struct is the backend method's own
// second parameter type, not a handler-local struct near the registration
// site) -- confirmed by hand against every one of the other 47 findings
// (see gopherstack-xhu2t slice 1 note in PARITY.md), and by this file's own
// declare-everything experiment: re-running reqfielddiff after declaring
// every field here still reports all 52, proving the tool is blind to this
// shape entirely, not just slow to notice a declaration.
func TestReqFieldSlice1_SSM_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testDescribeAutomationStepExecutionsReverseOrder, "describe_automation_step_executions_reverse_order"},
		{testDescribeParametersShared, "describe_parameters_shared"},
		{
			testGetDeployablePatchSnapshotUseS3DualStackEndpoint,
			"get_deployable_patch_snapshot_use_s3_dual_stack_endpoint",
		},
		{testMaintenanceWindowTaskCutoffBehavior, "maintenance_window_task_cutoff_behavior"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newReqFieldSlice1SSMClient(t *testing.T) *ssmsdk.Client {
	t.Helper()

	backend := ssm.NewInMemoryBackend()

	return newTestSSMClient(t, ssm.NewHandler(backend))
}

func testDescribeAutomationStepExecutionsReverseOrder(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1SSMClient(t)
	ctx := t.Context()

	content := `{"schemaVersion":"2.2","mainSteps":[` +
		`{"action":"aws:runShellScript","name":"step1"},` +
		`{"action":"aws:runShellScript","name":"step2"}]}`

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:           aws.String("rev-doc"),
		Content:        aws.String(content),
		DocumentType:   ssmtypes.DocumentTypeAutomation,
		DocumentFormat: ssmtypes.DocumentFormatJson,
	})
	require.NoError(t, err)

	start, err := client.StartAutomationExecution(ctx, &ssmsdk.StartAutomationExecutionInput{
		DocumentName: aws.String("rev-doc"),
	})
	require.NoError(t, err)

	forward, err := client.DescribeAutomationStepExecutions(ctx, &ssmsdk.DescribeAutomationStepExecutionsInput{
		AutomationExecutionId: start.AutomationExecutionId,
	})
	require.NoError(t, err)
	require.Len(t, forward.StepExecutions, 2, "expected two mainSteps materialized")

	reversed, err := client.DescribeAutomationStepExecutions(ctx, &ssmsdk.DescribeAutomationStepExecutionsInput{
		AutomationExecutionId: start.AutomationExecutionId,
		ReverseOrder:          aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, reversed.StepExecutions, 2)

	assert.Equal(t, forward.StepExecutions[0].StepName, reversed.StepExecutions[1].StepName)
	assert.Equal(t, forward.StepExecutions[1].StepName, reversed.StepExecutions[0].StepName)
}

func testDescribeParametersShared(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1SSMClient(t)
	ctx := t.Context()

	_, err := client.PutParameter(ctx, &ssmsdk.PutParameterInput{
		Name:  aws.String("/shared-test/p1"),
		Type:  ssmtypes.ParameterTypeString,
		Value: aws.String("v1"),
	})
	require.NoError(t, err)

	unshared, err := client.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{})
	require.NoError(t, err)
	assert.Len(t, unshared.Parameters, 1)

	shared, err := client.DescribeParameters(ctx, &ssmsdk.DescribeParametersInput{Shared: aws.Bool(true)})
	require.NoError(t, err)
	assert.Empty(t, shared.Parameters, "no parameter is ever shared cross-account in this backend")
}

func testGetDeployablePatchSnapshotUseS3DualStackEndpoint(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1SSMClient(t)
	ctx := t.Context()

	regularInput := &ssmsdk.GetDeployablePatchSnapshotForInstanceInput{
		InstanceId: aws.String("i-abc123"),
		SnapshotId: aws.String("snap-1"),
	}

	regular, err := client.GetDeployablePatchSnapshotForInstance(ctx, regularInput)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(regular.SnapshotDownloadUrl), ".s3.")
	assert.NotContains(t, aws.ToString(regular.SnapshotDownloadUrl), "dualstack")

	dualStackInput := &ssmsdk.GetDeployablePatchSnapshotForInstanceInput{
		InstanceId:             aws.String("i-abc123"),
		SnapshotId:             aws.String("snap-1"),
		UseS3DualStackEndpoint: true,
	}

	dualStack, err := client.GetDeployablePatchSnapshotForInstance(ctx, dualStackInput)
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(dualStack.SnapshotDownloadUrl), ".s3.dualstack.")
}

func testMaintenanceWindowTaskCutoffBehavior(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1SSMClient(t)
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("cutoff-mw"),
		Schedule: aws.String("rate(7 days)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)

	task, err := client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId:       win.WindowId,
		TaskArn:        aws.String("AWS-RunShellScript"),
		TaskType:       ssmtypes.MaintenanceWindowTaskTypeRunCommand,
		CutoffBehavior: ssmtypes.MaintenanceWindowTaskCutoffBehaviorCancelTask,
	})
	require.NoError(t, err)

	got, err := client.GetMaintenanceWindowTask(ctx, &ssmsdk.GetMaintenanceWindowTaskInput{
		WindowId:     win.WindowId,
		WindowTaskId: task.WindowTaskId,
	})
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowTaskCutoffBehaviorCancelTask, got.CutoffBehavior)

	_, err = client.UpdateMaintenanceWindowTask(ctx, &ssmsdk.UpdateMaintenanceWindowTaskInput{
		WindowId:       win.WindowId,
		WindowTaskId:   task.WindowTaskId,
		CutoffBehavior: ssmtypes.MaintenanceWindowTaskCutoffBehaviorContinueTask,
	})
	require.NoError(t, err)

	got2, err := client.GetMaintenanceWindowTask(ctx, &ssmsdk.GetMaintenanceWindowTaskInput{
		WindowId:     win.WindowId,
		WindowTaskId: task.WindowTaskId,
	})
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowTaskCutoffBehaviorContinueTask, got2.CutoffBehavior)

	tasks, err := client.DescribeMaintenanceWindowTasks(ctx, &ssmsdk.DescribeMaintenanceWindowTasksInput{
		WindowId: win.WindowId,
	})
	require.NoError(t, err)
	require.Len(t, tasks.Tasks, 1)
	assert.Equal(t, ssmtypes.MaintenanceWindowTaskCutoffBehaviorContinueTask, tasks.Tasks[0].CutoffBehavior)
}

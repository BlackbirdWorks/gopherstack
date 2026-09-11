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

// TestMaintenanceWindowExecutionStatus_RealClientDecode covers
// gopherstack-yqrl5: this backend emitted MaintenanceWindowExecutionStatus
// fields as Title-case "Success" (borrowed from the CommandStatus-typed
// commandStatusSuccess constant) where the real enum is screaming case
// (ssm@v1.73.4 types/enums.go:1223:
// MaintenanceWindowExecutionStatusSuccess = "SUCCESS"). Comparing against the
// SDK's typed enum constant -- not a raw string -- is the only assertion a
// case mismatch actually fails: json.Unmarshal into a string-based enum
// accepts any string, so it doesn't itself catch wrong casing.
func TestMaintenanceWindowExecutionStatus_RealClientDecode(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	win, err := client.CreateMaintenanceWindow(ctx, &ssmsdk.CreateMaintenanceWindowInput{
		Name:     aws.String("mw-status-case"),
		Schedule: aws.String("cron(0 0 ? * * *)"),
		Duration: aws.Int32(2),
		Cutoff:   1,
	})
	require.NoError(t, err)
	windowID := aws.ToString(win.WindowId)

	_, err = client.RegisterTaskWithMaintenanceWindow(ctx, &ssmsdk.RegisterTaskWithMaintenanceWindowInput{
		WindowId: aws.String(windowID),
		TaskArn:  aws.String("AWS-RunShellScript"),
		TaskType: ssmtypes.MaintenanceWindowTaskTypeRunCommand,
		Priority: aws.Int32(1),
	})
	require.NoError(t, err)

	execsOut, err := client.DescribeMaintenanceWindowExecutions(ctx, &ssmsdk.DescribeMaintenanceWindowExecutionsInput{
		WindowId: aws.String(windowID),
	})
	require.NoError(t, err)
	require.Len(t, execsOut.WindowExecutions, 1)
	assert.Equal(t, ssmtypes.MaintenanceWindowExecutionStatusSuccess, execsOut.WindowExecutions[0].Status)

	execID := aws.ToString(execsOut.WindowExecutions[0].WindowExecutionId)

	getExecOut, err := client.GetMaintenanceWindowExecution(ctx, &ssmsdk.GetMaintenanceWindowExecutionInput{
		WindowExecutionId: aws.String(execID),
	})
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowExecutionStatusSuccess, getExecOut.Status)

	tasksOut, err := client.DescribeMaintenanceWindowExecutionTasks(
		ctx, &ssmsdk.DescribeMaintenanceWindowExecutionTasksInput{WindowExecutionId: aws.String(execID)},
	)
	require.NoError(t, err)
	require.Len(t, tasksOut.WindowExecutionTaskIdentities, 1)
	assert.Equal(t, ssmtypes.MaintenanceWindowExecutionStatusSuccess, tasksOut.WindowExecutionTaskIdentities[0].Status)

	taskExecID := aws.ToString(tasksOut.WindowExecutionTaskIdentities[0].TaskExecutionId)

	getTaskOut, err := client.GetMaintenanceWindowExecutionTask(ctx, &ssmsdk.GetMaintenanceWindowExecutionTaskInput{
		WindowExecutionId: aws.String(execID),
		TaskId:            aws.String(taskExecID),
	})
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowExecutionStatusSuccess, getTaskOut.Status)

	invsOut, err := client.DescribeMaintenanceWindowExecutionTaskInvocations(
		ctx, &ssmsdk.DescribeMaintenanceWindowExecutionTaskInvocationsInput{
			WindowExecutionId: aws.String(execID),
			TaskId:            aws.String(taskExecID),
		},
	)
	require.NoError(t, err)
	require.Len(t, invsOut.WindowExecutionTaskInvocationIdentities, 1)
	assert.Equal(
		t, ssmtypes.MaintenanceWindowExecutionStatusSuccess, invsOut.WindowExecutionTaskInvocationIdentities[0].Status,
	)

	invID := aws.ToString(invsOut.WindowExecutionTaskInvocationIdentities[0].InvocationId)

	getInvOut, err := client.GetMaintenanceWindowExecutionTaskInvocation(
		ctx, &ssmsdk.GetMaintenanceWindowExecutionTaskInvocationInput{
			WindowExecutionId: aws.String(execID),
			TaskId:            aws.String(taskExecID),
			InvocationId:      aws.String(invID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, ssmtypes.MaintenanceWindowExecutionStatusSuccess, getInvOut.Status)
}

// TestPatchComplianceData_State_RealClientDecode covers gopherstack-yqrl5's
// second finding: patchComplianceStateMissing/Installed were Title-case
// ("Missing"/"Installed") where PatchComplianceData.State's real enum is
// screaming case (ssm@v1.73.4 types/enums.go:1938,1942:
// PatchComplianceDataStateInstalled="INSTALLED",
// PatchComplianceDataStateMissing="MISSING"). Only one of the two seeded
// AmazonLinux2 catalogue patches (patch_inventory.go) is approved, so a
// single Install run exercises both branches of patchComplianceFromEffective.
func TestPatchComplianceData_State_RealClientDecode(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	client := newTestSSMClient(t, h)
	ctx := t.Context()

	created, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
		Name:                           aws.String("patch-state-case-baseline"),
		OperatingSystem:                ssmtypes.OperatingSystemAmazonLinux2,
		ApprovedPatches:                []string{"ALAS2-2024-2451"},
		ApprovedPatchesComplianceLevel: ssmtypes.PatchComplianceLevelCritical,
	})
	require.NoError(t, err)

	_, err = client.RegisterPatchBaselineForPatchGroup(ctx, &ssmsdk.RegisterPatchBaselineForPatchGroupInput{
		BaselineId: created.BaselineId,
		PatchGroup: aws.String("state-case-group"),
	})
	require.NoError(t, err)

	_, err = client.SendCommand(ctx, &ssmsdk.SendCommandInput{
		DocumentName: aws.String("AWS-RunPatchBaseline"),
		InstanceIds:  []string{"i-state-case"},
		Parameters:   map[string][]string{"PatchGroup": {"state-case-group"}, "Operation": {"Install"}},
	})
	require.NoError(t, err)

	patchesOut, err := client.DescribeInstancePatches(ctx, &ssmsdk.DescribeInstancePatchesInput{
		InstanceId: aws.String("i-state-case"),
	})
	require.NoError(t, err)

	states := make(map[string]ssmtypes.PatchComplianceDataState, len(patchesOut.Patches))
	for _, p := range patchesOut.Patches {
		states[aws.ToString(p.Title)] = p.State
	}

	assert.Equal(t, ssmtypes.PatchComplianceDataStateInstalled, states["ALAS2-2024-2451"],
		"the explicitly-approved patch must be reported INSTALLED")
	assert.Equal(t, ssmtypes.PatchComplianceDataStateMissing, states["ALAS2-2024-2460"],
		"the unapproved catalogue patch must be reported MISSING")
}

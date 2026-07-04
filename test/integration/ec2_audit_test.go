package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runAuditInstance launches a single instance for the audit tests and registers
// a cleanup that terminates it.
func runAuditInstance(t *testing.T, client *ec2sdk.Client) string {
	t.Helper()

	ctx := t.Context()

	runOut, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-0c55b159cbfafe1f0"),
		InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, runOut.Instances, 1)

	instanceID := aws.ToString(runOut.Instances[0].InstanceId)
	require.NotEmpty(t, instanceID)

	t.Cleanup(func() {
		_, _ = client.TerminateInstances(ctx, &ec2sdk.TerminateInstancesInput{
			InstanceIds: []string{instanceID},
		})
	})

	return instanceID
}

// TestIntegration_EC2Audit_ModifyInstanceCpuOptions verifies that
// ModifyInstanceCpuOptions persists CPU core/thread settings that are then
// reflected in DescribeInstances.
func TestIntegration_EC2Audit_ModifyInstanceCpuOptions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	instanceID := runAuditInstance(t, client)

	_, err := client.ModifyInstanceCpuOptions(ctx, &ec2sdk.ModifyInstanceCpuOptionsInput{
		InstanceId:     aws.String(instanceID),
		CoreCount:      aws.Int32(4),
		ThreadsPerCore: aws.Int32(2),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)

	inst := descOut.Reservations[0].Instances[0]
	require.NotNil(t, inst.CpuOptions, "CpuOptions should be reflected after modification")
	assert.Equal(t, int32(4), aws.ToInt32(inst.CpuOptions.CoreCount))
	assert.Equal(t, int32(2), aws.ToInt32(inst.CpuOptions.ThreadsPerCore))
}

// TestIntegration_EC2Audit_ModifyInstancePlacement verifies that
// ModifyInstancePlacement persists placement settings that are then reflected
// in DescribeInstances.
func TestIntegration_EC2Audit_ModifyInstancePlacement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	instanceID := runAuditInstance(t, client)

	_, err := client.ModifyInstancePlacement(ctx, &ec2sdk.ModifyInstancePlacementInput{
		InstanceId: aws.String(instanceID),
		Tenancy:    ec2types.HostTenancyDedicated,
		Affinity:   ec2types.AffinityHost,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)

	inst := descOut.Reservations[0].Instances[0]
	require.NotNil(t, inst.Placement, "Placement should be reflected after modification")
	assert.Equal(t, ec2types.TenancyDedicated, inst.Placement.Tenancy)
	assert.Equal(t, string(ec2types.AffinityHost), aws.ToString(inst.Placement.Affinity))
}

// TestIntegration_EC2Audit_ModifyInstanceMaintenanceOptions verifies that
// ModifyInstanceMaintenanceOptions persists the auto-recovery setting that is
// then reflected in DescribeInstances.
func TestIntegration_EC2Audit_ModifyInstanceMaintenanceOptions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	instanceID := runAuditInstance(t, client)

	_, err := client.ModifyInstanceMaintenanceOptions(
		ctx,
		&ec2sdk.ModifyInstanceMaintenanceOptionsInput{
			InstanceId:   aws.String(instanceID),
			AutoRecovery: ec2types.InstanceAutoRecoveryStateDisabled,
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.Reservations, 1)
	require.Len(t, descOut.Reservations[0].Instances, 1)

	inst := descOut.Reservations[0].Instances[0]
	require.NotNil(t, inst.MaintenanceOptions, "MaintenanceOptions should be reflected")
	assert.Equal(
		t,
		ec2types.InstanceAutoRecoveryStateDisabled,
		inst.MaintenanceOptions.AutoRecovery,
	)
}

// TestIntegration_EC2Audit_AssociateInstanceEventWindow verifies that
// AssociateInstanceEventWindow records the association so that the target is
// reflected in DescribeInstanceEventWindows.
func TestIntegration_EC2Audit_AssociateInstanceEventWindow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	instanceID := runAuditInstance(t, client)

	createOut, err := client.CreateInstanceEventWindow(ctx, &ec2sdk.CreateInstanceEventWindowInput{
		Name:           aws.String("audit-event-window"),
		CronExpression: aws.String("* 21-23 * * 2,3"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.InstanceEventWindow)
	windowID := aws.ToString(createOut.InstanceEventWindow.InstanceEventWindowId)
	require.NotEmpty(t, windowID)

	t.Cleanup(func() {
		_, _ = client.DeleteInstanceEventWindow(ctx, &ec2sdk.DeleteInstanceEventWindowInput{
			InstanceEventWindowId: aws.String(windowID),
		})
	})

	_, err = client.AssociateInstanceEventWindow(
		ctx,
		&ec2sdk.AssociateInstanceEventWindowInput{
			InstanceEventWindowId: aws.String(windowID),
			AssociationTarget: &ec2types.InstanceEventWindowAssociationRequest{
				InstanceIds: []string{instanceID},
			},
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeInstanceEventWindows(
		ctx,
		&ec2sdk.DescribeInstanceEventWindowsInput{
			InstanceEventWindowIds: []string{windowID},
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.InstanceEventWindows, 1)

	window := descOut.InstanceEventWindows[0]
	require.NotNil(t, window.AssociationTarget, "association target should be recorded")
	assert.Contains(t, window.AssociationTarget.InstanceIds, instanceID)
}

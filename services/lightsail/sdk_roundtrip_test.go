package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/require"
)

// TestInstanceLifecycleRoundTrip exercises family B/C/D/E/F/Z end to end
// through the real SDK client: CreateInstances, GetInstance(s),
// GetInstanceState, port rules, an instance snapshot, an add-on, and
// Operation polling.
func TestInstanceLifecycleRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	out, err := client.CreateInstances(ctx, &lightsailsdk.CreateInstancesInput{
		InstanceNames:    []string{"web-1"},
		AvailabilityZone: aws.String("us-east-1a"),
		BlueprintId:      aws.String("amazon_linux_2023"),
		BundleId:         aws.String("nano_3_0"),
		Tags:             []lightsailtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	require.Len(t, out.Operations, 1)
	require.Equal(t, lightsailtypes.OperationStatusStarted, out.Operations[0].Status)
	require.NotEmpty(t, aws.ToString(out.Operations[0].Id))

	getOut, err := client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{InstanceName: aws.String("web-1")})
	require.NoError(t, err)
	require.Equal(t, "web-1", aws.ToString(getOut.Instance.Name))
	require.Equal(t, "amazon_linux_2023", aws.ToString(getOut.Instance.BlueprintId))
	require.Equal(t, int32(0), aws.ToInt32(getOut.Instance.State.Code))
	require.Equal(t, "env", aws.ToString(getOut.Instance.Tags[0].Key))

	listOut, err := client.GetInstances(ctx, &lightsailsdk.GetInstancesInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Instances, 1)

	require.Eventually(t, func() bool {
		st, stErr := client.GetInstanceState(
			ctx,
			&lightsailsdk.GetInstanceStateInput{InstanceName: aws.String("web-1")},
		)

		return stErr == nil && aws.ToString(st.State.Name) == "running"
	}, defaultAsyncWait, defaultAsyncPoll, "instance never reached running")

	// Operation polling (family Z).
	opID := aws.ToString(out.Operations[0].Id)

	require.Eventually(t, func() bool {
		op, opErr := client.GetOperation(ctx, &lightsailsdk.GetOperationInput{OperationId: aws.String(opID)})

		return opErr == nil && op.Operation.Status == lightsailtypes.OperationStatusSucceeded
	}, defaultAsyncWait, defaultAsyncPoll, "operation never reached Succeeded")

	opsOut, err := client.GetOperations(ctx, &lightsailsdk.GetOperationsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, opsOut.Operations)

	opsForResourceOut, err := client.GetOperationsForResource(
		ctx,
		&lightsailsdk.GetOperationsForResourceInput{ResourceName: aws.String("web-1")},
	)
	require.NoError(t, err)
	require.NotEmpty(t, opsForResourceOut.Operations)

	// Instance ports (family C).
	putOut, err := client.PutInstancePublicPorts(ctx, &lightsailsdk.PutInstancePublicPortsInput{
		InstanceName: aws.String("web-1"),
		PortInfos: []lightsailtypes.PortInfo{
			{FromPort: 80, ToPort: 80, Protocol: lightsailtypes.NetworkProtocolTcp},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.Operation)

	portsOut, err := client.GetInstancePortStates(
		ctx,
		&lightsailsdk.GetInstancePortStatesInput{InstanceName: aws.String("web-1")},
	)
	require.NoError(t, err)
	require.Len(t, portsOut.PortStates, 1)
	require.Equal(t, lightsailtypes.PortStateOpen, portsOut.PortStates[0].State)

	// Instance snapshot (family D).
	_, err = client.CreateInstanceSnapshot(ctx, &lightsailsdk.CreateInstanceSnapshotInput{
		InstanceName: aws.String("web-1"), InstanceSnapshotName: aws.String("web-1-snap"),
	})
	require.NoError(t, err)

	snapOut, err := client.GetInstanceSnapshot(
		ctx,
		&lightsailsdk.GetInstanceSnapshotInput{InstanceSnapshotName: aws.String("web-1-snap")},
	)
	require.NoError(t, err)
	require.Equal(t, "web-1-snap", aws.ToString(snapOut.InstanceSnapshot.Name))

	// Add-on (family F).
	addOnOut, err := client.EnableAddOn(ctx, &lightsailsdk.EnableAddOnInput{
		ResourceName: aws.String("web-1"),
		AddOnRequest: &lightsailtypes.AddOnRequest{
			AddOnType:                lightsailtypes.AddOnTypeAutoSnapshot,
			AutoSnapshotAddOnRequest: &lightsailtypes.AutoSnapshotAddOnRequest{SnapshotTimeOfDay: aws.String("06:00")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, addOnOut.Operations)

	autoSnapOut, err := client.GetAutoSnapshots(
		ctx,
		&lightsailsdk.GetAutoSnapshotsInput{ResourceName: aws.String("web-1")},
	)
	require.NoError(t, err)
	require.Equal(t, lightsailtypes.ResourceTypeInstance, autoSnapOut.ResourceType)

	// Instance access details and reboot/stop/start (family B/C).
	accessOut, err := client.GetInstanceAccessDetails(
		ctx,
		&lightsailsdk.GetInstanceAccessDetailsInput{InstanceName: aws.String("web-1")},
	)
	require.NoError(t, err)
	require.Equal(t, "web-1", aws.ToString(accessOut.AccessDetails.InstanceName))

	_, err = client.RebootInstance(ctx, &lightsailsdk.RebootInstanceInput{InstanceName: aws.String("web-1")})
	require.NoError(t, err)

	_, err = client.StopInstance(ctx, &lightsailsdk.StopInstanceInput{InstanceName: aws.String("web-1")})
	require.NoError(t, err)

	stateOut, err := client.GetInstanceState(
		ctx,
		&lightsailsdk.GetInstanceStateInput{InstanceName: aws.String("web-1")},
	)
	require.NoError(t, err)
	require.Equal(t, "stopped", aws.ToString(stateOut.State.Name))

	_, err = client.StartInstance(ctx, &lightsailsdk.StartInstanceInput{InstanceName: aws.String("web-1")})
	require.NoError(t, err)

	_, err = client.DeleteInstance(ctx, &lightsailsdk.DeleteInstanceInput{InstanceName: aws.String("web-1")})
	require.NoError(t, err)

	_, err = client.GetInstance(ctx, &lightsailsdk.GetInstanceInput{InstanceName: aws.String("web-1")})
	require.Error(t, err)
}

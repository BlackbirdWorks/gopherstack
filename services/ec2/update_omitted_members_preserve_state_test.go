package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestModifyInstancePlacement_PreservesOmittedMembers proves the zeroguard
// fix (cmd/zeroguard): ModifyInstancePlacementInput.GroupId, .HostId and
// .HostResourceGroupArn are plain strings in the real SDK's request type
// (api_op_ModifyInstancePlacement.go). EC2 is awsquery/form-encoded, so
// "omitted" means the form key is absent -- before the fix each field read
// via vals.Get alone, so an omitted field and one explicitly sent empty
// were indistinguishable. InstanceId is left a plain string: it is a
// required lookup identifier, never written back to state.
func TestModifyInstancePlacement_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)
	ctx := t.Context()

	runOut, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId: aws.String("ami-uds-test"), InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)
	instanceID := aws.ToString(runOut.Instances[0].InstanceId)

	_, err = client.StopInstances(ctx, &ec2sdk.StopInstancesInput{InstanceIds: []string{instanceID}})
	require.NoError(t, err)
	backend.TickLifecycleForTest()

	hostsOut, err := client.AllocateHosts(ctx, &ec2sdk.AllocateHostsInput{
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceType:     aws.String("c5.large"),
		Quantity:         aws.Int32(1),
	})
	require.NoError(t, err)
	hostID := hostsOut.HostIds[0]

	_, err = client.ModifyInstancePlacement(ctx, &ec2sdk.ModifyInstancePlacementInput{
		InstanceId:           aws.String(instanceID),
		GroupId:              aws.String("pg-1"),
		HostId:               aws.String(hostID),
		HostResourceGroupArn: aws.String("arn:aws:resource-groups:us-east-1:123456789012:group/hrg-1"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	require.NoError(t, err)
	require.Len(t, desc.Reservations, 1)
	require.Len(t, desc.Reservations[0].Instances, 1)
	placement := desc.Reservations[0].Instances[0].Placement
	require.NotNil(t, placement)
	assert.Equal(t, "pg-1", aws.ToString(placement.GroupId))
	assert.Equal(t, hostID, aws.ToString(placement.HostId))
	assert.Equal(t, "arn:aws:resource-groups:us-east-1:123456789012:group/hrg-1",
		aws.ToString(placement.HostResourceGroupArn))

	// Omitted fields must preserve every prior value.
	_, err = client.ModifyInstancePlacement(ctx, &ec2sdk.ModifyInstancePlacementInput{
		InstanceId: aws.String(instanceID),
	})
	require.NoError(t, err)

	desc, err = client.DescribeInstances(ctx, &ec2sdk.DescribeInstancesInput{InstanceIds: []string{instanceID}})
	require.NoError(t, err)
	placement = desc.Reservations[0].Instances[0].Placement
	require.NotNil(t, placement)
	assert.Equal(t, "pg-1", aws.ToString(placement.GroupId), "omitted GroupId must survive")
	assert.Equal(t, hostID, aws.ToString(placement.HostId), "omitted HostId must survive")
	assert.Equal(t, "arn:aws:resource-groups:us-east-1:123456789012:group/hrg-1",
		aws.ToString(placement.HostResourceGroupArn), "omitted HostResourceGroupArn must survive")
}

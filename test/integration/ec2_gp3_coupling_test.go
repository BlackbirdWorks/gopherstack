package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EC2_CreateVolume_GP3Coupling mirrors terraform-provider-aws's
// EBS gp3 volume tests: a gp3 volume created with custom IOPS/throughput within
// the AWS coupling bounds succeeds and reflects the requested values, while a
// request that violates the throughput-to-IOPS ratio is rejected.
func TestIntegration_EC2_CreateVolume_GP3Coupling(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// Valid gp3 volume with custom IOPS/throughput inside AWS bounds.
	volOut, err := client.CreateVolume(ctx, &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(100),
		VolumeType:       ec2types.VolumeTypeGp3,
		Iops:             aws.Int32(6000),
		Throughput:       aws.Int32(500),
	})
	require.NoError(t, err)

	volumeID := aws.ToString(volOut.VolumeId)
	require.NotEmpty(t, volumeID)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteVolume(cleanupCtx, &ec2sdk.DeleteVolumeInput{VolumeId: aws.String(volumeID)})
	})

	assert.Equal(t, int32(6000), aws.ToInt32(volOut.Iops))
	assert.Equal(t, int32(500), aws.ToInt32(volOut.Throughput))

	// Ratio violation: throughput 1000 MiB/s requires >= 4000 IOPS, but IOPS is
	// only 3000 -> AWS rejects (max 0.25 MiB/s per provisioned IOPS).
	_, err = client.CreateVolume(ctx, &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(100),
		VolumeType:       ec2types.VolumeTypeGp3,
		Iops:             aws.Int32(3000),
		Throughput:       aws.Int32(1000),
	})
	require.Error(t, err)
}

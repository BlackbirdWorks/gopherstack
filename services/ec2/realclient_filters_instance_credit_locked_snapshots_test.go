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

// TestRealClient_DescribeInstanceCreditSpecificationsFilters covers
// DescribeInstanceCreditSpecifications' instance-id filter, which previously
// ignored Filters entirely (only the InstanceId.N member list worked).
func TestRealClient_DescribeInstanceCreditSpecificationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	runA, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-test"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		CreditSpecification: &types.CreditSpecificationRequest{
			CpuCredits: aws.String("unlimited"),
		},
	})
	require.NoError(t, err)
	runB, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-test"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
		CreditSpecification: &types.CreditSpecificationRequest{
			CpuCredits: aws.String("standard"),
		},
	})
	require.NoError(t, err)
	require.Len(t, runA.Instances, 1)
	require.Len(t, runB.Instances, 1)

	idA := aws.ToString(runA.Instances[0].InstanceId)

	out, err := client.DescribeInstanceCreditSpecifications(
		t.Context(),
		&ec2sdk.DescribeInstanceCreditSpecificationsInput{
			Filters: []types.Filter{{Name: aws.String("instance-id"), Values: []string{idA}}},
		},
	)
	require.NoError(t, err)

	got := make([]string, 0, len(out.InstanceCreditSpecifications))
	for _, s := range out.InstanceCreditSpecifications {
		got = append(got, aws.ToString(s.InstanceId))
	}
	assert.Equal(t, []string{idA}, got)
}

// TestRealClient_DescribeLockedSnapshotsFilters covers DescribeLockedSnapshots'
// lock-state filter, which previously ignored Filters entirely.
func TestRealClient_DescribeLockedSnapshotsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	vol, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(10),
	})
	require.NoError(t, err)

	snapGovernance, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)
	snapCompliance, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{VolumeId: vol.VolumeId})
	require.NoError(t, err)

	_, err = client.LockSnapshot(t.Context(), &ec2sdk.LockSnapshotInput{
		SnapshotId:   snapGovernance.SnapshotId,
		LockMode:     types.LockModeGovernance,
		LockDuration: aws.Int32(30),
	})
	require.NoError(t, err)
	_, err = client.LockSnapshot(t.Context(), &ec2sdk.LockSnapshotInput{
		SnapshotId:   snapCompliance.SnapshotId,
		LockMode:     types.LockModeCompliance,
		LockDuration: aws.Int32(30),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "lock-state governance",
			filters: []types.Filter{{Name: aws.String("lock-state"), Values: []string{"governance"}}},
			want:    []string{aws.ToString(snapGovernance.SnapshotId)},
		},
		{
			name:    "lock-state compliance",
			filters: []types.Filter{{Name: aws.String("lock-state"), Values: []string{"compliance"}}},
			want:    []string{aws.ToString(snapCompliance.SnapshotId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("lock-state"), Values: []string{"expired"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeLockedSnapshots(
				t.Context(), &ec2sdk.DescribeLockedSnapshotsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.Snapshots))
			for _, s := range out.Snapshots {
				got = append(got, aws.ToString(s.SnapshotId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

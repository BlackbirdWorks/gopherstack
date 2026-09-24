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

// TestRealClient_DescribeVolumeStatusFilters covers DescribeVolumeStatus,
// which previously ignored Filters entirely.
func TestRealClient_DescribeVolumeStatusFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	volA, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(10),
	})
	require.NoError(t, err)
	volB, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1b"),
		Size:             aws.Int32(20),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "availability-zone",
			filters: []types.Filter{{Name: aws.String("availability-zone"), Values: []string{"us-east-1a"}}},
			want:    []string{aws.ToString(volA.VolumeId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("availability-zone"), Values: []string{"us-east-1c"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVolumeStatus(
				t.Context(), &ec2sdk.DescribeVolumeStatusInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VolumeStatuses))
			for _, v := range out.VolumeStatuses {
				got = append(got, aws.ToString(v.VolumeId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}

	require.NotEmpty(t, aws.ToString(volB.VolumeId))
}

// TestRealClient_DescribeVolumesModificationsFilters covers
// DescribeVolumesModifications, which previously ignored Filters entirely.
func TestRealClient_DescribeVolumesModificationsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	volA, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(10),
		VolumeType:       types.VolumeTypeGp2,
	})
	require.NoError(t, err)
	volB, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(10),
		VolumeType:       types.VolumeTypeGp2,
	})
	require.NoError(t, err)

	_, err = client.ModifyVolume(t.Context(), &ec2sdk.ModifyVolumeInput{
		VolumeId:   volA.VolumeId,
		Size:       aws.Int32(50),
		VolumeType: types.VolumeTypeGp3,
		Iops:       aws.Int32(4000),
	})
	require.NoError(t, err)
	_, err = client.ModifyVolume(t.Context(), &ec2sdk.ModifyVolumeInput{
		VolumeId:   volB.VolumeId,
		Size:       aws.Int32(100),
		VolumeType: types.VolumeTypeGp3,
		Iops:       aws.Int32(5000),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "volume-id",
			filters: []types.Filter{{Name: aws.String("volume-id"), Values: []string{aws.ToString(volA.VolumeId)}}},
			want:    []string{aws.ToString(volA.VolumeId)},
		},
		{
			name:    "target-size",
			filters: []types.Filter{{Name: aws.String("target-size"), Values: []string{"100"}}},
			want:    []string{aws.ToString(volB.VolumeId)},
		},
		{
			name:    "target-iops",
			filters: []types.Filter{{Name: aws.String("target-iops"), Values: []string{"4000"}}},
			want:    []string{aws.ToString(volA.VolumeId)},
		},
		{
			name:    "original-volume-type",
			filters: []types.Filter{{Name: aws.String("original-volume-type"), Values: []string{"gp2"}}},
			want:    []string{aws.ToString(volA.VolumeId), aws.ToString(volB.VolumeId)},
		},
		{
			name:    "modification-state",
			filters: []types.Filter{{Name: aws.String("modification-state"), Values: []string{"completed"}}},
			want:    []string{aws.ToString(volA.VolumeId), aws.ToString(volB.VolumeId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("target-size"), Values: []string{"999"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeVolumesModifications(
				t.Context(), &ec2sdk.DescribeVolumesModificationsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.VolumesModifications))
			for _, m := range out.VolumesModifications {
				got = append(got, aws.ToString(m.VolumeId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

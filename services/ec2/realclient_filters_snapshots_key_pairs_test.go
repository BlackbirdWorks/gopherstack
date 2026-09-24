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

// TestRealClient_DescribeSnapshotsFilters covers DescribeSnapshots
// sub-filters that were missing: description, owner-id, volume-size, and
// tag-key.
func TestRealClient_DescribeSnapshotsFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	vol, err := client.CreateVolume(t.Context(), &ec2sdk.CreateVolumeInput{
		AvailabilityZone: aws.String("us-east-1a"),
		Size:             aws.Int32(30),
	})
	require.NoError(t, err)

	snap1, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{
		VolumeId:    vol.VolumeId,
		Description: aws.String("nightly backup"),
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeSnapshot,
			Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("infra")}},
		}},
	})
	require.NoError(t, err)
	snap2, err := client.CreateSnapshot(t.Context(), &ec2sdk.CreateSnapshotInput{
		VolumeId:    vol.VolumeId,
		Description: aws.String("ad-hoc backup"),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "description",
			filters: []types.Filter{{Name: aws.String("description"), Values: []string{"nightly backup"}}},
			want:    []string{aws.ToString(snap1.SnapshotId)},
		},
		{
			name:    "owner-id",
			filters: []types.Filter{{Name: aws.String("owner-id"), Values: []string{"000000000000"}}},
			want:    []string{aws.ToString(snap1.SnapshotId), aws.ToString(snap2.SnapshotId)},
		},
		{
			name:    "volume-size",
			filters: []types.Filter{{Name: aws.String("volume-size"), Values: []string{"30"}}},
			want:    []string{aws.ToString(snap1.SnapshotId), aws.ToString(snap2.SnapshotId)},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Team"}}},
			want:    []string{aws.ToString(snap1.SnapshotId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("description"), Values: []string{"missing"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeSnapshots(
				t.Context(), &ec2sdk.DescribeSnapshotsInput{Filters: tt.filters},
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

// TestRealClient_DescribeKeyPairsTagKeyFilter covers DescribeKeyPairs'
// tag-key filter, which was missing (key-name/key-pair-id/fingerprint/tag:
// were already implemented).
func TestRealClient_DescribeKeyPairsTagKeyFilter(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	tagged, err := client.CreateKeyPair(t.Context(), &ec2sdk.CreateKeyPairInput{
		KeyName: aws.String("tagged-key"),
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeKeyPair,
			Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("infra")}},
		}},
	})
	require.NoError(t, err)
	_, err = client.CreateKeyPair(t.Context(), &ec2sdk.CreateKeyPairInput{KeyName: aws.String("untagged-key")})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Team"}}},
			want:    []string{aws.ToString(tagged.KeyName)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Missing"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeKeyPairs(
				t.Context(), &ec2sdk.DescribeKeyPairsInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.KeyPairs))
			for _, kp := range out.KeyPairs {
				got = append(got, aws.ToString(kp.KeyName))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

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

// TestRealClient_DescribeImagesFilters covers DescribeImages sub-filters that
// were missing: owner-id, virtualization-type, tag-key, and the
// block-device-mapping.* family.
func TestRealClient_DescribeImagesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	hvm, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name:               aws.String("hvm-image"),
		VirtualizationType: aws.String("hvm"),
		BlockDeviceMappings: []types.BlockDeviceMapping{
			{
				DeviceName: aws.String("/dev/sda1"),
				Ebs: &types.EbsBlockDevice{
					VolumeSize: aws.Int32(20),
					VolumeType: types.VolumeTypeGp3,
					SnapshotId: aws.String("snap-hvm"),
				},
			},
		},
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeImage,
			Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("infra")}},
		}},
	})
	require.NoError(t, err)
	paravirtual, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{
		Name:               aws.String("pv-image"),
		VirtualizationType: aws.String("paravirtual"),
		BlockDeviceMappings: []types.BlockDeviceMapping{
			{
				DeviceName: aws.String("/dev/sda1"),
				Ebs: &types.EbsBlockDevice{
					VolumeSize: aws.Int32(8),
					VolumeType: types.VolumeTypeGp2,
					SnapshotId: aws.String("snap-pv"),
				},
			},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "virtualization-type",
			filters: []types.Filter{{Name: aws.String("virtualization-type"), Values: []string{"paravirtual"}}},
			want:    []string{aws.ToString(paravirtual.ImageId)},
		},
		{
			name:    "owner-id",
			filters: []types.Filter{{Name: aws.String("owner-id"), Values: []string{"000000000000"}}},
			want:    []string{aws.ToString(hvm.ImageId), aws.ToString(paravirtual.ImageId)},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Team"}}},
			want:    []string{aws.ToString(hvm.ImageId)},
		},
		{
			name: "block-device-mapping.volume-size",
			filters: []types.Filter{
				{Name: aws.String("block-device-mapping.volume-size"), Values: []string{"20"}},
			},
			want: []string{aws.ToString(hvm.ImageId)},
		},
		{
			name: "block-device-mapping.snapshot-id",
			filters: []types.Filter{
				{Name: aws.String("block-device-mapping.snapshot-id"), Values: []string{"snap-pv"}},
			},
			want: []string{aws.ToString(paravirtual.ImageId)},
		},
		{
			name: "block-device-mapping.volume-type",
			filters: []types.Filter{
				{Name: aws.String("block-device-mapping.volume-type"), Values: []string{"gp3"}},
			},
			want: []string{aws.ToString(hvm.ImageId)},
		},
		{
			name: "block-device-mapping.device-name",
			filters: []types.Filter{
				{Name: aws.String("block-device-mapping.device-name"), Values: []string{"/dev/sda1"}},
			},
			want: []string{aws.ToString(hvm.ImageId), aws.ToString(paravirtual.ImageId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("virtualization-type"), Values: []string{"missing"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{Filters: tt.filters})
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.Images))
			for _, img := range out.Images {
				got = append(got, aws.ToString(img.ImageId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

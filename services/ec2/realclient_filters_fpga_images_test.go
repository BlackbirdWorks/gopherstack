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

// TestRealClient_DescribeFpgaImagesFilters covers DescribeFpgaImages, which
// previously ignored Filters entirely.
func TestRealClient_DescribeFpgaImagesFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	img1, err := client.CreateFpgaImage(t.Context(), &ec2sdk.CreateFpgaImageInput{
		InputStorageLocation: &types.StorageLocation{Bucket: aws.String("bucket"), Key: aws.String("checkpoint1")},
		Name:                 aws.String("afi-one"),
		TagSpecifications: []types.TagSpecification{{
			ResourceType: types.ResourceTypeFpgaImage,
			Tags:         []types.Tag{{Key: aws.String("Team"), Value: aws.String("fpga")}},
		}},
	})
	require.NoError(t, err)
	img2, err := client.CreateFpgaImage(t.Context(), &ec2sdk.CreateFpgaImageInput{
		InputStorageLocation: &types.StorageLocation{Bucket: aws.String("bucket"), Key: aws.String("checkpoint2")},
		Name:                 aws.String("afi-two"),
	})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "name",
			filters: []types.Filter{{Name: aws.String("name"), Values: []string{"afi-one"}}},
			want:    []string{aws.ToString(img1.FpgaImageId)},
		},
		{
			name: "fpga-image-global-id",
			filters: []types.Filter{
				{Name: aws.String("fpga-image-global-id"), Values: []string{aws.ToString(img2.FpgaImageGlobalId)}},
			},
			want: []string{aws.ToString(img2.FpgaImageId)},
		},
		{
			name:    "owner-id",
			filters: []types.Filter{{Name: aws.String("owner-id"), Values: []string{"000000000000"}}},
			want:    []string{aws.ToString(img1.FpgaImageId), aws.ToString(img2.FpgaImageId)},
		},
		{
			name:    "state",
			filters: []types.Filter{{Name: aws.String("state"), Values: []string{"available"}}},
			want:    []string{aws.ToString(img1.FpgaImageId), aws.ToString(img2.FpgaImageId)},
		},
		{
			name:    "tag:Team",
			filters: []types.Filter{{Name: aws.String("tag:Team"), Values: []string{"fpga"}}},
			want:    []string{aws.ToString(img1.FpgaImageId)},
		},
		{
			name:    "tag-key",
			filters: []types.Filter{{Name: aws.String("tag-key"), Values: []string{"Team"}}},
			want:    []string{aws.ToString(img1.FpgaImageId)},
		},
		{
			name:    "no match",
			filters: []types.Filter{{Name: aws.String("name"), Values: []string{"missing"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeFpgaImages(
				t.Context(), &ec2sdk.DescribeFpgaImagesInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.FpgaImages))
			for _, img := range out.FpgaImages {
				got = append(got, aws.ToString(img.FpgaImageId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

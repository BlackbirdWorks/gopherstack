package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// registerTestImage registers a fresh account-owned AMI, returning its ID.
func registerTestImage(t *testing.T, client *ec2sdk.Client, name string) string {
	t.Helper()

	out, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{Name: aws.String(name)})
	require.NoError(t, err)

	return aws.ToString(out.ImageId)
}

func TestReplaceImageInstanceTypeSpecification(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	imageID := registerTestImage(t, client, "spec-test-image")

	t.Run("set supported and unsupported instance types", func(t *testing.T) {
		t.Parallel()

		out, err := client.ReplaceImageInstanceTypeSpecification(
			t.Context(), &ec2sdk.ReplaceImageInstanceTypeSpecificationInput{
				ImageId: aws.String(imageID),
				InstanceTypeSpecification: &types.InstanceTypeSpecificationRequest{
					SupportedInstanceTypes:   []string{"t3.*", "m5.large"},
					UnsupportedInstanceTypes: []string{"t3.nano"},
				},
			},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(out.ReturnValue))

		descOut, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{ImageIds: []string{imageID}})
		require.NoError(t, err)
		require.Len(t, descOut.Images, 1)
		require.NotNil(t, descOut.Images[0].InstanceTypeSpecification)

		spec := descOut.Images[0].InstanceTypeSpecification
		require.Len(t, spec.SupportedInstanceTypes, 2)
		assert.Equal(t, "t3.*", aws.ToString(spec.SupportedInstanceTypes[0].InstanceType))
		require.Len(t, spec.UnsupportedInstanceTypes, 1)
		assert.Equal(t, "t3.nano", aws.ToString(spec.UnsupportedInstanceTypes[0].InstanceType))
	})

	t.Run("omitting the specification clears it", func(t *testing.T) {
		t.Parallel()

		myImageID := registerTestImage(t, client, "clear-test-image")

		_, err := client.ReplaceImageInstanceTypeSpecification(
			t.Context(), &ec2sdk.ReplaceImageInstanceTypeSpecificationInput{
				ImageId: aws.String(myImageID),
				InstanceTypeSpecification: &types.InstanceTypeSpecificationRequest{
					SupportedInstanceTypes: []string{"t3.*"},
				},
			},
		)
		require.NoError(t, err)

		_, err = client.ReplaceImageInstanceTypeSpecification(
			t.Context(), &ec2sdk.ReplaceImageInstanceTypeSpecificationInput{ImageId: aws.String(myImageID)},
		)
		require.NoError(t, err)

		descOut, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{ImageIds: []string{myImageID}})
		require.NoError(t, err)
		require.Len(t, descOut.Images, 1)
		assert.Nil(t, descOut.Images[0].InstanceTypeSpecification)
	})

	t.Run("not found for an unknown image", func(t *testing.T) {
		t.Parallel()

		_, err := client.ReplaceImageInstanceTypeSpecification(
			t.Context(), &ec2sdk.ReplaceImageInstanceTypeSpecificationInput{ImageId: aws.String("ami-doesnotexist")},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidAMIID.NotFound", apiErr.ErrorCode())
	})

	t.Run("not owner for a public catalog image", func(t *testing.T) {
		t.Parallel()

		_, err := client.ReplaceImageInstanceTypeSpecification(
			t.Context(), &ec2sdk.ReplaceImageInstanceTypeSpecificationInput{
				ImageId: aws.String("ami-0c55b159cbfafe1f0"),
			},
		)
		require.Error(t, err)

		var apiErr smithy.APIError

		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
	})
}

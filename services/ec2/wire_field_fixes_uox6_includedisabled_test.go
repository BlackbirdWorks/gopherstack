package ec2_test

// uox6-includedisabled: DescribeImages declares IncludeDisabled ("Specifies
// whether to include disabled AMIs. Default: No disabled AMIs are included
// in the response." -- api_op_DescribeImages.go, ec2@v1.319.1), but the
// handler never read it: a disabled image (DisableImage already marks it
// via b.imageDisabled, surfaced as State="disabled" -- images.go) still
// came back on every plain, unfiltered DescribeImages listing. An image
// named explicitly by ImageId is still returned regardless of
// IncludeDisabled -- that's the pre-existing, already-tested behavior of
// TestDescribeImages_DisabledState_RealClient (wire_field_fixes_test.go),
// so this fix only changes what a general listing (no ImageIds) returns.
// IncludeDeprecated is deliberately left alone: its own doc carries an
// ownership exception ("If you are the AMI owner, all deprecated AMIs
// appear in the response regardless of what you specify") that this
// single-account emulator has no OwnerID field to evaluate -- recorded
// rather than approximated.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeImages_ExcludesDisabledByDefault(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	visible, err := client.RegisterImage(
		t.Context(),
		&ec2sdk.RegisterImageInput{Name: aws.String("uox6-visible-ami")},
	)
	require.NoError(t, err)

	disabled, err := client.RegisterImage(
		t.Context(),
		&ec2sdk.RegisterImageInput{Name: aws.String("uox6-disabled-ami")},
	)
	require.NoError(t, err)

	_, err = client.DisableImage(t.Context(), &ec2sdk.DisableImageInput{ImageId: disabled.ImageId})
	require.NoError(t, err)

	// No ImageIds: a general listing, filtered down to just these two by name.
	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		Filters: []types.Filter{{
			Name:   aws.String("name"),
			Values: []string{"uox6-visible-ami", "uox6-disabled-ami"},
		}},
	})
	require.NoError(t, err)

	ids := make([]string, 0, len(out.Images))
	for _, img := range out.Images {
		ids = append(ids, aws.ToString(img.ImageId))
	}
	assert.Contains(t, ids, aws.ToString(visible.ImageId))
	assert.NotContains(t, ids, aws.ToString(disabled.ImageId))
}

func TestDescribeImages_ExplicitImageId_StillReturnsDisabled(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	disabled, err := client.RegisterImage(
		t.Context(),
		&ec2sdk.RegisterImageInput{Name: aws.String("uox6-disabled-explicit")},
	)
	require.NoError(t, err)

	_, err = client.DisableImage(t.Context(), &ec2sdk.DisableImageInput{ImageId: disabled.ImageId})
	require.NoError(t, err)

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds: []string{aws.ToString(disabled.ImageId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Images, 1)
	assert.Equal(t, types.ImageStateDisabled, out.Images[0].State)
}

func TestDescribeImages_IncludeDisabledTrue_ShowsDisabled(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	disabled, err := client.RegisterImage(
		t.Context(),
		&ec2sdk.RegisterImageInput{Name: aws.String("uox6-disabled-ami-2")},
	)
	require.NoError(t, err)

	_, err = client.DisableImage(t.Context(), &ec2sdk.DisableImageInput{ImageId: disabled.ImageId})
	require.NoError(t, err)

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds:        []string{aws.ToString(disabled.ImageId)},
		IncludeDisabled: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, out.Images, 1)
	assert.Equal(t, "disabled", string(out.Images[0].State))
}

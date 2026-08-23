package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestEnableFastLaunch_WireShape_RealClient covers handleEnableFastLaunch,
// which pre-fix rendered a bare <return>true</return> via stubResponse. The
// real EnableFastLaunchOutput has no Return member -- only imageId/
// resourceType/maxParallelLaunches/ownerId/state/... (ec2@v1.319.1
// deserializers.go, awsEc2query_deserializeOpDocumentEnableFastLaunchOutput
// has no case for "return") -- so a client checking which AMI it just
// enabled, or the resolved MaxParallelLaunches default, saw every field
// empty pre-fix.
func TestEnableFastLaunch_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	h.AccountID = "000000000000"
	client := newTestEC2Client(t, h)

	out, err := client.EnableFastLaunch(t.Context(), &ec2sdk.EnableFastLaunchInput{
		ImageId: aws.String("ami-sweep16"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ami-sweep16", aws.ToString(out.ImageId), "ImageId empty - never rendered pre-fix")
	assert.Equal(
		t, int32(6), aws.ToInt32(out.MaxParallelLaunches),
		"MaxParallelLaunches empty - real AWS default of 6 was never applied pre-fix",
	)
	assert.Equal(t, "snapshot", string(out.ResourceType))
	assert.Equal(t, "000000000000", aws.ToString(out.OwnerId))
}

// TestDisableFastLaunch_WireShape_RealClient covers handleDisableFastLaunch,
// same shape gap as Enable (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDisableFastLaunchOutput).
func TestDisableFastLaunch_WireShape_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	h.AccountID = "000000000000"
	client := newTestEC2Client(t, h)

	_, err := client.EnableFastLaunch(t.Context(), &ec2sdk.EnableFastLaunchInput{
		ImageId: aws.String("ami-sweep16b"),
	})
	require.NoError(t, err)

	out, err := client.DisableFastLaunch(t.Context(), &ec2sdk.DisableFastLaunchInput{
		ImageId: aws.String("ami-sweep16b"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ami-sweep16b", aws.ToString(out.ImageId), "ImageId empty - never rendered pre-fix")
	assert.Equal(t, "000000000000", aws.ToString(out.OwnerId))
}

// DeregisterImage (Category B, real Return plus missing DeleteSnapshotResults
// sibling) is NOT fixed here: DeleteSnapshotResults is only ever populated
// when the request's DeleteAssociatedSnapshots=true and a backing snapshot
// was actually deleted, but AMIStub (images.go) tracks no block-device-
// mapping/snapshot association at all. Adding a DeleteSnapshotResults field
// that's always empty would be byte-identical to today's response in every
// observable case, so it's left as-is; see PARITY.md for the real gap
// (AMI-to-snapshot association tracking).

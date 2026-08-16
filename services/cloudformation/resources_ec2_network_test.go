package cloudformation_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestResourceCreator_Extra_EC2Volume verifies a real EBS volume is created and deleted, and
// that Fn::GetAtt VolumeId returns the real physical ID.
func TestResourceCreator_Extra_EC2Volume(t *testing.T) {
	t.Parallel()

	backends := newDependentServiceBackends(t)
	rc := cloudformation.NewResourceCreator(backends)
	ctx := t.Context()
	ec2b, ok := backends.EC2.Backend.(*ec2backend.InMemoryBackend)
	require.True(t, ok)

	volPhys, err := rc.Create(ctx, "DataVol", "AWS::EC2::Volume",
		map[string]any{"AvailabilityZone": "us-east-1a", "Size": float64(20), "VolumeType": "gp3"}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, volPhys)

	vols := ec2b.DescribeVolumes([]string{volPhys})
	require.Len(t, vols, 1)
	assert.Equal(t, 20, vols[0].Size)

	// GetAtt VolumeId returns the physical volume ID.
	got := cloudformation.GetResourceAttribute("AWS::EC2::Volume", volPhys, "VolumeId", "000000000000", "us-east-1")
	assert.Equal(t, volPhys, got)

	require.NoError(t, rc.Delete(ctx, "AWS::EC2::Volume", volPhys, nil))
	assert.Empty(t, ec2b.DescribeVolumes([]string{volPhys}))
}

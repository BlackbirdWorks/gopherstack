package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParityFinal_SendDiagnosticInterrupt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	require.NoError(t, b.SendDiagnosticInterrupt(instances[0].ID))
	require.ErrorIs(t, b.SendDiagnosticInterrupt("i-missing"), ec2.ErrInstanceNotFound)
	require.ErrorIs(t, b.SendDiagnosticInterrupt(""), ec2.ErrInvalidParameter)
}

func TestParityFinal_DescribeElasticGpus(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.Empty(t, b.DescribeElasticGpus(nil))
}

// TestDescribeInstancesByVPC verifies secondary-index instance lookup.
func TestDescribeInstancesByVPC(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.RunInstances("ami-0c55b159cbfafe1f0", "t3.micro", "", 3)
	require.NoError(t, err)

	insts := b.DescribeInstancesByVPC("vpc-default")
	assert.Len(t, insts, 3)
}

// TestDescribeInstancesByVPC_EmptyOnNoInstances verifies no panics on empty VPC.

// TestDescribeInstancesByVPC_EmptyOnNoInstances verifies no panics on empty VPC.
func TestDescribeInstancesByVPC_EmptyOnNoInstances(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	insts := b.DescribeInstancesByVPC("vpc-nonexistent")
	assert.Nil(t, insts)
}

// TestDescribeInstanceTypeOfferings verifies the instance type list.
func TestDescribeInstanceTypeOfferings(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	offerings := b.DescribeInstanceTypeOfferings()

	assert.NotEmpty(t, offerings)

	var foundT3Micro bool

	for _, o := range offerings {
		if o.InstanceType == "t3.micro" {
			foundT3Micro = true

			assert.Equal(t, "availability-zone", o.LocationType)

			break
		}
	}

	assert.True(t, foundT3Micro, "t3.micro should be in offerings")
}

// TestCreateVpcPeeringConnection tests peering creation.

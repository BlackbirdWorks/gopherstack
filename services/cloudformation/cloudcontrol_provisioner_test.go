package cloudformation_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestProvisionableTypes(t *testing.T) {
	t.Parallel()

	types := cloudformation.ProvisionableTypes()
	require.Greater(t, len(types), 400)
	assert.True(t, slices.IsSorted(types))

	for _, name := range []string{"AWS::S3::Bucket", "AWS::IAM::User", "AWS::EC2::VPC"} {
		assert.Contains(t, types, name)
	}

	for _, name := range types {
		assert.False(t, strings.HasPrefix(name, "AWS::CloudFormation::"), name)
		assert.False(t, strings.HasPrefix(name, "AWS::Serverless::"), name)
	}
}

func TestCloudControlProvisioner_NoBackendsIsUnsupported(t *testing.T) {
	t.Parallel()

	_, err := newBackend().CloudControlProvisioner().Provision(t.Context(), "AWS::IAM::User", map[string]any{})
	require.ErrorContains(t, err, "UnsupportedActionException")
}

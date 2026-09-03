package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestVpcEncryptionControl_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	var vpc *ec2.VPC

	{
		v, err := b.CreateVpc("10.90.0.0/16", "default")
		require.NoError(t, err)

		vpc = v
	}

	t.Run("create requires vpc id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcEncryptionControl("", nil)
		require.Error(t, err)
	})

	t.Run("create fails for unknown vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcEncryptionControl("vpc-doesnotexist", nil)
		require.Error(t, err)
	})

	var controlID string

	t.Run("create succeeds with defaults", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vec, err := b.CreateVpcEncryptionControl(vpc.ID, map[string]string{"Name": "test"})
		require.NoError(t, err)
		assert.NotEmpty(t, vec.VpcEncryptionControlID)
		assert.Equal(t, vpc.ID, vec.VpcID)
		assert.Equal(t, "monitor", vec.Mode)
		assert.Equal(t, "available", vec.State)
		assert.Equal(t, "disabled", vec.ResourceExclusions.InternetGateway.State)
		assert.Equal(t, "test", vec.Tags["Name"])
		controlID = vec.VpcEncryptionControlID
	})

	t.Run("create rejects a second control for the vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcEncryptionControl(vpc.ID, nil)
		require.Error(t, err)
	})

	t.Run("describe by id and by vpc id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		byID := b.DescribeVpcEncryptionControls([]string{controlID}, nil)
		require.Len(t, byID, 1)
		assert.Equal(t, controlID, byID[0].VpcEncryptionControlID)

		byVpc := b.DescribeVpcEncryptionControls(nil, []string{vpc.ID})
		require.Len(t, byVpc, 1)
		assert.Equal(t, controlID, byVpc[0].VpcEncryptionControlID)

		assert.Empty(t, b.DescribeVpcEncryptionControls([]string{"vpc-ec-doesnotexist"}, nil))
	})

	t.Run("modify mode and exclusions", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vec, err := b.ModifyVpcEncryptionControl(controlID, "enforce", ec2.VpcEncryptionControlExclusionModify{
			InternetGateway: "enable",
			NatGateway:      "disable",
		})
		require.NoError(t, err)
		assert.Equal(t, "enforce", vec.Mode)
		assert.Equal(t, "enabled", vec.ResourceExclusions.InternetGateway.State)
		assert.Equal(t, "disabled", vec.ResourceExclusions.NatGateway.State)
	})

	t.Run("modify rejects invalid mode", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVpcEncryptionControl(controlID, "bogus", ec2.VpcEncryptionControlExclusionModify{})
		require.Error(t, err)
	})

	t.Run("modify unknown control errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVpcEncryptionControl(
			"vpc-ec-doesnotexist",
			"monitor",
			ec2.VpcEncryptionControlExclusionModify{},
		)
		require.Error(t, err)
	})

	t.Run("get blocking resources requires real vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.GetVpcResourcesBlockingEncryptionEnforcement("vpc-doesnotexist")
		require.Error(t, err)

		resources, err := b.GetVpcResourcesBlockingEncryptionEnforcement(vpc.ID)
		require.NoError(t, err)
		assert.Empty(t, resources)
	})

	t.Run("delete removes control", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vec, err := b.DeleteVpcEncryptionControl(controlID)
		require.NoError(t, err)
		assert.Equal(t, "deleted", vec.State)
		assert.Empty(t, b.DescribeVpcEncryptionControls([]string{controlID}, nil))
	})

	t.Run("delete unknown control errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteVpcEncryptionControl(controlID)
		require.Error(t, err)
	})

	t.Run("delete requires id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteVpcEncryptionControl("")
		require.Error(t, err)
	})
}

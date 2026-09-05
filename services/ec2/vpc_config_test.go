package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- VPC ClassicLink ----

func TestVpcClassicLink_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	var vpc *ec2.VPC

	var instanceID string

	{
		v, err := b.CreateVpc("10.50.0.0/16", "default")
		require.NoError(t, err)

		vpc = v

		insts, err := b.RunInstances("ami-123", "t3.micro", "", 1)
		require.NoError(t, err)

		instanceID = insts[0].ID
	}

	t.Run("attach requires an enabled vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		err := b.AttachClassicLinkVpc(instanceID, vpc.ID, nil)
		require.Error(t, err)
	})

	t.Run("enable vpc classic link", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableVpcClassicLink(vpc.ID))

		vpcs, err := b.DescribeVpcClassicLink([]string{vpc.ID})
		require.NoError(t, err)
		require.Len(t, vpcs, 1)
		assert.True(t, vpcs[0].ClassicLinkEnabled)
	})

	t.Run("attach rejects unknown security group", func(t *testing.T) { //nolint:paralleltest // existing issue.
		err := b.AttachClassicLinkVpc(instanceID, vpc.ID, []string{"sg-doesnotexist"})
		require.Error(t, err)
	})

	var groupID string

	t.Run("attach instance succeeds once enabled", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sg, err := b.CreateSecurityGroup("classic-link-sg", "classic link test sg", vpc.ID)
		require.NoError(t, err)

		groupID = sg.ID

		require.NoError(t, b.AttachClassicLinkVpc(instanceID, vpc.ID, []string{groupID}))

		links := b.DescribeClassicLinkInstances([]string{instanceID})
		require.Len(t, links, 1)
		assert.Equal(t, vpc.ID, links[0].VpcID)
		assert.Equal(t, []string{groupID}, links[0].Groups)
	})

	t.Run("disable fails while an instance is linked", func(t *testing.T) { //nolint:paralleltest // existing issue.
		err := b.DisableVpcClassicLink(vpc.ID)
		require.Error(t, err)
	})

	t.Run("detach then disable succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DetachClassicLinkVpc(instanceID, vpc.ID))
		assert.Empty(t, b.DescribeClassicLinkInstances(nil))
		require.NoError(t, b.DisableVpcClassicLink(vpc.ID))
	})

	t.Run("detach non-linked instance errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		err := b.DetachClassicLinkVpc(instanceID, vpc.ID)
		require.Error(t, err)
	})

	t.Run("describe unknown vpc errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DescribeVpcClassicLink([]string{"vpc-doesnotexist"})
		require.Error(t, err)
	})
}

func TestVpcClassicLinkDNSSupport_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	var vpc *ec2.VPC

	{
		v, err := b.CreateVpc("10.60.0.0/16", "default")
		require.NoError(t, err)

		vpc = v
	}

	t.Run("dns support defaults to disabled", func(t *testing.T) { //nolint:paralleltest // existing issue.
		vpcs, err := b.DescribeVpcClassicLinkDNSSupport([]string{vpc.ID})
		require.NoError(t, err)
		require.Len(t, vpcs, 1)
		assert.False(t, vpcs[0].ClassicLinkDNSSupported)
	})

	t.Run("enable then disable", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableVpcClassicLinkDNSSupport(vpc.ID))

		vpcs, err := b.DescribeVpcClassicLinkDNSSupport([]string{vpc.ID})
		require.NoError(t, err)
		assert.True(t, vpcs[0].ClassicLinkDNSSupported)

		require.NoError(t, b.DisableVpcClassicLinkDNSSupport(vpc.ID))

		vpcs, err = b.DescribeVpcClassicLinkDNSSupport([]string{vpc.ID})
		require.NoError(t, err)
		assert.False(t, vpcs[0].ClassicLinkDNSSupported)
	})

	t.Run("enable unknown vpc errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.EnableVpcClassicLinkDNSSupport("vpc-doesnotexist"))
	})
}

// ---- VPC Block Public Access ----

func TestVpcBlockPublicAccessOptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	t.Run("defaults to off", func(t *testing.T) { //nolint:paralleltest // existing issue.
		opts := b.DescribeVpcBlockPublicAccessOptions()
		assert.Equal(t, "off", opts.InternetGatewayBlockMode)
		assert.Equal(t, "123456789012", opts.AwsAccountID)
		assert.Equal(t, "us-east-1", opts.AwsRegion)
	})

	t.Run("modify to block-bidirectional", func(t *testing.T) { //nolint:paralleltest // existing issue.
		opts, err := b.ModifyVpcBlockPublicAccessOptions("block-bidirectional")
		require.NoError(t, err)
		assert.Equal(t, "block-bidirectional", opts.InternetGatewayBlockMode)
		assert.Equal(t, "update-complete", opts.State)

		opts = b.DescribeVpcBlockPublicAccessOptions()
		assert.Equal(t, "block-bidirectional", opts.InternetGatewayBlockMode)
	})

	t.Run("invalid mode errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVpcBlockPublicAccessOptions("bogus-mode")
		require.Error(t, err)
	})
}

func TestVpcBlockPublicAccessExclusion_CRUD(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	var vpc *ec2.VPC

	{
		v, err := b.CreateVpc("10.70.0.0/16", "default")
		require.NoError(t, err)

		vpc = v
	}

	var exclusionID string

	t.Run("create requires vpc or subnet", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcBlockPublicAccessExclusion("", "", "allow-bidirectional", nil)
		require.Error(t, err)
	})

	t.Run("create requires valid mode", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcBlockPublicAccessExclusion(vpc.ID, "", "bogus", nil)
		require.Error(t, err)
	})

	t.Run("create fails for unknown vpc", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateVpcBlockPublicAccessExclusion("vpc-doesnotexist", "", "allow-egress", nil)
		require.Error(t, err)
	})

	t.Run("create succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		excl, err := b.CreateVpcBlockPublicAccessExclusion(
			vpc.ID,
			"",
			"allow-egress",
			map[string]string{"Name": "test"},
		)
		require.NoError(t, err)
		assert.NotEmpty(t, excl.ExclusionID)
		assert.Equal(t, vpc.ID, excl.VpcID)
		assert.Equal(t, "allow-egress", excl.InternetGatewayExclusionMode)
		assert.Equal(t, "create-complete", excl.State)
		assert.Contains(t, excl.ResourceArn, excl.ExclusionID)
		assert.Equal(t, "test", b.TagsForResource(excl.ExclusionID)["Name"])
		exclusionID = excl.ExclusionID
	})

	t.Run("describe returns created exclusion", func(t *testing.T) { //nolint:paralleltest // existing issue.
		excls := b.DescribeVpcBlockPublicAccessExclusions([]string{exclusionID})
		require.Len(t, excls, 1)
		assert.Equal(t, exclusionID, excls[0].ExclusionID)
	})

	t.Run("describe all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.NotEmpty(t, b.DescribeVpcBlockPublicAccessExclusions(nil))
	})

	t.Run("modify updates mode", func(t *testing.T) { //nolint:paralleltest // existing issue.
		excl, err := b.ModifyVpcBlockPublicAccessExclusion(exclusionID, "allow-bidirectional")
		require.NoError(t, err)
		assert.Equal(t, "allow-bidirectional", excl.InternetGatewayExclusionMode)
		assert.Equal(t, "update-complete", excl.State)
	})

	t.Run("modify unknown exclusion errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.ModifyVpcBlockPublicAccessExclusion("vpcbpa-exclusion-doesnotexist", "allow-egress")
		require.Error(t, err)
	})

	t.Run("delete removes exclusion", func(t *testing.T) { //nolint:paralleltest // existing issue.
		excl, err := b.DeleteVpcBlockPublicAccessExclusion(exclusionID)
		require.NoError(t, err)
		assert.Equal(t, "delete-complete", excl.State)
		assert.Empty(t, b.DescribeVpcBlockPublicAccessExclusions([]string{exclusionID}))
	})

	t.Run("delete unknown exclusion errors", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.DeleteVpcBlockPublicAccessExclusion(exclusionID)
		require.Error(t, err)
	})
}

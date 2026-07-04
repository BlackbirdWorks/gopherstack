package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// newVerifiedAccessEndpointFixture creates a fresh backend with an instance,
// group, and endpoint of the given endpoint type for use by a single subtest.
func newVerifiedAccessEndpointFixture(t *testing.T, endpointType string) (*ec2.InMemoryBackend, string) {
	t.Helper()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, instErr := b.CreateVerifiedAccessInstance("policy test")
	require.NoError(t, instErr)
	grp, grpErr := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "policy group")
	require.NoError(t, grpErr)
	ep, epErr := b.CreateVerifiedAccessEndpoint(grp.VerifiedAccessGroupID, endpointType, "policy endpoint")
	require.NoError(t, epErr)

	return b, ep.VerifiedAccessEndpointID
}

func TestBackend_VerifiedAccessEndpointPolicy(t *testing.T) {
	t.Parallel()

	t.Run("default policy is disabled with no document", func(t *testing.T) {
		t.Parallel()

		b, epID := newVerifiedAccessEndpointFixture(t, "load-balancer")

		pol, getErr := b.GetVerifiedAccessEndpointPolicy(epID)
		require.NoError(t, getErr)
		assert.False(t, pol.PolicyEnabled)
		assert.Empty(t, pol.PolicyDocument)
	})

	t.Run("modify sets and get reflects policy", func(t *testing.T) {
		t.Parallel()

		b, epID := newVerifiedAccessEndpointFixture(t, "load-balancer")

		pol, modErr := b.ModifyVerifiedAccessEndpointPolicy(epID, true, `permit(principal,action,resource);`)
		require.NoError(t, modErr)
		assert.True(t, pol.PolicyEnabled)
		assert.Equal(t, `permit(principal,action,resource);`, pol.PolicyDocument)

		got, getErr := b.GetVerifiedAccessEndpointPolicy(epID)
		require.NoError(t, getErr)
		assert.True(t, got.PolicyEnabled)
		assert.Equal(t, `permit(principal,action,resource);`, got.PolicyDocument)
	})

	t.Run("unknown endpoint returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, getErr := b.GetVerifiedAccessEndpointPolicy("vae-doesnotexist")
		require.ErrorIs(t, getErr, ec2.ErrVerifiedAccessEndpointNotFound)

		_, modErr := b.ModifyVerifiedAccessEndpointPolicy("vae-doesnotexist", true, "doc")
		require.ErrorIs(t, modErr, ec2.ErrVerifiedAccessEndpointNotFound)
	})
}

// newVerifiedAccessGroupFixture creates a fresh backend with an instance and
// group for use by a single subtest.
func newVerifiedAccessGroupFixture(t *testing.T) (*ec2.InMemoryBackend, string) {
	t.Helper()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, instErr := b.CreateVerifiedAccessInstance("policy test")
	require.NoError(t, instErr)
	grp, grpErr := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "policy group")
	require.NoError(t, grpErr)

	return b, grp.VerifiedAccessGroupID
}

func TestBackend_VerifiedAccessGroupPolicy(t *testing.T) {
	t.Parallel()

	t.Run("default policy is disabled with no document", func(t *testing.T) {
		t.Parallel()

		b, grpID := newVerifiedAccessGroupFixture(t)

		pol, getErr := b.GetVerifiedAccessGroupPolicy(grpID)
		require.NoError(t, getErr)
		assert.False(t, pol.PolicyEnabled)
		assert.Empty(t, pol.PolicyDocument)
	})

	t.Run("modify sets and get reflects policy", func(t *testing.T) {
		t.Parallel()

		b, grpID := newVerifiedAccessGroupFixture(t)

		pol, modErr := b.ModifyVerifiedAccessGroupPolicy(grpID, true, `permit(principal,action,resource);`)
		require.NoError(t, modErr)
		assert.True(t, pol.PolicyEnabled)

		got, getErr := b.GetVerifiedAccessGroupPolicy(grpID)
		require.NoError(t, getErr)
		assert.True(t, got.PolicyEnabled)
		assert.Equal(t, `permit(principal,action,resource);`, got.PolicyDocument)
	})

	t.Run("unknown group returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, getErr := b.GetVerifiedAccessGroupPolicy("vagr-doesnotexist")
		require.ErrorIs(t, getErr, ec2.ErrVerifiedAccessGroupNotFound)

		_, modErr := b.ModifyVerifiedAccessGroupPolicy("vagr-doesnotexist", true, "doc")
		require.ErrorIs(t, modErr, ec2.ErrVerifiedAccessGroupNotFound)
	})
}

// newVerifiedAccessInstanceFixture creates a fresh backend with a single
// Verified Access instance for use by a single subtest.
func newVerifiedAccessInstanceFixture(t *testing.T) (*ec2.InMemoryBackend, string) {
	t.Helper()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	inst, instErr := b.CreateVerifiedAccessInstance("logging test")
	require.NoError(t, instErr)

	return b, inst.VerifiedAccessInstanceID
}

func TestBackend_VerifiedAccessInstanceLoggingConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("describe with no config set returns default disabled config", func(t *testing.T) {
		t.Parallel()

		b, instID := newVerifiedAccessInstanceFixture(t)

		cfgs := b.DescribeVerifiedAccessInstanceLoggingConfigurations([]string{instID})
		require.Len(t, cfgs, 1)
		assert.Equal(t, instID, cfgs[0].VerifiedAccessInstanceID)
		assert.False(t, cfgs[0].AccessLogs.IncludeTrustContext)
		assert.Nil(t, cfgs[0].AccessLogs.CloudWatchLogs)
	})

	t.Run("modify sets logging config and describe reflects it", func(t *testing.T) {
		t.Parallel()

		b, instID := newVerifiedAccessInstanceFixture(t)

		opts := ec2.VerifiedAccessLogOptions{
			LogVersion:          "ocsf-1.0.0-rc.2",
			IncludeTrustContext: true,
			CloudWatchLogs: &ec2.VerifiedAccessLogCloudWatchLogs{
				Enabled:  true,
				LogGroup: "my-log-group",
			},
			S3: &ec2.VerifiedAccessLogS3{
				Enabled:    true,
				BucketName: "my-bucket",
			},
		}

		cfg, modErr := b.ModifyVerifiedAccessInstanceLoggingConfiguration(instID, opts)
		require.NoError(t, modErr)
		assert.True(t, cfg.AccessLogs.IncludeTrustContext)
		require.NotNil(t, cfg.AccessLogs.CloudWatchLogs)
		assert.Equal(t, "my-log-group", cfg.AccessLogs.CloudWatchLogs.LogGroup)

		cfgs := b.DescribeVerifiedAccessInstanceLoggingConfigurations([]string{instID})
		require.Len(t, cfgs, 1)
		assert.True(t, cfgs[0].AccessLogs.CloudWatchLogs.Enabled)
		require.NotNil(t, cfgs[0].AccessLogs.S3)
		assert.Equal(t, "my-bucket", cfgs[0].AccessLogs.S3.BucketName)
	})

	t.Run("modify unknown instance returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, modErr := b.ModifyVerifiedAccessInstanceLoggingConfiguration(
			"vai-doesnotexist",
			ec2.VerifiedAccessLogOptions{},
		)
		require.ErrorIs(t, modErr, ec2.ErrVerifiedAccessInstanceNotFound)
	})
}

func TestBackend_GetVerifiedAccessEndpointTargets(t *testing.T) {
	t.Parallel()

	t.Run("existing endpoint returns empty target list", func(t *testing.T) {
		t.Parallel()

		b, epID := newVerifiedAccessEndpointFixture(t, "cidr")

		targets, getErr := b.GetVerifiedAccessEndpointTargets(epID)
		require.NoError(t, getErr)
		assert.Empty(t, targets)
	})

	t.Run("unknown endpoint returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, getErr := b.GetVerifiedAccessEndpointTargets("vae-doesnotexist")
		require.ErrorIs(t, getErr, ec2.ErrVerifiedAccessEndpointNotFound)
	})
}

func TestBackend_ExportVerifiedAccessInstanceClientConfiguration(t *testing.T) {
	t.Parallel()

	t.Run("existing instance returns generated config", func(t *testing.T) {
		t.Parallel()

		b, instID := newVerifiedAccessInstanceFixture(t)

		cfg, exportErr := b.ExportVerifiedAccessInstanceClientConfiguration(instID)
		require.NoError(t, exportErr)
		assert.Equal(t, instID, cfg.VerifiedAccessInstanceID)
		assert.Equal(t, "us-east-1", cfg.Region)
		assert.NotEmpty(t, cfg.Version)
	})

	t.Run("unknown instance returns not found", func(t *testing.T) {
		t.Parallel()

		b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

		_, exportErr := b.ExportVerifiedAccessInstanceClientConfiguration("vai-doesnotexist")
		require.ErrorIs(t, exportErr, ec2.ErrVerifiedAccessInstanceNotFound)
	})
}

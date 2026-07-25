package transfer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// validWebAppInput returns a minimal CreateWebAppInput satisfying the real-AWS
// required IdentityProviderDetails.IdentityCenterConfig field (InstanceArn + Role).
func validWebAppInput(tags map[string]string) *transfer.CreateWebAppInput {
	return &transfer.CreateWebAppInput{
		IdentityCenterConfig: &transfer.WebAppIdentityCenterConfig{
			InstanceArn: "arn:aws:sso:::instance/ssoins-1234567890abcdef0",
			Role:        "arn:aws:iam::123456789012:role/webapp-idp",
		},
		Tags: tags,
	}
}

// TestWebAppCountExport verifies WebAppCount export.
func TestWebAppCountExport(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	assert.Equal(t, 0, transfer.WebAppCount(b))

	_, err := b.CreateWebApp(validWebAppInput(nil))
	require.NoError(t, err)

	assert.Equal(t, 1, transfer.WebAppCount(b))
}

// TestAddWebAppInternal verifies the AddWebAppInternal seed helper.
func TestAddWebAppInternal(t *testing.T) {
	t.Parallel()

	b := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	b.AddWebAppInternal("webapp-test123")

	assert.Equal(t, 1, transfer.WebAppCount(b))
}

func TestCreateWebApp(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	w, err := b.CreateWebApp(validWebAppInput(map[string]string{"env": "prod"}))
	require.NoError(t, err)
	assert.NotEmpty(t, w.WebAppID)
	assert.NotEmpty(t, w.WebAppEndpoint, "WebAppEndpoint should be synthesized")
	assert.Equal(t, w.WebAppEndpoint, w.AccessEndpoint, "AccessEndpoint defaults to WebAppEndpoint")
	assert.Equal(t, "STANDARD", w.WebAppEndpointPolicy)
	require.NotNil(t, w.IdentityCenterConfig)
	assert.NotEmpty(t, w.IdentityCenterConfig.ApplicationArn, "ApplicationArn is assigned by AWS")
	assert.Equal(t, "arn:aws:sso:::instance/ssoins-1234567890abcdef0", w.IdentityCenterConfig.InstanceArn)
}

// TestCreateWebApp_RequiresIdentityCenterConfig verifies that real AWS's required
// CreateWebAppInput.IdentityProviderDetails field is enforced.
func TestCreateWebApp_RequiresIdentityCenterConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.CreateWebApp(&transfer.CreateWebAppInput{})
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrInvalidParameter)
}

// TestCreateWebApp_VpcConfig verifies VPC-hosted web apps get a synthesized
// VpcEndpointId and report EndpointType VPC via the backend model.
func TestCreateWebApp_VpcConfig(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	in := validWebAppInput(nil)
	in.VpcConfig = &transfer.WebAppVpcConfig{
		SubnetIDs:        []string{"subnet-1", "subnet-2"},
		SecurityGroupIDs: []string{"sg-1"},
		VpcID:            "vpc-1",
	}

	w, err := b.CreateWebApp(in)
	require.NoError(t, err)
	require.NotNil(t, w.VpcConfig)
	assert.NotEmpty(t, w.VpcConfig.VpcEndpointID, "VpcEndpointId is assigned by AWS")
	assert.Equal(t, []string{"subnet-1", "subnet-2"}, w.VpcConfig.SubnetIDs)
}

// TestUpdateWebApp_PartialFieldsOnly verifies UpdateWebApp only mutates the fields
// real AWS allows updating (AccessEndpoint, VPC subnets, IdentityCenter Role,
// WebAppUnits) and leaves everything else -- including the immutable InstanceArn --
// unchanged.
func TestUpdateWebApp_PartialFieldsOnly(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	w, err := b.CreateWebApp(validWebAppInput(nil))
	require.NoError(t, err)

	newRole := "arn:aws:iam::123456789012:role/updated-role"
	updated, err := b.UpdateWebApp(&transfer.UpdateWebAppInput{
		WebAppID:           w.WebAppID,
		IdentityCenterRole: &newRole,
	})
	require.NoError(t, err)
	assert.Equal(t, newRole, updated.IdentityCenterConfig.Role)
	assert.Equal(t, w.IdentityCenterConfig.InstanceArn, updated.IdentityCenterConfig.InstanceArn,
		"InstanceArn is not updatable via UpdateWebApp in real AWS")
}

// TestUpdateWebApp_NotFound verifies ResourceNotFoundException semantics.
func TestUpdateWebApp_NotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	_, err := b.UpdateWebApp(&transfer.UpdateWebAppInput{WebAppID: "webapp-missing"})
	require.Error(t, err)
	assert.ErrorIs(t, err, awserr.ErrNotFound)
}

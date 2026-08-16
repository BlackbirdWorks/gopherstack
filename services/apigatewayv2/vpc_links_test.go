package apigatewayv2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestInMemoryBackend_VpcLinks(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	// CreateVpcLink.
	vl, err := b.CreateVpcLink(apigatewayv2.CreateVpcLinkInput{
		Name:             "my-vpc-link",
		SubnetIDs:        []string{"subnet-aaa", "subnet-bbb"},
		SecurityGroupIDs: []string{"sg-111"},
		Tags:             map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-vpc-link", vl.Name)
	assert.Equal(t, "AVAILABLE", vl.VpcLinkStatus)
	assert.NotEmpty(t, vl.VpcLinkID)
	assert.Equal(t, []string{"subnet-aaa", "subnet-bbb"}, vl.SubnetIDs)
	assert.Equal(t, []string{"sg-111"}, vl.SecurityGroupIDs)

	// GetVpcLink.
	got, err := b.GetVpcLink(vl.VpcLinkID)
	require.NoError(t, err)
	assert.Equal(t, vl.VpcLinkID, got.VpcLinkID)

	// GetVpcLinks.
	all, err := b.GetVpcLinks()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// UpdateVpcLink.
	upd, err := b.UpdateVpcLink(vl.VpcLinkID, apigatewayv2.UpdateVpcLinkInput{Name: "updated-link"})
	require.NoError(t, err)
	assert.Equal(t, "updated-link", upd.Name)

	// DeleteVpcLink.
	err = b.DeleteVpcLink(vl.VpcLinkID)
	require.NoError(t, err)

	_, err = b.GetVpcLink(vl.VpcLinkID)
	require.ErrorIs(t, err, apigatewayv2.ErrVpcLinkNotFound)
}

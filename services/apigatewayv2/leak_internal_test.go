package apigatewayv2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDeletePortalProduct_CleansSharingPolicy verifies that deleting a portal
// product also removes its sharing-policy entry, so the internal map does not
// leak a dangling policy document for a product that no longer exists.
func TestDeletePortalProduct_CleansSharingPolicy(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()

	product, err := b.CreatePortalProduct(CreatePortalProductInput{DisplayName: "leaky"})
	require.NoError(t, err)

	_, err = b.PutPortalProductSharingPolicy(product.PortalProductID, `{"Version":"2012-10-17"}`)
	require.NoError(t, err)
	require.Len(t, b.portalProductSharingPolicies, 1)

	require.NoError(t, b.DeletePortalProduct(product.PortalProductID))

	require.Empty(t, b.portalProductSharingPolicies,
		"sharing policy entry must be cleaned up when the product is deleted")
}

package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestDeletePortal_NonExistent_IsIdempotent drives DeletePortal for a portal
// that was never created through the real aws-sdk-go-v2 client.
// apigatewayv2@v1.37.4's own deserializeOpErrorDeletePortal has no
// NotFoundException case (unlike GetPortal/UpdatePortal/DisablePortal/
// DeletePortalProduct, which all model it), so before the fix a real client
// got an untyped smithy.GenericAPIError instead of succeeding or a typed
// exception -- neither errors.As into a concrete exception type nor a clean
// nil error was possible. The fix makes the delete idempotent, matching that
// asymmetry: DeletePortal must return no error at all for a missing portal.
func TestDeletePortal_NonExistent_IsIdempotent(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

	_, err := client.DeletePortal(t.Context(), &apigatewayv2sdk.DeletePortalInput{
		PortalId: aws.String("nonexistent-portal"),
	})

	require.NoError(t, err)
}

package networkmanager_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestHandler_OversizedBodySurfacesInternalServerException drives a real
// Network Manager client's CreateGlobalNetwork with a Description large
// enough to push the request body past httputils.MaxRequestBodyBytes (a
// real client can legitimately send this; aws-sdk-go-v2 imposes no
// client-side cap). CreateGlobalNetwork is used rather than a List op
// because List pagination fields serialize into the query string, not the
// body -- an oversized query string trips Go's net/http server's own 431
// header-size limit before the request ever reaches Handler(), which would
// mask the bug this test targets.
//
// Before this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the restJson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of the typed InternalServerException classifyError's default
// branch already produces for every unmatched backend error
// (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{
		Description: huge,
	}, func(o *networkmanagersdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

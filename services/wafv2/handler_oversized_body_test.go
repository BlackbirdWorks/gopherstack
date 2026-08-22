package wafv2_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wafv2sdk "github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

// TestHandler_OversizedBodySurfacesWAFInternalErrorException drives a real
// wafv2 client's CreateIPSet with a Description large enough to push the
// request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the awsjson1.1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, which
// awsAwsjson11_deserializeOpError calls) cannot parse plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of the
// typed WAFInternalErrorException handleError's default branch already
// produces for every unmatched backend error (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesWAFInternalErrorException(t *testing.T) {
	t.Parallel()

	backend := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestWAFV2Client(t, wafv2.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateIPSet(t.Context(), &wafv2sdk.CreateIPSetInput{
		Name:             aws.String("oversized-body-ipset"),
		Scope:            types.ScopeRegional,
		IPAddressVersion: types.IPAddressVersionIpv4,
		Addresses:        []string{"192.0.2.0/24"},
		Description:      aws.String(huge),
	}, func(o *wafv2sdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "WAFInternalErrorException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

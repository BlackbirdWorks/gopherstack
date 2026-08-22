package outposts_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestHandler_OversizedBodySurfacesInternalServerException drives a real
// Outposts client's CreateOutpost with a Description large enough to push
// the request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the restJson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of the typed InternalServerException handleError's default
// branch already produces for every unmatched backend error
// (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.CreateOutpost(t.Context(), &outpostssdk.CreateOutpostInput{
		Name:        aws.String("oversized-body-outpost"),
		SiteId:      aws.String("os-0123456789abcdef0"),
		Description: huge,
	}, func(o *outpostssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

package personalize_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/personalizeruntime"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// TestHandleRuntimeREST_OversizedBodySurfacesInternalServerException drives
// a real personalizeruntime client's GetRecommendations with a FilterArn
// large enough to push the request body past
// httputils.MaxRequestBodyBytes (a real client can legitimately send this;
// aws-sdk-go-v2 imposes no client-side cap). Before this fix,
// handleRuntimeREST's ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- personalizeruntime's REST-JSON1
// error decoder reads the error code from the X-Amzn-ErrorType response
// header (personalizeruntime@v1.36.4 deserializers.go's
// awsRestjson1_deserializeOpErrorGetRecommendations), which a bare text/plain
// body never sets, so the client saw
// smithy.GenericAPIError{Code:"UnknownError"} instead of the typed
// InternalServerException handleRuntimeRESTError's default branch already
// produces for every unmatched dispatch error (gopherstack-o7gx).
func TestHandleRuntimeREST_OversizedBodySurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	backend := personalize.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestPersonalizeRuntimeClient(t, personalize.NewHandler(backend))

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.GetRecommendations(t.Context(), &personalizeruntime.GetRecommendationsInput{
		CampaignArn: aws.String("arn:aws:personalize:us-east-1:123456789012:campaign/x"),
		FilterArn:   huge,
	}, func(o *personalizeruntime.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

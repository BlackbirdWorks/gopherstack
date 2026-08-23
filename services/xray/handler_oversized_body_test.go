package xray_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	xraysdk "github.com/aws/aws-sdk-go-v2/service/xray"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real xray
// client's CreateGroup with a FilterExpression large enough to push the
// request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the restjson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}.
// handleError's default branch now produces "InternalFailure" for every
// unmatched backend error: xray@v1.39.4 types/errors.go models no 5xx
// exception at all, so "InternalServiceError" (another service's modeled
// type, e.g. transfer@v1.75.4 types/errors.go:82) was never this service's
// code; InternalFailure is the generic AWS-wide fallback (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	client := newTestXRayClient(t)

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateGroup(t.Context(), &xraysdk.CreateGroupInput{
		GroupName:        aws.String("oversized-body-group"),
		FilterExpression: aws.String(huge),
	}, func(o *xraysdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

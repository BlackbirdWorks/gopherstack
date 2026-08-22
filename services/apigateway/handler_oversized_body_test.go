package apigateway_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// oversizedAPIGWBody is one byte past httputils.MaxRequestBodyBytes, so a
// real client's request legitimately overflows httputils.ReadBody's cap
// (aws-sdk-go-v2 sets no client-side limit of its own for these operations).
const oversizedAPIGWBody = httputils.MaxRequestBodyBytes + 1

// TestHandleRESTAPI_OversizedBodySurfacesInternalFailure drives a real
// apigateway client's CreateRestApi (POST /restapis, JSON body) with a
// Description large enough to push the request body past
// httputils.MaxRequestBodyBytes. Before this fix, handleRESTAPI's
// ReadBody-failure branch (handler.go, formerly line 481) wrote a bare
// "internal server error" text/plain body -- the REST-JSON1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, driven from
// deserializers.go via the header/body __type-or-code lookup at
// apigateway@v1.42.4 deserializers.go:93-120) cannot read plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"} instead of the real
// code -- the same asymmetry already fixed for handleJSONProtocol in
// gopherstack-wlo1/c6554e9f8 (handleRESTAPI is "a different site" per
// gopherstack-o7gx).
func TestHandleRESTAPI_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClient(t, apigateway.NewHandler(backend))

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(oversizedAPIGWBody))))

	_, err := client.CreateRestApi(t.Context(), &apigatewaysdk.CreateRestApiInput{
		Name:        aws.String("oversized-body-api"),
		Description: huge,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestDispatchRestAPISpec_OversizedBodySurfacesInternalFailure drives a real
// apigateway client's ImportRestApi (POST /restapis?mode=import, raw
// application/octet-stream body -- apigateway@v1.42.4 serializers.go:7874,
// 7899: the request body is input.Body verbatim, no JSON envelope) with a
// Body past httputils.MaxRequestBodyBytes, reaching
// dispatchRestAPISpec's own ReadBody call (handler_import_export.go,
// formerly line 189), which had the identical bare-text bug.
func TestDispatchRestAPISpec_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClient(t, apigateway.NewHandler(backend))

	huge := bytes.Repeat([]byte("x"), int(oversizedAPIGWBody))

	_, err := client.ImportRestApi(t.Context(), &apigatewaysdk.ImportRestApiInput{Body: huge})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

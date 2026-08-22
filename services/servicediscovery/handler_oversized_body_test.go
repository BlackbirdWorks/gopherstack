package servicediscovery_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real
// servicediscovery client's CreateService with a Description large enough
// to push the request body past httputils.MaxRequestBodyBytes (a real
// client can legitimately send this; aws-sdk-go-v2 imposes no client-side
// cap). Before this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the awsjson1.1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, which
// awsAwsjson11_deserializeOpError calls) cannot parse plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"}. handleError's
// default branch now produces "InternalFailure" for every unmatched backend
// error: servicediscovery@v1.43.4 types/errors.go models no 5xx exception at
// all, so "InternalServiceError" (another service's modeled type, e.g.
// secretsmanager@v1.44.4 types/errors.go:85) was never this service's code;
// InternalFailure is the generic AWS-wide fallback (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := servicediscovery.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestServiceDiscoveryClient(t, servicediscovery.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateService(t.Context(), &sdsdk.CreateServiceInput{
		Name:        aws.String("oversized-body-service"),
		Description: aws.String(huge),
	}, func(o *sdsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

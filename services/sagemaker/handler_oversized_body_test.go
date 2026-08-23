package sagemaker_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real sagemaker
// client's CreateModelPackageGroup with a description large enough to push
// the request body past httputils.MaxRequestBodyBytes (a real client can
// legitimately send this; aws-sdk-go-v2 imposes no client-side cap). Before
// this fix, Handler()'s ReadBody-failure branch wrote a bare
// "internal server error" text/plain body -- the awsjson1.1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo, which
// awsAwsjson11_deserializeOpError calls) cannot parse plain text, so the
// client saw smithy.GenericAPIError{Code:"UnknownError"} (gopherstack-o7gx).
// sagemaker@v1.263.2 types/errors.go models only
// ConflictException/ResourceInUse/ResourceLimitExceeded/ResourceNotFound --
// no generic internal-failure exception exists to reuse, so the fix sends
// the standard unmodeled "InternalFailure" code instead of inventing a new
// exception type; this handler's own genuine default fallback (a separate,
// deliberately untyped path for a different reason -- no single wire
// exception fits every unmatched backend error either -- is left alone by
// this fix, which only touches the ReadBody-failure branch).
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateModelPackageGroup(t.Context(), &sagemakersdk.CreateModelPackageGroupInput{
		ModelPackageGroupName:        aws.String("oversized-body-group"),
		ModelPackageGroupDescription: aws.String(huge),
	}, func(o *sagemakersdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

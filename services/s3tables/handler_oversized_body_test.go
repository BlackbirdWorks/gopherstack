package s3tables_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

// TestHandler_OversizedBodySurfacesInternalError drives a real s3tables
// client's CreateTableBucket with a Name large enough to push the request
// body past httputils.MaxRequestBodyBytes (a real client can legitimately
// send this; aws-sdk-go-v2 imposes no client-side cap). Before this fix,
// Handler()'s ReadBody-failure branch wrote a bare "internal server error"
// text/plain body -- the restjson1 error decoder (aws-sdk-go-v2@v1.43.4
// aws/protocol/restjson.GetErrorInfo) cannot parse plain text, so the client
// saw smithy.GenericAPIError{Code:"UnknownError"} instead of the typed
// InternalError handleError's default branch already produces for every
// unmatched backend error (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalError(t *testing.T) {
	t.Parallel()

	backend := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestS3TablesClient(t, s3tables.NewHandler(backend))

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateTableBucket(t.Context(), &s3tablessdk.CreateTableBucketInput{
		Name: aws.String(huge),
	}, func(o *s3tablessdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalError", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

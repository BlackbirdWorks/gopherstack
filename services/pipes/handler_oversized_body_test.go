package pipes_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestHandler_OversizedBodySurfacesInternalException drives a real Pipes
// client's CreatePipe with a Description large enough to push the request
// body past httputils.MaxRequestBodyBytes (a real client can legitimately
// send this; aws-sdk-go-v2 imposes no client-side cap). Before this fix,
// Handler()'s ReadBody-failure branch wrote a bare "internal server error"
// text/plain body -- the restJson1 error decoder
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) cannot parse
// plain text, so the client saw smithy.GenericAPIError{Code:"UnknownError"}
// instead of the typed InternalException handleError's default branch
// already produces for every unmatched backend error (gopherstack-o7gx).
func TestHandler_OversizedBodySurfacesInternalException(t *testing.T) {
	t.Parallel()

	backend := pipes.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestPipesClient(t, pipes.NewHandler(backend))

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.CreatePipe(t.Context(), &pipessdk.CreatePipeInput{
		Name:        aws.String("oversized-body-pipe"),
		RoleArn:     aws.String("arn:aws:iam::123456789012:role/pipes-role"),
		Source:      aws.String("arn:aws:sqs:us-east-1:123456789012:src"),
		Target:      aws.String("arn:aws:sqs:us-east-1:123456789012:dst"),
		Description: huge,
	}, func(o *pipessdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

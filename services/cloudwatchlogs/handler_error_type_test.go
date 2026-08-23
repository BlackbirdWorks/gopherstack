package cloudwatchlogs_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// corruptJSONBody replaces the outgoing request body with syntactically
// invalid JSON in a Finalize middleware, after the SDK's own client-side
// input validation has already passed a well-formed input through
// serialization. A spec-compliant generated client can never organically
// send malformed JSON (it only ever serializes its own valid typed input),
// so this stands in for the wire-level malformation that reaches
// handleError's default branch: cloudwatchlogs's handleError has no
// json.SyntaxError/json.UnmarshalTypeError case of its own (unlike e.g.
// ecr's or timestreamwrite's), so any body the dispatch layer can't
// unmarshal falls straight to default.
func corruptJSONBody(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
		"corruptJSONBody",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				bad := []byte(`{"logGroupName": not-valid-json`)

				corrupted, err := req.SetStream(bytes.NewReader(bad))
				if err == nil {
					corrupted.ContentLength = int64(len(bad))
					in.Request = corrupted
				}
			}

			return next.HandleFinalize(ctx, in)
		},
	), middleware.After)
}

// TestCreateLogGroup_MalformedBodySurfacesServiceUnavailableException drives
// CreateLogGroup then corrupts the outgoing JSON body to invalid syntax via
// corruptJSONBody, forcing dispatch's json.Unmarshal to fail with an error
// none of handleError's sentinel cases match. Before this fix, the default
// branch wrote "InternalServerError". cloudwatchlogs@v1.81.1 does model an
// "InternalServerException" (types/errors.go:116), but its own deserializer
// only wires that code for 9 of 118 operations; "ServiceUnavailableException"
// (types/errors.go, ErrorFault: FaultServer) is the one wired into 101 of
// 118 -- including CreateLogGroup's own deserializeOpErrorCreateLogGroup --
// making it the real dominant 5xx fault for this service, not
// InternalServerException (gopherstack-o7gx).
func TestCreateLogGroup_MalformedBodySurfacesServiceUnavailableException(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))

	_, err := client.CreateLogGroup(
		t.Context(),
		&cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("valid-group")},
		func(o *cwlsdk.Options) {
			o.APIOptions = append(o.APIOptions, corruptJSONBody)
			o.RetryMaxAttempts = 1
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ServiceUnavailableException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())

	var sue *cwltypes.ServiceUnavailableException
	require.ErrorAs(t, err, &sue, "client must map this to the modeled ServiceUnavailableException type")
	assert.Equal(t, smithy.FaultServer, sue.ErrorFault())
}

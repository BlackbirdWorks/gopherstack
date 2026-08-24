package batch_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/batch"
)

// TestHandler_OversizedBodySurfacesServerException drives a real batch client's
// CreateComputeEnvironment with a ComputeEnvironmentName large enough to push
// the request body past httputils.MaxRequestBodyBytes -- a real client can
// legitimately send this; batch's validators.go performs no client-side length
// check on ComputeEnvironmentName, only a required-ness check. Before this fix,
// Handler()'s ReadBody-failure branch wrote "InternalFailure", a code absent
// from every one of batch@v1.68.4's 44 deserializeOpError switches (which model
// only ClientException and ServerException), so the client saw
// smithy.GenericAPIError{Code:"InternalFailure"} instead of the typed
// *types.ServerException.
func TestHandler_OversizedBodySurfacesServerException(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateComputeEnvironment(t.Context(), &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(huge),
		Type:                   types.CETypeManaged,
	}, func(o *batchsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ServerException", apiErr.ErrorCode())
	assert.NotEqual(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())

	var se *types.ServerException
	require.ErrorAs(t, err, &se, "client must map this to the modeled ServerException type")
	assert.Equal(t, smithy.FaultServer, se.ErrorFault())
}

package fis_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	"github.com/aws/aws-sdk-go-v2/service/fis/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/fis"
)

// TestSDK_CreateExperimentTemplate_OversizedBody_TypedValidationException
// drives the real fis client's CreateExperimentTemplate with a body large
// enough to fail httputils.ReadBody. Before the fix, writeError
// (services/fis/handler.go) called writeTypedError with an empty errType,
// which errorResponseDTO's `json:"__type,omitempty"` tag then drops
// entirely -- restjson.GetErrorInfo (aws-sdk-go-v2 aws/protocol/restjson/
// decoder_util.go:15) finds no code in the header (fis sets none) or the
// body, so every malformed-request and dispatch-miss site decoded as
// smithy.GenericAPIError{Code:"UnknownError"} rather than the ValidationException/
// ResourceNotFoundException fis's own handler.go classifyError already
// produces for the equivalent backend error.
func TestSDK_CreateExperimentTemplate_OversizedBody_TypedValidationException(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend("123456789012", "us-east-1")
	h := fis.NewHandler(backend)
	client, _ := newTestFISClient(t, h)

	huge := string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1)))

	_, err := client.CreateExperimentTemplate(t.Context(), &fissdk.CreateExperimentTemplateInput{
		ClientToken: aws.String("token-1"),
		Description: aws.String(huge),
		Actions:     map[string]types.CreateExperimentTemplateActionInput{},
		StopConditions: []types.CreateExperimentTemplateStopConditionInput{
			{Source: aws.String("none")},
		},
		RoleArn: aws.String("arn:aws:iam::123456789012:role/test"),
	}, func(o *fissdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalServerError", apiErr.ErrorCode(),
		"ReadBody failure maps to http.StatusInternalServerError in handler.go")
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestSDK_GetExperimentTemplate_NotFound_TypedResourceNotFoundException
// proves the dispatch-miss/not-found path (handler.go's writeError calls at
// StatusNotFound) now carries a code the real client can decode into
// fis's own modeled ResourceNotFoundException, not UnknownError.
func TestSDK_GetExperimentTemplate_NotFound_TypedResourceNotFoundException(t *testing.T) {
	t.Parallel()

	backend := fis.NewInMemoryBackend("123456789012", "us-east-1")
	h := fis.NewHandler(backend)
	client, _ := newTestFISClient(t, h)

	_, err := client.GetExperimentTemplate(t.Context(), &fissdk.GetExperimentTemplateInput{
		Id: aws.String("no-such-template"),
	})
	require.Error(t, err)

	var target *types.ResourceNotFoundException
	require.ErrorAs(t, err, &target,
		"expected a typed ResourceNotFoundException from the SDK deserializer, got: %v", err)
	require.NotEmpty(t, target.ErrorMessage())
}

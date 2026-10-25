package sagemakerruntime_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakerruntimesdk "github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

func TestInvoke_NoContainerBackend_NeverFabricatesOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
	}{
		{name: "sync", suffix: "/invocations"},
		{name: "stream", suffix: "/invocations-response-stream"},
		{name: "async", suffix: "/async-invocations"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1"))
			rec := doRequestWithHeaders(t, h, http.MethodPost, "/endpoints/ep"+tt.suffix, map[string]any{"a": 1}, nil)

			require.Equal(t, http.StatusServiceUnavailable, rec.Code)

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "ServiceUnavailable", body["__type"])
			assert.NotContains(t, rec.Body.String(), "mock")
			assert.Empty(t, h.Backend.ListInvocations(), "a failed invoke must not be recorded")
		})
	}
}

func TestInvoke_ErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		suffix   string
		wantType string
		wantCode int
	}{
		{name: "model_error", endpoint: "model-error", suffix: "/invocations", wantCode: 424, wantType: "ModelError"},
		{
			name:     "model_error_stream",
			endpoint: "model-error",
			suffix:   "/invocations-response-stream",
			wantCode: 424,
			wantType: "ModelError",
		},
		{
			name:     "engine_off",
			endpoint: "engine-off",
			suffix:   "/invocations",
			wantCode: 503,
			wantType: "ServiceUnavailable",
		},
		{
			name:     "not_async",
			endpoint: "not-async",
			suffix:   "/async-invocations",
			wantCode: 400,
			wantType: "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t,
				h,
				http.MethodPost,
				"/endpoints/"+tt.endpoint+tt.suffix,
				map[string]any{"a": 1},
				nil,
			)

			require.Equal(t, tt.wantCode, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantType, body["__type"])
		})
	}
}

func TestSDKInvokeEndpoint_ModelErrorShape(t *testing.T) {
	t.Parallel()

	client := newTestSDKClient(t, newTestHandler(t))

	_, err := client.InvokeEndpoint(t.Context(), &sagemakerruntimesdk.InvokeEndpointInput{
		EndpointName: aws.String("model-error"), Body: []byte("x"),
	})

	var me *types.ModelError

	require.ErrorAs(t, err, &me)
	assert.Equal(t, int32(500), aws.ToInt32(me.OriginalStatusCode))
	assert.Equal(t, "boom", aws.ToString(me.OriginalMessage))
	assert.NotEmpty(t, aws.ToString(me.LogStreamArn))
}

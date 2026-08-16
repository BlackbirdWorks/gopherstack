package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestHandler_Transformer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo)
		body     map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutTransformer/OK",
			action: "PutTransformer",
			body: map[string]any{
				"logGroupIdentifier": "/aws/lambda/fn",
				"transformerConfig":  []map[string]any{{"parseJSON": map[string]any{}}},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "PutTransformer/EmptyIdentifier",
			action: "PutTransformer",
			body: map[string]any{
				"logGroupIdentifier": "",
				"transformerConfig":  []map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "GetTransformer/OK",
			action: "GetTransformer",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutTransformer",
					`{"logGroupIdentifier":"/aws/lambda/fn","transformerConfig":[{"parseJSON":{}}]}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "GetTransformer/NotFound",
			action:   "GetTransformer",
			body:     map[string]any{"logGroupIdentifier": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "DeleteTransformer/OK",
			action: "DeleteTransformer",
			body:   map[string]any{"logGroupIdentifier": "/aws/lambda/fn"},
			setup: func(t *testing.T, h *cloudwatchlogs.Handler, e *echo.Echo) {
				t.Helper()
				doLogsRequest(t, h, e, "PutTransformer",
					`{"logGroupIdentifier":"/aws/lambda/fn","transformerConfig":[{"parseJSON":{}}]}`)
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteTransformer/NotFound",
			action:   "DeleteTransformer",
			body:     map[string]any{"logGroupIdentifier": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:   "TestTransformer/OK",
			action: "TestTransformer",
			body: map[string]any{
				"logGroupIdentifier": "/grp",
				"logEventMessages":   []string{`{"level":"ERROR","msg":"oops"}`},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h, e)
			}

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)
			rec := doLogsRequest(t, h, e, tt.action, string(bodyBytes))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_GetTransformer_Timestamps proves GetTransformer echoes
// CreationTime/LastModifiedTime -- real GetTransformerOutput members
// (api_op_GetTransformer.go) the backend already tracks as
// Transformer.CreatedAt but previously never emitted, so a real SDK
// client's typed fields were always nil regardless of what PutTransformer
// had stored.
func TestHandler_GetTransformer_Timestamps(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	h := cloudwatchlogs.NewHandler(backend)
	client := newTestCloudWatchLogsClient(t, h)
	ctx := t.Context()

	_, err := client.PutTransformer(ctx, &cwlsdk.PutTransformerInput{
		LogGroupIdentifier: aws.String("/aws/lambda/ts-fn"),
		TransformerConfig:  []cwltypes.Processor{{ParseJSON: &cwltypes.ParseJSON{}}},
	})
	require.NoError(t, err)

	out, err := client.GetTransformer(ctx, &cwlsdk.GetTransformerInput{
		LogGroupIdentifier: aws.String("/aws/lambda/ts-fn"),
	})
	require.NoError(t, err)
	assert.NotNil(t, out.CreationTime, "CreationTime must be populated, not left nil")
	assert.NotNil(t, out.LastModifiedTime, "LastModifiedTime must be populated, not left nil")
	assert.Positive(t, aws.ToInt64(out.CreationTime))
	assert.Positive(t, aws.ToInt64(out.LastModifiedTime))
	assert.Len(t, out.TransformerConfig, 1)
}

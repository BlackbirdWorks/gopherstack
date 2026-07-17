package cloudwatchlogs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

package iotanalytics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_LoggingOptions(t *testing.T) {
	t.Parallel()

	type testCase struct {
		loggingOpts  any
		name         string
		wantDescribe int
		wantPut      int
		putFirst     bool
	}

	tests := []testCase{
		{
			name:         "describe_not_set",
			putFirst:     false,
			wantDescribe: http.StatusNotFound,
		},
		{
			name:     "put_and_describe",
			putFirst: true,
			loggingOpts: map[string]any{
				"loggingOptions": map[string]any{
					"roleArn": "arn:aws:iam::000000000000:role/test-role",
					"level":   "ERROR",
					"enabled": true,
				},
			},
			wantPut:      http.StatusNoContent,
			wantDescribe: http.StatusOK,
		},
		{
			name:        "put_invalid_body",
			putFirst:    true,
			loggingOpts: "not-json",
			wantPut:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.putFirst && tt.loggingOpts != nil {
				putRec := doRequest(t, h, http.MethodPut, "/logging", tt.loggingOpts)
				assert.Equal(t, tt.wantPut, putRec.Code)
			}

			if tt.wantDescribe != 0 {
				descRec := doRequest(t, h, http.MethodGet, "/logging", nil)
				assert.Equal(t, tt.wantDescribe, descRec.Code)

				if tt.wantDescribe == http.StatusOK {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
					opts, ok := resp["loggingOptions"].(map[string]any)
					require.True(t, ok)
					assert.Equal(t, "ERROR", opts["level"])
					assert.Equal(t, true, opts["enabled"])
				}
			}
		})
	}
}

// TestHandler_PutLoggingOptions_Validation verifies roleArn and level validation.
func TestHandler_PutLoggingOptions_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid_error_level",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"roleArn": "arn:aws:iam::000000000000:role/test",
					"level":   "ERROR",
					"enabled": true,
				},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "invalid_level_rejected",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"roleArn": "arn:aws:iam::000000000000:role/test",
					"level":   "DEBUG",
					"enabled": true,
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "enabled_without_role_arn_rejected",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"level":   "ERROR",
					"enabled": true,
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "disabled_without_role_arn_ok",
			body: map[string]any{
				"loggingOptions": map[string]any{
					"level":   "ERROR",
					"enabled": false,
				},
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/logging", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

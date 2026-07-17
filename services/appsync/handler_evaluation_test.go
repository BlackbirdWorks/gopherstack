package appsync_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_EvaluateMappingTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "valid_template",
			body: map[string]any{
				"template": `{"version": "2017-02-28", "payload": {}}`,
				"context":  "",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_body",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluations/template", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_EvaluateCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "valid_code",
			body: map[string]any{
				"code":    `export function request(ctx) { return {}; }`,
				"context": "",
				"runtime": map[string]any{"name": "APPSYNC_JS"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_body",
			body:       "not-json-string",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()
			rec := doRequest(t, h, http.MethodPost, "/v1/dataplane-evaluations/code", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

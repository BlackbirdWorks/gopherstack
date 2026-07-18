package dlm_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// handleTagResource: invalid body
// ---------------------------------------------------------------------------

func TestHandler_TagResource_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "malformed JSON on POST tags returns 400",
			body:     []byte(`bad json`),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			// Get the ARN.
			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/tags/%s", arn), bytes.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// HandleREST via HTTP: UntagResource with multiple tagKeys query params
// ---------------------------------------------------------------------------

func TestHandler_UntagResource_MultipleQueryKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tagKeys  []string
		wantCode int
	}{
		{
			name:     "untag multiple keys via repeated query params",
			tagKeys:  []string{"env", "team"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			policyID := createPolicy(t, h)

			rec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/policies/%s", policyID), nil)
			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			arn := resp["Policy"].(map[string]any)["PolicyArn"].(string)

			// Tag first.
			doRequest(t, h, http.MethodPost, fmt.Sprintf("/tags/%s", arn), map[string]any{
				"Tags": map[string]string{"env": "prod", "team": "ops"},
			})

			// Build query string with multiple tagKeys.
			var parts []string
			for _, k := range tc.tagKeys {
				parts = append(parts, "tagKeys="+k)
			}
			path := fmt.Sprintf("/tags/%s?%s", arn, strings.Join(parts, "&"))

			req := httptest.NewRequest(http.MethodDelete, path, nil)
			rec2 := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec2)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tc.wantCode, rec2.Code)
		})
	}
}

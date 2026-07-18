package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// TestAppStream_Themes covers Theme CRUD.
func TestAppStream_Themes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateThemeForStack returns theme",
			action: "CreateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "theme-stk")
			},
			body:     map[string]any{"StackName": "theme-stk"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				th := resp["Theme"].(map[string]any)
				assert.Equal(t, "theme-stk", th["StackName"])
				assert.Equal(t, "ENABLED", th["State"])
			},
		},
		{
			name:   "DescribeThemeForStack returns theme",
			action: "DescribeThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "desc-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", map[string]any{"StackName": "desc-theme-stk"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "desc-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateThemeForStack returns theme",
			action: "UpdateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "upd-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", map[string]any{"StackName": "upd-theme-stk"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "upd-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteThemeForStack removes theme",
			action: "DeleteThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "del-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", map[string]any{"StackName": "del-theme-stk"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "del-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DescribeThemeForStack unknown returns error",
			action:   "DescribeThemeForStack",
			body:     map[string]any{"StackName": "no-such"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

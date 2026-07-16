package iot_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParityB_DeviceShadows verifies GetThingShadow, UpdateThingShadow, DeleteThingShadow,
// and ListNamedShadowsForThing operations.
func TestDeviceShadows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state      map[string]any
		name       string
		shadowName string
		op         string
		wantStatus int
	}{
		{
			name:       "update_classic_shadow",
			op:         "update",
			shadowName: "",
			state:      map[string]any{"desired": map[string]any{"temp": 22}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_classic_shadow",
			op:         "get",
			shadowName: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_named_shadow",
			op:         "update",
			shadowName: "config",
			state:      map[string]any{"desired": map[string]any{"mode": "auto"}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_missing_shadow_returns_404",
			op:         "get_missing",
			shadowName: "nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_shadow",
			op:         "delete",
			shadowName: "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			doRequest(t, h, http.MethodPost, "/things/shadow-thing", nil)

			shadowPath := "/things/shadow-thing/shadow"
			if tt.shadowName != "" {
				shadowPath += "?name=" + tt.shadowName
			}

			var rec *httptest.ResponseRecorder

			switch tt.op {
			case "update":
				body := map[string]any{"state": tt.state}
				rec = doRequest(t, h, http.MethodPost, shadowPath, body)
			case "get":
				doRequest(t, h, http.MethodPost, shadowPath,
					map[string]any{"state": map[string]any{"desired": map[string]any{"x": 1}}})
				rec = doRequest(t, h, http.MethodGet, shadowPath, nil)
			case "get_missing":
				rec = doRequest(t, h, http.MethodGet, shadowPath, nil)
			case "delete":
				doRequest(t, h, http.MethodPost, shadowPath,
					map[string]any{"state": map[string]any{"desired": map[string]any{"y": 2}}})
				rec = doRequest(t, h, http.MethodDelete, shadowPath, nil)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestParityB_ListNamedShadowsForThing verifies listing named shadows works correctly.
func TestListNamedShadowsForThing(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doRequest(t, h, http.MethodPost, "/things/ns-thing", nil)
	doRequest(t, h, http.MethodPost, "/things/ns-thing/shadow?name=alpha",
		map[string]any{"state": map[string]any{}})
	doRequest(t, h, http.MethodPost, "/things/ns-thing/shadow?name=beta",
		map[string]any{"state": map[string]any{}})

	rec := doRequest(t, h, http.MethodGet,
		"/api/things/shadow/ListNamedShadowsForThing/ns-thing", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results, _ := resp["results"].([]any)
	assert.Len(t, results, 2)
}

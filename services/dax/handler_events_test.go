package dax_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// ---- DescribeEvents ----

func TestHandlerDescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dax.Handler)
		body       map[string]any
		check      func(t *testing.T, resp map[string]any)
		name       string
		wantStatus int
	}{
		{
			name: "events after create",
			setup: func(t *testing.T, h *dax.Handler) {
				t.Helper()
				daxRequest(t, h, "CreateCluster", validClusterBody("evt-cluster"))
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				events := resp["Events"].([]any)
				assert.NotEmpty(t, events)
			},
		},
		{
			name:       "empty when no activity",
			setup:      func(_ *testing.T, _ *dax.Handler) {},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, resp map[string]any) {
				t.Helper()
				events := resp["Events"].([]any)
				assert.Empty(t, events)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			tt.setup(t, h)

			rec := daxRequest(t, h, "DescribeEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tt.check(t, resp)
			}
		})
	}
}

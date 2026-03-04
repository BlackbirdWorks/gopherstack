package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func TestSettingsPage_RendersAccountAndRegion(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		GlobalConfig: config.GlobalConfig{
			AccountID: "111111111111",
			Region:    "eu-west-1",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
	rec := httptest.NewRecorder()
	h.SubRouter.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "111111111111")
	assert.Contains(t, body, "eu-west-1")
	assert.Contains(t, body, "Runtime configuration")
}

func TestSettingsPage_DefaultsShown(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		GlobalConfig: config.GlobalConfig{
			AccountID: "000000000000",
			Region:    "us-east-1",
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
	rec := httptest.NewRecorder()
	h.SubRouter.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "000000000000")
	assert.Contains(t, rec.Body.String(), "us-east-1")
}

func TestSettingsPage_SidebarLink(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		GlobalConfig: config.GlobalConfig{AccountID: "000000000000", Region: "us-east-1"},
	})

	req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
	rec := httptest.NewRecorder()
	h.SubRouter.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	// Sidebar should contain the settings link.
	assert.Contains(t, rec.Body.String(), "/dashboard/settings",
		"settings link should appear in the sidebar")
}

func TestSettingsPage_LatencyMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantText  string
		latencyMs int
	}{
		{
			name:      "latency_disabled",
			latencyMs: 0,
			wantText:  "disabled",
		},
		{
			name:      "latency_enabled",
			latencyMs: 200,
			wantText:  "200",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GlobalConfig: config.GlobalConfig{
					AccountID: "000000000000",
					Region:    "us-east-1",
					LatencyMs: tt.latencyMs,
				},
			})

			req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
			rec := httptest.NewRecorder()
			h.SubRouter.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantText)
		})
	}
}

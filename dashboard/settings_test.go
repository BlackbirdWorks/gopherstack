package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func TestSettingsPage_RendersAccountAndRegion(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		GlobalConfig: config.NewGlobalConfig("111111111111", "eu-west-1", 0, 0, false, 0),
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
		GlobalConfig: config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, false, 0),
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
		GlobalConfig: config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, false, 0),
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
			wantText:  `value="0"`,
		},
		{
			name:      "latency_enabled",
			latencyMs: 200,
			wantText:  `value="200"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GlobalConfig: config.NewGlobalConfig("000000000000", "us-east-1", tt.latencyMs, 0, false, 0),
			})

			req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
			rec := httptest.NewRecorder()
			h.SubRouter.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantText)
		})
	}
}

func TestSettingsPage_EnforceIAM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantText   string
		enforceIAM bool
	}{
		{
			name:       "enforcement_disabled",
			enforceIAM: false,
			wantText:   "disabled",
		},
		{
			name:       "enforcement_enabled",
			enforceIAM: true,
			wantText:   "enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GlobalConfig: config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, tt.enforceIAM, 0),
			})

			req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
			rec := httptest.NewRecorder()
			h.SubRouter.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()
			if tt.enforceIAM {
				assert.Contains(t, body, `name="enforceIAM" value="true" class="sr-only peer" checked`)
			} else {
				assert.NotContains(t, body, `name="enforceIAM" value="true" class="sr-only peer" checked`)
			}
			assert.Contains(t, body, "IAM Execution")
		})
	}
}

func TestSettingsPage_ServiceSettings(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		GlobalConfig: config.NewGlobalConfig("000000000000", "us-east-1", 0, 0, false, 0),
	})

	req := httptest.NewRequest(http.MethodGet, "/dashboard/settings", nil)
	rec := httptest.NewRecorder()
	h.SubRouter.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	
	// Check for service-specific labels and sections
	assert.Contains(t, body, "Service TTLs & Intervals")
	assert.Contains(t, body, "S3")
	assert.Contains(t, body, "Lambda")
	assert.Contains(t, body, "DynamoDB")
	
	// Check for specific field names (subset)
	assert.Contains(t, body, `name="s3-DefaultRegion"`)
	assert.Contains(t, body, `name="lambda-PoolSize"`)
	assert.Contains(t, body, `name="dynamodb-EnforceThroughput"`)
	assert.Contains(t, body, `name="ec2-TerminatedTTL"`)
}

func TestSettingsUpdate_ServiceFields(t *testing.T) {
	t.Parallel()

	// Mock ConfigManager
	mc := &mockConfigManager{
		settings: dashboard.Settings{
			AccountID: "000000000000",
			Region:    "us-east-1",
		},
	}

	h := dashboard.NewHandler(dashboard.Config{
		ConfigManager: mc,
	})

	// Form values for service settings
	form := "s3-DefaultRegion=us-west-2&" +
		"lambda-PoolSize=10&" +
		"dynamodb-EnforceThroughput=true&" +
		"ec2-TerminatedTTL=2h"

	req := httptest.NewRequest(http.MethodPost, "/dashboard/settings/update", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	
	h.SubRouter.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	
	// Verify mock was updated
	assert.Equal(t, "us-west-2", mc.settings.S3.DefaultRegion)
	assert.Equal(t, 10, mc.settings.Lambda.PoolSize)
	assert.True(t, mc.settings.DynamoDB.EnforceThroughput)
	assert.Equal(t, 2*time.Hour, mc.settings.EC2.TerminatedTTL)
}

type mockConfigManager struct {
	settings dashboard.Settings
}

func (m *mockConfigManager) GetSettings() dashboard.Settings { return m.settings }
func (m *mockConfigManager) UpdateSettings(s dashboard.Settings) { m.settings = s }
func (m *mockConfigManager) SaveConfig() error                { return nil }

package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

// fakeConfigManager is a minimal dashboard.ConfigManager implementation for
// tests. It also carries host filesystem/network detail (DataDir,
// DNSListenAddr, DNSResolveIP) so tests can assert those never reach the
// unauthenticated settings endpoint.
type fakeConfigManager struct {
	settings dashboard.Settings
}

func (m *fakeConfigManager) GetSettings() dashboard.Settings     { return m.settings }
func (m *fakeConfigManager) UpdateSettings(s dashboard.Settings) { m.settings = s }
func (m *fakeConfigManager) SaveConfig() error                   { return nil }

// doGet drives a GET request through the dashboard's lazily-built SubRouter,
// mirroring how the real echo/v5 server invokes DashboardHandler.Handler().
func doGet(t *testing.T, h *dashboard.DashboardHandler, path string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	c := echo.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func TestSystemSettings_HappyPath(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		ConfigManager: &fakeConfigManager{
			settings: dashboard.Settings{
				AccountID:         "123456789012",
				Region:            "eu-west-1",
				LatencyMs:         42,
				EnforceIAM:        true,
				AutoPurgeTTL:      10 * time.Minute,
				PortRangeStart:    5000,
				PortRangeEnd:      10000,
				InitScriptTimeout: 30 * time.Second,
				Persist:           true,
				Demo:              false,
				// Host filesystem/network detail must never appear in the
				// response - see the "no leak" assertions below.
				DataDir:       "/very/secret/host/path",
				DNSListenAddr: "127.0.0.1:53",
				DNSResolveIP:  "10.0.0.99",
			},
		},
	})

	rec := doGet(t, h, "/dashboard/api/system/settings")
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	assert.Equal(t, "123456789012", body["accountID"])
	assert.Equal(t, "eu-west-1", body["region"])
	assert.InEpsilon(t, float64(42), body["latencyMs"], 0)
	assert.Equal(t, true, body["enforceIAM"])
	assert.Equal(t, "10m0s", body["autoPurgeTTL"])
	assert.InEpsilon(t, float64(5000), body["portRangeStart"], 0)
	assert.InEpsilon(t, float64(10000), body["portRangeEnd"], 0)
	assert.Equal(t, "30s", body["initScriptTimeout"])
	assert.Equal(t, true, body["persist"])
	assert.Equal(t, false, body["demo"])

	// Exactly the ten allow-listed fields - nothing more.
	assert.Len(t, body, 10)

	// Host filesystem/network detail (DataDir, DNSListenAddr, DNSResolveIP)
	// must not leak onto this unauthenticated endpoint.
	_, hasDataDir := body["dataDir"]
	_, hasDNSListenAddr := body["dnsListenAddr"]
	_, hasDNSResolveIP := body["dnsResolveIP"]
	assert.False(t, hasDataDir, "dataDir must not be exposed")
	assert.False(t, hasDNSListenAddr, "dnsListenAddr must not be exposed")
	assert.False(t, hasDNSResolveIP, "dnsResolveIP must not be exposed")

	raw := rec.Body.String()
	assert.NotContains(t, raw, "/very/secret/host/path")
	assert.NotContains(t, raw, "127.0.0.1:53")
	assert.NotContains(t, raw, "10.0.0.99")
}

func TestSystemSettings_NilConfigManager(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	rec := doGet(t, h, "/dashboard/api/system/settings")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, "{}", rec.Body.String())
}

// TestSystemSettings_NoSecretShapedKeys is the regression guard for the
// "explicit allow-list, not json.Marshal(Settings)" requirement: the
// dashboard is unauthenticated, so nothing shaped like a credential may ever
// be reachable through this endpoint, now or after future fields are added
// to dashboard.Settings.
func TestSystemSettings_NoSecretShapedKeys(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{
		ConfigManager: &fakeConfigManager{
			settings: dashboard.Settings{
				AccountID: "123456789012",
				Region:    "eu-west-1",
			},
		},
	})

	rec := doGet(t, h, "/dashboard/api/system/settings")
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))

	secretLike := regexp.MustCompile(`(?i)secret|password|token|credential|accesskey`)
	for key := range body {
		assert.NotRegexp(t, secretLike, key, "response key %q looks credential-shaped", key)
	}
}

package dashboard_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

const profileBody = `{"applicationIdentifier":"a","environmentIdentifier":"e",` +
	`"configurationProfileIdentifier":"p","content":"{}"}`

func profileCount(t *testing.T, h *dashboard.DashboardHandler, query string) int {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/appconfigdata/profiles"+query, nil)
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Profiles []any `json:"profiles"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return len(out.Profiles)
}

func TestAppConfigDataProfiles_RegionQueryParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		writeQuery string
		wantHome   int
		wantEU     int
	}{
		{name: "write-home-by-default", writeQuery: "", wantHome: 1, wantEU: 0},
		{name: "write-eu", writeQuery: "?region=eu-west-1", wantHome: 0, wantEU: 1},
		{name: "invalid-region-falls-back-home", writeQuery: "?region=not-a-region", wantHome: 1, wantEU: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ops := appconfigdata.NewHandler(appconfigdata.NewInMemoryBackend())
			ops.EnableRegions("us-east-1")
			h := dashboard.NewHandler(dashboard.Config{AppConfigDataOps: ops})

			req := httptest.NewRequest(http.MethodPost,
				"/dashboard/api/appconfigdata/profiles"+tc.writeQuery, strings.NewReader(profileBody))
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
			require.Equal(t, http.StatusNoContent, rec.Code)

			assert.Equal(t, tc.wantHome, profileCount(t, h, ""))
			assert.Equal(t, tc.wantEU, profileCount(t, h, "?region=eu-west-1"))
		})
	}
}

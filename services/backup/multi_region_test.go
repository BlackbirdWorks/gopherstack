package backup_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/backup"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *backup.Handler {
	t.Helper()

	h := backup.NewHandler(backup.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions(t.Context())

	return h
}

func regionCall(t *testing.T, h *backup.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func vaults(t *testing.T, h *backup.Handler, region string) map[string]string {
	t.Helper()

	got := map[string]string{}

	list, _ := regionCall(t, h, region, http.MethodGet, "/backup-vaults/", "")["BackupVaultList"].([]any)
	for _, v := range list {
		m, _ := v.(map[string]any)
		name, _ := m["BackupVaultName"].(string)
		arn, _ := m["BackupVaultArn"].(string)
		got[name] = arn
	}

	return got
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{"us-east-1", "eu-west-1"}},
		{name: "three-regions", regions: []string{"us-east-1", "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				regionCall(t, h, r, http.MethodPut, "/backup-vaults/shared", `{}`)
				regionCall(t, h, r, http.MethodPut, "/backup-vaults/only-"+r, `{}`)
			}

			for _, r := range tc.regions {
				got := vaults(t, h, r)
				assert.Len(t, got, 2)
				assert.Contains(t, got, "only-"+r)
				assert.Contains(t, got["shared"], ":"+r+":")
			}
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		remote bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "peer-round-trips", remote: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			regionCall(t, src, mrHome, http.MethodPut, "/backup-vaults/home", `{}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", http.MethodPut, "/backup-vaults/eu", `{}`)
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Contains(t, vaults(t, dst, mrHome), "home")
			assert.Equal(t, tc.remote, len(vaults(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.Restore(t.Context(), snap))
			assert.Contains(t, vaults(t, old, mrHome), "home")
		})
	}
}

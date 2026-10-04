package securityhub_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *securityhub.Handler {
	t.Helper()

	h := securityhub.NewHandler(securityhub.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *securityhub.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	code, out := regionDo(t, h, region, method, path, body)
	require.Less(t, code, http.StatusMultipleChoices, out)

	return decode(t, out)
}

func decode(t *testing.T, body string) map[string]any {
	t.Helper()

	out := map[string]any{}
	if body != "" {
		require.NoError(t, json.Unmarshal([]byte(body), &out))
	}

	return out
}

func regionDo(t *testing.T, h *securityhub.Handler, region, method, path, body string) (int, string) {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))

	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec.Code, rec.Body.String()
}

func createOne(t *testing.T, h *securityhub.Handler, region, name string) {
	t.Helper()

	regionDo(t, h, region, http.MethodPost, "/accounts", `{}`)

	regionCall(
		t,
		h,
		region,
		http.MethodPost,
		"/actionTargets",
		`{"Name":"`+name+`","Description":"d","Id":"`+strings.ReplaceAll(name, "-", "")+`"}`,
	)
}

func listed(t *testing.T, h *securityhub.Handler, region string) []map[string]any {
	t.Helper()

	resp := regionCall(t, h, region, http.MethodPost, "/actionTargets/get", `{}`)
	raw, _ := resp["ActionTargets"].([]any)

	items := make([]map[string]any, 0, len(raw))

	for _, v := range raw {
		m, _ := v.(map[string]any)
		items = append(items, m)
	}

	return items
}

func names(t *testing.T, h *securityhub.Handler, region string) []string {
	t.Helper()

	items := listed(t, h, region)
	out := make([]string, 0, len(items))

	for _, m := range items {
		n, _ := m["Name"].(string)
		out = append(out, n)
	}

	return out
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
				createOne(t, h, r, "shared")
				createOne(t, h, r, "only-"+r)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, names(t, h, r))

				for _, m := range listed(t, h, r) {
					assert.Contains(t, m["ActionTargetArn"], ":"+r+":")
				}
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
			createOne(t, src, mrHome, "home")

			if tc.remote {
				createOne(t, src, "eu-west-1", "eu")
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.Snapshot(context.Background()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, names(t, dst, mrHome))

			if tc.remote {
				assert.Equal(t, []string{"eu"}, names(t, dst, "eu-west-1"))
			} else {
				assert.Empty(t, names(t, dst, "eu-west-1"))
			}

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, names(t, old, mrHome))
		})
	}
}

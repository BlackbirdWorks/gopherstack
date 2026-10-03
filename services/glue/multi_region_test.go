package glue_test

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
	"github.com/blackbirdworks/gopherstack/services/glue"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *glue.Handler {
	t.Helper()

	h := glue.NewHandler(glue.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions(t.Context())
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	return h
}

func regionCall(t *testing.T, h *glue.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AWSGlue."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func databases(t *testing.T, h *glue.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, "GetDatabases", `{}`)["DatabaseList"].([]any)

	names := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		n, _ := m["Name"].(string)
		names = append(names, n)
	}

	return names
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
				regionCall(t, h, r, "CreateDatabase", `{"DatabaseInput":{"Name":"shared"},"Tags":{"k":"`+r+`"}}`)
				regionCall(t, h, r, "CreateDatabase", `{"DatabaseInput":{"Name":"only-`+r+`"}}`)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, databases(t, h, r))

				arn := "arn:aws:glue:" + r + ":000000000000:database/shared"
				tags, _ := regionCall(t, h, r, "GetTags", `{"ResourceArn":"`+arn+`"}`)["Tags"].(map[string]any)
				assert.Equal(t, r, tags["k"])
			}

			assert.Same(t, h.Backend, h.BackendFor(mrHome))
			assert.NotSame(t, h.Backend, h.BackendFor("eu-west-1"))
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
			regionCall(t, src, mrHome, "CreateDatabase", `{"DatabaseInput":{"Name":"home"}}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", "CreateDatabase", `{"DatabaseInput":{"Name":"eu"}}`)
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.(*glue.InMemoryBackend).Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Equal(t, []string{"home"}, databases(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(databases(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.(*glue.InMemoryBackend).Restore(t.Context(), snap))
			assert.Equal(t, []string{"home"}, databases(t, old, mrHome))
		})
	}
}

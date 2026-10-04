package athena_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *athena.Handler {
	t.Helper()

	h := athena.NewHandler(athena.NewInMemoryBackend(mrHome, "000000000000"))
	h.EnableRegions(t.Context())

	return h
}

func regionCall(t *testing.T, h *athena.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AmazonAthena."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func workGroups(t *testing.T, h *athena.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, "ListWorkGroups", `{}`)["WorkGroups"].([]any)

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
				regionCall(t, h, r, "CreateWorkGroup", `{"Name":"shared"}`)
				regionCall(t, h, r, "CreateWorkGroup", `{"Name":"only-`+r+`"}`)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"primary", "shared", "only-" + r}, workGroups(t, h, r))

				arn := "arn:aws:athena:" + r + ":000000000000:workgroup/shared"
				regionCall(t, h, r, "TagResource", `{"ResourceARN":"`+arn+`","Tags":[{"Key":"k","Value":"v"}]}`)

				tags, _ := regionCall(t, h, r, "ListTagsForResource", `{"ResourceARN":"`+arn+`"}`)["Tags"].([]any)
				assert.Len(t, tags, 1)
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
			regionCall(t, src, mrHome, "CreateWorkGroup", `{"Name":"home"}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", "CreateWorkGroup", `{"Name":"eu"}`)
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.(*athena.InMemoryBackend).Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Contains(t, workGroups(t, dst, mrHome), "home")
			assert.Equal(t, tc.remote, slices.Contains(workGroups(t, dst, "eu-west-1"), "eu"))
			assert.NotContains(t, workGroups(t, dst, mrHome), "eu")

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.(*athena.InMemoryBackend).Restore(t.Context(), snap))
			assert.Contains(t, workGroups(t, old, mrHome), "home")
		})
	}
}

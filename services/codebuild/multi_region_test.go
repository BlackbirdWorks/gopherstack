package codebuild_test

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
	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *codebuild.Handler {
	t.Helper()

	h := codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions(context.Background())

	return h
}

func regionCall(t *testing.T, h *codebuild.Handler, region, target, body string) map[string]any {
	t.Helper()

	code, out := regionDo(t, h, region, target, body)
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

func regionDo(t *testing.T, h *codebuild.Handler, region, target, body string) (int, string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", target)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec.Code, rec.Body.String()
}

func createOne(t *testing.T, h *codebuild.Handler, region, name string) {
	t.Helper()

	regionCall(
		t,
		h,
		region,
		"CodeBuild_20161006.CreateProject",
		`{"name":"`+name+`","serviceRole":"arn:aws:iam::000000000000:role/cb",`+
			`"source":{"type":"NO_SOURCE","buildspec":"version: 0.2"},"artifacts":{"type":"NO_ARTIFACTS"},`+
			`"environment":{"type":"LINUX_CONTAINER","image":"aws/codebuild/standard:7.0",`+
			`"computeType":"BUILD_GENERAL1_SMALL"}}`,
	)
}

func listed(t *testing.T, h *codebuild.Handler, region string) []map[string]any {
	t.Helper()

	resp := regionCall(
		t,
		h,
		region,
		"CodeBuild_20161006.BatchGetProjects",
		`{"names":["shared","only-us-east-1","only-eu-west-1","only-ap-south-1","home","eu"]}`,
	)
	raw, _ := resp["projects"].([]any)

	items := make([]map[string]any, 0, len(raw))

	for _, v := range raw {
		m, _ := v.(map[string]any)
		items = append(items, m)
	}

	return items
}

func names(t *testing.T, h *codebuild.Handler, region string) []string {
	t.Helper()

	items := listed(t, h, region)
	out := make([]string, 0, len(items))

	for _, m := range items {
		n, _ := m["name"].(string)
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
					assert.Contains(t, m["arn"], ":"+r+":")
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

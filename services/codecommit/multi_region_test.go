package codecommit_test

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
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *codecommit.Handler {
	t.Helper()

	h := codecommit.NewHandler(codecommit.NewInMemoryBackend("000000000000", mrHome))
	h.EnableRegions()

	return h
}

func regionCall(t *testing.T, h *codecommit.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "CodeCommit_20150413."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func repoNames(t *testing.T, h *codecommit.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, "ListRepositories", `{}`)["repositories"].([]any)

	names := make([]string, 0, len(list))

	for _, v := range list {
		m, _ := v.(map[string]any)
		n, _ := m["repositoryName"].(string)
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
				regionCall(t, h, r, "CreateRepository", `{"repositoryName":"shared"}`)
				regionCall(t, h, r, "CreateRepository", `{"repositoryName":"only-`+r+`"}`)
			}

			for _, r := range tc.regions {
				assert.ElementsMatch(t, []string{"shared", "only-" + r}, repoNames(t, h, r))

				md, _ := regionCall(t, h, r, "GetRepository", `{"repositoryName":"shared"}`)["repositoryMetadata"].(map[string]any)
				assert.Contains(t, md["Arn"], ":"+r+":")
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
			regionCall(t, src, mrHome, "CreateRepository", `{"repositoryName":"home"}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", "CreateRepository", `{"repositoryName":"eu"}`)
			}

			snap := src.Snapshot(context.Background())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.Snapshot(context.Background()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, repoNames(t, dst, mrHome))
			assert.Equal(t, tc.remote, len(repoNames(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.Restore(context.Background(), snap))
			assert.Equal(t, []string{"home"}, repoNames(t, old, mrHome))
		})
	}
}

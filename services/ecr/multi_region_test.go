package ecr_test

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
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *ecr.Handler {
	t.Helper()

	h := ecr.NewHandler(ecr.NewInMemoryBackend("000000000000", mrHome, ""), nil)
	h.EnableRegions(t.Context())

	return h
}

func regionCall(t *testing.T, h *ecr.Handler, region, op, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-Amz-Target", "AmazonEC2ContainerRegistry_V20150921."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := map[string]any{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func repos(t *testing.T, h *ecr.Handler, region string) map[string]string {
	t.Helper()

	got := map[string]string{}

	list, _ := regionCall(t, h, region, "DescribeRepositories", `{}`)["repositories"].([]any)
	for _, v := range list {
		m, _ := v.(map[string]any)
		name, _ := m["repositoryName"].(string)
		arn, _ := m["repositoryArn"].(string)
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
				regionCall(t, h, r, "CreateRepository", `{"repositoryName":"shared"}`)
				regionCall(t, h, r, "CreateRepository", `{"repositoryName":"only-`+r+`"}`)
			}

			for _, r := range tc.regions {
				got := repos(t, h, r)
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
			regionCall(t, src, mrHome, "CreateRepository", `{"repositoryName":"home"}`)

			if tc.remote {
				regionCall(t, src, "eu-west-1", "CreateRepository", `{"repositoryName":"eu"}`)
			}

			snap := src.Snapshot(t.Context())
			require.NotNil(t, snap)

			if !tc.remote {
				assert.Equal(t, src.Backend.(*ecr.InMemoryBackend).Snapshot(t.Context()), snap)
			}

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Contains(t, repos(t, dst, mrHome), "home")
			assert.Equal(t, tc.remote, len(repos(t, dst, "eu-west-1")) == 1)

			old := newRegionHandler(t)
			require.NoError(t, old.Backend.(*ecr.InMemoryBackend).Restore(t.Context(), snap))
			assert.Contains(t, repos(t, old, mrHome), "home")
		})
	}
}

package eks_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

const mrHome = "us-east-1"

func newRegionHandler(t *testing.T) *eks.Handler {
	t.Helper()

	h := eks.NewHandler(eks.NewInMemoryBackend(t.Context(), "000000000000", mrHome))
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	return h
}

func regionCall(t *testing.T, h *eks.Handler, region, method, path, body string) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(awsmeta.Set(req.Context(), &awsmeta.Metadata{Region: region, Account: "000000000000"}))

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Less(t, rec.Code, http.StatusMultipleChoices, rec.Body.String())

	out := map[string]any{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

const clusterBody = `{"name":"shared","roleArn":"arn:aws:iam::000000000000:role/eks",` +
	`"resourcesVpcConfig":{"subnetIds":["subnet-1","subnet-2"]}}`

func clusterNames(t *testing.T, h *eks.Handler, region string) []string {
	t.Helper()

	list, _ := regionCall(t, h, region, http.MethodGet, "/clusters", "")["clusters"].([]any)

	out := make([]string, 0, len(list))

	for _, v := range list {
		s, _ := v.(string)
		out = append(out, s)
	}

	return out
}

func clusterARN(t *testing.T, h *eks.Handler, region string) string {
	t.Helper()

	c, _ := regionCall(t, h, region, http.MethodGet, "/clusters/shared", "")["cluster"].(map[string]any)
	arn, _ := c["arn"].(string)

	return arn
}

func TestHandler_MultiRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		regions []string
	}{
		{name: "two-regions", regions: []string{mrHome, "eu-west-1"}},
		{name: "three-regions", regions: []string{mrHome, "eu-west-1", "ap-south-1"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRegionHandler(t)

			for _, r := range tc.regions {
				regionCall(t, h, r, http.MethodPost, "/clusters", clusterBody)
			}

			for _, r := range tc.regions {
				assert.Equal(t, []string{"shared"}, clusterNames(t, h, r))
				assert.Contains(t, clusterARN(t, h, r), ":eks:"+r+":")
			}

			assert.Len(t, h.RegionBackends(), len(tc.regions))
		})
	}
}

func TestHandler_MultiRegionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		extraRegion string
		wantRegions bool
	}{
		{name: "home-only-snapshot-unchanged"},
		{name: "with-sibling", extraRegion: "eu-west-1", wantRegions: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			src := newRegionHandler(t)
			regionCall(t, src, mrHome, http.MethodPost, "/clusters", clusterBody)

			if tc.extraRegion != "" {
				regionCall(t, src, tc.extraRegion, http.MethodPost, "/clusters", clusterBody)
			}

			snap := src.Snapshot(t.Context())

			var doc map[string]json.RawMessage

			require.NoError(t, json.Unmarshal(snap, &doc))

			_, hasRegions := doc["regions"]
			assert.Equal(t, tc.wantRegions, hasRegions)

			dst := newRegionHandler(t)
			require.NoError(t, dst.Restore(t.Context(), snap))
			assert.Equal(t, []string{"shared"}, clusterNames(t, dst, mrHome))

			if tc.extraRegion != "" {
				assert.Contains(t, clusterARN(t, dst, tc.extraRegion), ":eks:"+tc.extraRegion+":")
			}
		})
	}
}

func TestHandler_MultiRegionLegacyRestore(t *testing.T) {
	t.Parallel()

	legacy := eks.NewHandler(eks.NewInMemoryBackend(t.Context(), "000000000000", mrHome))
	t.Cleanup(legacy.Backend.Close)
	regionCall(t, legacy, mrHome, http.MethodPost, "/clusters", clusterBody)

	dst := newRegionHandler(t)
	regionCall(t, dst, "eu-west-1", http.MethodPost, "/clusters", clusterBody)

	require.NoError(t, dst.Restore(t.Context(), legacy.Snapshot(t.Context())))
	assert.Equal(t, []string{"shared"}, clusterNames(t, dst, mrHome))
	assert.Empty(t, clusterNames(t, dst, "eu-west-1"))
}

func TestHandler_MultiRegionDockerClusters(t *testing.T) {
	t.Parallel()

	rt := &fakeClusterRuntime{}
	probe := newGatedClusterProbe()

	home := eks.NewInMemoryBackend(t.Context(), "000000000000", mrHome)
	require.NoError(t, home.EnableClusters(eks.ClusterEngineConfig{
		Runtime: rt, Probe: probe.probe, Token: testToken, Host: "eks.test", StartTimeout: time.Minute,
	}))

	h := eks.NewHandler(home)
	h.EnableRegions()
	t.Cleanup(func() { h.Shutdown(t.Context()) })

	for _, r := range []string{mrHome, "eu-west-1"} {
		regionCall(t, h, r, http.MethodPost, "/clusters", clusterBody)
	}

	assert.Eventually(t, func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()

		return len(rt.specs) == 2
	}, time.Minute, time.Millisecond)
}

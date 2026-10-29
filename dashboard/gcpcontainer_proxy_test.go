//nolint:staticcheck // the API still serves fields the proto marks deprecated
package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

const gkeBase = "/dashboard/api/v1/gcp/container/v1/projects/p/locations"

func newGKEDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	b := gcpcontainer.NewInMemoryBackend()
	b.SetClock(func() time.Time { return now })

	_, err := b.CreateCluster(&containerpb.CreateClusterRequest{
		Parent: "projects/p/locations/us-central1-a",
		Cluster: &containerpb.Cluster{
			Name: "c1", InitialNodeCount: 2,
			MasterAuth: &containerpb.MasterAuth{
				Username:  "admin",
				Password:  "correct-horse-battery",
				ClientKey: "private-key",
			},
		},
	})
	require.NoError(t, err)

	now = now.Add(time.Hour)

	return dashboard.NewHandler(dashboard.Config{GCPContainerOps: gcpcontainer.NewHandler(b)})
}

func TestGCPContainerProxy(t *testing.T) {
	t.Parallel()

	cluster := gkeBase + "/us-central1-a/clusters/c1"

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"clusters", []gcpStep{
			{http.MethodGet, gkeBase + "/-/clusters", "", `"name":"c1"`, 200},
			{http.MethodGet, gkeBase + "/-/clusters", "", `"status":"RUNNING"`, 200},
			{http.MethodGet, cluster, "", `"name":"c1"`, 200},
			{http.MethodGet, cluster + "/nodePools", "", `"name":"default-pool"`, 200},
			{http.MethodGet, cluster + "/nodePools/default-pool", "", `"name":"default-pool"`, 200},
			{http.MethodGet, cluster + "/nodePools/missing", "", "NOT_FOUND", 404},
		}},
		{"pool sizes", []gcpStep{
			{http.MethodGet, cluster + ":poolSizes", "", `"default-pool":2`, 200},
		}},
		{"resize", []gcpStep{
			{
				http.MethodPost,
				cluster + "/nodePools/default-pool:setSize",
				`{"nodeCount":5000}`,
				"INVALID_ARGUMENT",
				400,
			},
			{
				http.MethodPost,
				cluster + "/nodePools/default-pool:setSize",
				`{"nodeCount":4}`,
				"SET_NODE_POOL_SIZE",
				200,
			},
			{http.MethodGet, cluster + ":poolSizes", "", `"default-pool":4`, 200},
		}},
		{"operations", []gcpStep{
			{http.MethodGet, gkeBase + "/-/operations", "", "CREATE_CLUSTER", 200},
		}},
		{"delete", []gcpStep{
			{http.MethodDelete, cluster, "", "DELETE_CLUSTER", 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodPost, gkeBase + "/us-central1-a/clusters", `{}`, "NOT_FOUND", 404},
			{http.MethodPut, cluster, `{}`, "NOT_FOUND", 404},
			{http.MethodPost, cluster + ":setMasterAuth", `{}`, "NOT_FOUND", 404},
			{http.MethodPost, cluster + "/nodePools/default-pool:delete", ``, "NOT_FOUND", 404},
			{http.MethodDelete, cluster + "/nodePools/default-pool", ``, "NOT_FOUND", 404},
			{http.MethodGet, gkeBase + "/us-central1-a/other", "", "NOT_FOUND", 404},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newGKEDashboard(t), tt.steps)
		})
	}

	t.Run("credentials", func(t *testing.T) {
		t.Parallel()

		h := newGKEDashboard(t)
		for _, p := range []string{cluster, gkeBase + "/-/clusters"} {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, p, nil)
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
			assert.NotContains(t, rec.Body.String(), "correct-horse-battery")
			assert.NotContains(t, rec.Body.String(), "private-key")
			assert.NotContains(t, rec.Body.String(), "admin")
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, gkeBase+"/-/clusters", nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}

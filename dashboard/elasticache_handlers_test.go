package dashboard_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

// TestDashboard_ElastiCache_Index tests the ElastiCache clusters index page.
func TestDashboard_ElastiCache_Index(t *testing.T) {
	t.Parallel()

	t.Run("empty cluster list", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/elasticache", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ElastiCache")
	})

	t.Run("shows created cluster", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create a cluster via form POST
		req := httptest.NewRequest(http.MethodPost, "/dashboard/elasticache/create",
			strings.NewReader("cluster_id=my-cluster&engine=redis&node_type=cache.t3.micro"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Verify cluster appears in index
		req = httptest.NewRequest(http.MethodGet, "/dashboard/elasticache", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "my-cluster")
	})

	t.Run("nil ElastiCacheOps renders empty", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/elasticache", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "ElastiCache")
	})
}

// TestDashboard_ElastiCache_CreateCluster tests cluster creation via the dashboard.
func TestDashboard_ElastiCache_CreateCluster(t *testing.T) {
	t.Parallel()

	t.Run("creates cluster and redirects", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/elasticache/create",
			strings.NewReader("cluster_id=new-cluster&engine=redis&node_type=cache.t3.micro"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/elasticache", w.Header().Get("Location"))
	})

	t.Run("default engine when not specified", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodPost, "/dashboard/elasticache/create",
			strings.NewReader("cluster_id=default-engine-cluster"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
	})

	t.Run("nil ElastiCacheOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodPost, "/dashboard/elasticache/create",
			strings.NewReader("cluster_id=x"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_ElastiCache_DeleteCluster tests cluster deletion via the dashboard.
func TestDashboard_ElastiCache_DeleteCluster(t *testing.T) {
	t.Parallel()

	t.Run("deletes cluster and redirects", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/elasticache/create",
			strings.NewReader("cluster_id=del-cluster&engine=redis"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// Delete it
		req = httptest.NewRequest(http.MethodDelete, "/dashboard/elasticache/delete?id=del-cluster", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusFound, w.Code)
		assert.Equal(t, "/dashboard/elasticache", w.Header().Get("Location"))
	})

	t.Run("delete non-existent cluster returns 400", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/elasticache/delete?id=nope", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusBadRequest, w.Code)
	})

	t.Run("nil ElastiCacheOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodDelete, "/dashboard/elasticache/delete?id=x", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

// TestDashboard_ElastiCache_ClusterDetail tests the cluster detail page.
func TestDashboard_ElastiCache_ClusterDetail(t *testing.T) {
	t.Parallel()

	t.Run("shows cluster detail", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		// Create cluster first
		req := httptest.NewRequest(http.MethodPost, "/dashboard/elasticache/create",
			strings.NewReader("cluster_id=detail-cluster&engine=redis&node_type=cache.t3.micro"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)
		require.Equal(t, http.StatusFound, w.Code)

		// View detail
		req = httptest.NewRequest(http.MethodGet, "/dashboard/elasticache/cluster?id=detail-cluster", nil)
		w = httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), "detail-cluster")
	})

	t.Run("not found cluster returns 404", func(t *testing.T) {
		t.Parallel()
		stack := newStack(t)

		req := httptest.NewRequest(http.MethodGet, "/dashboard/elasticache/cluster?id=does-not-exist", nil)
		w := httptest.NewRecorder()
		serveHandler(stack.Dashboard, w, req)

		require.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("nil ElastiCacheOps returns 503", func(t *testing.T) {
		t.Parallel()
		h := dashboard.NewHandler(dashboard.Config{Logger: slog.Default()})

		req := httptest.NewRequest(http.MethodGet, "/dashboard/elasticache/cluster?id=x", nil)
		w := httptest.NewRecorder()
		serveHandler(h, w, req)

		require.Equal(t, http.StatusServiceUnavailable, w.Code)
	})
}

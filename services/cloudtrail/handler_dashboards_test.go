package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestCloudTrailDashboard exercises CreateDashboard and DeleteDashboard.
func TestCloudTrailDashboard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "create_dashboard_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "my-dashboard",
					"Type": "CUSTOM",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.NotEmpty(t, resp["DashboardArn"])
				assert.Equal(t, "my-dashboard", resp["Name"])
				assert.Equal(t, "CREATED", resp["Status"])
			},
		},
		{
			name: "create_dashboard_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Type": "CUSTOM",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "delete_dashboard_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "del-dashboard",
				})
				createResp := parseCloudTrailResp(t, createRec)
				dashboardARN := createResp["DashboardArn"].(string)
				rec := doCloudTrailOp(t, h, "DeleteDashboard", map[string]any{
					"DashboardId": dashboardARN,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_dashboard_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteDashboard", map[string]any{
					"DashboardId": "dashboard-missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestCloudTrailDashboardLifecycle covers CreateDashboard, GetDashboard, ListDashboards,
// UpdateDashboard, StartDashboardRefresh, DeleteDashboard.
func TestCloudTrailDashboardLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// CreateDashboard.
	rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
		"Name": "test-dashboard",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	dashARN, _ := resp["DashboardArn"].(string)
	require.NotEmpty(t, dashARN)

	// GetDashboard.
	rec = doCloudTrailOp(t, h, "GetDashboard", map[string]any{"DashboardId": dashARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListDashboards.
	rec = doCloudTrailOp(t, h, "ListDashboards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateDashboard.
	rec = doCloudTrailOp(t, h, "UpdateDashboard", map[string]any{
		"DashboardId": dashARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StartDashboardRefresh.
	rec = doCloudTrailOp(t, h, "StartDashboardRefresh", map[string]any{
		"DashboardId": dashARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteDashboard.
	rec = doCloudTrailOp(t, h, "DeleteDashboard", map[string]any{"DashboardId": dashARN})
	assert.Equal(t, http.StatusOK, rec.Code)
}

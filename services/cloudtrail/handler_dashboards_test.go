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

// TestCreateDashboard_WidgetsAndRefreshSchedule verifies Widgets and
// RefreshSchedule -- previously accepted on the wire but not modeled or
// stored at all -- round-trip through Create/Get, and that
// TerminationProtectionEnabled is honored too.
func TestCreateDashboard_WidgetsAndRefreshSchedule(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
		"Name": "widget-dashboard",
		"Type": "CUSTOM",
		"Widgets": []any{
			map[string]any{
				"QueryStatement": "SELECT eventName FROM eds-1",
				"ViewProperties": map[string]any{"title": "Events by name"},
			},
		},
		"RefreshSchedule": map[string]any{
			"Frequency": map[string]any{"Unit": "HOURS", "Value": 6},
			"Status":    "ENABLED",
		},
		"TerminationProtectionEnabled": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)

	widgets, ok := resp["Widgets"].([]any)
	require.True(t, ok, "Widgets must round-trip on CreateDashboard")
	require.Len(t, widgets, 1)
	w := widgets[0].(map[string]any)
	assert.Equal(t, "SELECT eventName FROM eds-1", w["QueryStatement"])

	schedule, ok := resp["RefreshSchedule"].(map[string]any)
	require.True(t, ok, "RefreshSchedule must round-trip on CreateDashboard")
	assert.Equal(t, "ENABLED", schedule["Status"])

	assert.Equal(t, true, resp["TerminationProtectionEnabled"])

	dashARN, _ := resp["DashboardArn"].(string)
	require.NotEmpty(t, dashARN)

	getRec := doCloudTrailOp(t, h, "GetDashboard", map[string]any{"DashboardId": dashARN})
	require.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseCloudTrailResp(t, getRec)
	getWidgets, ok := getResp["Widgets"].([]any)
	require.True(t, ok, "Widgets must round-trip on GetDashboard")
	assert.Len(t, getWidgets, 1)
}

// TestUpdateDashboard_NoNameField verifies UpdateDashboard has no Name
// (rename) capability -- the real UpdateDashboardInput has none -- and that
// it does support the real Widgets/TerminationProtectionEnabled fields.
func TestUpdateDashboard_NoNameField(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	createRec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{"Name": "update-dash"})
	require.Equal(t, http.StatusOK, createRec.Code)
	dashARN, _ := parseCloudTrailResp(t, createRec)["DashboardArn"].(string)

	updateRec := doCloudTrailOp(t, h, "UpdateDashboard", map[string]any{
		"DashboardId":                  dashARN,
		"TerminationProtectionEnabled": true,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	resp := parseCloudTrailResp(t, updateRec)
	assert.Equal(t, "update-dash", resp["Name"], "Name is unchanged -- there is no rename capability")
	assert.Equal(t, true, resp["TerminationProtectionEnabled"])
	assert.Equal(t, "UPDATED", resp["Status"])
}

// TestStartDashboardRefresh_RefreshIDOnly verifies StartDashboardRefresh
// returns only RefreshId -- the real StartDashboardRefreshOutput's only
// field -- not the previous (fabricated) DashboardArn/Status shape.
func TestStartDashboardRefresh_RefreshIDOnly(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	createRec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{"Name": "refresh-dash"})
	require.Equal(t, http.StatusOK, createRec.Code)
	dashARN, _ := parseCloudTrailResp(t, createRec)["DashboardArn"].(string)

	rec := doCloudTrailOp(t, h, "StartDashboardRefresh", map[string]any{"DashboardId": dashARN})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)

	assert.NotEmpty(t, resp["RefreshId"])
	_, hasStatus := resp["Status"]
	assert.False(t, hasStatus, "StartDashboardRefreshOutput has no Status field")
}

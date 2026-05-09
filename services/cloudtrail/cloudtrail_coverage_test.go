package cloudtrail_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloudTrailCoverage_EventDataStore covers CreateEventDataStore, GetEventDataStore,
// UpdateEventDataStore, ListEventDataStores, RestoreEventDataStore,
// StartEventDataStoreIngestion, StopEventDataStoreIngestion, DeleteEventDataStore.
func TestCloudTrailCoverage_EventDataStore(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// CreateEventDataStore.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name":                         "test-eds",
		"MultiRegionEnabled":           true,
		"OrganizationEnabled":          false,
		"RetentionPeriod":              90,
		"TerminationProtectionEnabled": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// GetEventDataStore.
	rec = doCloudTrailOp(t, h, "GetEventDataStore", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateEventDataStore.
	rec = doCloudTrailOp(t, h, "UpdateEventDataStore", map[string]any{
		"EventDataStore":  edsARN,
		"RetentionPeriod": 180,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListEventDataStores.
	rec = doCloudTrailOp(t, h, "ListEventDataStores", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StartEventDataStoreIngestion.
	rec = doCloudTrailOp(t, h, "StartEventDataStoreIngestion", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StopEventDataStoreIngestion.
	rec = doCloudTrailOp(t, h, "StopEventDataStoreIngestion", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// RestoreEventDataStore.
	rec = doCloudTrailOp(t, h, "RestoreEventDataStore", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteEventDataStore.
	rec = doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCloudTrailCoverage_Channel covers CreateChannel, GetChannel, ListChannels,
// UpdateChannel, DeleteChannel.
func TestCloudTrailCoverage_Channel(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// CreateEventDataStore (needed for channel source).
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "ch-eds",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)

	// CreateChannel.
	rec = doCloudTrailOp(t, h, "CreateChannel", map[string]any{
		"Name":   "test-channel",
		"Source": "Custom",
		"Destinations": []map[string]any{
			{
				"Type":     "EVENT_DATA_STORE",
				"Location": edsARN,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	channelResp := parseCloudTrailResp(t, rec)
	channelARN, _ := channelResp["ChannelArn"].(string)
	require.NotEmpty(t, channelARN)

	// GetChannel.
	rec = doCloudTrailOp(t, h, "GetChannel", map[string]any{"Channel": channelARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListChannels.
	rec = doCloudTrailOp(t, h, "ListChannels", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateChannel.
	rec = doCloudTrailOp(t, h, "UpdateChannel", map[string]any{
		"Channel": channelARN,
		"Name":    "updated-channel",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteChannel.
	rec = doCloudTrailOp(t, h, "DeleteChannel", map[string]any{"Channel": channelARN})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCloudTrailCoverage_Dashboard covers CreateDashboard, GetDashboard, ListDashboards,
// UpdateDashboard, StartDashboardRefresh, DeleteDashboard.
func TestCloudTrailCoverage_Dashboard(t *testing.T) {
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

// TestCloudTrailCoverage_Queries covers StartQuery, GetQueryResults, ListQueries.
func TestCloudTrailCoverage_Queries(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create an EDS first.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "query-eds"})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// StartQuery.
	rec = doCloudTrailOp(t, h, "StartQuery", map[string]any{
		"QueryStatement": "SELECT * FROM " + edsARN + " LIMIT 10",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var queryID string

	if rec.Code == http.StatusOK {
		qresp := parseCloudTrailResp(t, rec)
		queryID, _ = qresp["QueryId"].(string)
	}

	// ListQueries.
	rec = doCloudTrailOp(t, h, "ListQueries", map[string]any{"EventDataStore": edsARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetQueryResults (if we have a query ID).
	if queryID != "" {
		rec = doCloudTrailOp(t, h, "GetQueryResults", map[string]any{
			"EventDataStore": edsARN,
			"QueryId":        queryID,
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	}
}

// TestCloudTrailCoverage_Imports covers StartImport, GetImport, ListImports, StopImport.
func TestCloudTrailCoverage_Imports(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create an EDS first.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{"Name": "import-eds"})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// StartImport.
	rec = doCloudTrailOp(t, h, "StartImport", map[string]any{
		"Destinations": []string{edsARN},
		"ImportSource": map[string]any{
			"S3": map[string]any{
				"S3LocationUri":  "s3://my-bucket/cloudtrail-logs/",
				"S3BucketRegion": "us-east-1",
				"S3PrefixType":   "Dynamic",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	importResp := parseCloudTrailResp(t, rec)
	importID, _ := importResp["ImportId"].(string)
	require.NotEmpty(t, importID)

	// GetImport.
	rec = doCloudTrailOp(t, h, "GetImport", map[string]any{"ImportId": importID})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListImports.
	rec = doCloudTrailOp(t, h, "ListImports", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// StopImport.
	rec = doCloudTrailOp(t, h, "StopImport", map[string]any{"ImportId": importID})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCloudTrailCoverage_BackendDirect covers backend operations directly.
func TestCloudTrailCoverage_BackendDirect(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Verify handler doesn't crash on various operations.
	ops := []string{
		"ListEventDataStores",
		"ListChannels",
		"ListDashboards",
		"ListQueries",
		"ListImports",
	}

	for _, op := range ops {
		rec := doCloudTrailOp(t, h, op, map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code, "op %s should return 200", op)

		var m map[string]any
		assert.NoError(t, json.NewDecoder(rec.Body).Decode(&m))
	}
}

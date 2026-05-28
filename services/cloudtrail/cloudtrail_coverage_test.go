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

// TestCloudTrailCoverage_InsightSelectors covers PutInsightSelectors and GetInsightSelectors.
func TestCloudTrailCoverage_InsightSelectors(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create a trail.
	rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "insights-trail",
		"S3BucketName": "bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// PutInsightSelectors.
	rec = doCloudTrailOp(t, h, "PutInsightSelectors", map[string]any{
		"TrailName": "insights-trail",
		"InsightSelectors": []map[string]any{
			{"InsightType": "ApiCallRateInsight"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	assert.NotEmpty(t, resp["TrailARN"])
	sels, ok := resp["InsightSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, sels, 1)

	// GetInsightSelectors.
	rec = doCloudTrailOp(t, h, "GetInsightSelectors", map[string]any{
		"TrailName": "insights-trail",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	getResp := parseCloudTrailResp(t, rec)
	assert.NotEmpty(t, getResp["TrailARN"])
	getSels, ok := getResp["InsightSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, getSels, 1)
	sel := getSels[0].(map[string]any)
	assert.Equal(t, "ApiCallRateInsight", sel["InsightType"])
}

// TestCloudTrailCoverage_AdvancedEventSelectors covers PutEventSelectors and
// GetEventSelectors with AdvancedEventSelectors.
func TestCloudTrailCoverage_AdvancedEventSelectors(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create a trail.
	rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "adv-trail-cov",
		"S3BucketName": "bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// PutEventSelectors with AdvancedEventSelectors.
	rec = doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
		"TrailName": "adv-trail-cov",
		"AdvancedEventSelectors": []map[string]any{
			{
				"Name": "All management events",
				"FieldSelectors": []map[string]any{
					{"Field": "eventCategory", "Equals": []string{"Management"}},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	assert.NotEmpty(t, resp["TrailARN"])
	advSels, ok := resp["AdvancedEventSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, advSels, 1)

	// GetEventSelectors returns AdvancedEventSelectors.
	rec = doCloudTrailOp(t, h, "GetEventSelectors", map[string]any{
		"TrailName": "adv-trail-cov",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	getResp := parseCloudTrailResp(t, rec)
	getAdvSels, ok := getResp["AdvancedEventSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, getAdvSels, 1)
}

// TestCloudTrailCoverage_Federation covers EnableFederation and DisableFederation.
func TestCloudTrailCoverage_Federation(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create an EDS.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "fed-eds-cov",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)

	// New EDS has DISABLED federation.
	assert.Equal(t, "DISABLED", resp["FederationStatus"])

	// EnableFederation.
	roleArn := "arn:aws:iam::123456789012:role/FedRole"
	rec = doCloudTrailOp(t, h, "EnableFederation", map[string]any{
		"EventDataStore":    edsARN,
		"FederationRoleArn": roleArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	enableResp := parseCloudTrailResp(t, rec)
	assert.Equal(t, "ENABLED", enableResp["FederationStatus"])
	assert.Equal(t, roleArn, enableResp["FederationRoleArn"])

	// DisableFederation.
	rec = doCloudTrailOp(t, h, "DisableFederation", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	disableResp := parseCloudTrailResp(t, rec)
	assert.Equal(t, "DISABLED", disableResp["FederationStatus"])
}

// TestCloudTrailCoverage_TerminationProtection verifies termination protection
// on event data stores.
func TestCloudTrailCoverage_TerminationProtection(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create a protected EDS.
	rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name":                         "term-prot-eds",
		"TerminationProtectionEnabled": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	edsARN, _ := resp["EventDataStoreArn"].(string)
	require.NotEmpty(t, edsARN)
	assert.Equal(t, true, resp["TerminationProtectionEnabled"])

	// Delete should fail.
	rec = doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Disable protection.
	boolFalse := false
	rec = doCloudTrailOp(t, h, "UpdateEventDataStore", map[string]any{
		"EventDataStore":               edsARN,
		"TerminationProtectionEnabled": &boolFalse,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete should now succeed.
	rec = doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCloudTrailCoverage_TrailStatus covers GetTrailStatus full response.
func TestCloudTrailCoverage_TrailStatus(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create and start logging.
	rec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "status-cov-trail",
		"S3BucketName": "bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doCloudTrailOp(t, h, "StartLogging", map[string]any{
		"Name": "status-cov-trail",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
		"Name": "status-cov-trail",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	assert.Equal(t, true, resp["IsLogging"])
	assert.NotNil(t, resp["StartLoggingTime"])
	assert.NotNil(t, resp["LatestDeliveryTime"])

	rec = doCloudTrailOp(t, h, "StopLogging", map[string]any{
		"Name": "status-cov-trail",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doCloudTrailOp(t, h, "GetTrailStatus", map[string]any{
		"Name": "status-cov-trail",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	stopResp := parseCloudTrailResp(t, rec)
	assert.Equal(t, false, stopResp["IsLogging"])
	assert.NotNil(t, stopResp["StopLoggingTime"])
}

// TestCloudTrailCoverage_MiscStubs covers misc stub handlers.
func TestCloudTrailCoverage_MiscStubs(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		body map[string]any
		name string
		op   string
	}{
		{
			name: "list_public_keys",
			op:   "ListPublicKeys",
			body: map[string]any{},
		},
		{
			name: "list_insights_data",
			op:   "ListInsightsData",
			body: map[string]any{},
		},
		{
			name: "list_insights_metric_data",
			op:   "ListInsightsMetricData",
			body: map[string]any{},
		},
		{
			name: "search_sample_queries",
			op:   "SearchSampleQueries",
			body: map[string]any{},
		},
		{
			name: "register_org_delegated_admin",
			op:   "RegisterOrganizationDelegatedAdmin",
			body: map[string]any{"MemberAccountId": "123456789012"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doCloudTrailOp(t, h, tt.op, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code, "op %s should return 200", tt.op)
		})
	}
}

// TestCloudTrailCoverage_LookupEvents covers LookupEvents with various attributes.
func TestCloudTrailCoverage_LookupEvents(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no_filters",
			body: map[string]any{},
		},
		{
			name: "with_event_name_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "EventName", "AttributeValue": "CreateTrail"},
				},
				"MaxResults": 10,
			},
		},
		{
			name: "with_event_id_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "EventId", "AttributeValue": "some-event-id"},
				},
			},
		},
		{
			name: "with_read_only_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "ReadOnly", "AttributeValue": "false"},
				},
			},
		},
		{
			name: "with_access_key_filter",
			body: map[string]any{
				"LookupAttributes": []any{
					map[string]any{"AttributeKey": "AccessKeyId", "AttributeValue": "AKIAIOSFODNN7EXAMPLE"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doCloudTrailOp(t, h, "LookupEvents", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
			resp := parseCloudTrailResp(t, rec)
			events, ok := resp["Events"].([]any)
			require.True(t, ok)
			assert.NotNil(t, events)
		})
	}
}

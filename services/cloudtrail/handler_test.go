package cloudtrail_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func newTestCloudTrailHandler() *cloudtrail.Handler {
	backend := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return cloudtrail.NewHandler(backend)
}

func doCloudTrailOp(
	t *testing.T,
	h *cloudtrail.Handler,
	operation string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CloudTrail_20131101."+operation)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseCloudTrailResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// TestCloudTrailMetadata exercises RouteMatcher, Name, and ChaosServiceName.
func TestCloudTrailMetadata(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "name",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "CloudTrail", h.Name())
			},
		},
		{
			name: "chaos_service_name",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "cloudtrail", h.ChaosServiceName())
			},
		},
		{
			name: "supported_operations",
			fn: func(t *testing.T) {
				t.Helper()
				ops := h.GetSupportedOperations()
				assert.NotEmpty(t, ops)
				assert.Contains(t, ops, "CreateTrail")
				assert.Contains(t, ops, "DeleteTrail")
			},
		},
		{
			name: "chaos_operations",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
			},
		},
		{
			name: "chaos_regions",
			fn: func(t *testing.T) {
				t.Helper()
				regions := h.ChaosRegions()
				assert.NotEmpty(t, regions)
			},
		},
		{
			name: "match_priority",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Positive(t, h.MatchPriority())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}

// TestCloudTrailRouteMatcher verifies the route matcher accepts/rejects requests.
func TestCloudTrailRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{
			name:      "matches_CreateTrail",
			target:    "CloudTrail_20131101.CreateTrail",
			wantMatch: true,
		},
		{
			name:      "matches_DescribeTrails",
			target:    "CloudTrail_20131101.DescribeTrails",
			wantMatch: true,
		},
		{
			name:      "no_match_ssm",
			target:    "AmazonSSM.GetParameter",
			wantMatch: false,
		},
		{
			name:      "no_match_empty",
			target:    "",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, matcher(c))
		})
	}
}

// TestCloudTrailUnknownOperation verifies an unknown operation returns an error.
func TestCloudTrailUnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	rec := doCloudTrailOp(t, h, "NonExistentOperation", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCloudTrailExtractOperation verifies ExtractOperation returns correct name.
func TestCloudTrailExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "create_trail",
			target: "CloudTrail_20131101.CreateTrail",
			wantOp: "CreateTrail",
		},
		{
			name:   "describe_trails",
			target: "CloudTrail_20131101.DescribeTrails",
			wantOp: "DescribeTrails",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

// TestCloudTrailExtractResource verifies ExtractResource always returns empty string.
func TestCloudTrailExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	e := echo.New()
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c))
}

// TestCloudTrailProvider exercises the Provider methods.
func TestCloudTrailProvider(t *testing.T) {
	t.Parallel()

	p := &cloudtrail.Provider{}

	tests := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{
			name: "name",
			fn: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "CloudTrail", p.Name())
			},
		},
		{
			name: "init",
			fn: func(t *testing.T) {
				t.Helper()
				// Provider.Init requires service.AppContext; test basic init with nil config.
				appCtx := &service.AppContext{}
				reg, err := p.Init(appCtx)
				require.NoError(t, err)
				require.NotNil(t, reg)
				assert.Equal(t, "CloudTrail", reg.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.fn(t)
		})
	}
}

// TestCloudTrailReset exercises Reset() on backend and handler.
func TestCloudTrailReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "reset_clears_trails",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name": "trail-a", "S3BucketName": "bucket",
				})
				h.Reset()
				rec := doCloudTrailOp(t, h, "ListTrails", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				list, ok := resp["Trails"].([]any)
				require.True(t, ok)
				assert.Empty(t, list)
			},
		},
		{
			name: "reset_clears_channels",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "chan-a", "Source": "src",
				})
				h.Reset()
				// after Reset, creating a channel with same name succeeds (uniqueness cleared)
				rec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "chan-a", "Source": "src",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "reset_clears_event_data_stores",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "eds-a",
				})
				h.Reset()
				// After reset, can create again with same name
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "eds-a",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
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

// TestCloudTrailDuplicateNameRejected verifies duplicate-name detection for new resource types.
func TestCloudTrailDuplicateNameRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "duplicate_channel_name_rejected",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec1 := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "dup-chan", "Source": "src",
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "dup-chan", "Source": "src",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
			},
		},
		{
			name: "duplicate_dashboard_name_rejected",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec1 := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "dup-dash",
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "dup-dash",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
			},
		},
		{
			name: "duplicate_event_data_store_name_rejected",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec1 := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "dup-eds",
				})
				assert.Equal(t, http.StatusOK, rec1.Code)
				rec2 := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "dup-eds",
				})
				assert.Equal(t, http.StatusConflict, rec2.Code)
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

// TestCloudTrailRequiredFieldValidation exercises required-field validation for newly validated ops.
func TestCloudTrailRequiredFieldValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "start_logging_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "StartLogging", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "stop_logging_missing_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "StopLogging", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "put_event_selectors_missing_trail_name",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
					"EventSelectors": []any{},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "cancel_query_missing_query_id",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CancelQuery", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_query_missing_query_id",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "event_data_store_status_is_enabled",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "status-check-eds",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				assert.Equal(t, "ENABLED", resp["Status"])
			},
		},
		{
			name: "dashboard_status_is_created",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "status-check-dash",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseCloudTrailResp(t, rec)
				// CreateDashboardOutput has no Status field -- only GetDashboardOutput does.
				_, hasStatus := resp["Status"]
				assert.False(t, hasStatus, "CreateDashboardOutput has no Status field")

				dashARN, _ := resp["DashboardArn"].(string)
				getRec := doCloudTrailOp(t, h, "GetDashboard", map[string]any{"DashboardId": dashARN})
				assert.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseCloudTrailResp(t, getRec)
				assert.Equal(t, "CREATED", getResp["Status"])
			},
		},
		{
			name: "channel_not_found_gives_404",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteChannel", map[string]any{
					"Channel": "nonexistent-channel",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "event_data_store_not_found_gives_404",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeleteEventDataStore", map[string]any{
					"EventDataStore": "nonexistent-eds",
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

// TestCloudTrailProviderInitNilCtx ensures Provider.Init is nil-safe.
func TestCloudTrailProviderInitNilCtx(t *testing.T) {
	t.Parallel()

	p := &cloudtrail.Provider{}
	reg, err := p.Init(nil)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "CloudTrail", reg.Name())
}

// TestCloudTrailListOperationsSmoke covers backend operations directly.
func TestCloudTrailListOperationsSmoke(t *testing.T) {
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

// TestCloudTrailAncillaryOperationsSmoke covers ancillary read-only handlers
// (ListPublicKeys, ListInsightsData, ListInsightsMetricData,
// SearchSampleQueries, RegisterOrganizationDelegatedAdmin).
func TestCloudTrailAncillaryOperationsSmoke(t *testing.T) {
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

// TestCloudTrailDeregisterOrgDelegatedAdmin exercises DeregisterOrganizationDelegatedAdmin.
func TestCloudTrailDeregisterOrgDelegatedAdmin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "deregister_success",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeregisterOrganizationDelegatedAdmin", map[string]any{
					"DelegatedAdminAccountId": "123456789012",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "deregister_missing_account_id",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "DeregisterOrganizationDelegatedAdmin", map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
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

// TestCloudTrailTrailStatusSmoke covers GetTrailStatus full response.
func TestCloudTrailTrailStatusSmoke(t *testing.T) {
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

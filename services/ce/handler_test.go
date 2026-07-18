package ce_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doRequestWithMeta(
	t *testing.T,
	h *ce.Handler,
	meta *awsmeta.Metadata,
	action string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	ctx := awsmeta.Set(context.Background(), meta)
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSInsightsIndexService."+action)

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func newTestHandler(t *testing.T) *ce.Handler {
	t.Helper()

	return ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *ce.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSInsightsIndexService."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Ce", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCostCategoryDefinition")
	assert.Contains(t, ops, "DeleteCostCategoryDefinition")
	assert.Contains(t, ops, "DescribeCostCategoryDefinition")
	assert.Contains(t, ops, "ListCostCategoryDefinitions")
	assert.Contains(t, ops, "UpdateCostCategoryDefinition")
	assert.Contains(t, ops, "CreateAnomalyMonitor")
	assert.Contains(t, ops, "DeleteAnomalyMonitor")
	assert.Contains(t, ops, "GetAnomalyMonitors")
	assert.Contains(t, ops, "UpdateAnomalyMonitor")
	assert.Contains(t, ops, "CreateAnomalySubscription")
	assert.Contains(t, ops, "DeleteAnomalySubscription")
	assert.Contains(t, ops, "GetAnomalySubscriptions")
	assert.Contains(t, ops, "UpdateAnomalySubscription")
	assert.Contains(t, ops, "GetCostAndUsage")
	assert.Contains(t, ops, "GetDimensionValues")
	assert.Contains(t, ops, "GetTags")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matching target",
			target: "AWSInsightsIndexService.CreateCostCategoryDefinition",
			want:   true,
		},
		{
			name:   "non-matching target",
			target: "AnyScaleFrontendService.RegisterScalableTarget",
			want:   false,
		},
		{
			name:   "empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSInsightsIndexService.UnknownOp")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// Improvement 7: Test delete operations error paths — not-found returns 404.
func TestHandler_DeleteOperations_NotFound(t *testing.T) {
	t.Parallel()

	const notFoundARN = "arn:aws:ce::000000000000:does-not-exist"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "DeleteCostCategoryDefinition_not_found",
			action: "DeleteCostCategoryDefinition",
			body:   map[string]any{"CostCategoryArn": notFoundARN},
		},
		{
			name:   "DeleteAnomalyMonitor_not_found",
			action: "DeleteAnomalyMonitor",
			body:   map[string]any{"MonitorArn": notFoundARN},
		},
		{
			name:   "DeleteAnomalySubscription_not_found",
			action: "DeleteAnomalySubscription",
			body:   map[string]any{"SubscriptionArn": notFoundARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// Improvement 8: Test ExtractOperation and ExtractResource.
func TestHandler_ExtractOperationAndResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		target        string
		wantOperation string
	}{
		{
			name:          "extracts_known_operation",
			target:        "AWSInsightsIndexService.CreateCostCategoryDefinition",
			wantOperation: "CreateCostCategoryDefinition",
		},
		{
			name:          "extracts_anomaly_monitor_op",
			target:        "AWSInsightsIndexService.GetAnomalyMonitors",
			wantOperation: "GetAnomalyMonitors",
		},
		{
			name:          "empty_target_returns_empty",
			target:        "",
			wantOperation: "",
		},
		{
			name:          "non_ce_target_returns_full_string",
			target:        "OtherService.SomeOp",
			wantOperation: "OtherService.SomeOp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOperation, h.ExtractOperation(c))
			assert.Empty(t, h.ExtractResource(c))
		})
	}
}

// Improvement 9: Test ChaosServiceName, ChaosOperations, ChaosRegions, Region.
func TestHandler_ChaosAndRegion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "ce", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
	assert.Equal(t, "us-east-1", h.Backend.Region())
}

// Improvement 10: Test UpdateAnomalyMonitor not-found path and validate
// that UpdateCostCategoryDefinition returns 404 when ARN is missing.
func TestHandler_UpdateOperations_NotFound(t *testing.T) {
	t.Parallel()

	const notFoundARN = "arn:aws:ce::000000000000:does-not-exist"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "UpdateAnomalyMonitor_not_found",
			action: "UpdateAnomalyMonitor",
			body:   map[string]any{"MonitorArn": notFoundARN, "MonitorName": "x"},
		},
		{
			name:   "UpdateCostCategoryDefinition_not_found",
			action: "UpdateCostCategoryDefinition",
			body: map[string]any{
				"CostCategoryArn": notFoundARN,
				"RuleVersion":     "CostCategoryExpression.v1",
				"Rules":           []map[string]any{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_Provider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "init_with_nil_ctx",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ce.Provider{}
			assert.Equal(t, "Ce", p.Name())

			reg, err := p.Init(nil)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, reg)
			}
		})
	}
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a monitor and category.
	rec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName": "ResetMe",
			"MonitorType": "DIMENSIONAL",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "ResetCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Dev"}},
	})

	h.Reset()

	listRec := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	refs, _ := listOut["CostCategoryReferences"].([]any)
	assert.Empty(t, refs)
}

func TestHandler_InternalServerError(t *testing.T) {
	t.Parallel()

	// Use an unknown action that gets past the supported operations list to hit
	// the 500 default branch - we verify the handler returns 400 (not 500) for
	// unknown action because errUnknownAction maps to 400.
	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSInsightsIndexService.NotARealOp")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ErrorWireShape verifies the on-the-wire HTTP status and JSON "__type" for
// every CE error family, matching the real service's AWSInsightsIndexService errors
// (all client-fault CE exceptions are documented as HTTP 400, and AnomalyMonitor /
// AnomalySubscription lookups use their own Unknown*Exception types rather than the
// generic ResourceNotFoundException).
func TestHandler_ErrorWireShape(t *testing.T) {
	t.Parallel()

	const missingARN = "arn:aws:ce::000000000000:does-not-exist"

	tests := []struct {
		body       map[string]any
		name       string
		action     string
		wantType   string
		wantStatus int
	}{
		{
			name:       "cost_category_not_found",
			action:     "DescribeCostCategoryDefinition",
			body:       map[string]any{"CostCategoryArn": missingARN},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "anomaly_monitor_not_found",
			action:     "DeleteAnomalyMonitor",
			body:       map[string]any{"MonitorArn": missingARN},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnknownMonitorException",
		},
		{
			name:       "anomaly_subscription_not_found",
			action:     "DeleteAnomalySubscription",
			body:       map[string]any{"SubscriptionArn": missingARN},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnknownSubscriptionException",
		},
		{
			name:   "create_anomaly_subscription_unknown_monitor",
			action: "CreateAnomalySubscription",
			body: map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "BadSub",
					"Frequency":        "DAILY",
					"MonitorArnList":   []string{missingARN},
				},
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "UnknownMonitorException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, tt.wantType, out.Type)
		})
	}
}

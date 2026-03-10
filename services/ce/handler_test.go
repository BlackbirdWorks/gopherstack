package ce_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ce"
)

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

func TestHandler_CostCategoryCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *ce.Handler)
		wantStatus int
	}{
		{
			name: "create_and_list",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "MyCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Engineering"}},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out["CostCategoryArn"])
				assert.NotEmpty(t, out["EffectiveStart"])
			},
		},
		{
			name: "list_cost_categories",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create first
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "ListCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Marketing"}},
				})
				// Then list
				rec := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				refs, ok := out["CostCategoryReferences"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, refs)
			},
		},
		{
			name: "describe_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "DescribeCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Finance"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Describe
				rec2 := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var describeOut map[string]any
				require.NoError(t, json.NewDecoder(rec2.Body).Decode(&describeOut))
				cat := describeOut["CostCategory"].(map[string]any)
				assert.Equal(t, "DescribeCategory", cat["Name"])
			},
		},
		{
			name: "update_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "UpdateCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Ops"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Update
				rec2 := doRequest(t, h, "UpdateCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
					"RuleVersion":     "CostCategoryExpression.v1",
					"Rules":           []map[string]any{{"Value": "Ops-Updated"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var updateOut map[string]any
				require.NoError(t, json.NewDecoder(rec2.Body).Decode(&updateOut))
				assert.Equal(t, arn, updateOut["CostCategoryArn"])
			},
		},
		{
			name: "delete_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				// Create
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "DeleteCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Temp"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Delete
				rec2 := doRequest(t, h, "DeleteCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// Describe should return 404
				rec3 := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
					"CostCategoryArn": arn,
				})
				assert.Equal(t, http.StatusNotFound, rec3.Code)
			},
		},
		{
			name: "create_missing_name_returns_400",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"RuleVersion": "CostCategoryExpression.v1",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_not_found_returns_404",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
					"CostCategoryArn": "arn:aws:ce::000000000000:costcategory/nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
		})
	}
}

func TestHandler_AnomalyMonitorCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, h *ce.Handler)
	}{
		{
			name: "create_and_get",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
					"AnomalyMonitor": map[string]any{
						"MonitorName": "MyMonitor",
						"MonitorType": "DIMENSIONAL",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out["MonitorArn"])

				monARN := out["MonitorArn"].(string)

				rec2 := doRequest(t, h, "GetAnomalyMonitors", map[string]any{
					"MonitorArnList": []string{monARN},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
			},
		},
		{
			name: "update_monitor",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
					"AnomalyMonitor": map[string]any{
						"MonitorName": "OldName",
						"MonitorType": "DIMENSIONAL",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				monARN := createOut["MonitorArn"].(string)

				rec2 := doRequest(t, h, "UpdateAnomalyMonitor", map[string]any{
					"MonitorArn":  monARN,
					"MonitorName": "NewName",
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
			},
		},
		{
			name: "delete_monitor",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
					"AnomalyMonitor": map[string]any{
						"MonitorName": "ToDelete",
						"MonitorType": "DIMENSIONAL",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				monARN := createOut["MonitorArn"].(string)

				rec2 := doRequest(t, h, "DeleteAnomalyMonitor", map[string]any{
					"MonitorArn": monARN,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
		})
	}
}

func TestHandler_AnomalySubscriptionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, h *ce.Handler)
	}{
		{
			name: "create_and_get",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
					"AnomalySubscription": map[string]any{
						"SubscriptionName": "MySub",
						"Frequency":        "DAILY",
						"Subscribers": []map[string]any{
							{"Address": "test@example.com", "Type": "EMAIL"},
						},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out["SubscriptionArn"])
			},
		},
		{
			name: "delete_subscription",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
					"AnomalySubscription": map[string]any{
						"SubscriptionName": "ToDelete",
						"Frequency":        "DAILY",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				subARN := createOut["SubscriptionArn"].(string)

				rec2 := doRequest(t, h, "DeleteAnomalySubscription", map[string]any{
					"SubscriptionArn": subARN,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
		})
	}
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, h *ce.Handler)
	}{
		{
			name: "tag_and_untag_cost_category",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "TaggedCategory",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Test"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
				arn := createOut["CostCategoryArn"].(string)

				// Tag
				rec2 := doRequest(t, h, "TagResource", map[string]any{
					"ResourceArn":  arn,
					"ResourceTags": map[string]string{"Env": "prod"},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// List tags
				rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec3.Code)

				var listOut map[string]any
				require.NoError(t, json.NewDecoder(rec3.Body).Decode(&listOut))
				tags, _ := listOut["ResourceTags"].(map[string]any)
				assert.Equal(t, "prod", tags["Env"])

				// Untag
				rec4 := doRequest(t, h, "UntagResource", map[string]any{
					"ResourceArn":     arn,
					"ResourceTagKeys": []string{"Env"},
				})
				assert.Equal(t, http.StatusOK, rec4.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
		})
	}
}

func TestHandler_CostQueryStubs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		body   any
	}{
		{
			name:   "get_cost_and_usage",
			action: "GetCostAndUsage",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
		},
		{
			name:   "get_dimension_values",
			action: "GetDimensionValues",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Dimension":  "SERVICE",
			},
		},
		{
			name:   "get_tags",
			action: "GetTags",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)
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

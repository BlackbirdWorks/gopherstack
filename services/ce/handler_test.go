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
		setup func(t *testing.T, h *ce.Handler)
		name  string
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
		setup func(t *testing.T, h *ce.Handler)
		name  string
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
		setup func(t *testing.T, h *ce.Handler)
		name  string
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
		setup func(t *testing.T, h *ce.Handler)
		name  string
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
					"ResourceTags": []map[string]string{{"Key": "Env", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				// List tags
				rec3 := doRequest(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec3.Code)

				var listOut map[string]any
				require.NoError(t, json.NewDecoder(rec3.Body).Decode(&listOut))
				tags, _ := listOut["ResourceTags"].([]any)
				var envVal string
				for _, tagAny := range tags {
					tag, _ := tagAny.(map[string]any)
					if tag["Key"] == "Env" {
						envVal, _ = tag["Value"].(string)
					}
				}
				assert.Equal(t, "prod", envVal)

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
		body   any
		name   string
		action string
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

func TestHandler_ForecastStubs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		action string
	}{
		{
			name:   "get_cost_forecast",
			action: "GetCostForecast",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
				"Granularity": "MONTHLY",
				"Metric":      "BLENDED_COST",
			},
		},
		{
			name:   "get_usage_forecast",
			action: "GetUsageForecast",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-02-01", "End": "2024-03-01"},
				"Granularity": "MONTHLY",
				"Metric":      "USAGE_QUANTITY",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out["Total"])
			forecastResults, ok := out["ForecastResultsByTime"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, forecastResults)
		})
	}
}

func TestCEHandler_UpdateCostCategoryDefinition_DeepCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		initialRules      []map[string]any
		updatedRules      []map[string]any
		wantRulesAfterGet int
	}{
		{
			name:              "rules_are_deep_copied_on_update",
			initialRules:      []map[string]any{{"value": "old"}},
			updatedRules:      []map[string]any{{"value": "new"}, {"value": "extra"}},
			wantRulesAfterGet: 2,
		},
		{
			name:              "empty_rules_on_update",
			initialRules:      []map[string]any{{"value": "initial"}},
			updatedRules:      []map[string]any{},
			wantRulesAfterGet: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
				"Name":        "test-cat",
				"RuleVersion": "CostCategoryExpression.v1",
				"Rules":       tt.initialRules,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				CostCategoryArn string `json:"CostCategoryArn"`
			}
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))

			updateRec := doRequest(t, h, "UpdateCostCategoryDefinition", map[string]any{
				"CostCategoryArn": createOut.CostCategoryArn,
				"RuleVersion":     "CostCategoryExpression.v1",
				"Rules":           tt.updatedRules,
			})
			require.Equal(t, http.StatusOK, updateRec.Code)

			describeRec := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
				"CostCategoryArn": createOut.CostCategoryArn,
			})
			require.Equal(t, http.StatusOK, describeRec.Code)

			var describeOut struct {
				CostCategory struct {
					Rules []map[string]any `json:"rules"`
				} `json:"CostCategory"`
			}

			require.NoError(t, json.NewDecoder(describeRec.Body).Decode(&describeOut))
			assert.Len(t, describeOut.CostCategory.Rules, tt.wantRulesAfterGet)
		})
	}
}

// Improvement 1: Test GetAnomalySubscriptions (handler was at 0% coverage).
func TestHandler_GetAnomalySubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filterARNs     []string
		wantLen        int
		wantStatusCode int
	}{
		{
			name:           "returns_all_when_no_filter",
			filterARNs:     nil,
			wantLen:        2,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "returns_empty_for_missing_arns",
			filterARNs:     []string{"arn:aws:ce::000:sub/does-not-exist"},
			wantLen:        0,
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create two subscriptions to query.
			for _, name := range []string{"Sub-A", "Sub-B"} {
				rec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
					"AnomalySubscription": map[string]any{
						"SubscriptionName": name,
						"Frequency":        "DAILY",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			body := map[string]any{}
			if tt.filterARNs != nil {
				body["SubscriptionArnList"] = tt.filterARNs
			}

			rec := doRequest(t, h, "GetAnomalySubscriptions", body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				AnomalySubscriptions []map[string]any `json:"AnomalySubscriptions"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.AnomalySubscriptions, tt.wantLen)
		})
	}
}

// Improvement 2: Test UpdateAnomalySubscription (handler + backend were at 0% coverage).
func TestHandler_UpdateAnomalySubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody     map[string]any
		name           string
		wantFrequency  string
		wantStatusCode int
	}{
		{
			name: "updates_frequency",
			updateBody: map[string]any{
				"Frequency": "WEEKLY",
			},
			wantStatusCode: http.StatusOK,
			wantFrequency:  "WEEKLY",
		},
		{
			name: "missing_subscription_arn_returns_400",
			updateBody: map[string]any{
				"Frequency": "WEEKLY",
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "not_found_returns_404",
			updateBody: map[string]any{
				"SubscriptionArn": "arn:aws:ce::000:sub/not-found",
				"Frequency":       "WEEKLY",
			},
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantFrequency != "" {
				// Create a subscription first.
				createRec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
					"AnomalySubscription": map[string]any{
						"SubscriptionName": "UpdateMe",
						"Frequency":        "DAILY",
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createOut map[string]any
				require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
				tt.updateBody["SubscriptionArn"] = createOut["SubscriptionArn"]
			}

			rec := doRequest(t, h, "UpdateAnomalySubscription", tt.updateBody)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantFrequency != "" {
				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.NotEmpty(t, out["SubscriptionArn"])
			}
		})
	}
}

// Improvement 3: Test tag operations on anomaly monitors.
func TestHandler_TagOperations_AnomalyMonitor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a monitor to tag.
	createRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName": "MonitorToTag",
			"MonitorType": "DIMENSIONAL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	monitorARN := createOut["MonitorArn"].(string)

	// Tag.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn":  monitorARN,
		"ResourceTags": []map[string]string{{"Key": "Team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags — expect "Team=platform".
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": monitorARN,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	tags, _ := listOut["ResourceTags"].([]any)

	var found bool

	for _, tagAny := range tags {
		tag, _ := tagAny.(map[string]any)
		if tag["Key"] == "Team" && tag["Value"] == "platform" {
			found = true
		}
	}

	assert.True(t, found, "expected Team=platform tag on monitor")

	// Untag.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":     monitorARN,
		"ResourceTagKeys": []string{"Team"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

// Improvement 4: Test tag operations on anomaly subscriptions.
func TestHandler_TagOperations_AnomalySubscription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create subscription to tag.
	createRec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
		"AnomalySubscription": map[string]any{
			"SubscriptionName": "SubToTag",
			"Frequency":        "DAILY",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	subARN := createOut["SubscriptionArn"].(string)

	// Tag.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn":  subARN,
		"ResourceTags": []map[string]string{{"Key": "Owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// List tags.
	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": subARN,
	})
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	tags, _ := listOut["ResourceTags"].([]any)
	assert.NotEmpty(t, tags)

	// Untag.
	untagRec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn":     subARN,
		"ResourceTagKeys": []string{"Owner"},
	})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

// Improvement 5: Test tag operations error paths — missing ARN returns 400.
func TestHandler_TagOperations_MissingARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "ListTagsForResource_missing_arn",
			action: "ListTagsForResource",
			body:   map[string]any{},
		},
		{
			name:   "TagResource_missing_arn",
			action: "TagResource",
			body:   map[string]any{"ResourceTags": []map[string]string{{"Key": "k", "Value": "v"}}},
		},
		{
			name:   "UntagResource_missing_arn",
			action: "UntagResource",
			body:   map[string]any{"ResourceTagKeys": []string{"k"}},
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

// Improvement 6: Test tag operations error paths — not-found ARN returns 404.
func TestHandler_TagOperations_NotFound(t *testing.T) {
	t.Parallel()

	const notFoundARN = "arn:aws:ce::000000000000:costcategory/does-not-exist"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "ListTagsForResource_not_found",
			action: "ListTagsForResource",
			body:   map[string]any{"ResourceArn": notFoundARN},
		},
		{
			name:   "TagResource_not_found",
			action: "TagResource",
			body: map[string]any{
				"ResourceArn":  notFoundARN,
				"ResourceTags": []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name:   "UntagResource_not_found",
			action: "UntagResource",
			body: map[string]any{
				"ResourceArn":     notFoundARN,
				"ResourceTagKeys": []string{"k"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
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
			assert.Equal(t, http.StatusNotFound, rec.Code)
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
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_GetAnomalies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name:           "returns_empty_with_no_anomalies",
			body:           map[string]any{},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "filter_by_monitor_arn",
			body: map[string]any{
				"MonitorArn": "arn:aws:ce::000000000000:anomalymonitor/test",
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "with_date_interval",
			body: map[string]any{
				"DateInterval": map[string]string{
					"StartDate": "2024-01-01",
					"EndDate":   "2024-02-01",
				},
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetAnomalies", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				Anomalies []any `json:"Anomalies"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotNil(t, out.Anomalies)
		})
	}
}

func TestHandler_GetApproximateUsageRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty",
			body: map[string]any{
				"ApproximationDimension": "SERVICE",
				"Granularity":            "MONTHLY",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetApproximateUsageRecords", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				Services     map[string]any `json:"Services"`
				TotalRecords string         `json:"TotalRecords"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Equal(t, "0", out.TotalRecords)
		})
	}
}

func TestHandler_GetCommitmentPurchaseAnalysis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name:           "not_found_for_unknown_analysis",
			body:           map[string]any{"AnalysisId": "analysis-123"},
			wantStatusCode: http.StatusNotFound,
		},
		{
			name:           "missing_analysis_id_returns_400",
			body:           map[string]any{},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCommitmentPurchaseAnalysis", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

func TestHandler_GetCostAndUsageComparisons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_comparisons",
			body: map[string]any{
				"BaseTimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"ComparisonTimePeriod": map[string]string{"Start": "2023-01-01", "End": "2023-02-01"},
				"Granularity":          "MONTHLY",
				"Metrics":              []string{"BlendedCost"},
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsageComparisons", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				CostAndUsages []any `json:"CostAndUsages"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Empty(t, out.CostAndUsages)
		})
	}
}

func TestHandler_GetCostAndUsageWithResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_results",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsageWithResources", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				ResultsByTime []any `json:"ResultsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Empty(t, out.ResultsByTime)
		})
	}
}

func TestHandler_GetCostCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup             func(*testing.T, *ce.Handler)
		name              string
		costCategoryName  string
		wantValuesContain string
		wantLen           int
	}{
		{
			name:    "returns_empty_when_no_categories",
			wantLen: 0,
		},
		{
			name:             "returns_values_for_existing_category",
			costCategoryName: "Env",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "Env",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Production"}},
				})
			},
			wantValuesContain: "Production",
			wantLen:           1,
		},
		{
			name: "returns_all_values_when_no_filter",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "Env",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Production"}},
				})
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "Team",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "Platform"}},
				})
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			}
			if tt.costCategoryName != "" {
				body["CostCategoryName"] = tt.costCategoryName
			}

			rec := doRequest(t, h, "GetCostCategories", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				CostCategoryValues []string `json:"CostCategoryValues"`
				ReturnSize         int      `json:"ReturnSize"`
				TotalSize          int      `json:"TotalSize"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.CostCategoryValues, tt.wantLen)
			assert.Equal(t, tt.wantLen, out.ReturnSize)
			assert.Equal(t, tt.wantLen, out.TotalSize)

			if tt.wantValuesContain != "" {
				assert.Contains(t, out.CostCategoryValues, tt.wantValuesContain)
			}
		})
	}
}

func TestHandler_GetCostComparisonDrivers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_drivers",
			body: map[string]any{
				"BaselineTimePeriod":   map[string]string{"Start": "2023-01-01", "End": "2024-01-01"},
				"ComparisonTimePeriod": map[string]string{"Start": "2024-01-01", "End": "2025-01-01"},
				"Metric":               "BlendedCost",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostComparisonDrivers", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				CostComparisonDrivers []any `json:"CostComparisonDrivers"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Empty(t, out.CostComparisonDrivers)
		})
	}
}

func TestHandler_GetReservationCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_coverage",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationCoverage", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				CoveragesByTime []any `json:"CoveragesByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.CoveragesByTime)
		})
	}
}

func TestHandler_GetReservationPurchaseRecommendation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_recommendations_with_synthetic_data",
			body: map[string]any{
				"Service":              "Amazon Elastic Compute Cloud - Compute",
				"LookbackPeriodInDays": "SIXTY_DAYS",
				"TermInYears":          "ONE_YEAR",
				"PaymentOption":        "NO_UPFRONT",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationPurchaseRecommendation", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				Recommendations []any `json:"Recommendations"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.Recommendations)
		})
	}
}

func TestHandler_GetReservationUtilization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_utilization_with_synthetic_data",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
			},
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetReservationUtilization", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				UtilizationsByTime []any `json:"UtilizationsByTime"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.NotEmpty(t, out.UtilizationsByTime)
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

func TestHandler_MonitorTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "valid_dimensional_type",
			body: map[string]any{
				"AnomalyMonitor": map[string]any{
					"MonitorName": "DimMonitor",
					"MonitorType": "DIMENSIONAL",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "valid_custom_type",
			body: map[string]any{
				"AnomalyMonitor": map[string]any{
					"MonitorName": "CustomMonitor",
					"MonitorType": "CUSTOM",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "invalid_monitor_type_returns_400",
			body: map[string]any{
				"AnomalyMonitor": map[string]any{
					"MonitorName": "BadMonitor",
					"MonitorType": "INVALID_TYPE",
				},
			},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateAnomalyMonitor", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

func TestHandler_FrequencyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "valid_daily_frequency",
			body: map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "DailySub",
					"Frequency":        "DAILY",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "valid_immediate_frequency",
			body: map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "ImmediateSub",
					"Frequency":        "IMMEDIATE",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "valid_weekly_frequency",
			body: map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "WeeklySub",
					"Frequency":        "WEEKLY",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "invalid_frequency_returns_400",
			body: map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "BadSub",
					"Frequency":        "YEARLY",
				},
			},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateAnomalySubscription", tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)
		})
	}
}

func TestHandler_GetAnomalySubscriptions_MonitorArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		monitorFilter  string
		wantLen        int
		wantStatusCode int
	}{
		{
			name:           "filter_by_monitor_arn_matches",
			monitorFilter:  "PLACEHOLDER",
			wantLen:        1,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "filter_by_nonexistent_monitor_arn",
			monitorFilter:  "arn:aws:ce::000:anomalymonitor/does-not-exist",
			wantLen:        0,
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "no_monitor_filter_returns_all",
			monitorFilter:  "",
			wantLen:        2,
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a monitor to attach subscriptions to.
			monRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
				"AnomalyMonitor": map[string]any{
					"MonitorName": "FilterMon",
					"MonitorType": "DIMENSIONAL",
				},
			})
			require.Equal(t, http.StatusOK, monRec.Code)

			var monOut map[string]any
			require.NoError(t, json.NewDecoder(monRec.Body).Decode(&monOut))
			monARN := monOut["MonitorArn"].(string)

			// Create subscription attached to monitor.
			doRequest(t, h, "CreateAnomalySubscription", map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "AttachedSub",
					"Frequency":        "DAILY",
					"MonitorArnList":   []string{monARN},
				},
			})

			// Create subscription NOT attached to monitor.
			doRequest(t, h, "CreateAnomalySubscription", map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "UnattachedSub",
					"Frequency":        "WEEKLY",
				},
			})

			monitorFilter := tt.monitorFilter
			if monitorFilter == "PLACEHOLDER" {
				monitorFilter = monARN
			}

			body := map[string]any{}
			if monitorFilter != "" {
				body["MonitorArn"] = monitorFilter
			}

			rec := doRequest(t, h, "GetAnomalySubscriptions", body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				AnomalySubscriptions []map[string]any `json:"AnomalySubscriptions"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.AnomalySubscriptions, tt.wantLen)
		})
	}
}

func TestHandler_GetAnomalies_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(*testing.T, *ce.Handler) (monARN string)
		body           map[string]any
		name           string
		wantLen        int
		wantStatusCode int
	}{
		{
			name: "returns_all_when_no_filter",
			setup: func(t *testing.T, h *ce.Handler) string {
				t.Helper()
				h.Backend.AddAnomaly(ce.Anomaly{AnomalyID: "a1", MonitorARN: "m1", FeedbackType: "YES"})
				h.Backend.AddAnomaly(ce.Anomaly{AnomalyID: "a2", MonitorARN: "m2", FeedbackType: "NO"})

				return ""
			},
			body:           map[string]any{},
			wantLen:        2,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "filter_by_monitor_arn",
			setup: func(t *testing.T, h *ce.Handler) string {
				t.Helper()
				h.Backend.AddAnomaly(ce.Anomaly{AnomalyID: "a3", MonitorARN: "m3", FeedbackType: "YES"})
				h.Backend.AddAnomaly(ce.Anomaly{AnomalyID: "a4", MonitorARN: "m4", FeedbackType: "YES"})

				return "m3"
			},
			body:           map[string]any{},
			wantLen:        1,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "filter_by_feedback",
			setup: func(t *testing.T, h *ce.Handler) string {
				t.Helper()
				h.Backend.AddAnomaly(ce.Anomaly{AnomalyID: "a5", MonitorARN: "m5", FeedbackType: "YES"})
				h.Backend.AddAnomaly(ce.Anomaly{AnomalyID: "a6", MonitorARN: "m5", FeedbackType: "NO"})

				return ""
			},
			body:           map[string]any{"Feedback": "YES"},
			wantLen:        1,
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			monARN := tt.setup(t, h)

			body := tt.body
			if monARN != "" {
				body["MonitorArn"] = monARN
			}

			rec := doRequest(t, h, "GetAnomalies", body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			var out struct {
				Anomalies []map[string]any `json:"Anomalies"`
			}
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
			assert.Len(t, out.Anomalies, tt.wantLen)
		})
	}
}

func TestHandler_DuplicateCostCategory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"Name":        "DupCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Prod"}},
	}

	rec1 := doRequest(t, h, "CreateCostCategoryDefinition", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "CreateCostCategoryDefinition", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandler_SortedOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(*testing.T, *ce.Handler)
		verify func(*testing.T, *ce.Handler)
		name   string
	}{
		{
			name: "list_cost_categories_sorted_by_name",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()

				for _, name := range []string{"Zebra", "Alpha", "Mango"} {
					doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
						"Name":        name,
						"RuleVersion": "CostCategoryExpression.v1",
						"Rules":       []map[string]any{{"Value": name}},
					})
				}
			},
			verify: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					CostCategoryReferences []struct {
						Name string `json:"Name"`
					} `json:"CostCategoryReferences"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.Len(t, out.CostCategoryReferences, 3)
				assert.Equal(t, "Alpha", out.CostCategoryReferences[0].Name)
				assert.Equal(t, "Mango", out.CostCategoryReferences[1].Name)
				assert.Equal(t, "Zebra", out.CostCategoryReferences[2].Name)
			},
		},
		{
			name: "get_cost_categories_values_sorted",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "SortCat",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules": []map[string]any{
						{"Value": "Zulu"},
						{"Value": "Alpha"},
						{"Value": "Mike"},
					},
				})
			},
			verify: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "GetCostCategories", map[string]any{
					"CostCategoryName": "SortCat",
					"TimePeriod":       map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					CostCategoryValues []string `json:"CostCategoryValues"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.Len(t, out.CostCategoryValues, 3)
				assert.Equal(t, []string{"Alpha", "Mike", "Zulu"}, out.CostCategoryValues)
			},
		},
		{
			name: "list_tags_for_resource_sorted_by_key",
			setup: func(t *testing.T, h *ce.Handler) {
				t.Helper()
				rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
					"Name":        "TagSortCat",
					"RuleVersion": "CostCategoryExpression.v1",
					"Rules":       []map[string]any{{"Value": "x"}},
					"ResourceTags": []map[string]string{
						{"Key": "Zebra", "Value": "z"},
						{"Key": "Alpha", "Value": "a"},
						{"Key": "Mike", "Value": "m"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			verify: func(t *testing.T, h *ce.Handler) {
				t.Helper()

				cats, _ := h.Backend.ListCostCategoryDefinitions(0, "")
				require.Len(t, cats, 1)

				rec := doRequest(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": cats[0].ARN,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					ResourceTags []struct {
						Key string `json:"Key"`
					} `json:"ResourceTags"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				require.Len(t, out.ResourceTags, 3)
				assert.Equal(t, "Alpha", out.ResourceTags[0].Key)
				assert.Equal(t, "Mike", out.ResourceTags[1].Key)
				assert.Equal(t, "Zebra", out.ResourceTags[2].Key)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)
			tt.verify(t, h)
		})
	}
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

func TestHandler_ListTagsForResource_EmptyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cost category without tags.
	rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "NoTagsCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Test"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createOut))
	arn := createOut["CostCategoryArn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		ResourceTags []map[string]string `json:"ResourceTags"`
	}
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&out))
	// Must be an empty array, not nil/absent.
	assert.NotNil(t, out.ResourceTags)
	assert.Empty(t, out.ResourceTags)
}

func TestHandler_UpdateAnomalySubscription_AllBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody     map[string]any
		name           string
		wantFrequency  string
		wantSubName    string
		wantStatusCode int
	}{
		{
			name: "update_only_frequency",
			updateBody: map[string]any{
				"Frequency": "WEEKLY",
			},
			wantStatusCode: http.StatusOK,
			wantFrequency:  "WEEKLY",
			wantSubName:    "OriginalName",
		},
		{
			name: "update_only_subscription_name",
			updateBody: map[string]any{
				"SubscriptionName": "UpdatedName",
			},
			wantStatusCode: http.StatusOK,
			wantFrequency:  "DAILY",
			wantSubName:    "UpdatedName",
		},
		{
			name: "update_threshold",
			updateBody: map[string]any{
				"Threshold": 100.0,
			},
			wantStatusCode: http.StatusOK,
			wantFrequency:  "DAILY",
			wantSubName:    "OriginalName",
		},
		{
			name: "update_monitor_arn_list",
			updateBody: map[string]any{
				"MonitorArnList": []string{"arn:aws:ce::000:anomalymonitor/test"},
			},
			wantStatusCode: http.StatusOK,
			wantFrequency:  "DAILY",
			wantSubName:    "OriginalName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
				"AnomalySubscription": map[string]any{
					"SubscriptionName": "OriginalName",
					"Frequency":        "DAILY",
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut map[string]any
			require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
			subARN := createOut["SubscriptionArn"].(string)

			tt.updateBody["SubscriptionArn"] = subARN

			rec := doRequest(t, h, "UpdateAnomalySubscription", tt.updateBody)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			// Verify via GetAnomalySubscriptions.
			getRec := doRequest(t, h, "GetAnomalySubscriptions", map[string]any{
				"SubscriptionArnList": []string{subARN},
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut struct {
				AnomalySubscriptions []struct {
					SubscriptionName string  `json:"SubscriptionName"`
					Frequency        string  `json:"Frequency"`
					Threshold        float64 `json:"Threshold"`
				} `json:"AnomalySubscriptions"`
			}
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getOut))
			require.Len(t, getOut.AnomalySubscriptions, 1)
			assert.Equal(t, tt.wantFrequency, getOut.AnomalySubscriptions[0].Frequency)
			assert.Equal(t, tt.wantSubName, getOut.AnomalySubscriptions[0].SubscriptionName)
		})
	}
}

func TestHandler_SnapshotRestoreWithAnomalies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:    "snap-anomaly-1",
		MonitorARN:   "arn:aws:ce::000:anomalymonitor/test",
		FeedbackType: "YES",
	})

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, fresh.Restore(t.Context(), snap))

	rec := doRequest(t, fresh, "GetAnomalies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Anomalies []map[string]any `json:"Anomalies"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.Anomalies, 1)
	assert.Equal(t, "snap-anomaly-1", out.Anomalies[0]["AnomalyId"])
}

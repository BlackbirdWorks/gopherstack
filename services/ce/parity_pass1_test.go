package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestParity_GetCostAndUsage_GranularityRequired verifies real AWS requires Granularity.
func TestParity_GetCostAndUsage_GranularityRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_granularity_returns_400",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Metrics":    []string{"BlendedCost"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_granularity_returns_200",
			body: map[string]any{
				"TimePeriod":  map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Granularity": "MONTHLY",
				"Metrics":     []string{"BlendedCost"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetCostAndUsage", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_GetDimensionValues_DimensionRequired verifies real AWS requires Dimension.
func TestParity_GetDimensionValues_DimensionRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "missing_dimension_returns_400",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_dimension_returns_200",
			body: map[string]any{
				"TimePeriod": map[string]string{"Start": "2024-01-01", "End": "2024-02-01"},
				"Dimension":  "SERVICE",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "GetDimensionValues", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_GetAnomalyMonitors_HasCreationDateAndLastUpdatedDate verifies real AWS
// returns CreationDate and LastUpdatedDate as epoch-second floats in GetAnomalyMonitors.
func TestParity_GetAnomalyMonitors_HasCreationDateAndLastUpdatedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a monitor
	createRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName": "test-monitor",
			"MonitorType": "DIMENSIONAL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	monARN := createOut["MonitorArn"].(string)

	// Describe it
	getRec := doRequest(t, h, "GetAnomalyMonitors", map[string]any{
		"MonitorArnList": []string{monARN},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		AnomalyMonitors []struct {
			MonitorArn      string  `json:"MonitorArn"`
			CreationDate    float64 `json:"CreationDate"`
			LastUpdatedDate float64 `json:"LastUpdatedDate"`
		} `json:"AnomalyMonitors"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	require.Len(t, getOut.AnomalyMonitors, 1)

	m := getOut.AnomalyMonitors[0]
	assert.Equal(t, monARN, m.MonitorArn)
	assert.Positive(t, m.CreationDate, "CreationDate must be a positive epoch-second value")
	assert.Positive(t, m.LastUpdatedDate, "LastUpdatedDate must be a positive epoch-second value")
}

// TestParity_GetAnomalyMonitors_Pagination verifies MaxResults/NextPageToken pagination.
func TestParity_GetAnomalyMonitors_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
			"AnomalyMonitor": map[string]any{
				"MonitorName": "monitor-" + string(rune('a'+i)),
				"MonitorType": "DIMENSIONAL",
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "GetAnomalyMonitors", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["AnomalyMonitors"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextPageToken"].(string)
	assert.True(t, ok && nextToken != "", "NextPageToken must be present after partial page")

	rec2 := doRequest(t, h, "GetAnomalyMonitors", map[string]any{
		"MaxResults":    2,
		"NextPageToken": nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2 := out2["AnomalyMonitors"].([]any)
	assert.Len(t, page2, 2)
}

// TestParity_GetAnomalySubscriptions_HasAccountId verifies real AWS returns AccountId
// in each subscription entry.
func TestParity_GetAnomalySubscriptions_HasAccountId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	monRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName": "mon1",
			"MonitorType": "DIMENSIONAL",
		},
	})
	require.Equal(t, http.StatusOK, monRec.Code)

	var monOut map[string]any
	require.NoError(t, json.Unmarshal(monRec.Body.Bytes(), &monOut))
	monARN := monOut["MonitorArn"].(string)

	subRec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
		"AnomalySubscription": map[string]any{
			"SubscriptionName": "test-sub",
			"Frequency":        "DAILY",
			"MonitorArnList":   []string{monARN},
			"Subscribers": []map[string]string{
				{"Address": "test@example.com", "Type": "EMAIL", "Status": "CONFIRMED"},
			},
		},
	})
	require.Equal(t, http.StatusOK, subRec.Code)

	getRec := doRequest(t, h, "GetAnomalySubscriptions", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		AnomalySubscriptions []struct {
			AccountID string `json:"AccountId"`
		} `json:"AnomalySubscriptions"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	require.Len(t, getOut.AnomalySubscriptions, 1)
	assert.NotEmpty(t, getOut.AnomalySubscriptions[0].AccountID, "AccountId must be present in subscription")
}

// TestParity_GetAnomalySubscriptions_Pagination verifies MaxResults/NextPageToken pagination.
func TestParity_GetAnomalySubscriptions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	monRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
		"AnomalyMonitor": map[string]any{
			"MonitorName": "mon-pag",
			"MonitorType": "DIMENSIONAL",
		},
	})
	require.Equal(t, http.StatusOK, monRec.Code)

	var monOut map[string]any
	require.NoError(t, json.Unmarshal(monRec.Body.Bytes(), &monOut))
	monARN := monOut["MonitorArn"].(string)

	for i := range 5 {
		rec := doRequest(t, h, "CreateAnomalySubscription", map[string]any{
			"AnomalySubscription": map[string]any{
				"SubscriptionName": "sub-" + string(rune('a'+i)),
				"Frequency":        "DAILY",
				"MonitorArnList":   []string{monARN},
				"Subscribers": []map[string]string{
					{"Address": "test@example.com", "Type": "EMAIL", "Status": "CONFIRMED"},
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "GetAnomalySubscriptions", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["AnomalySubscriptions"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextPageToken"].(string)
	assert.True(t, ok && nextToken != "", "NextPageToken must be present after partial page")
}

// TestParity_GetAnomalies_DateIntervalFilters verifies DateInterval is used to filter anomalies.
func TestParity_GetAnomalies_DateIntervalFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:        "old-anomaly",
		MonitorARN:       "arn:aws:ce::000:anomalymonitor/test",
		AnomalyStartDate: "2024-01-01",
		AnomalyEndDate:   "2024-01-05",
	})
	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:        "recent-anomaly",
		MonitorARN:       "arn:aws:ce::000:anomalymonitor/test",
		AnomalyStartDate: "2024-06-01",
		AnomalyEndDate:   "2024-06-05",
	})

	// Filter to recent only
	rec := doRequest(t, h, "GetAnomalies", map[string]any{
		"DateInterval": map[string]string{
			"StartDate": "2024-05-01",
			"EndDate":   "2024-07-01",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Anomalies []struct {
			AnomalyID string `json:"AnomalyId"`
		} `json:"Anomalies"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Anomalies, 1)
	assert.Equal(t, "recent-anomaly", out.Anomalies[0].AnomalyID)
}

// TestParity_GetAnomalies_Pagination verifies MaxResults/NextPageToken pagination.
func TestParity_GetAnomalies_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		h.Backend.AddAnomaly(ce.Anomaly{
			AnomalyID:        "anomaly-" + string(rune('a'+i)),
			MonitorARN:       "arn:aws:ce::000:anomalymonitor/test",
			AnomalyStartDate: "2024-01-01",
			AnomalyEndDate:   "2024-01-05",
		})
	}

	rec1 := doRequest(t, h, "GetAnomalies", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["Anomalies"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextPageToken"].(string)
	assert.True(t, ok && nextToken != "", "NextPageToken must be present after partial page")
}

// TestParity_DescribeCostCategory_HasProcessingStatus verifies real AWS returns
// ProcessingStatus in the describe response.
func TestParity_DescribeCostCategory_HasProcessingStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
		"Name":        "MyCat",
		"RuleVersion": "CostCategoryExpression.v1",
		"Rules":       []map[string]any{{"Value": "Engineering"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	catARN := createOut["CostCategoryArn"].(string)

	descRec := doRequest(t, h, "DescribeCostCategoryDefinition", map[string]any{
		"CostCategoryArn": catARN,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		CostCategory struct {
			ProcessingStatus []struct {
				Component string `json:"Component"`
				Status    string `json:"Status"`
			} `json:"ProcessingStatus"`
		} `json:"CostCategory"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	require.NotEmpty(t, descOut.CostCategory.ProcessingStatus, "ProcessingStatus must be present")
	ps := descOut.CostCategory.ProcessingStatus[0]
	assert.Equal(t, "COST_EXPLORER", ps.Component)
	assert.Equal(t, "APPLIED", ps.Status)
}

// TestParity_ListCostCategoryDefinitions_Pagination verifies MaxResults/NextToken pagination.
func TestParity_ListCostCategoryDefinitions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, "CreateCostCategoryDefinition", map[string]any{
			"Name":        "Cat-" + string(rune('A'+i)),
			"RuleVersion": "CostCategoryExpression.v1",
			"Rules":       []map[string]any{{"Value": "val"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec1 := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["CostCategoryReferences"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextPageToken"].(string)
	assert.True(t, ok && nextToken != "", "NextPageToken must be present after partial page")

	rec2 := doRequest(t, h, "ListCostCategoryDefinitions", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	page2 := out2["CostCategoryReferences"].([]any)
	assert.Len(t, page2, 2)
}

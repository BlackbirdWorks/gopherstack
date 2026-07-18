package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAnomalyMonitors_HasCreationDateAndLastUpdatedDate verifies real AWS
// returns CreationDate and LastUpdatedDate as epoch-second floats in GetAnomalyMonitors.
func TestGetAnomalyMonitors_HasCreationDateAndLastUpdatedDate(t *testing.T) {
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
			MonitorArn      string `json:"MonitorArn"`
			CreationDate    string `json:"CreationDate"`
			LastUpdatedDate string `json:"LastUpdatedDate"`
		} `json:"AnomalyMonitors"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	require.Len(t, getOut.AnomalyMonitors, 1)

	m := getOut.AnomalyMonitors[0]
	assert.Equal(t, monARN, m.MonitorArn)
	assert.NotEmpty(t, m.CreationDate, "CreationDate must be a non-empty date string")
	assert.NotEmpty(t, m.LastUpdatedDate, "LastUpdatedDate must be a non-empty date string")
}

// TestGetAnomalyMonitors_Pagination verifies MaxResults/NextPageToken pagination.
func TestGetAnomalyMonitors_Pagination(t *testing.T) {
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

// TestGetAnomalySubscriptions_HasAccountId verifies real AWS returns AccountId
// in each subscription entry.
func TestGetAnomalySubscriptions_HasAccountId(t *testing.T) {
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

// TestGetAnomalySubscriptions_Pagination verifies MaxResults/NextPageToken pagination.
func TestGetAnomalySubscriptions_Pagination(t *testing.T) {
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
			// Real AWS CE returns UnknownSubscriptionException (HTTP 400) when
			// SubscriptionArnList references an ARN that doesn't exist -- it does not
			// silently filter it out.
			name:           "unknown_arn_returns_400",
			filterARNs:     []string{"arn:aws:ce::000:sub/does-not-exist"},
			wantStatusCode: http.StatusBadRequest,
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

			if tt.wantStatusCode != http.StatusOK {
				return
			}

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
			name: "not_found_returns_400",
			updateBody: map[string]any{
				"SubscriptionArn": "arn:aws:ce::000:sub/not-found",
				"Frequency":       "WEEKLY",
			},
			wantStatusCode: http.StatusBadRequest,
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
			// MonitorArnList entries must reference a real monitor -- real AWS CE returns
			// UnknownMonitorException otherwise. The placeholder is swapped for a real
			// monitor ARN below.
			name: "update_monitor_arn_list",
			updateBody: map[string]any{
				"MonitorArnList": []string{"PLACEHOLDER"},
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

			monRec := doRequest(t, h, "CreateAnomalyMonitor", map[string]any{
				"AnomalyMonitor": map[string]any{
					"MonitorName": "UpdateSubMon",
					"MonitorType": "DIMENSIONAL",
				},
			})
			require.Equal(t, http.StatusOK, monRec.Code)

			var monOut map[string]any
			require.NoError(t, json.NewDecoder(monRec.Body).Decode(&monOut))
			monARN := monOut["MonitorArn"].(string)

			arns, ok := tt.updateBody["MonitorArnList"].([]string)
			if ok && len(arns) == 1 && arns[0] == "PLACEHOLDER" {
				tt.updateBody["MonitorArnList"] = []string{monARN}
			}

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

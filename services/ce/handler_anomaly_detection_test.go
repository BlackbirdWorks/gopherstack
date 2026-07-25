package ce_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ce"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnomalyFeedback_PreviousFeedbackOverwritten verifies feedback updates.
func TestAnomalyFeedback_PreviousFeedbackOverwritten(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:  "feedback-overwrite-1",
		MonitorARN: "arn:aws:ce::000:anomalymonitor/test",
	})

	// First feedback: YES
	rec1 := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"AnomalyId": "feedback-overwrite-1",
		"Feedback":  "YES",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second feedback: NO (overwrite)
	rec2 := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"AnomalyId": "feedback-overwrite-1",
		"Feedback":  "NO",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	getRec := doRequest(t, h, "GetAnomalies", map[string]any{
		"Feedback":     "NO",
		"DateInterval": map[string]string{"StartDate": "2024-01-01"},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		Anomalies []struct {
			AnomalyID string `json:"AnomalyId"`
			Feedback  string `json:"Feedback"`
		} `json:"Anomalies"`
	}
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	require.Len(t, out.Anomalies, 1)
	assert.Equal(t, "feedback-overwrite-1", out.Anomalies[0].AnomalyID)
	assert.Equal(t, "NO", out.Anomalies[0].Feedback)
}

func TestProvideAnomalyFeedback_PersistsAndValidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		feedback       string
		wantFeedback   string
		wantStatusCode int
	}{
		{
			name:           "yes_feedback",
			feedback:       "YES",
			wantStatusCode: http.StatusOK,
			wantFeedback:   "YES",
		},
		{
			name:           "no_feedback",
			feedback:       "NO",
			wantStatusCode: http.StatusOK,
			wantFeedback:   "NO",
		},
		{
			name:           "planned_activity_feedback",
			feedback:       "PLANNED_ACTIVITY",
			wantStatusCode: http.StatusOK,
			wantFeedback:   "PLANNED_ACTIVITY",
		},
		{
			name:           "invalid_feedback_returns_400",
			feedback:       "MAYBE",
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			h.Backend.AddAnomaly(ce.Anomaly{
				AnomalyID:  "feedback-test-1",
				MonitorARN: "arn:aws:ce::000:anomalymonitor/test",
			})

			rec := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
				"AnomalyId": "feedback-test-1",
				"Feedback":  tt.feedback,
			})
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantFeedback == "" {
				return
			}

			// Verify persisted
			getRec := doRequest(t, h, "GetAnomalies", map[string]any{
				"Feedback":     tt.wantFeedback,
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var out struct {
				Anomalies []struct {
					AnomalyID string `json:"AnomalyId"`
					Feedback  string `json:"Feedback"`
				} `json:"Anomalies"`
			}
			require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
			require.Len(t, out.Anomalies, 1)
			assert.Equal(t, "feedback-test-1", out.Anomalies[0].AnomalyID)
			assert.Equal(t, tt.wantFeedback, out.Anomalies[0].Feedback)
		})
	}
}

func TestProvideAnomalyFeedback_NotFoundReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"AnomalyId": "nonexistent-anomaly",
		"Feedback":  "YES",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestProvideAnomalyFeedback_MissingIDReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ProvideAnomalyFeedback", map[string]any{
		"Feedback": "YES",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetAnomalies_ScoreAndImpactAreObjects(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.AddAnomaly(ce.Anomaly{
		AnomalyID:   "struct-test-1",
		MonitorARN:  "arn:aws:ce::000:anomalymonitor/test",
		TotalImpact: 500.0,
		AnomalyScore: ce.AnomalyScore{
			MaxScore:     0.95,
			CurrentScore: 0.87,
		},
	})

	rec := doRequest(t, h, "GetAnomalies", map[string]any{
		"DateInterval": map[string]string{"StartDate": "2024-01-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Anomalies []struct {
			AnomalyScore struct {
				MaxScore     float64 `json:"MaxScore"`
				CurrentScore float64 `json:"CurrentScore"`
			} `json:"AnomalyScore"`
			Impact struct {
				MaxImpact        float64 `json:"MaxImpact"`
				TotalImpact      float64 `json:"TotalImpact"`
				TotalActualSpend float64 `json:"TotalActualSpend"`
			} `json:"Impact"`
		} `json:"Anomalies"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.Len(t, out.Anomalies, 1)

	a := out.Anomalies[0]
	assert.InDelta(t, 0.95, a.AnomalyScore.MaxScore, 0.001)
	assert.InDelta(t, 0.87, a.AnomalyScore.CurrentScore, 0.001)
	assert.Positive(t, a.Impact.MaxImpact)
	assert.Positive(t, a.Impact.TotalActualSpend)
}

// TestGetAnomalies_DateIntervalFilters verifies DateInterval is used to filter anomalies.
func TestGetAnomalies_DateIntervalFilters(t *testing.T) {
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

// TestGetAnomalies_Pagination verifies MaxResults/NextPageToken pagination.
func TestGetAnomalies_Pagination(t *testing.T) {
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
		"MaxResults":   2,
		"DateInterval": map[string]string{"StartDate": "2024-01-01"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	page1 := out1["Anomalies"].([]any)
	assert.Len(t, page1, 2)
	nextToken, ok := out1["NextPageToken"].(string)
	assert.True(t, ok && nextToken != "", "NextPageToken must be present after partial page")
}

func TestHandler_GetAnomalies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name: "returns_empty_with_no_anomalies",
			body: map[string]any{
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "filter_by_monitor_arn",
			body: map[string]any{
				"MonitorArn":   "arn:aws:ce::000000000000:anomalymonitor/test",
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
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
			body: map[string]any{
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
			},
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
			body: map[string]any{
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
			},
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
			body: map[string]any{
				"Feedback":     "YES",
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
			},
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

	rec := doRequest(t, fresh, "GetAnomalies", map[string]any{
		"DateInterval": map[string]string{"StartDate": "2024-01-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Anomalies []map[string]any `json:"Anomalies"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.Anomalies, 1)
	assert.Equal(t, "snap-anomaly-1", out.Anomalies[0]["AnomalyId"])
}

// TestHandler_GetAnomalies_RequiredStartDate verifies DateInterval.StartDate is enforced
// as required, matching real AWS CE's validateAnomalyDateInterval.
func TestHandler_GetAnomalies_RequiredStartDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body           map[string]any
		name           string
		wantStatusCode int
	}{
		{
			name:           "missing_date_interval",
			body:           map[string]any{},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "missing_start_date",
			body: map[string]any{
				"DateInterval": map[string]string{"EndDate": "2024-02-01"},
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "start_date_present_succeeds",
			body: map[string]any{
				"DateInterval": map[string]string{"StartDate": "2024-01-01"},
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
		})
	}
}

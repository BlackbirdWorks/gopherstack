package ce_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func usageBody(start, end, granularity string, extra map[string]any) map[string]any {
	body := map[string]any{
		"TimePeriod":  map[string]string{"Start": start, "End": end},
		"Granularity": granularity,
		"Metrics":     []string{"UnblendedCost"},
	}
	maps.Copy(body, extra)

	return body
}

func TestGetCostAndUsage_RequestValidation(t *testing.T) {
	t.Parallel()

	dim := func(k string) []map[string]string { return []map[string]string{{"Type": "DIMENSION", "Key": k}} }
	three := []map[string]string{
		{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": "REGION"}, {"Type": "TAG", "Key": "Env"},
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantType string
		wantCode int
	}{
		{usageBody("2026-02-01", "2026-01-01", "MONTHLY", nil), "start_after_end", "ValidationError", 400},
		{usageBody("2026-01-01", "2026-01-01", "DAILY", nil), "start_equals_end", "ValidationError", 400},
		{usageBody("2026-01-01", "2026-02-01", "MONTHLY", map[string]any{"Metrics": []string{"Bogus"}}),
			"bad_metric", "ValidationError", 400},
		{usageBody("2026-01-01", "2026-02-01", "MONTHLY", map[string]any{"GroupBy": dim("BOGUS")}),
			"bad_dimension", "ValidationError", 400},
		{usageBody("2026-01-01", "2026-02-01", "MONTHLY", map[string]any{"GroupBy": three}),
			"three_group_bys", "ValidationError", 400},
		{usageBody("2026-01-01", "2026-02-01", "MONTHLY", map[string]any{"NextPageToken": "%%not-base64"}),
			"garbage_page_token", "InvalidNextTokenException", 400},
		{usageBody("2026-01-01T00:00:00Z", "2026-01-01T03:00:00Z", "HOURLY", nil), "hourly_datetime", "", 200},
		{usageBody("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z", "DAILY", nil),
			"daily_rejects_datetime", "ValidationError", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), "GetCostAndUsage", tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantType != "" {
				var we wireError
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &we))
				assert.Contains(t, we.Type, tt.wantType)
			}
		})
	}
}

func TestGetCostAndUsage_HourlyBuckets(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "GetCostAndUsage", map[string]any{
		"TimePeriod":  map[string]string{"Start": "2026-01-01T00:00:00Z", "End": "2026-01-01T03:00:00Z"},
		"Granularity": "HOURLY", "Metrics": []string{"UnblendedCost"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResultsByTime []struct {
			TimePeriod map[string]string `json:"TimePeriod"`
		} `json:"ResultsByTime"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ResultsByTime, 3)
	assert.Equal(t, "2026-01-01T01:00:00Z", out.ResultsByTime[1].TimePeriod["Start"])
}

func TestRequestEnumValidation(t *testing.T) {
	t.Parallel()

	period := map[string]string{"Start": "2026-01-01", "End": "2026-02-01"}
	sub := func(typ, addr string) map[string]any {
		return map[string]any{"AnomalySubscription": map[string]any{
			"SubscriptionName": "s", "MonitorArnList": []string{"arn:aws:ce::000000000000:anomalymonitor/x"},
			"Frequency": "DAILY", "Subscribers": []map[string]string{{"Type": typ, "Address": addr}},
		}}
	}

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{map[string]any{"AnomalyMonitor": map[string]any{
			"MonitorName": "m", "MonitorType": "DIMENSIONAL", "MonitorDimension": "BOGUS",
		}}, "monitor_dimension", "CreateAnomalyMonitor"},
		{sub("BOGUS", "a@b.com"), "subscriber_type", "CreateAnomalySubscription"},
		{sub("EMAIL", "notanemail"), "subscriber_email", "CreateAnomalySubscription"},
		{sub("SNS", "notarn"), "subscriber_sns", "CreateAnomalySubscription"},
		{map[string]any{"TimePeriod": period, "Dimension": "BOGUS"}, "dimension_values", "GetDimensionValues"},
		{
			map[string]any{"TimePeriod": period, "Metric": "BOGUS", "Granularity": "MONTHLY"},
			"forecast_metric",
			"GetCostForecast",
		},
		{map[string]any{
			"TimePeriod": map[string]string{"Start": "2026-02-01", "End": "2026-01-01"},
			"Metric":     "UNBLENDED_COST", "Granularity": "MONTHLY",
		}, "forecast_period", "GetCostForecast"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), tt.action, tt.body)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

			var we wireError
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &we))
			assert.Contains(t, we.Type, "ValidationError")
		})
	}
}

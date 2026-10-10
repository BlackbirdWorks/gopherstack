package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

func seedFilterFindings(t *testing.T, h *securityhub.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			securityhub.ValidFinding(map[string]any{
				"Id": "af-1", "AwsAccountId": "111111111111", "Severity": map[string]any{"Label": "HIGH"},
				"Resources": []any{map[string]any{
					"Type": "AwsEc2Instance", "Id": "i-1", "Region": "us-east-1",
					"Tags": map[string]any{"Team": "a"},
				}},
			}),
			securityhub.ValidFinding(map[string]any{
				"Id": "af-2", "AwsAccountId": "222222222222", "Severity": map[string]any{"Label": "LOW"},
				"Resources": []any{map[string]any{"Type": "AwsS3Bucket", "Id": "b-1", "Region": "us-west-2"}},
			}),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func ocsfAccountFilter(account string) map[string]any {
	return map[string]any{"CompositeFilters": []any{map[string]any{"StringFilters": []any{map[string]any{
		"FieldName": "cloud.account.uid", "Filter": map[string]any{"Comparison": "EQUALS", "Value": account},
	}}}}}
}

func resourceStringFilter(field, value string) map[string]any {
	return map[string]any{"CompositeFilters": []any{map[string]any{"StringFilters": []any{map[string]any{
		"FieldName": field, "Filter": map[string]any{"Comparison": "EQUALS", "Value": value},
	}}}}}
}

func decodeMap(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	return out
}

func TestFindingsTrendsV2_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters  map[string]any
		name     string
		wantHigh float64
		wantLow  float64
	}{
		{name: "unfiltered", wantHigh: 1, wantLow: 1},
		{name: "account_a", filters: ocsfAccountFilter("111111111111"), wantHigh: 1},
		{name: "account_b", filters: ocsfAccountFilter("222222222222"), wantLow: 1},
		{name: "no_match", filters: ocsfAccountFilter("999999999999")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedFilterFindings(t, h)

			body := map[string]any{"StartTime": "2024-01-01T00:00:00Z", "EndTime": "2024-01-02T00:00:00Z"}
			if tt.filters != nil {
				body["Filters"] = tt.filters
			}

			rec := doRequest(t, h, http.MethodPost, "/findingsTrendsv2", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			metrics, _ := decodeMap(t, rec.Body.Bytes())["TrendsMetrics"].([]any)
			require.Len(t, metrics, 1)
			point, _ := metrics[0].(map[string]any)
			values, _ := point["TrendsValues"].(map[string]any)
			sev, _ := values["SeverityTrends"].(map[string]any)
			assert.InDelta(t, tt.wantHigh, sev["High"], 0)
			assert.InDelta(t, tt.wantLow, sev["Low"], 0)
		})
	}
}

func TestResourcesV2_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filters   map[string]any
		name      string
		wantCount int
	}{
		{name: "unfiltered", wantCount: 2},
		{name: "by_type", filters: resourceStringFilter("ResourceType", "AwsS3Bucket"), wantCount: 1},
		{name: "by_account", filters: resourceStringFilter("AccountId", "111111111111"), wantCount: 1},
		{name: "by_region", filters: resourceStringFilter("Region", "eu-west-1"), wantCount: 0},
		{
			name: "by_tag",
			filters: map[string]any{"CompositeFilters": []any{map[string]any{"MapFilters": []any{
				map[string]any{
					"FieldName": "ResourceTags",
					"Filter":    map[string]any{"Key": "Team", "Value": "a", "Comparison": "EQUALS"},
				},
			}}}},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedFilterFindings(t, h)

			body := map[string]any{}
			if tt.filters != nil {
				body["Filters"] = tt.filters
			}

			rec := doRequest(t, h, http.MethodPost, "/resourcesv2", body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			resources, _ := decodeMap(t, rec.Body.Bytes())["Resources"].([]any)
			assert.Len(t, resources, tt.wantCount)

			trendBody := map[string]any{"StartTime": "2024-01-01T00:00:00Z", "EndTime": "2024-01-02T00:00:00Z"}
			if tt.filters != nil {
				trendBody["Filters"] = tt.filters
			}

			rec = doRequest(t, h, http.MethodPost, "/resourcesTrendsv2", trendBody)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			metrics, _ := decodeMap(t, rec.Body.Bytes())["TrendsMetrics"].([]any)
			point, _ := metrics[0].(map[string]any)
			values, _ := point["TrendsValues"].(map[string]any)
			count, _ := values["ResourcesCount"].(map[string]any)
			assert.InDelta(t, float64(tt.wantCount), count["AllResources"], 0)
		})
	}
}

func TestStatisticsV2_RuleFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedFilterFindings(t, h)

	rec := doRequest(t, h, http.MethodPost, "/findingsv2/statistics", map[string]any{
		"GroupByRules": []any{
			map[string]any{"GroupByField": "severity"},
			map[string]any{"GroupByField": "severity", "Filters": ocsfAccountFilter("111111111111")},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	results, _ := decodeMap(t, rec.Body.Bytes())["GroupByResults"].([]any)
	require.Len(t, results, 2)
	assert.Len(t, groupValues(results[0]), 2)
	assert.Len(t, groupValues(results[1]), 1)

	rec = doRequest(t, h, http.MethodPost, "/resourcesv2/statistics", map[string]any{
		"GroupByRules": []any{
			map[string]any{"GroupByField": "ResourceType"},
			map[string]any{
				"GroupByField": "ResourceType",
				"Filters":      resourceStringFilter("AccountId", "222222222222"),
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	results, _ = decodeMap(t, rec.Body.Bytes())["GroupByResults"].([]any)
	require.Len(t, results, 2)
	assert.Len(t, groupValues(results[0]), 2)
	assert.Len(t, groupValues(results[1]), 1)
}

func groupValues(result any) []any {
	m, _ := result.(map[string]any)
	values, _ := m["GroupByValues"].([]any)

	return values
}

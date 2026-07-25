package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetFindingsV2_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantCode   int
	}{
		{name: "default max results", maxResults: 0, wantCode: http.StatusOK},
		{name: "explicit max results", maxResults: 10, wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := map[string]any{}
			if tc.maxResults > 0 {
				body["MaxResults"] = tc.maxResults
			}

			rec := doRequest(t, h, http.MethodPost, "/findingsv2", body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetFindingsV2_InvalidNextToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantCode  int
	}{
		{name: "valid next token", nextToken: "", wantCode: http.StatusOK},
		{
			name:      "non-numeric next token falls back",
			nextToken: "notanumber",
			wantCode:  http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := map[string]any{}
			if tc.nextToken != "" {
				body["NextToken"] = tc.nextToken
			}

			rec := doRequest(t, h, http.MethodPost, "/findingsv2", body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestGetFindingsV2_ReturnsSameStoreAsV1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	importRec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{securityhub.ValidFinding(map[string]any{
			"Id":         "finding-001",
			"ProductArn": "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
		})},
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	rec := doRequest(t, h, http.MethodPost, "/findingsv2", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	findings, _ := resp["Findings"].([]any)
	assert.NotEmpty(t, findings)
}

// TestGetFindingsV2_CompositeFilters verifies the real GetFindingsV2 wire
// shape (Filters.CompositeFilters/CompositeOperator, each CompositeFilter's
// StringFilters/NumberFilters/Operator -- types.OcsfFindingFilters) is parsed
// and applied, not silently ignored (the pre-fix behavior: the V1 filter
// matcher looked for top-level "Id"/"ProductArn" keys that never appear in
// the real V2 request shape, so every V2 filter was a no-op).
func TestGetFindingsV2_CompositeFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	productArn := "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default"

	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			securityhub.ValidFinding(map[string]any{
				"Id": "f1", "ProductArn": productArn, "AwsAccountId": "111111111111",
			}),
			securityhub.ValidFinding(map[string]any{
				"Id": "f2", "ProductArn": productArn, "AwsAccountId": "222222222222",
			}),
		},
	})

	tests := []struct {
		filters   map[string]any
		name      string
		wantCount int
	}{
		{
			name: "string filter EQUALS on cloud.account.uid",
			filters: map[string]any{
				"CompositeFilters": []any{
					map[string]any{
						"StringFilters": []any{
							map[string]any{
								"FieldName": "cloud.account.uid",
								"Filter":    map[string]any{"Comparison": "EQUALS", "Value": "111111111111"},
							},
						},
					},
				},
			},
			wantCount: 1,
		},
		{
			name: "CompositeOperator OR across two composite filters",
			filters: map[string]any{
				"CompositeOperator": "OR",
				"CompositeFilters": []any{
					map[string]any{
						"StringFilters": []any{
							map[string]any{
								"FieldName": "cloud.account.uid",
								"Filter":    map[string]any{"Comparison": "EQUALS", "Value": "111111111111"},
							},
						},
					},
					map[string]any{
						"StringFilters": []any{
							map[string]any{
								"FieldName": "cloud.account.uid",
								"Filter":    map[string]any{"Comparison": "EQUALS", "Value": "222222222222"},
							},
						},
					},
				},
			},
			wantCount: 2,
		},
		{
			name:      "no CompositeFilters returns all",
			filters:   map[string]any{},
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, "/findingsv2", map[string]any{"Filters": tc.filters})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			findings, _ := resp["Findings"].([]any)
			assert.Len(t, findings, tc.wantCount)
		})
	}
}

// TestBatchUpdateFindingsV2_WireShape verifies the real (flat)
// BatchUpdateFindingsV2 request shape -- Comment/SeverityId/StatusId at the
// top level, FindingIdentifiers using OcsfFindingIdentifier
// (CloudAccountUid/FindingInfoUid/MetadataProductUid) -- is honored. The
// previous implementation read a nonexistent "FindingFieldsUpdate" wrapper
// key and passed V1 ProductArn/Id identifiers straight through, so it could
// never actually apply an update from a real client.
func TestBatchUpdateFindingsV2_WireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	productArn := "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default"

	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{securityhub.ValidFinding(map[string]any{
			"Id":           "finding-v2-001",
			"ProductArn":   productArn,
			"AwsAccountId": "000000000000",
		})},
	})

	rec := doRequest(t, h, http.MethodPatch, "/findingsv2/batchupdatev2", map[string]any{
		"FindingIdentifiers": []any{
			map[string]any{
				"CloudAccountUid":    "000000000000",
				"FindingInfoUid":     "finding-v2-001",
				"MetadataProductUid": productArn,
			},
		},
		"Comment":    "reviewed",
		"SeverityId": 3,
		"StatusId":   1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ProcessedFindings   []map[string]any `json:"ProcessedFindings"`
		UnprocessedFindings []map[string]any `json:"UnprocessedFindings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.ProcessedFindings, 1, "matching identifier must process")
	assert.Empty(t, resp.UnprocessedFindings)

	// Confirm the update actually mutated the stored finding.
	getRec := doRequest(t, h, http.MethodPost, "/findingsv2", map[string]any{
		"Filters": map[string]any{
			"CompositeFilters": []any{
				map[string]any{
					"NumberFilters": []any{
						map[string]any{
							"FieldName": "severity_id",
							"Filter":    map[string]any{"Eq": 3},
						},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	findings, _ := getResp["Findings"].([]any)
	require.Len(t, findings, 1)

	f, _ := findings[0].(map[string]any)
	assert.InDelta(t, float64(3), f["SeverityId"], 0.01)
	assert.Equal(t, "reviewed", f["Comment"])
}

// TestBatchUpdateFindingsV2_UnmatchedIdentifiers verifies both unresolvable
// identifier shapes report ResourceNotFoundException in UnprocessedFindings:
// a FindingIdentifier whose CloudAccountUid doesn't match the stored
// finding's AwsAccountId, and any MetadataUids entry (this mock has no OCSF
// ingestion path that would ever hand a caller a real metadata.uid).
func TestBatchUpdateFindingsV2_UnmatchedIdentifiers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	productArn := "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default"

	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{securityhub.ValidFinding(map[string]any{
			"Id":           "finding-v2-002",
			"ProductArn":   productArn,
			"AwsAccountId": "000000000000",
		})},
	})

	rec := doRequest(t, h, http.MethodPatch, "/findingsv2/batchupdatev2", map[string]any{
		"FindingIdentifiers": []any{
			map[string]any{
				"CloudAccountUid":    "999999999999",
				"FindingInfoUid":     "finding-v2-002",
				"MetadataProductUid": productArn,
			},
		},
		"MetadataUids": []any{"some-opaque-uid"},
		"Comment":      "should not apply",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ProcessedFindings   []map[string]any `json:"ProcessedFindings"`
		UnprocessedFindings []map[string]any `json:"UnprocessedFindings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.ProcessedFindings)
	require.Len(t, resp.UnprocessedFindings, 2)

	for _, u := range resp.UnprocessedFindings {
		assert.Equal(t, "ResourceNotFoundException", u["ErrorCode"])
	}
}

func TestGetFindingStatisticsV2_ReturnsStats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findingsv2/statistics", map[string]any{
		"GroupByAttributes": []any{"Severity.Label"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["FindingStatistics"])
}

func TestGetFindingsTrendsV2_ReturnsTrends(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findingsTrendsv2", map[string]any{
		"GroupByAttribute": "Severity.Label",
		"StartTime":        "2024-01-01T00:00:00Z",
		"EndTime":          "2024-12-31T23:59:59Z",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["FindingsTrends"])
}

package securityhub_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	securityhubtypes "github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
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
				"Confidence": 80,
			}),
			securityhub.ValidFinding(map[string]any{
				"Id": "f2", "ProductArn": productArn, "AwsAccountId": "222222222222",
				"Confidence": 20,
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
		{
			name: "number filter Gte on confidence_score narrows to the high-confidence finding",
			filters: map[string]any{
				"CompositeFilters": []any{
					map[string]any{
						"NumberFilters": []any{
							map[string]any{
								"FieldName": "confidence_score",
								"Filter":    map[string]any{"Gte": 50},
							},
						},
					},
				},
			},
			wantCount: 1,
		},
		{
			name: "number filter Lt on confidence_score narrows to the low-confidence finding",
			filters: map[string]any{
				"CompositeFilters": []any{
					map[string]any{
						"NumberFilters": []any{
							map[string]any{
								"FieldName": "confidence_score",
								"Filter":    map[string]any{"Lt": 50},
							},
						},
					},
				},
			},
			wantCount: 1,
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

// TestGetFindingsV2_CompositeFilters_DateMapIPBooleanNested verifies the
// previously-silently-ignored CompositeFilter facets (DateFilters,
// MapFilters, IpFilters, BooleanFilters, NestedCompositeFilters --
// gopherstack-8j08) are now actually evaluated against real ASFF-backed
// data, not merely accepted on the wire. Two findings are seeded with
// deliberately different values on every field under test, and each case
// asserts the filter narrows to exactly the one finding that should match --
// proving genuine discrimination (a filter that silently matched everything,
// the pre-fix behavior, would return both findings for every case here).
func TestGetFindingsV2_CompositeFilters_DateMapIPBooleanNested(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	productArn := "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default"

	now := time.Now().UTC()
	recent := now.AddDate(0, 0, -1).Format(time.RFC3339)
	old := now.AddDate(0, 0, -100).Format(time.RFC3339)

	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			securityhub.ValidFinding(map[string]any{
				"Id": "v2ext-f1", "ProductArn": productArn, "AwsAccountId": "111111111111",
				"CreatedAt": recent,
				"Resources": []any{
					map[string]any{
						"Type": "AwsEc2Instance", "Id": "i-1",
						"Tags": map[string]any{"Department": "Security"},
					},
				},
				"UserDefinedFields": map[string]any{"Owner": "teamA"},
				"Compliance": map[string]any{
					"SecurityControlParameters": []any{
						map[string]any{"Name": "allowedPorts", "Value": []any{"22", "443"}},
					},
				},
				"Network":         map[string]any{"SourceIpV4": "10.0.0.5"},
				"Vulnerabilities": []any{map[string]any{"Id": "CVE-1", "ExploitAvailable": "YES"}},
			}),
			securityhub.ValidFinding(map[string]any{
				"Id": "v2ext-f2", "ProductArn": productArn, "AwsAccountId": "222222222222",
				"CreatedAt": old,
				"Resources": []any{
					map[string]any{
						"Type": "AwsEc2Instance", "Id": "i-2",
						"Tags": map[string]any{"Department": "Finance"},
					},
				},
				"UserDefinedFields": map[string]any{"Owner": "teamB"},
				"Compliance": map[string]any{
					"SecurityControlParameters": []any{
						map[string]any{"Name": "allowedPorts", "Value": []any{"80"}},
					},
				},
				"Network":         map[string]any{"SourceIpV4": "172.16.0.9"},
				"Vulnerabilities": []any{map[string]any{"Id": "CVE-2", "ExploitAvailable": "NO"}},
			}),
		},
	})

	tests := []struct {
		filters   map[string]any
		name      string
		wantIDs   []string
		wantCount int
	}{
		{
			name: "date filter DateRange WITHIN selects the recent finding",
			filters: compositeFilter(map[string]any{
				"DateFilters": []any{
					map[string]any{
						"FieldName": "finding_info.created_time_dt",
						"Filter": map[string]any{
							"DateRange": map[string]any{"Comparison": "WITHIN", "Unit": "DAYS", "Value": 7},
						},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f1"},
		},
		{
			name: "date filter DateRange OLDER_THAN selects the old finding",
			filters: compositeFilter(map[string]any{
				"DateFilters": []any{
					map[string]any{
						"FieldName": "finding_info.created_time_dt",
						"Filter": map[string]any{
							"DateRange": map[string]any{"Comparison": "OLDER_THAN", "Unit": "DAYS", "Value": 7},
						},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f2"},
		},
		{
			name: "map filter EQUALS resources.tags selects the Security-tagged finding",
			filters: compositeFilter(map[string]any{
				"MapFilters": []any{
					map[string]any{
						"FieldName": "resources.tags",
						"Filter":    map[string]any{"Key": "Department", "Value": "Security", "Comparison": "EQUALS"},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f1"},
		},
		{
			name: "map filter NOT_EQUALS resources.tags excludes the Security-tagged finding",
			filters: compositeFilter(map[string]any{
				"MapFilters": []any{
					map[string]any{
						"FieldName": "resources.tags",
						"Filter": map[string]any{
							"Key":        "Department",
							"Value":      "Security",
							"Comparison": "NOT_EQUALS",
						},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f2"},
		},
		{
			name: "map filter EQUALS finding_info.tags selects teamA owner",
			filters: compositeFilter(map[string]any{
				"MapFilters": []any{
					map[string]any{
						"FieldName": "finding_info.tags",
						"Filter":    map[string]any{"Key": "Owner", "Value": "teamA", "Comparison": "EQUALS"},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f1"},
		},
		{
			name: "map filter EQUALS compliance.control_parameters selects allowedPorts=22",
			filters: compositeFilter(map[string]any{
				"MapFilters": []any{
					map[string]any{
						"FieldName": "compliance.control_parameters",
						"Filter":    map[string]any{"Key": "allowedPorts", "Value": "22", "Comparison": "EQUALS"},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f1"},
		},
		{
			name: "map filter on unmapped databucket.tags is accepted but not enforced",
			filters: compositeFilter(map[string]any{
				"MapFilters": []any{
					map[string]any{
						"FieldName": "databucket.tags",
						"Filter":    map[string]any{"Key": "anything", "Value": "anything", "Comparison": "EQUALS"},
					},
				},
			}),
			wantCount: 2,
		},
		{
			name: "ip filter CIDR selects the finding whose source IP falls inside it",
			filters: compositeFilter(map[string]any{
				"IpFilters": []any{
					map[string]any{
						"FieldName": "evidences.src_endpoint.ip",
						"Filter":    map[string]any{"Cidr": "10.0.0.0/24"},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f1"},
		},
		{
			name: "ip filter CIDR selects the other finding's source IP",
			filters: compositeFilter(map[string]any{
				"IpFilters": []any{
					map[string]any{
						"FieldName": "evidences.src_endpoint.ip",
						"Filter":    map[string]any{"Cidr": "172.16.0.0/16"},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f2"},
		},
		{
			name: "boolean filter Value=true selects the exploit-available finding",
			filters: compositeFilter(map[string]any{
				"BooleanFilters": []any{
					map[string]any{
						"FieldName": "vulnerabilities.is_exploit_available",
						"Filter":    map[string]any{"Value": true},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f1"},
		},
		{
			name: "boolean filter Value=false selects the finding without an exploit",
			filters: compositeFilter(map[string]any{
				"BooleanFilters": []any{
					map[string]any{
						"FieldName": "vulnerabilities.is_exploit_available",
						"Filter":    map[string]any{"Value": false},
					},
				},
			}),
			wantCount: 1, wantIDs: []string{"v2ext-f2"},
		},
		{
			name: "NestedCompositeFilters OR recurses and matches either branch",
			filters: map[string]any{
				"CompositeFilters": []any{
					map[string]any{
						"Operator": "OR",
						"NestedCompositeFilters": []any{
							accountUIDEqualsFilter("111111111111"),
							accountUIDEqualsFilter("222222222222"),
						},
					},
				},
			},
			wantCount: 2,
		},
		{
			// A single finding can't have two different AwsAccountId values, so an
			// AND across these two nested branches must match nothing. Before this
			// fix, NestedCompositeFilters was never evaluated at all, so this case
			// would have (wrongly) matched both findings -- this is the regression
			// guard for that.
			name: "NestedCompositeFilters AND recurses and requires both branches",
			filters: map[string]any{
				"CompositeFilters": []any{
					map[string]any{
						"Operator": "AND",
						"NestedCompositeFilters": []any{
							accountUIDEqualsFilter("111111111111"),
							accountUIDEqualsFilter("222222222222"),
						},
					},
				},
			},
			wantCount: 0,
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

			if len(tc.wantIDs) == 0 {
				return
			}

			gotIDs := make([]string, 0, len(findings))
			for _, f := range findings {
				fm, _ := f.(map[string]any)
				gotIDs = append(gotIDs, fm["Id"].(string))
			}

			assert.ElementsMatch(t, tc.wantIDs, gotIDs)
		})
	}
}

// compositeFilter wraps a single CompositeFilter body (StringFilters/
// NumberFilters/DateFilters/MapFilters/IpFilters/BooleanFilters) into the
// full Filters.CompositeFilters wire shape used by GetFindingsV2.
func compositeFilter(body map[string]any) map[string]any {
	return map[string]any{"CompositeFilters": []any{body}}
}

// accountUIDEqualsFilter builds one OcsfStringFilter (EQUALS on
// cloud.account.uid) entry for use inside a CompositeFilter's
// NestedCompositeFilters.
func accountUIDEqualsFilter(value string) map[string]any {
	return map[string]any{
		"StringFilters": []any{
			map[string]any{
				"FieldName": "cloud.account.uid",
				"Filter":    map[string]any{"Comparison": "EQUALS", "Value": value},
			},
		},
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

// seedSeverityFindings imports two HIGH-severity findings and one LOW, via
// the raw ASFF BatchImportFindings wire (POST /findings/import).
func seedSeverityFindings(t *testing.T, h *securityhub.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			securityhub.ValidFinding(map[string]any{"Id": "sev-finding-1", "SeverityLabel": "HIGH"}),
			securityhub.ValidFinding(map[string]any{"Id": "sev-finding-2", "SeverityLabel": "HIGH"}),
			securityhub.ValidFinding(map[string]any{"Id": "sev-finding-3", "SeverityLabel": "LOW"}),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestGetFindingStatisticsV2_RoundTrip drives GetFindingStatisticsV2 through
// the real SDK client. Before the fix, the handler read a fabricated
// body["GroupByAttributes"] ([]string) where the real required input member
// is GroupByRules ([]types.GroupByRule), and emitted "FindingStatistics"
// where the real (optional but only meaningful) output key is
// "GroupByResults" (securityhub@v1.75.4 api_op_GetFindingStatisticsV2.go:
// 22-57) -- a real client's request grouped by nothing, and its response
// decoded a nil slice regardless.
func TestGetFindingStatisticsV2_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(backend)
	seedSeverityFindings(t, h)
	client := newTestSecurityHubClient(t, h)

	out, err := client.GetFindingStatisticsV2(t.Context(), &securityhubsdk.GetFindingStatisticsV2Input{
		GroupByRules: []securityhubtypes.GroupByRule{
			{GroupByField: securityhubtypes.GroupByFieldSeverity},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.GroupByResults,
		"unfixed handler emits FindingStatistics where the real key is GroupByResults; SDK decodes a nil slice")

	result := out.GroupByResults[0]
	assert.Equal(t, "severity", aws.ToString(result.GroupByField))
	require.NotEmpty(t, result.GroupByValues)

	byLabel := make(map[string]int32)
	for _, v := range result.GroupByValues {
		byLabel[aws.ToString(v.FieldValue)] = aws.ToInt32(v.Count)
	}

	assert.Equal(t, int32(2), byLabel["HIGH"])
	assert.Equal(t, int32(1), byLabel["LOW"])
}

// TestGetFindingsTrendsV2_RoundTrip drives GetFindingsTrendsV2 through the
// real SDK client. Before the fix, the handler read a fabricated
// body["GroupByAttribute"] (which the real GetFindingsTrendsV2Input doesn't
// have at all) and emitted "FindingsTrends", dropping the required
// Granularity and TrendsMetrics members (securityhub@v1.75.4
// api_op_GetFindingsTrendsV2.go:22-58) -- a real client decoded a nil slice
// and an empty Granularity string, even though the backend already computed
// real trend data.
func TestGetFindingsTrendsV2_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	h := securityhub.NewHandler(backend)
	seedSeverityFindings(t, h)
	client := newTestSecurityHubClient(t, h)

	start, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	require.NoError(t, err)
	end, err := time.Parse(time.RFC3339, "2024-01-02T00:00:00Z")
	require.NoError(t, err)

	out, err := client.GetFindingsTrendsV2(t.Context(), &securityhubsdk.GetFindingsTrendsV2Input{
		StartTime: aws.Time(start),
		EndTime:   aws.Time(end),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Granularity, "Granularity is required on the real wire")
	require.NotEmpty(t, out.TrendsMetrics,
		"unfixed handler emits FindingsTrends where the real key is TrendsMetrics; SDK decodes a nil slice")

	point := out.TrendsMetrics[0]
	require.NotNil(t, point.TrendsValues)
	require.NotNil(t, point.TrendsValues.SeverityTrends)
	assert.Equal(t, int64(2), aws.ToInt64(point.TrendsValues.SeverityTrends.High))
	assert.Equal(t, int64(1), aws.ToInt64(point.TrendsValues.SeverityTrends.Low))
}

package securityhub_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch-1 accuracy gap: GetFindings is POST /findings (body with Filters).
func TestGetFindingsIsPOSTFindings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
		"Filters": map[string]any{},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasFindings := resp["Findings"]
	assert.True(t, hasFindings, "GetFindings must return 'Findings' key")
}

// Batch-1 accuracy gap: BatchImportFindings is POST /findings/import (not POST /findings).
func TestBatchImportFindingsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []map[string]any{
			{
				"SchemaVersion": "2018-10-08",
				"Id":            "finding-1",
				"ProductArn":    "arn:aws:securityhub:us-east-1::product/aws/guardduty",
				"GeneratorId":   "test-generator",
				"AwsAccountId":  "000000000000",
				"Types":         []string{"Software and Configuration Checks"},
				"CreatedAt":     "2024-01-01T00:00:00Z",
				"UpdatedAt":     "2024-01-01T00:00:00Z",
				"Severity":      map[string]any{"Label": "HIGH"},
				"Title":         "Test Finding",
				"Description":   "Test finding description",
				"Resources":     []map[string]any{{"Type": "AwsEc2Instance", "Id": "i-1234567890abcdef0"}},
			},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "SuccessCount")
	assert.Contains(t, resp, "FailedCount")
	assert.Contains(t, resp, "FailedFindings")

	assert.InDelta(t, float64(1), resp["SuccessCount"], 0.01)
	assert.InDelta(t, float64(0), resp["FailedCount"], 0.01)
}

// Batch-1 accuracy gap: BatchUpdateFindings is PATCH /findings/batchupdate (not PATCH /findings).
func TestBatchUpdateFindingsPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Import a finding first
	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []map[string]any{
			{
				"SchemaVersion": "2018-10-08",
				"Id":            "finding-batch-update",
				"ProductArn":    "arn:aws:securityhub:us-east-1::product/aws/guardduty",
				"GeneratorId":   "gen",
				"AwsAccountId":  "000000000000",
				"Types":         []string{"Software and Configuration Checks"},
				"CreatedAt":     "2024-01-01T00:00:00Z",
				"UpdatedAt":     "2024-01-01T00:00:00Z",
				"Severity":      map[string]any{"Label": "LOW"},
				"Title":         "Batch Update Test",
				"Description":   "desc",
				"Resources":     []map[string]any{{"Type": "AwsS3Bucket", "Id": "my-bucket"}},
			},
		},
	})

	rec := doRequest(t, h, http.MethodPatch, "/findings/batchupdate", map[string]any{
		"FindingIdentifiers": []map[string]any{
			{
				"Id":         "finding-batch-update",
				"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
			},
		},
		"Workflow": map[string]any{"Status": "NOTIFIED"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "ProcessedFindings")
	assert.Contains(t, resp, "UnprocessedFindings")
}

// Batch-1 accuracy gap: UpdateFindings is PATCH /findings (with Filters/Note/RecordState).
func TestUpdateFindingsIsPATCHFindings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", nil)

	rec := doRequest(t, h, http.MethodPatch, "/findings", map[string]any{
		"Filters":     map[string]any{},
		"RecordState": "ARCHIVED",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// Batch-1 accuracy gap: GetFindingHistory is POST /findingHistory/get.
func TestGetFindingHistoryPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findingHistory/get", map[string]any{
		"FindingIdentifier": map[string]any{
			"Id":         "finding-1",
			"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp, "Records")
}

// validFinding returns a minimal ASFF-compliant finding for HTTP handler tests.
func validFinding(overrides map[string]any) map[string]any {
	return securityhub.ValidFinding(overrides)
}

func TestBatchImportFindings_RequiredFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		finding       map[string]any
		name          string
		wantErrSubstr string
		wantSuccess   int
		wantFailed    int
	}{
		{
			name:        "valid_finding_accepted",
			finding:     validFinding(nil),
			wantSuccess: 1,
			wantFailed:  0,
		},
		{
			name:          "missing_ProductArn_rejected",
			finding:       validFinding(map[string]any{"ProductArn": ""}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "ProductArn",
		},
		{
			name:          "missing_Id_rejected",
			finding:       validFinding(map[string]any{"Id": ""}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "Id",
		},
		{
			name:          "missing_AwsAccountId_rejected",
			finding:       validFinding(map[string]any{"AwsAccountId": ""}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "AwsAccountId",
		},
		{
			name:          "missing_GeneratorId_rejected",
			finding:       validFinding(map[string]any{"GeneratorId": ""}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "GeneratorId",
		},
		{
			name:          "missing_Title_rejected",
			finding:       validFinding(map[string]any{"Title": ""}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "Title",
		},
		{
			name:          "missing_Description_rejected",
			finding:       validFinding(map[string]any{"Description": ""}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "Description",
		},
		{
			name: "missing_all_timestamps_rejected",
			finding: validFinding(map[string]any{
				"CreatedAt":       "",
				"UpdatedAt":       "",
				"FirstObservedAt": "",
				"LastObservedAt":  "",
			}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "CreatedAt",
		},
		{
			name:        "only_UpdatedAt_timestamp_accepted",
			finding:     validFinding(map[string]any{"CreatedAt": "", "UpdatedAt": "2024-01-01T00:00:00Z"}),
			wantSuccess: 1,
			wantFailed:  0,
		},
		{
			name: "only_FirstObservedAt_accepted",
			finding: validFinding(
				map[string]any{"CreatedAt": "", "UpdatedAt": "", "FirstObservedAt": "2024-01-01T00:00:00Z"},
			),
			wantSuccess: 1,
			wantFailed:  0,
		},
		{
			name:          "empty_Resources_rejected",
			finding:       validFinding(map[string]any{"Resources": []any{}}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "Resources",
		},
		{
			name:          "nil_Resources_rejected",
			finding:       validFinding(map[string]any{"Resources": nil}),
			wantSuccess:   0,
			wantFailed:    1,
			wantErrSubstr: "Resources",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Enable hub first so findings are accepted.
			doRequest(t, h, http.MethodPost, "/accounts", map[string]any{"EnableDefaultStandards": false})

			rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
				"Findings": []any{tc.finding},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				FailedFindings []map[string]any `json:"FailedFindings"`
				SuccessCount   int              `json:"SuccessCount"`
				FailedCount    int              `json:"FailedCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantSuccess, resp.SuccessCount, "SuccessCount")
			assert.Equal(t, tc.wantFailed, resp.FailedCount, "FailedCount")

			if tc.wantErrSubstr != "" {
				require.NotEmpty(t, resp.FailedFindings, "expected FailedFindings")
				errMsg, _ := resp.FailedFindings[0]["ErrorMessage"].(string)
				assert.Contains(t, errMsg, tc.wantErrSubstr, "error message should mention %s", tc.wantErrSubstr)
			}
		})
	}
}

func TestBatchImportFindings_MixedValidity(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", map[string]any{"EnableDefaultStandards": false})

	// Two valid, one invalid (missing ProductArn).
	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			validFinding(map[string]any{"Id": "f1"}),
			validFinding(map[string]any{"ProductArn": ""}),
			validFinding(map[string]any{"Id": "f3"}),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		FailedFindings []map[string]any `json:"FailedFindings"`
		SuccessCount   int              `json:"SuccessCount"`
		FailedCount    int              `json:"FailedCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 2, resp.SuccessCount)
	assert.Equal(t, 1, resp.FailedCount)
	assert.Len(t, resp.FailedFindings, 1)
}

func TestBatchImportFindings_EmptyFindingsList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/accounts", map[string]any{"EnableDefaultStandards": false})

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SuccessCount int `json:"SuccessCount"`
		FailedCount  int `json:"FailedCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.SuccessCount)
	assert.Equal(t, 0, resp.FailedCount)
}

// TestParity_BatchImportFindings_MaxLimit verifies that BatchImportFindings
// rejects requests with more than 100 findings.
func TestBatchImportFindings_MaxLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    int
		wantCode int
	}{
		{name: "100_findings_accepted", count: 100, wantCode: http.StatusOK},
		{name: "101_findings_rejected", count: 101, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			enableHub(t, h)

			findings := make([]any, tt.count)
			for i := range findings {
				findings[i] = validFinding(nil)
			}

			rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
				"Findings": findings,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParity_GetFindings_FiltersApplied verifies that GetFindings applies
// multiple filter fields and comparison operators correctly.
func TestGetFindings_FiltersApplied(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	// Import two findings with different properties
	f1 := validFinding(map[string]any{
		"Id":           "finding-1",
		"Title":        "Finding Alpha",
		"RecordState":  "ACTIVE",
		"AwsAccountId": "111111111111",
	})
	f2 := validFinding(map[string]any{
		"Id":           "finding-2",
		"Title":        "Finding Beta",
		"RecordState":  "ARCHIVED",
		"AwsAccountId": "222222222222",
	})

	importRec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{f1, f2},
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	tests := []struct {
		filters   map[string]any
		name      string
		wantCount int
	}{
		{
			name: "filter_by_record_state_equals",
			filters: map[string]any{
				"RecordState": []any{map[string]any{"Value": "ACTIVE", "Comparison": "EQUALS"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_account_id_not_equals",
			filters: map[string]any{
				"AwsAccountId": []any{map[string]any{"Value": "222222222222", "Comparison": "NOT_EQUALS"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_title_prefix",
			filters: map[string]any{
				"Title": []any{map[string]any{"Value": "Finding", "Comparison": "PREFIX"}},
			},
			wantCount: 2,
		},
		{
			name: "filter_by_title_contains",
			filters: map[string]any{
				"Title": []any{map[string]any{"Value": "Alpha", "Comparison": "CONTAINS"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_title_not_contains",
			filters: map[string]any{
				"Title": []any{map[string]any{"Value": "Alpha", "Comparison": "NOT_CONTAINS"}},
			},
			wantCount: 1,
		},
		{
			name:      "no_filter_returns_all",
			filters:   map[string]any{},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
				"Filters": tt.filters,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			findings, _ := resp["Findings"].([]any)
			assert.Len(t, findings, tt.wantCount)
		})
	}
}

// TestParity_GetFindings_MultipleFilterCombinations verifies that compound
// filters (multiple fields) are applied with AND semantics.
func TestGetFindings_MultipleFilterCombinations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	f1 := validFinding(map[string]any{
		"Id":          "f-compound-1",
		"Title":       "Critical Alert",
		"RecordState": "ACTIVE",
	})
	f2 := validFinding(map[string]any{
		"Id":          "f-compound-2",
		"Title":       "Low Alert",
		"RecordState": "ACTIVE",
	})
	f3 := validFinding(map[string]any{
		"Id":          "f-compound-3",
		"Title":       "Critical Alert",
		"RecordState": "ARCHIVED",
	})

	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{f1, f2, f3},
	})

	// Filter: Title contains "Critical" AND RecordState = ACTIVE → should match only f1
	rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
		"Filters": map[string]any{
			"Title":       []any{map[string]any{"Value": "Critical", "Comparison": "CONTAINS"}},
			"RecordState": []any{map[string]any{"Value": "ACTIVE", "Comparison": "EQUALS"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	findings, _ := resp["Findings"].([]any)
	assert.Len(t, findings, 1)

	if len(findings) > 0 {
		id, _ := findings[0].(map[string]any)["Id"].(string)
		assert.True(t, strings.HasSuffix(id, "f-compound-1"), "only Critical+ACTIVE finding should match")
	}
}

func TestBackend_UpdateFindings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		hubEnabled bool
	}{
		{name: "hub enabled, updates findings", hubEnabled: true},
		{name: "hub not enabled returns error", hubEnabled: false, wantErrMsg: "not enabled"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

			if tc.hubEnabled {
				require.NoError(t, b.EnableHub(false, nil))
				_, _, _ = b.ImportFindings([]map[string]any{
					securityhub.ValidFinding(
						map[string]any{
							"Id":         "f1",
							"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
						},
					),
				})
			}

			err := b.UpdateFindings(map[string]any{}, map[string]any{"Text": "note"}, "ACTIVE")
			if tc.wantErrMsg != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_MatchesStringFilter(t *testing.T) {
	t.Parallel()

	// Tested indirectly through GetFindings with filters
	tests := []struct {
		filter    map[string]any
		name      string
		wantFound bool
	}{
		{
			name:      "no filter returns all",
			filter:    map[string]any{},
			wantFound: true,
		},
		{
			name: "filter by ID equals match",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "filter-finding-1", "Comparison": "EQUALS"},
				},
			},
			wantFound: true,
		},
		{
			name: "filter by ID equals no match",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "wrong-id", "Comparison": "EQUALS"},
				},
			},
			wantFound: false,
		},
		{
			name: "filter by ID NOT_EQUALS includes others",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "other-id", "Comparison": "NOT_EQUALS"},
				},
			},
			wantFound: true,
		},
		{
			name: "filter by ID NOT_EQUALS excludes match",
			filter: map[string]any{
				"Id": []any{
					map[string]any{"Value": "filter-finding-1", "Comparison": "NOT_EQUALS"},
				},
			},
			wantFound: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			_, _, _ = b.ImportFindings([]map[string]any{
				securityhub.ValidFinding(
					map[string]any{
						"Id":         "filter-finding-1",
						"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
					},
				),
			})

			results, _ := b.GetFindings(tc.filter, nil, "", 100)
			if tc.wantFound {
				assert.NotEmpty(t, results)
			} else {
				assert.Empty(t, results)
			}
		})
	}
}

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

func TestFindingsV2(t *testing.T) {
	t.Parallel()

	type step struct {
		body   any
		check  func(t *testing.T, code int, resp map[string]any)
		name   string
		method string
		path   string
	}

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "GetFindingsV2 returns findings from same store as V1",
			steps: []step{
				{
					name:   "import finding via V1",
					method: http.MethodPost,
					path:   "/findings/import",
					body: map[string]any{
						"Findings": []any{
							map[string]any{
								"AwsAccountId":  "000000000000",
								"CreatedAt":     "2024-01-01T00:00:00Z",
								"Description":   "test",
								"GeneratorId":   "gen1",
								"Id":            "finding-001",
								"ProductArn":    "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
								"SchemaVersion": "2018-10-08",
								"Severity":      map[string]any{"Label": "HIGH"},
								"Title":         "Test Finding",
								"Types":         []any{"Software and Configuration Checks"},
								"UpdatedAt":     "2024-01-01T00:00:00Z",
								"Resources":     []any{map[string]any{"Id": "resource-001", "Type": "AwsEc2Instance"}},
							},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "get via V2",
					method: http.MethodPost,
					path:   "/findingsv2",
					body:   map[string]any{},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						findings, _ := resp["Findings"].([]any)
						assert.NotEmpty(t, findings)
					},
				},
			},
		},
		{
			name: "BatchUpdateFindingsV2",
			steps: []step{
				{
					name:   "import finding",
					method: http.MethodPost,
					path:   "/findings/import",
					body: map[string]any{
						"Findings": []any{
							map[string]any{
								"AwsAccountId":  "000000000000",
								"CreatedAt":     "2024-01-01T00:00:00Z",
								"Description":   "test",
								"GeneratorId":   "gen1",
								"Id":            "finding-v2-001",
								"ProductArn":    "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
								"SchemaVersion": "2018-10-08",
								"Severity":      map[string]any{"Label": "HIGH"},
								"Title":         "Test Finding V2",
								"Types":         []any{"Software and Configuration Checks"},
								"UpdatedAt":     "2024-01-01T00:00:00Z",
								"Resources":     []any{},
							},
						},
					},
					check: func(t *testing.T, code int, _ map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
				{
					name:   "batch update V2",
					method: http.MethodPatch,
					path:   "/findingsv2/batchupdatev2",
					body: map[string]any{
						"FindingIdentifiers": []any{
							map[string]any{
								"Id":         "finding-v2-001",
								"ProductArn": "arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default",
							},
						},
						"FindingFieldsUpdate": map[string]any{
							"Note": map[string]any{
								"Text":      "Updated via V2",
								"UpdatedBy": "tester",
							},
						},
					},
					check: func(t *testing.T, code int, resp map[string]any) { //nolint:revive // existing issue.
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
					},
				},
			},
		},
		{
			name: "GetFindingStatisticsV2 returns stats",
			steps: []step{
				{
					name:   "get stats on empty store",
					method: http.MethodPost,
					path:   "/findingsv2/statistics",
					body:   map[string]any{"GroupByAttributes": []any{"Severity.Label"}},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["FindingStatistics"])
					},
				},
			},
		},
		{
			name: "GetFindingsTrendsV2",
			steps: []step{
				{
					name:   "get trends",
					method: http.MethodPost,
					path:   "/findingsTrendsv2",
					body: map[string]any{
						"GroupByAttribute": "Severity.Label",
						"StartTime":        "2024-01-01T00:00:00Z",
						"EndTime":          "2024-12-31T23:59:59Z",
					},
					check: func(t *testing.T, code int, resp map[string]any) {
						t.Helper()
						assert.Equal(t, http.StatusOK, code)
						assert.NotNil(t, resp["FindingsTrends"])
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			for _, s := range tc.steps {
				rec := doRequest(t, h, s.method, s.path, s.body)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				s.check(t, rec.Code, resp)
			}
		})
	}
}

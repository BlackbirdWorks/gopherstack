package securityhub_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
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

// TestGetFindings_MultiValueSameFieldCombination verifies the documented
// StringFilter same-field combination rule (securityhub@v1.75.4
// types.StringFilter doc comment): CONTAINS/EQUALS/PREFIX entries on the
// same field are joined by OR ("a finding matches if it matches any one of
// those filters"), NOT_CONTAINS/NOT_EQUALS/PREFIX_NOT_EQUALS entries are
// joined by AND, and a PREFIX group combines with a NOT_EQUALS/
// PREFIX_NOT_EQUALS group by first taking the OR of the PREFIX matches and
// then excluding anything the negative group rejects.
func TestGetFindings_MultiValueSameFieldCombination(t *testing.T) {
	t.Parallel()

	t.Run("positive comparisons on the same field are OR'd", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		enableHub(t, h)

		f1 := validFinding(map[string]any{"Id": "f-or-1", "Title": "Finding CloudFront issue"})
		f2 := validFinding(map[string]any{"Id": "f-or-2", "Title": "Finding CloudWatch issue"})
		f3 := validFinding(map[string]any{"Id": "f-or-3", "Title": "Finding Unrelated issue"})

		doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
			"Findings": []any{f1, f2, f3},
		})

		// AWS doc example: "Title CONTAINS CloudFront OR Title CONTAINS
		// CloudWatch match a finding that includes either CloudFront,
		// CloudWatch, or both strings in the title."
		rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
			"Filters": map[string]any{
				"Title": []any{
					map[string]any{"Value": "CloudFront", "Comparison": "CONTAINS"},
					map[string]any{"Value": "CloudWatch", "Comparison": "CONTAINS"},
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		findings, _ := resp["Findings"].([]any)

		gotIDs := make([]string, 0, len(findings))
		for _, f := range findings {
			id, _ := f.(map[string]any)["Id"].(string)
			gotIDs = append(gotIDs, id)
		}

		assert.ElementsMatch(t, []string{"f-or-1", "f-or-2"}, gotIDs)
	})

	t.Run("prefix group ORs then excludes the not-equals group", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		enableHub(t, h)

		// AWS doc example on types.StringFilter: PREFIX AwsIam OR PREFIX
		// AwsEc2, then exclude AwsIamPolicy and AwsEc2NetworkInterface.
		f1 := validFinding(map[string]any{"Id": "f-mix-1", "ResourceType": "AwsIamRole"})
		f2 := validFinding(map[string]any{"Id": "f-mix-2", "ResourceType": "AwsIamPolicy"})
		f3 := validFinding(map[string]any{"Id": "f-mix-3", "ResourceType": "AwsEc2Instance"})
		f4 := validFinding(map[string]any{"Id": "f-mix-4", "ResourceType": "AwsEc2NetworkInterface"})
		f5 := validFinding(map[string]any{"Id": "f-mix-5", "ResourceType": "AwsS3Bucket"})

		doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
			"Findings": []any{f1, f2, f3, f4, f5},
		})

		rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{
			"Filters": map[string]any{
				"ResourceType": []any{
					map[string]any{"Value": "AwsIam", "Comparison": "PREFIX"},
					map[string]any{"Value": "AwsEc2", "Comparison": "PREFIX"},
					map[string]any{"Value": "AwsIamPolicy", "Comparison": "NOT_EQUALS"},
					map[string]any{"Value": "AwsEc2NetworkInterface", "Comparison": "NOT_EQUALS"},
				},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		findings, _ := resp["Findings"].([]any)

		gotIDs := make([]string, 0, len(findings))
		for _, f := range findings {
			id, _ := f.(map[string]any)["Id"].(string)
			gotIDs = append(gotIDs, id)
		}

		assert.ElementsMatch(t, []string{"f-mix-1", "f-mix-3"}, gotIDs)
	})
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

// TestGetFindings_SortCriteria verifies that GetFindings applies SortCriteria
// (types.SortCriterion: Field + SortOrder "asc"/"desc") instead of returning
// findings in undefined map-iteration order.
func TestGetFindings_SortCriteria(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	_, _, _ = b.ImportFindings([]map[string]any{
		securityhub.ValidFinding(map[string]any{"Id": "c", "Title": "Charlie"}),
		securityhub.ValidFinding(map[string]any{"Id": "a", "Title": "Alpha"}),
		securityhub.ValidFinding(map[string]any{"Id": "b", "Title": "Bravo"}),
	})

	tests := []struct {
		name         string
		sortCriteria []map[string]any
		wantOrder    []string
	}{
		{
			name:         "ascending by Title",
			sortCriteria: []map[string]any{{"Field": "Title", "SortOrder": "asc"}},
			wantOrder:    []string{"Alpha", "Bravo", "Charlie"},
		},
		{
			name:         "descending by Title",
			sortCriteria: []map[string]any{{"Field": "Title", "SortOrder": "desc"}},
			wantOrder:    []string{"Charlie", "Bravo", "Alpha"},
		},
		{
			name:         "no sort criteria leaves order unspecified but stable count",
			sortCriteria: nil,
			wantOrder:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			results, _ := b.GetFindings(map[string]any{}, tc.sortCriteria, "", 100)
			require.Len(t, results, 3)

			if tc.wantOrder == nil {
				return
			}

			gotOrder := make([]string, len(results))
			for i, f := range results {
				gotOrder[i], _ = f["Title"].(string)
			}

			assert.Equal(t, tc.wantOrder, gotOrder)
		})
	}
}

// TestBatchImportFindings_PreservesCustomerManagedFields verifies that
// re-importing an existing finding never overwrites the Note,
// UserDefinedFields, VerificationState, or Workflow fields -- AWS documents
// these as fields "BatchImportFindings cannot update" once a finding exists,
// since they're managed by Security Hub customers, not finding providers.
func TestBatchImportFindings_PreservesCustomerManagedFields(t *testing.T) {
	t.Parallel()

	b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")

	base := securityhub.ValidFinding(map[string]any{"Id": "preserve-me"})
	_, _, _ = b.ImportFindings([]map[string]any{base})

	// Customer annotates the finding via BatchUpdateFindings.
	_, unprocessed := b.BatchUpdateFindings(
		[]map[string]any{{"Id": "preserve-me", "ProductArn": base["ProductArn"]}},
		map[string]any{
			"Note":              map[string]any{"Text": "investigating", "UpdatedBy": "analyst"},
			"Workflow":          map[string]any{"Status": "NOTIFIED"},
			"UserDefinedFields": map[string]any{"ticket": "JIRA-1"},
			"VerificationState": "TRUE_POSITIVE",
		},
	)
	require.Empty(t, unprocessed)

	// Finding provider re-imports the same finding, attempting to reset the
	// customer-managed fields (and omitting UserDefinedFields entirely).
	reimport := securityhub.ValidFinding(map[string]any{
		"Id":       "preserve-me",
		"Title":    "Updated by provider",
		"Note":     map[string]any{"Text": "provider tried to overwrite", "UpdatedBy": "provider"},
		"Workflow": map[string]any{"Status": "NEW"},
	})
	successCount, failedCount, _ := b.ImportFindings([]map[string]any{reimport})
	require.Equal(t, 1, successCount)
	require.Equal(t, 0, failedCount)

	results, _ := b.GetFindings(
		map[string]any{"Id": []any{map[string]any{"Value": "preserve-me", "Comparison": "EQUALS"}}},
		nil, "", 100,
	)
	require.Len(t, results, 1)

	f := results[0]
	assert.Equal(t, "Updated by provider", f["Title"], "provider-owned field must update")

	note, _ := f["Note"].(map[string]any)
	assert.Equal(t, "investigating", note["Text"], "Note must be preserved from the customer update")

	workflow, _ := f["Workflow"].(map[string]any)
	assert.Equal(t, "NOTIFIED", workflow["Status"], "Workflow must be preserved from the customer update")

	udf, _ := f["UserDefinedFields"].(map[string]any)
	assert.Equal(t, "JIRA-1", udf["ticket"], "UserDefinedFields must be preserved from the customer update")

	assert.Equal(t, "TRUE_POSITIVE", f["VerificationState"], "VerificationState must be preserved")
}

// TestGetFindingHistory_RecordsChanges verifies that finding creation and
// subsequent field updates (via BatchImportFindings re-import,
// BatchUpdateFindings, and UpdateFindings) each append a
// FindingHistoryRecord, and that GetFindingHistory returns them for the
// matching FindingIdentifier.
func TestGetFindingHistory_RecordsChanges(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	enableHub(t, h)

	productArn := "arn:aws:securityhub:us-east-1::product/aws/guardduty"
	ident := map[string]any{"Id": "history-finding", "ProductArn": productArn}

	// 1. Creation via BatchImportFindings.
	doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{securityhub.ValidFinding(map[string]any{
			"Id":         "history-finding",
			"ProductArn": productArn,
		})},
	})

	// 2. Field change via BatchUpdateFindings.
	doRequest(t, h, http.MethodPatch, "/findings/batchupdate", map[string]any{
		"FindingIdentifiers": []any{ident},
		"RecordState":        "ARCHIVED",
	})

	// 3. Field change via UpdateFindings (legacy note/record-state updater).
	doRequest(t, h, http.MethodPatch, "/findings", map[string]any{
		"Filters": map[string]any{
			"Id": []any{map[string]any{"Value": "history-finding", "Comparison": "EQUALS"}},
		},
		"Note": map[string]any{"Text": "legacy update", "UpdatedBy": "tester"},
	})

	rec := doRequest(t, h, http.MethodPost, "/findingHistory/get", map[string]any{
		"FindingIdentifier": ident,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Records []map[string]any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	require.Len(t, resp.Records, 3, "creation + 2 field-change events")

	assert.Equal(t, true, resp.Records[0]["FindingCreated"])

	src, _ := resp.Records[0]["UpdateSource"].(map[string]any)
	assert.Equal(t, "BATCH_IMPORT_FINDINGS", src["Type"])

	assert.Equal(t, false, resp.Records[1]["FindingCreated"])

	src1, _ := resp.Records[1]["UpdateSource"].(map[string]any)
	assert.Equal(t, "BATCH_UPDATE_FINDINGS", src1["Type"])

	updates1, _ := resp.Records[1]["Updates"].([]any)
	require.NotEmpty(t, updates1, "RecordState change must be recorded")

	updates2, _ := resp.Records[2]["Updates"].([]any)
	require.NotEmpty(t, updates2, "Note change must be recorded")
}

// TestGetFindingHistory_UnknownFinding verifies GetFindingHistory returns an
// empty Records list (not an error) for a finding with no recorded history.
func TestGetFindingHistory_UnknownFinding(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/findingHistory/get", map[string]any{
		"FindingIdentifier": map[string]any{
			"Id":         "never-existed",
			"ProductArn": "arn:aws:securityhub:us-east-1::product/aws/guardduty",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Records []map[string]any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Records)
}

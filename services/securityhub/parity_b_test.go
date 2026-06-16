package securityhub_test

// parity_b_test.go — §B parity: BatchImportFindings ASFF field validation

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// validFinding returns a minimal ASFF-compliant finding for HTTP handler tests.
func validFinding(overrides map[string]any) map[string]any {
	return securityhub.ValidFinding(overrides)
}

// ---------------------------------------------------------------------------
// BatchImportFindings validation
// ---------------------------------------------------------------------------

func TestParity_BatchImportFindings_RequiredFields(t *testing.T) {
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

func TestParity_BatchImportFindings_MixedValidity(t *testing.T) {
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

func TestParity_BatchImportFindings_EmptyFindingsList(t *testing.T) {
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

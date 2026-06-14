package securityhub_test

// Tests for parity §B: BatchImportFindings rejects missing required fields (go-nace).

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validFinding returns a well-formed ASFF finding map.
func validFinding(id string) map[string]any {
	return map[string]any{
		"SchemaVersion": "2018-10-08",
		"Id":            id,
		"ProductArn":    "arn:aws:securityhub:us-east-1::product/aws/guardduty",
		"GeneratorId":   "gen-1",
		"AwsAccountId":  "000000000000",
		"Types":         []string{"Software and Configuration Checks"},
		"CreatedAt":     "2024-01-01T00:00:00Z",
		"UpdatedAt":     "2024-01-01T00:00:00Z",
		"Severity":      map[string]any{"Label": "HIGH"},
		"Title":         "Test Finding",
		"Description":   "desc",
		"Resources":     []map[string]any{{"Type": "AwsEc2Instance", "Id": "i-1"}},
	}
}

// TestParityB_BatchImportFindings_TypesRequired verifies that BatchImportFindings
// rejects findings missing ProductArn, Id, or Types with an error in FailedFindings.
func TestParityB_BatchImportFindings_TypesRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		finding      map[string]any
		wantSuccess  int
		wantFailed   int
		wantErrCode  string
	}{
		{
			name:        "valid_finding_succeeds",
			finding:     validFinding("finding-valid"),
			wantSuccess: 1,
			wantFailed:  0,
		},
		{
			name: "missing_types_fails",
			finding: func() map[string]any {
				f := validFinding("finding-no-types")
				delete(f, "Types")
				return f
			}(),
			wantSuccess: 0,
			wantFailed:  1,
			wantErrCode: "InvalidInputException",
		},
		{
			name: "empty_types_fails",
			finding: func() map[string]any {
				f := validFinding("finding-empty-types")
				f["Types"] = []string{}
				return f
			}(),
			wantSuccess: 0,
			wantFailed:  1,
			wantErrCode: "InvalidInputException",
		},
		{
			name: "missing_product_arn_fails",
			finding: func() map[string]any {
				f := validFinding("finding-no-arn")
				delete(f, "ProductArn")
				return f
			}(),
			wantSuccess: 0,
			wantFailed:  1,
			wantErrCode: "InvalidInputException",
		},
		{
			name: "missing_id_fails",
			finding: func() map[string]any {
				f := validFinding("")
				f["Id"] = ""
				return f
			}(),
			wantSuccess: 0,
			wantFailed:  1,
			wantErrCode: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
				"Findings": []any{tt.finding},
			})
			require.Equal(t, http.StatusOK, rec.Code, "BatchImportFindings always returns 200: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			successCount := int(resp["SuccessCount"].(float64))
			failedCount := int(resp["FailedCount"].(float64))

			assert.Equal(t, tt.wantSuccess, successCount, "SuccessCount mismatch")
			assert.Equal(t, tt.wantFailed, failedCount, "FailedCount mismatch")

			if tt.wantErrCode != "" {
				failedFindings, ok := resp["FailedFindings"].([]any)
				require.True(t, ok, "FailedFindings must be present")
				require.Len(t, failedFindings, 1)

				ff, ok := failedFindings[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantErrCode, ff["ErrorCode"], "ErrorCode mismatch")
			}
		})
	}
}

// TestParityB_BatchImportFindings_MixedFindings verifies that a batch with both
// valid and invalid findings returns correct split counts.
func TestParityB_BatchImportFindings_MixedFindings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	missingTypes := validFinding("bad-finding")
	delete(missingTypes, "Types")

	rec := doRequest(t, h, http.MethodPost, "/findings/import", map[string]any{
		"Findings": []any{
			validFinding("good-1"),
			missingTypes,
			validFinding("good-2"),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.InDelta(t, float64(2), resp["SuccessCount"], 0.01)
	assert.InDelta(t, float64(1), resp["FailedCount"], 0.01)

	failedFindings, ok := resp["FailedFindings"].([]any)
	require.True(t, ok)
	require.Len(t, failedFindings, 1)

	ff := failedFindings[0].(map[string]any)
	assert.Equal(t, "InvalidInputException", ff["ErrorCode"])
}

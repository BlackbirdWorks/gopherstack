package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestConformancePackARN verifies PutConformancePack generates an ARN and ID.
func TestConformancePackARN(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutConformancePack", map[string]any{
		"ConformancePackName": "test-pack",
		"DeliveryS3Bucket":    "my-delivery-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeConformancePacks", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConformancePackDetails []struct {
			ConformancePackArn string `json:"ConformancePackArn"`
			ConformancePackID  string `json:"ConformancePackId"`
			DeliveryS3Bucket   string `json:"DeliveryS3Bucket"`
		} `json:"ConformancePackDetails"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConformancePackDetails, 1)
	assert.Contains(t, out.ConformancePackDetails[0].ConformancePackArn, "arn:aws:config:")
	assert.NotEmpty(t, out.ConformancePackDetails[0].ConformancePackID)
	assert.Equal(t, "my-delivery-bucket", out.ConformancePackDetails[0].DeliveryS3Bucket)
}

func TestAWSConfigHandler_DeleteConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConformancePack("my-pack", "", "", ""))
			},
			body:     map[string]any{"ConformancePackName": "my-pack"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConformancePackName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteConformancePack", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestAWSConfigHandler_PutConformancePack_TemplateBodyDeploysRules verifies
// the TemplateBody wire field reaches the backend and its declared config
// rules become visible via DescribeConformancePackCompliance.
func TestAWSConfigHandler_PutConformancePack_TemplateBodyDeploysRules(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	const template = `{"Resources":{"RuleA":{"Type":"AWS::Config::ConfigRule",
		"Properties":{"ConfigRuleName":"rule-a","Source":{"Owner":"AWS","SourceIdentifier":"ENCRYPTED_VOLUMES"}}}}}`

	putRec := doAWSConfigRequest(t, h, "PutConformancePack", map[string]any{
		"ConformancePackName": "pack1",
		"TemplateBody":        template,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	complianceRec := doAWSConfigRequest(t, h, "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "pack1",
	})
	require.Equal(t, http.StatusOK, complianceRec.Code)

	var out struct {
		ConformancePackRuleComplianceList []struct {
			ConfigRuleName string `json:"ConfigRuleName"`
			ComplianceType string `json:"ComplianceType"`
		} `json:"ConformancePackRuleComplianceList"`
	}
	require.NoError(t, json.Unmarshal(complianceRec.Body.Bytes(), &out))
	require.Len(t, out.ConformancePackRuleComplianceList, 1)
	assert.Equal(t, "rule-a", out.ConformancePackRuleComplianceList[0].ConfigRuleName)
	assert.Equal(t, "INSUFFICIENT_DATA", out.ConformancePackRuleComplianceList[0].ComplianceType)
}

// TestAWSConfigHandler_DescribeConformancePackCompliance_UnknownPack verifies
// the wire error type for a not-found conformance pack.
func TestAWSConfigHandler_DescribeConformancePackCompliance_UnknownPack(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "DescribeConformancePackCompliance", map[string]any{
		"ConformancePackName": "does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchConformancePackException")
}

// TestAWSConfigHandler_ListConformancePackComplianceScores verifies the
// nested Filters.ConformancePackNames wire field is parsed correctly.
func TestAWSConfigHandler_ListConformancePackComplianceScores(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	require.NoError(t, h.Backend.PutConformancePack("pack1", "", "", ""))

	rec := doAWSConfigRequest(t, h, "ListConformancePackComplianceScores", map[string]any{
		"Filters": map[string]any{"ConformancePackNames": []string{"pack1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConformancePackComplianceScores []struct {
			ConformancePackName string `json:"ConformancePackName"`
			Score               string `json:"Score"`
		} `json:"ConformancePackComplianceScores"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConformancePackComplianceScores, 1)
	assert.Equal(t, "pack1", out.ConformancePackComplianceScores[0].ConformancePackName)
	assert.Equal(t, "INSUFFICIENT_DATA", out.ConformancePackComplianceScores[0].Score)
}

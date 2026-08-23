package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestGetCisScanResultDetails_ScanResultDetailsKey_RealClient covers
// gopherstack-y1zn. GetCisScanResultDetails emitted "checkResults";
// GetCisScanResultDetailsOutput (inspector2@v1.54.1 deserializers.go's
// awsRestjson1_deserializeOpDocumentGetCisScanResultDetailsOutput) declares
// only nextToken/scanResultDetails.
func TestGetCisScanResultDetails_ScanResultDetailsKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/cis/scan-result/details/get", map[string]any{
		"scanArn": "arn:aws:inspector2:us-east-1:123456789012:cis-scan/does-not-exist",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"checkResults"`,
		"GetCisScanResultDetailsOutput has no checkResults member")
	assert.Contains(t, body, `"scanResultDetails"`,
		"GetCisScanResultDetailsOutput's real member is scanResultDetails")
}

// TestListCisScans_TargetsKey_RealClient covers gopherstack-y1zn. ListCisScans
// emitted a flat "targetAccountId" string; types.CisScan (inspector2@v1.54.1
// deserializers.go's awsRestjson1_deserializeDocumentCisScan) has no such
// member -- account IDs live under targets.accountIds
// (types.CisTargets).
func TestListCisScans_TargetsKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	createCisConfig(t, h, "y1zn-cis-cfg", []string{"123456789012"})

	rec := auditDo(t, h, http.MethodPost, "/cis/scan/list", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	require.Contains(t, body, "scanArn", "must have seeded at least one scan")
	assert.NotContains(t, body, `"targetAccountId"`,
		"types.CisScan has no targetAccountId member")
	assert.Contains(t, body, `"targets"`,
		"types.CisScan's real member is targets (with a nested accountIds array)")
}

// TestListFindingAggregations_NoLowKey_RealClient covers gopherstack-y1zn.
// ListFindingAggregations' severityCounts included a "low" key;
// types.SeverityCounts (inspector2@v1.54.1 deserializers.go's
// awsRestjson1_deserializeDocumentSeverityCounts) declares only
// all/critical/high/medium -- there is no low member, LOW findings only fold
// into "all".
func TestListFindingAggregations_NoLowKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	_, err := h.Backend.SeedFinding(inspector2.Finding{
		Severity: inspector2.FindingSeverity{Label: "LOW"},
	})
	require.NoError(t, err)

	rec := auditDo(t, h, http.MethodPost, "/findings/aggregation/list", map[string]any{
		"aggregationType": "ACCOUNT",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	responses, ok := out["responses"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, responses)

	resp0 := responses[0].(map[string]any)
	acct := resp0["accountAggregation"].(map[string]any)
	counts := acct["severityCounts"].(map[string]any)
	assert.NotContains(t, counts, "low",
		"types.SeverityCounts has no low member")
}

// TestGetEc2DeepInspectionConfiguration_NoScanModeState_RealClient covers
// gopherstack-y1zn. handleGetEc2DeepInspectionConfiguration wrapped a
// fabricated "ec2ScanModeState" object into its response;
// GetEc2DeepInspectionConfigurationOutput (inspector2@v1.54.1
// deserializers.go's
// awsRestjson1_deserializeOpDocumentGetEc2DeepInspectionConfigurationOutput)
// declares only errorMessage/orgPackagePaths/packagePaths/status.
func TestGetEc2DeepInspectionConfiguration_NoScanModeState_RealClient(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/ec2deepinspectionconfiguration/get", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"ec2ScanModeState"`,
		"GetEc2DeepInspectionConfigurationOutput has no ec2ScanModeState member")
}

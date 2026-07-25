package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestFindingsReportLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create, including filterCriteria -- real GetFindingsReportStatusOutput
	// echoes destination/filterCriteria back (see PARITY.md's prior gap note).
	rec := auditDo(t, h, http.MethodPost, "/reporting/create", map[string]any{
		"reportFormat":  "CSV",
		"s3Destination": map[string]any{"bucketName": "my-bucket", "keyPrefix": "reports/"},
		"filterCriteria": map[string]any{
			"severity": []any{map[string]any{"comparison": "EQUALS", "value": "HIGH"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	reportID, _ := createResp["reportId"].(string)
	require.NotEmpty(t, reportID)

	// Get status
	rec = auditDo(t, h, http.MethodPost, "/reporting/status/get", map[string]any{
		"reportId": reportID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statusResp))
	assert.Equal(t, reportID, statusResp["reportId"])
	assert.Equal(t, "SUCCEEDED", statusResp["status"])

	destination, ok := statusResp["destination"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-bucket", destination["bucketName"])

	filterCriteria, ok := statusResp["filterCriteria"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, filterCriteria, "severity")

	// Cancel
	rec = auditDo(t, h, http.MethodPost, "/reporting/cancel", map[string]any{
		"reportId": reportID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Cancel unknown report returns 404
	rec = auditDo(t, h, http.MethodPost, "/reporting/cancel", map[string]any{
		"reportId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFindingAggregations(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/findings/aggregation/list", map[string]any{
		"aggregationType": "ACCOUNT",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["responses"]
	assert.True(t, ok)
}

// TestSearchVulnerabilities exercises the real SeedVulnerability -> exact-ID
// lookup path: real SearchVulnerabilitiesFilterCriteria.vulnerabilityIds is a
// required exact-match list, not a free-text query, and gopherstack has no
// vulnerability intelligence database to search against, so results only
// ever come from explicitly seeded vulnerabilities (see Vulnerability's doc
// comment).
func TestSearchVulnerabilities(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	_, err := h.Backend.SeedVulnerability(inspector2.Vulnerability{
		ID:          "CVE-2023-1234",
		Description: "a seeded test vulnerability",
		Source:      "NVD",
		Cwes:        []string{"CWE-79"},
	})
	require.NoError(t, err)

	rec := auditDo(t, h, http.MethodPost, "/vulnerabilities/search", map[string]any{
		"filterCriteria": map[string]any{
			"vulnerabilityIds": []string{"CVE-2023-1234", "CVE-9999-0000"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	vulns, ok := resp["vulnerabilities"].([]any)
	require.True(t, ok)
	// Only the seeded ID matches; the unknown CVE is silently omitted,
	// matching real AWS behavior for an unrecognized vulnerability ID.
	require.Len(t, vulns, 1)

	v, ok := vulns[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "CVE-2023-1234", v["id"])
	assert.Equal(t, "a seeded test vulnerability", v["description"])
	assert.NotContains(t, v, "vulnerabilityId",
		`real wire key is "id", not the gopherstack-invented "vulnerabilityId"`)
}

func TestSearchVulnerabilitiesNoMatches(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/vulnerabilities/search", map[string]any{
		"filterCriteria": map[string]any{
			"vulnerabilityIds": []string{"CVE-0000-0000"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	vulns, ok := resp["vulnerabilities"].([]any)
	require.True(t, ok)
	assert.Empty(t, vulns)
}

func TestBatchGetCodeSnippet(t *testing.T) {
	t.Parallel()

	const seededArn = "arn:aws:inspector2:us-east-1:123456789012:finding/seeded"

	const missingArn = "arn:aws:inspector2:us-east-1:123456789012:finding/missing"

	h := newAuditHandler(t)
	require.NoError(t, h.Backend.SeedCodeSnippet(
		seededArn,
		[]inspector2.CodeLine{{Content: "func f() {}", LineNumber: 10}},
		[]inspector2.SuggestedFix{{Description: "add nil check"}},
	))

	rec := auditDo(t, h, http.MethodPost, "/codesnippet/batchget", map[string]any{
		"findingArns": []string{seededArn, missingArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["codeSnippetResults"].([]any)
	require.True(t, ok)
	require.Len(t, results, 1)

	result, ok := results[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, seededArn, result["findingArn"])

	errs, ok := resp["errors"].([]any)
	require.True(t, ok)
	require.Len(t, errs, 1)

	errEntry, ok := errs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, missingArn, errEntry["findingArn"])
	assert.Equal(t, "CODE_SNIPPET_NOT_FOUND", errEntry["errorCode"])
}

// TestBatchGetFindingDetails exercises the fixed request shape: real
// BatchGetFindingDetailsInput.findingArns is a plain string array, not an
// array of objects (the prior handler decoded into []map[string]any, which
// would fail json.Unmarshal on every real client request of the form
// {"findingArns":["arn1","arn2"]}).
func TestBatchGetFindingDetails(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	seeded, err := h.Backend.SeedFinding(inspector2.Finding{
		Cwes:          []string{"CWE-89"},
		ReferenceUrls: []string{"https://example.com/advisory"},
		RiskScore:     42,
	})
	require.NoError(t, err)

	const missingArn = "arn:aws:inspector2:us-east-1:123456789012:finding/missing"

	rec := auditDo(t, h, http.MethodPost, "/findings/details/batch/get", map[string]any{
		"findingArns": []string{seeded.FindingArn, missingArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	details, ok := resp["findingDetails"].([]any)
	require.True(t, ok)
	require.Len(t, details, 1)

	detail, ok := details[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, seeded.FindingArn, detail["findingArn"])
	assert.InDelta(t, float64(42), detail["riskScore"], 0)
	assert.Contains(t, detail["cwes"], "CWE-89")

	errs, ok := resp["errors"].([]any)
	require.True(t, ok)
	require.Len(t, errs, 1)

	errEntry, ok := errs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, missingArn, errEntry["findingArn"])
	assert.Equal(t, "FINDING_DETAILS_NOT_FOUND", errEntry["errorCode"])
}

// TestBatchGetFindingDetailsMalformedRequest verifies the old
// (wrong-shaped) request body -- an array of objects instead of an array of
// strings -- is now rejected as invalid JSON rather than silently decoding.
func TestBatchGetFindingDetailsMalformedRequest(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/findings/details/batch/get", map[string]any{
		"findingArns": []any{
			map[string]any{"findingArn": "arn:aws:inspector2:us-east-1:123456789012:finding/abc"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

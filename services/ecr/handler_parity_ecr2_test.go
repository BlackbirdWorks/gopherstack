package ecr_test

// handler_parity_ecr2_test.go — end-to-end (HTTP handler) coverage for the ECR
// parity work: enhanced-scan JSON wire shape, lifecycle-policy image expiry
// observed through DescribeImages, and per-destination replication status.

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerParity_EnhancedScan_WireShape(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "enh")
	digest := mustPutImage(t, h, "enh", "v1", `{"schemaVersion":2,"enh":"wire"}`)

	rec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{"scanType": "ENHANCED"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doAccuracy(t, h, "StartImageScan", map[string]any{
		"repositoryName": "enh",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doAccuracy(t, h, "DescribeImageScanFindings", map[string]any{
		"repositoryName": "enh",
		"imageId":        map[string]any{"imageDigest": digest},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := parseAccuracy(t, rec)
	findings, _ := out["imageScanFindings"].(map[string]any)
	require.NotNil(t, findings, "imageScanFindings must be present")

	enhanced, _ := findings["enhancedFindings"].([]any)
	require.NotEmpty(t, enhanced, "enhancedFindings must be emitted on the wire")

	_, hasBasic := findings["findings"]
	assert.False(t, hasBasic, "enhanced scan must not also emit basic findings")

	f0, _ := enhanced[0].(map[string]any)
	require.NotNil(t, f0)
	assert.Equal(t, "PACKAGE_VULNERABILITY", f0["type"])
	assert.NotEmpty(t, f0["fixAvailable"])

	pvd, _ := f0["packageVulnerabilityDetails"].(map[string]any)
	require.NotNil(t, pvd, "packageVulnerabilityDetails must be present")
	assert.NotEmpty(t, pvd["vulnerabilityId"])
	cvss, _ := pvd["cvss"].([]any)
	assert.NotEmpty(t, cvss, "cvss scores must be present")
}

func TestHandlerParity_LifecyclePolicy_DeletesViaDescribeImages(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc")

	for i, tag := range []string{"a", "b", "c", "d"} {
		manifest := `{"schemaVersion":2,"i":` + string(rune('0'+i)) + `}`
		mustPutImage(t, h, "lc", tag, manifest)
	}

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":2}}]}`

	rec := doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lc",
		"lifecyclePolicyText": policy,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "lc"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	assert.Len(t, details, 2, "lifecycle policy must expire images down to countNumber via DescribeImages")
}

package ecr_test

// scan_test.go — verifies that ENHANCED registry scanning produces a
// genuinely different finding shape from BASIC scanning: package-level
// vulnerability detail, CVSS scores, remediation, and fixAvailable, returned
// under enhancedFindings rather than findings (scan.go's mock finding
// generation).

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func setScanType(t *testing.T, b *ecr.InMemoryBackend, scanType string) {
	t.Helper()

	_, err := b.PutRegistryScanningConfiguration(context.Background(),
		&ecr.RegistryScanningSettings{ScanType: scanType})
	require.NoError(t, err)
}

// scanDigest picks a digest whose seed selects at least one CVE so both scan
// types yield non-empty results. "sha256:ffffffff" has many low bits set.
const scanDigest = "sha256:ffffffffffffffff"

func TestScan_BasicVsEnhanced_DistinctShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		scanType     string
		wantBasic    bool
		wantEnhanced bool
	}{
		{name: "basic populates findings only", scanType: "BASIC", wantBasic: true, wantEnhanced: false},
		{name: "enhanced populates enhancedFindings only", scanType: "ENHANCED", wantBasic: false, wantEnhanced: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("scan")
			b.AddImageInternal("scan", makeImage(scanDigest, "v1"))
			setScanType(t, b, tt.scanType)

			_, err := b.StartImageScan(context.Background(), "scan",
				ecr.ImageIdentifier{ImageDigest: scanDigest})
			require.NoError(t, err)

			res, _, err := b.DescribeImageScanFindings(context.Background(), "scan",
				ecr.ImageIdentifier{ImageDigest: scanDigest}, 0, "")
			require.NoError(t, err)

			assert.Equal(t, tt.wantBasic, len(res.Findings) > 0, "basic findings presence")
			assert.Equal(t, tt.wantEnhanced, len(res.EnhancedFindings) > 0, "enhanced findings presence")
			assert.NotEmpty(t, res.FindingSeverityCounts, "severity counts populated for both types")
		})
	}
}

func TestScan_Enhanced_FindingDetail(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("scan")
	b.AddImageInternal("scan", makeImage(scanDigest, "v1"))
	setScanType(t, b, "ENHANCED")

	_, err := b.StartImageScan(context.Background(), "scan",
		ecr.ImageIdentifier{ImageDigest: scanDigest})
	require.NoError(t, err)

	res, _, err := b.DescribeImageScanFindings(context.Background(), "scan",
		ecr.ImageIdentifier{ImageDigest: scanDigest}, 0, "")
	require.NoError(t, err)
	require.NotEmpty(t, res.EnhancedFindings)
	require.Empty(t, res.Findings, "enhanced scan must not also emit basic findings")

	f := res.EnhancedFindings[0]
	assert.Equal(t, "PACKAGE_VULNERABILITY", f.Type)
	assert.Equal(t, "ACTIVE", f.Status)
	assert.Contains(t, []string{"YES", "NO"}, f.FixAvailable, "fixAvailable must be set")
	assert.Positive(t, f.Score, "CVSS score must be positive")

	require.NotNil(t, f.PackageVulnerabilityDetails)
	pvd := f.PackageVulnerabilityDetails
	assert.NotEmpty(t, pvd.VulnerabilityID, "vulnerabilityId (CVE) must be set")
	require.NotEmpty(t, pvd.Cvss)
	assert.Positive(t, pvd.Cvss[0].BaseScore)
	assert.Equal(t, "3.1", pvd.Cvss[0].Version)
	require.NotEmpty(t, pvd.VulnerablePackages, "vulnerable packages must be listed")
	assert.NotEmpty(t, pvd.VulnerablePackages[0].Name)
	assert.NotEmpty(t, pvd.VulnerablePackages[0].FixedInVersion)

	require.NotNil(t, f.Remediation)
	assert.NotEmpty(t, f.Remediation.Recommendation.Text)

	require.NotEmpty(t, f.Resources)
	assert.Equal(t, "AWS_ECR_CONTAINER_IMAGE", f.Resources[0].Type)
	assert.Equal(t, "scan", f.Resources[0].Details.AwsEcrContainerImage.RepositoryName)
}

func TestScan_Deterministic_SameDigestSameFindings(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("scan")
	b.AddImageInternal("scan", makeImage(scanDigest, "v1"))
	setScanType(t, b, "ENHANCED")

	first := scanOnce(t, b)
	second := scanOnce(t, b)
	assert.Equal(t, first, second, "same digest must yield the same enhanced CVE set")
}

func scanOnce(t *testing.T, b *ecr.InMemoryBackend) []string {
	t.Helper()

	_, err := b.StartImageScan(context.Background(), "scan",
		ecr.ImageIdentifier{ImageDigest: scanDigest})
	require.NoError(t, err)

	res, _, err := b.DescribeImageScanFindings(context.Background(), "scan",
		ecr.ImageIdentifier{ImageDigest: scanDigest}, 0, "")
	require.NoError(t, err)

	ids := make([]string, 0, len(res.EnhancedFindings))
	for _, f := range res.EnhancedFindings {
		ids = append(ids, f.PackageVulnerabilityDetails.VulnerabilityID)
	}

	return ids
}

func TestScan_EnhancedWireShape(t *testing.T) {
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

package ecr_test

// scan_enhanced_test.go — verifies that ENHANCED registry scanning produces a
// genuinely different finding shape from BASIC scanning: package-level
// vulnerability detail, CVSS scores, remediation, and fixAvailable, returned
// under enhancedFindings rather than findings.

import (
	"context"
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

package ecr

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	severityCritical      = "CRITICAL"
	severityHigh          = "HIGH"
	severityMedium        = "MEDIUM"
	severityLow           = "LOW"
	severityInformational = "INFORMATIONAL"
)

type cveEntry struct {
	name     string
	severity string
	desc     string
	uri      string
}

// mockCVEList is a fixed catalogue of simulated scan findings used to populate
// image scan results deterministically based on the image digest.
func mockCVEList() []cveEntry {
	return []cveEntry{
		{
			"CVE-2021-44228", severityCritical,
			"Log4Shell: JNDI injection in Apache Log4j2",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-44228",
		},
		{
			"CVE-2022-22965", severityCritical,
			"Spring4Shell: RCE in Spring Framework",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-22965",
		},
		{
			"CVE-2021-3711", severityHigh,
			"OpenSSL buffer overflow in SM2 decryption",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-3711",
		},
		{
			"CVE-2022-0778", severityHigh,
			"OpenSSL infinite loop via BN_mod_sqrt()",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-0778",
		},
		{
			"CVE-2021-41773", severityHigh,
			"Apache httpd path traversal and RCE",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-41773",
		},
		{
			"CVE-2022-1292", severityMedium,
			"OpenSSL c_rehash command injection",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-1292",
		},
		{
			"CVE-2022-2068", severityMedium,
			"OpenSSL c_rehash further command injection",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-2068",
		},
		{
			"CVE-2021-3520", severityMedium,
			"liblz4 memory corruption on large input",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-3520",
		},
		{
			"CVE-2022-25315", severityLow,
			"libexpat integer overflow in XML_GetBuffer",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-25315",
		},
		{
			"CVE-2022-23218", severityLow,
			"glibc buffer overflow in svcunix_create",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-23218",
		},
		{
			"CVE-2022-1586", severityInformational,
			"pcre2 out-of-bounds read",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-1586",
		},
		{
			"CVE-2021-28165", severityInformational,
			"Jetty resource exhaustion via TLS packets",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-28165",
		},
	}
}

// generateMockScanFindings creates deterministic simulated CVE scan results for
// an image identified by imageDigest. The set of returned findings is derived
// from a hash of the digest so that the same image always yields the same
// results, while different images yield different subsets.
func generateMockScanFindings(
	imageDigest, repositoryName, registryID string,
	imageID ImageIdentifier,
) *ImageScanFindingsResult {
	// Derive a seed from the image digest.
	h := sha256.Sum256([]byte(imageDigest))
	seed := binary.BigEndian.Uint64(h[:8])

	cves := mockCVEList()
	severityCounts := map[string]int32{}
	findings := make([]ImageScanFinding, 0, len(cves))

	for i, cve := range cves {
		// Include the finding when the bit at position i is set in the seed.
		if (seed>>uint(i))&1 == 0 { //nolint:gosec // i is a loop index bounded by len(cves)
			continue
		}

		finding := ImageScanFinding{
			Name:        cve.name,
			Severity:    cve.severity,
			Description: cve.desc,
			URI:         cve.uri,
			Attributes: map[string]string{
				"package name":    fmt.Sprintf("pkg-%d", i),
				"package version": fmt.Sprintf("1.%d.0", i),
			},
		}
		findings = append(findings, finding)
		severityCounts[cve.severity]++
	}

	desc := "The scan completed successfully."
	if len(findings) == 0 {
		desc = msgNoScanFindings
	}

	return &ImageScanFindingsResult{
		CompletedAt:           time.Now(),
		FindingSeverityCounts: severityCounts,
		ImageID:               imageID,
		RepositoryName:        repositoryName,
		RegistryID:            registryID,
		Status:                scanStatusComplete,
		Description:           desc,
		Findings:              findings,
	}
}

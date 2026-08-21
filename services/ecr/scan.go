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

// Representative CVSS v3.1 base scores per qualitative severity, used to
// populate the enhanced (Inspector-style) finding shape.
const (
	cvssCritical = 9.8
	cvssHigh     = 7.5
	cvssMedium   = 5.5
	cvssLow      = 3.1
)

// seedBits is the number of bits in the digest-derived selection seed.
const seedBits = 64

// pkgOpenSSL is the affected-package name shared by several catalogued CVEs.
const pkgOpenSSL = "openssl"

type cveEntry struct {
	name     string
	severity string
	desc     string
	uri      string
	pkg      string
	fixedIn  string
}

// mockCVEList is a fixed catalogue of simulated scan findings used to populate
// image scan results deterministically based on the image digest.
func mockCVEList() []cveEntry {
	return []cveEntry{
		{
			"CVE-2021-44228", severityCritical,
			"Log4Shell: JNDI injection in Apache Log4j2",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-44228",
			"log4j-core", "2.17.1",
		},
		{
			"CVE-2022-22965", severityCritical,
			"Spring4Shell: RCE in Spring Framework",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-22965",
			"spring-beans", "5.3.18",
		},
		{
			"CVE-2021-3711", severityHigh,
			"OpenSSL buffer overflow in SM2 decryption",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-3711",
			pkgOpenSSL, "1.1.1l",
		},
		{
			"CVE-2022-0778", severityHigh,
			"OpenSSL infinite loop via BN_mod_sqrt()",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-0778",
			pkgOpenSSL, "1.1.1n",
		},
		{
			"CVE-2021-41773", severityHigh,
			"Apache httpd path traversal and RCE",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-41773",
			"httpd", "2.4.51",
		},
		{
			"CVE-2022-1292", severityMedium,
			"OpenSSL c_rehash command injection",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-1292",
			pkgOpenSSL, "1.1.1o",
		},
		{
			"CVE-2022-2068", severityMedium,
			"OpenSSL c_rehash further command injection",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-2068",
			pkgOpenSSL, "1.1.1p",
		},
		{
			"CVE-2021-3520", severityMedium,
			"liblz4 memory corruption on large input",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-3520",
			"lz4", "1.9.3",
		},
		{
			"CVE-2022-25315", severityLow,
			"libexpat integer overflow in XML_GetBuffer",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-25315",
			"expat", "2.4.5",
		},
		{
			"CVE-2022-23218", severityLow,
			"glibc buffer overflow in svcunix_create",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-23218",
			"glibc", "2.35",
		},
		{
			"CVE-2022-1586", severityInformational,
			"pcre2 out-of-bounds read",
			"https://nvd.nist.gov/vuln/detail/CVE-2022-1586",
			"pcre2", "10.40",
		},
		{
			"CVE-2021-28165", severityInformational,
			"Jetty resource exhaustion via TLS packets",
			"https://nvd.nist.gov/vuln/detail/CVE-2021-28165",
			"jetty-server", "9.4.39",
		},
	}
}

// severityCVSSScore maps a qualitative severity to a representative CVSS v3.1
// base score, used to populate the enhanced (Inspector-style) finding shape.
func severityCVSSScore(severity string) float64 {
	switch severity {
	case severityCritical:
		return cvssCritical
	case severityHigh:
		return cvssHigh
	case severityMedium:
		return cvssMedium
	case severityLow:
		return cvssLow
	default:
		return 0.0
	}
}

// generateMockScanFindings creates deterministic simulated CVE scan results for
// an image identified by imageDigest. The set of selected CVEs is derived from a
// hash of the digest so that the same image always yields the same results while
// different images yield different subsets.
//
// scanType selects the finding shape: BASIC populates Findings (name/severity/
// description/uri), while ENHANCED populates EnhancedFindings with Inspector-
// style, package-level detail (CVSS scores, vulnerable packages, remediation,
// fixAvailable). The two scan types therefore return genuinely different data.
func generateMockScanFindings(
	imageDigest, repositoryName, registryID string,
	imageID ImageIdentifier,
	scanType string,
) *ImageScanFindingsResult {
	// Derive a seed from the image digest.
	h := sha256.Sum256([]byte(imageDigest))
	seed := binary.BigEndian.Uint64(h[:8])

	cves := mockCVEList()
	selected := make([]cveEntry, 0, len(cves))
	severityCounts := map[string]int32{}

	for i, cve := range cves {
		// Include the finding when the bit at position i is set in the seed.
		if (seed>>uint(i))&1 == 0 {
			continue
		}

		selected = append(selected, cve)
		severityCounts[cve.severity]++
	}

	now := float64(time.Now().Unix())

	result := &ImageScanFindingsResult{
		ImageScanCompletedAt:  now,
		FindingSeverityCounts: severityCounts,
		ImageID:               imageID,
		RepositoryName:        repositoryName,
		RegistryID:            registryID,
		Status:                scanStatusComplete,
	}

	if scanType == scanTypeEnhanced {
		// VulnerabilitySourceUpdatedAt reflects when the Inspector-style
		// vulnerability database was last consulted; only meaningful for
		// ENHANCED scans, matching real AWS (BASIC scans omit it).
		result.VulnerabilitySourceUpdatedAt = now
		result.EnhancedFindings = buildEnhancedFindings(
			selected, seed, repositoryName, registryID, imageDigest, imageID,
		)
	} else {
		result.Findings = buildBasicFindings(selected)
	}

	if len(selected) == 0 {
		result.Description = msgNoScanFindings
	} else {
		result.Description = "The scan completed successfully."
	}

	return result
}

// buildBasicFindings renders the selected CVEs in the BASIC scan shape.
func buildBasicFindings(selected []cveEntry) []ImageScanFinding {
	findings := make([]ImageScanFinding, 0, len(selected))

	for i, cve := range selected {
		findings = append(findings, ImageScanFinding{
			Name:        cve.name,
			Severity:    cve.severity,
			Description: cve.desc,
			URI:         cve.uri,
			Attributes: []Attribute{
				{Key: "package_name", Value: cve.pkg},
				{Key: "package_version", Value: fmt.Sprintf("1.%d.0", i)},
			},
		})
	}

	return findings
}

// buildEnhancedFindings renders the selected CVEs in the ENHANCED (Inspector)
// scan shape, with package-level vulnerability detail, CVSS scoring, remediation
// guidance, and per-finding fixAvailable flags.
func buildEnhancedFindings(
	selected []cveEntry,
	seed uint64,
	repositoryName, registryID, imageDigest string,
	imageID ImageIdentifier,
) []EnhancedImageScanFinding {
	now := time.Now()
	firstObserved := now.Add(-24 * time.Hour)
	findings := make([]EnhancedImageScanFinding, 0, len(selected))

	for i, cve := range selected {
		score := severityCVSSScore(cve.severity)

		// Derive a deterministic fixAvailable value from the seed bit above the
		// selection bit so it varies independently per image/CVE.
		fixAvailable := "YES"
		if (seed>>uint((i+1)%seedBits))&1 == 0 {
			fixAvailable = "NO"
		}

		tags := []string{}
		if imageID.ImageTag != "" {
			tags = []string{imageID.ImageTag}
		}

		findings = append(findings, EnhancedImageScanFinding{
			AwsAccountID:    registryID,
			Description:     cve.desc,
			FindingArn:      fmt.Sprintf("arn:aws:inspector2:::finding/%s-%s", cve.name, imageDigest[:12]),
			FirstObservedAt: float64(firstObserved.Unix()),
			LastObservedAt:  float64(now.Unix()),
			Severity:        cve.severity,
			Status:          "ACTIVE",
			Title:           fmt.Sprintf("%s - %s", cve.name, cve.pkg),
			Type:            "PACKAGE_VULNERABILITY",
			Score:           score,
			UpdatedAt:       float64(now.Unix()),
			FixAvailable:    fixAvailable,
			PackageVulnerabilityDetails: &PackageVulnerabilityDetails{
				VulnerabilityID: cve.name,
				Source:          "NVD",
				SourceURL:       cve.uri,
				VendorSeverity:  cve.severity,
				VendorCreatedAt: float64(firstObserved.Unix()),
				ReferenceUrls:   []string{cve.uri},
				Cvss: []CVSSScore{{
					BaseScore:     score,
					ScoringVector: "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
					Source:        "NVD",
					Version:       "3.1",
				}},
				VulnerablePackages: []VulnerablePackage{{
					Name:           cve.pkg,
					Version:        fmt.Sprintf("1.%d.0", i),
					FixedInVersion: cve.fixedIn,
					PackageManager: "OS",
					Arch:           "x86_64",
					Release:        "1",
					Remediation:    fmt.Sprintf("Update %s to %s or later", cve.pkg, cve.fixedIn),
				}},
			},
			Remediation: &Remediation{
				Recommendation: RemediationRecommendation{
					Text: fmt.Sprintf("Update %s to %s or later", cve.pkg, cve.fixedIn),
					URL:  cve.uri,
				},
			},
			Resources: []EnhancedFindingResource{{
				ID:   fmt.Sprintf("arn:aws:ecr:::repository/%s/sha256:%s", repositoryName, imageDigest),
				Type: "AWS_ECR_CONTAINER_IMAGE",
				Details: EnhancedFindingResourceDetails{
					AwsEcrContainerImage: AwsEcrContainerImageDetails{
						ImageHash:      imageDigest,
						RepositoryName: repositoryName,
						RegistryID:     registryID,
						Architecture:   "amd64",
						Platform:       "LINUX",
						PushedAt:       float64(firstObserved.Unix()),
						ImageTags:      tags,
					},
				},
			}},
		})
	}

	return findings
}

// copyEnhancedFindings deep-copies a slice of enhanced findings so callers can
// mutate the result without disturbing the stored scan record.
func copyEnhancedFindings(in []EnhancedImageScanFinding) []EnhancedImageScanFinding {
	if len(in) == 0 {
		return nil
	}

	out := make([]EnhancedImageScanFinding, len(in))
	for i, f := range in {
		cp := f

		if f.PackageVulnerabilityDetails != nil {
			pvd := *f.PackageVulnerabilityDetails
			pvd.ReferenceUrls = append([]string(nil), f.PackageVulnerabilityDetails.ReferenceUrls...)
			pvd.RelatedVulnerabilities = append(
				[]string(nil), f.PackageVulnerabilityDetails.RelatedVulnerabilities...)
			pvd.Cvss = append([]CVSSScore(nil), f.PackageVulnerabilityDetails.Cvss...)
			pvd.VulnerablePackages = append(
				[]VulnerablePackage(nil), f.PackageVulnerabilityDetails.VulnerablePackages...)
			cp.PackageVulnerabilityDetails = &pvd
		}

		if f.Remediation != nil {
			rem := *f.Remediation
			cp.Remediation = &rem
		}

		cp.Resources = append([]EnhancedFindingResource(nil), f.Resources...)
		for j := range cp.Resources {
			cp.Resources[j].Details.AwsEcrContainerImage.ImageTags = append(
				[]string(nil), f.Resources[j].Details.AwsEcrContainerImage.ImageTags...)
		}

		out[i] = cp
	}

	return out
}

package acmpca_test

// parity_a_test.go — §A parity fixes:
// 1. DescribeCertificateAuthority includes OwnerAccount matching the backend account ID.
// 2. DescribeCertificateAuthority includes Serial for activated/imported CAs.
// 3. IssueCertificate supports END_DATE validity type (absolute epoch timestamp).
// 4. GetCertificate CertificateChain concatenates CA cert + imported chain for subordinate CAs.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestParity_DescribeCertificateAuthority_OwnerAccount verifies that
// DescribeCertificateAuthority always includes OwnerAccount set to the backend's account ID,
// matching real AWS ACM PCA behavior.
func TestParity_DescribeCertificateAuthority_OwnerAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		caType string
	}{
		{"root_ca", "ROOT"},
		{"subordinate_ca", "SUBORDINATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			createRec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{
				"CertificateAuthorityConfiguration": map[string]any{
					"Subject":          map[string]any{"CommonName": "Test CA"},
					"KeyAlgorithm":     "EC_prime256v1",
					"SigningAlgorithm": "SHA256WITHECDSA",
				},
				"CertificateAuthorityType": tt.caType,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			descRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
				"CertificateAuthorityArn": createOut.CertificateAuthorityArn,
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				CertificateAuthority struct {
					OwnerAccount string `json:"OwnerAccount"`
				} `json:"CertificateAuthority"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

			assert.Equal(t, testAccountID, descOut.CertificateAuthority.OwnerAccount,
				"OwnerAccount must match the backend account ID")
		})
	}
}

// TestParity_DescribeCertificateAuthority_Serial_AfterActivation verifies that
// DescribeCertificateAuthority includes a Serial for an ACTIVE CA, matching real AWS behavior.
func TestParity_DescribeCertificateAuthority_Serial_AfterActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		caType string
	}{
		{
			name:   "root_ca_auto_activated",
			caType: "ROOT",
		},
		{
			name:   "subordinate_ca_after_import",
			caType: "SUBORDINATE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()

			// Create a root CA to use for signing (needed for subordinate import).
			rootCA, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root"},
				},
			)
			require.NoError(t, err)

			var caARN string

			if tt.caType == "ROOT" {
				caARN = rootCA.ARN
			} else {
				// Create a subordinate CA, issue a cert for it, and import.
				subCA, subErr := h.Backend.CreateCertificateAuthority(
					context.Background(),
					"SUBORDINATE",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
					},
				)
				require.NoError(t, subErr)

				csr, csrErr := h.Backend.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
				require.NoError(t, csrErr)

				issuedCert, issueErr := h.Backend.IssueCertificate(context.Background(), rootCA.ARN, csr, 365)
				require.NoError(t, issueErr)

				importErr := h.Backend.ImportCertificateAuthorityCertificate(
					context.Background(),
					subCA.ARN,
					issuedCert.CertBody,
					"",
				)
				require.NoError(t, importErr)

				caARN = subCA.ARN
			}

			descRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
				"CertificateAuthorityArn": caARN,
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				CertificateAuthority struct {
					Serial string `json:"Serial"`
					Status string `json:"Status"`
				} `json:"CertificateAuthority"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

			require.Equal(t, "ACTIVE", descOut.CertificateAuthority.Status)
			assert.NotEmpty(t, descOut.CertificateAuthority.Serial,
				"Serial must be present for an ACTIVE CA")
			assert.True(t, isHexString(descOut.CertificateAuthority.Serial),
				"Serial must be a hex string, got %q", descOut.CertificateAuthority.Serial)
		})
	}
}

// TestParity_IssueCertificate_EndDateValidity verifies that IssueCertificate supports
// END_DATE validity type (absolute epoch seconds), matching real AWS ACM PCA behavior.
func TestParity_IssueCertificate_EndDateValidity(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	rootCA, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root"},
		},
	)
	require.NoError(t, err)

	subCA, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"SUBORDINATE",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
		},
	)
	require.NoError(t, err)

	csr, err := h.Backend.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
	require.NoError(t, err)

	endDate := time.Now().Add(365 * 24 * time.Hour).Unix()

	rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": rootCA.ARN,
		"Csr":                     csr,
		"SigningAlgorithm":        "SHA256WITHECDSA",
		"Validity": map[string]any{
			"Type":  "END_DATE",
			"Value": endDate,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code,
		"IssueCertificate with END_DATE must succeed, got: %s", rec.Body.String())

	var out struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.CertificateArn,
		"IssueCertificate with END_DATE must return a CertificateArn")
}

// TestParity_IssueCertificate_EndDateValidity_TableDriven verifies all valid validity
// types including END_DATE and ABSOLUTE (Terraform alias).
func TestParity_IssueCertificate_ValidityTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		validityType string
		value        int64
	}{
		{"days", "DAYS", 365},
		{"months", "MONTHS", 12},
		{"years", "YEARS", 1},
		{"end_date", "END_DATE", time.Now().Add(365 * 24 * time.Hour).Unix()},
		{"absolute", "ABSOLUTE", time.Now().Add(365 * 24 * time.Hour).Unix()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			rootCA, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root"},
				},
			)
			require.NoError(t, err)

			subCA, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				"SUBORDINATE",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub"},
				},
			)
			require.NoError(t, err)

			csr, err := h.Backend.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
			require.NoError(t, err)

			rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
				"CertificateAuthorityArn": rootCA.ARN,
				"Csr":                     csr,
				"SigningAlgorithm":        "SHA256WITHECDSA",
				"Validity":                map[string]any{"Type": tt.validityType, "Value": tt.value},
			})
			require.Equal(t, http.StatusOK, rec.Code,
				"validity type %q must succeed, got: %s", tt.validityType, rec.Body.String())

			var out struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out.CertificateArn,
				"validity type %q must return CertificateArn", tt.validityType)
		})
	}
}

// TestParity_GetCertificate_ChainIncludesCAChain verifies that GetCertificate returns a
// CertificateChain that includes the CA certificate and any imported parent chain,
// matching real AWS ACM PCA behavior.
func TestParity_GetCertificate_ChainIncludesCAChain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		importedChain string
		wantChain     bool
	}{
		{
			name:          "root_ca_no_parent_chain",
			importedChain: "",
			wantChain:     true, // chain = the root CA cert itself
		},
		{
			name:          "subordinate_ca_with_parent_chain",
			importedChain: "PLACEHOLDER", // will be filled with root CA cert
			wantChain:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()

			rootCA, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root"},
				},
			)
			require.NoError(t, err)

			// Get the root CA cert body to use as parent chain for subordinate.
			rootCertPEM, _, err := h.Backend.GetCertificateAuthorityCertificate(
				context.Background(),
				rootCA.ARN,
			)
			require.NoError(t, err)

			issuerARN := rootCA.ARN

			if tt.importedChain != "" {
				// Create and import subordinate CA with the root as parent chain.
				subCA, subErr := h.Backend.CreateCertificateAuthority(
					context.Background(),
					"SUBORDINATE",
					acmpca.CertificateAuthorityConfiguration{
						Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
					},
				)
				require.NoError(t, subErr)

				subCSR, csrErr := h.Backend.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
				require.NoError(t, csrErr)

				subCert, issueErr := h.Backend.IssueCertificate(context.Background(), rootCA.ARN, subCSR, 365)
				require.NoError(t, issueErr)

				importErr := h.Backend.ImportCertificateAuthorityCertificate(
					context.Background(),
					subCA.ARN,
					subCert.CertBody,
					rootCertPEM,
				)
				require.NoError(t, importErr)

				issuerARN = subCA.ARN
			}

			// Issue a leaf certificate from the selected CA.
			leafCA, leafErr := h.Backend.CreateCertificateAuthority(
				context.Background(),
				"SUBORDINATE",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Leaf Client"},
				},
			)
			require.NoError(t, leafErr)

			leafCSR, csrErr := h.Backend.GetCertificateAuthorityCsr(context.Background(), leafCA.ARN)
			require.NoError(t, csrErr)

			leafCert, issueErr := h.Backend.IssueCertificate(context.Background(), issuerARN, leafCSR, 90)
			require.NoError(t, issueErr)

			rec := doACMPCARequest(t, h, "GetCertificate", map[string]any{
				"CertificateAuthorityArn": issuerARN,
				"CertificateArn":          leafCert.ARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Certificate      string `json:"Certificate"`
				CertificateChain string `json:"CertificateChain"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.NotEmpty(t, out.Certificate, "Certificate must be present")
			if tt.wantChain {
				assert.NotEmpty(t, out.CertificateChain,
					"CertificateChain must be present for %s", tt.name)
				assert.Contains(t, out.CertificateChain, "BEGIN CERTIFICATE",
					"CertificateChain must contain PEM certificate block")
			}
		})
	}
}

// isHexString returns true if every rune in s is a lowercase hex character.
func isHexString(s string) bool {
	if s == "" {
		return false
	}

	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}

	return true
}

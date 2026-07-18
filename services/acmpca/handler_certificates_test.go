package acmpca_test

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

// ---- Certificate issuance and revocation ----

func TestACMPCAHandler_IssueCertAndRevoke(t *testing.T) {
	t.Parallel()

	b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(b)

	// Create CA
	ca, err := b.CreateCertificateAuthority(context.Background(), "ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test Issue CA"},
	})
	require.NoError(t, err)

	// Get CSR from backend
	csrPEM, err := b.GetCertificateAuthorityCsr(context.Background(), ca.ARN)
	require.NoError(t, err)

	// IssueCertificate using the actual CSR
	rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"Csr":                     b64(csrPEM),
		"SigningAlgorithm":        "SHA256WITHRSA",
		"Validity":                map[string]any{"Type": "DAYS", "Value": 365},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	certResp := parseACMPCAResponse(t, rec)
	certARN, _ := certResp["CertificateArn"].(string)
	require.NotEmpty(t, certARN)

	// GetCertificate via backend
	cert, err := b.GetCertificate(context.Background(), ca.ARN, certARN)
	require.NoError(t, err)
	assert.NotEmpty(t, cert.ARN)

	// GetCertificate - not found
	rec = doACMPCARequest(t, h, "GetCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"CertificateArn":          "arn:aws:acm-pca:us-east-1:000:certificate-authority/x/certificate/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// RevokeCertificate - cert exists but serial lookup uses backend directly
	issuedCert, err := b.GetCertificate(context.Background(), ca.ARN, certARN)
	require.NoError(t, err)

	rec = doACMPCARequest(t, h, "RevokeCertificate", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
		"CertificateSerial":       issuedCert.Serial,
		"RevocationReason":        "KEY_COMPROMISE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// RevokeCertificate - nonexistent CA
	rec = doACMPCARequest(t, h, "RevokeCertificate", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"CertificateSerial":       issuedCert.Serial,
		"RevocationReason":        "KEY_COMPROMISE",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// TestACMPCA_RevokeCertificate_DeletedCA verifies that revoking
// a certificate from a DELETED CA returns InvalidStateException.
func TestACMPCA_RevokeCertificate_DeletedCA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		wantCode int
		deleted  bool
	}{
		{
			name:     "active CA allows revocation",
			deleted:  false,
			wantCode: http.StatusOK,
		},
		{
			name:     "deleted CA rejects revocation",
			deleted:  true,
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			h := acmpca.NewHandler(b)

			ca, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
				},
			)
			require.NoError(t, err)

			subCA, err := b.CreateCertificateAuthority(
				context.Background(),
				"SUBORDINATE",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
				},
			)
			require.NoError(t, err)

			csrPEM, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
			require.NoError(t, err)

			issuedCert, err := b.IssueCertificate(context.Background(), ca.ARN, csrPEM, 365)
			require.NoError(t, err)

			if tt.deleted {
				require.NoError(t, b.UpdateCertificateAuthority(context.Background(), ca.ARN, "DISABLED"))
				require.NoError(t, b.DeleteCertificateAuthority(context.Background(), ca.ARN, 0))
			}

			rec := doACMPCARequest(t, h, "RevokeCertificate", map[string]any{
				"CertificateAuthorityArn": ca.ARN,
				"CertificateSerial":       issuedCert.Serial,
				"RevocationReason":        "KEY_COMPROMISE",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				resp := parseACMPCAResponse(t, rec)
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

// TestACMPCA_IssueCertificate_ValidityTypes verifies all supported
// validity types: DAYS, MONTHS, YEARS.
func TestACMPCA_IssueCertificate_ValidityTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		validityType  string
		validityValue int
		wantCode      int
	}{
		{validityType: "DAYS", validityValue: 90, wantCode: http.StatusOK},
		{validityType: "MONTHS", validityValue: 6, wantCode: http.StatusOK},
		{validityType: "YEARS", validityValue: 2, wantCode: http.StatusOK},
		{validityType: "INVALID_TYPE", validityValue: 1, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.validityType, func(t *testing.T) {
			t.Parallel()

			b := acmpca.NewInMemoryBackend(testAccountID, testRegion)
			h := acmpca.NewHandler(b)

			rootCA, err := b.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root CA"},
				},
			)
			require.NoError(t, err)

			subCA, err := b.CreateCertificateAuthority(
				context.Background(),
				"SUBORDINATE",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
				},
			)
			require.NoError(t, err)

			csrPEM, err := b.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
			require.NoError(t, err)

			rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
				"CertificateAuthorityArn": rootCA.ARN,
				"Csr":                     b64(csrPEM),
				"SigningAlgorithm":        "SHA256WITHECDSA",
				"Validity":                map[string]any{"Type": tt.validityType, "Value": tt.validityValue},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := parseACMPCAResponse(t, rec)
				assert.NotEmpty(t, resp["CertificateArn"])
			}
		})
	}
}

// TestACMPCA_IssueCertificate_EndDateValidity verifies that IssueCertificate supports
// END_DATE validity type (absolute epoch seconds), matching real AWS ACM PCA behavior.
func TestACMPCA_IssueCertificate_EndDateValidity(t *testing.T) {
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
		"Csr":                     b64(csr),
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

// TestACMPCA_IssueCertificate_ValidityTypeAliases verifies all valid validity
// types including END_DATE and ABSOLUTE (Terraform alias).
func TestACMPCA_IssueCertificate_ValidityTypeAliases(t *testing.T) {
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
				"Csr":                     b64(csr),
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

// TestACMPCA_GetCertificate_ChainIncludesCAChain verifies that GetCertificate returns a
// CertificateChain that includes the CA certificate and any imported parent chain,
// matching real AWS ACM PCA behavior.
func TestACMPCA_GetCertificate_ChainIncludesCAChain(t *testing.T) {
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

// TestACMPCAHandler_IssueCertificateMonthsValidity verifies IssueCertificate
// with a MONTHS validity period via the handler dispatch path.
func TestACMPCAHandler_IssueCertificateMonthsValidity(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

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

	rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Csr":                     b64(csr),
		"SigningAlgorithm":        "SHA256WITHECDSA",
		"Validity":                map[string]any{"Type": "MONTHS", "Value": 6},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseACMPCAResponse(t, rec)
	assert.NotEmpty(t, resp["CertificateArn"])
}

// TestACMPCAHandler_GetCertificateReturnsChain verifies GetCertificate returns
// both Certificate and CertificateChain via the handler dispatch path.
func TestACMPCAHandler_GetCertificateReturnsChain(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

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

	cert, err := h.Backend.IssueCertificate(context.Background(), caARN, csr, 365)
	require.NoError(t, err)

	rec := doACMPCARequest(t, h, "GetCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"CertificateArn":          cert.ARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseACMPCAResponse(t, rec)
	assert.NotEmpty(t, resp["Certificate"])
	assert.NotEmpty(t, resp["CertificateChain"])
}

// TestACMPCAHandler_IssueCertificateBase64Csr verifies that IssueCertificate.Csr
// is decoded as base64, matching the wire format aws-sdk-go-v2 produces for its
// []byte Go type (see serializers.go Base64EncodeBytes calls upstream). A raw
// (non-base64) PEM string must be rejected, and a base64-encoded PEM must be
// accepted.
func TestACMPCAHandler_IssueCertificateBase64Csr(t *testing.T) {
	t.Parallel()

	t.Run("rejects raw (non-base64) Csr", func(t *testing.T) {
		t.Parallel()

		h := newACMPCAHandler()
		caARN := createHandlerCA(t, h)

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

		rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
			"CertificateAuthorityArn": caARN,
			"Csr":                     csr, // raw PEM, NOT base64-encoded
			"SigningAlgorithm":        "SHA256WITHECDSA",
			"Validity":                map[string]any{"Type": "DAYS", "Value": 365},
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)
		resp := parseACMPCAResponse(t, rec)
		assert.Equal(t, "InvalidParameterException", resp["__type"])
	})

	t.Run("accepts base64-encoded Csr", func(t *testing.T) {
		t.Parallel()

		h := newACMPCAHandler()
		caARN := createHandlerCA(t, h)

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

		rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
			"CertificateAuthorityArn": caARN,
			"Csr":                     b64(csr),
			"SigningAlgorithm":        "SHA256WITHECDSA",
			"Validity":                map[string]any{"Type": "DAYS", "Value": 365},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		resp := parseACMPCAResponse(t, rec)
		assert.NotEmpty(t, resp["CertificateArn"])
	})
}

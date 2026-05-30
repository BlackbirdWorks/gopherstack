package acmpca_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestACMPCA_Accuracy_SubordinateCA_StartsAsPendingCertificate verifies that
// SUBORDINATE CAs are created with PENDING_CERTIFICATE status, matching AWS
// behaviour. ROOT CAs are auto-activated (intentional deviation for Terraform).
func TestACMPCA_Accuracy_SubordinateCA_StartsAsPendingCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		caType     string
		wantStatus string
	}{
		{caType: "ROOT", wantStatus: "ACTIVE"},
		{caType: "SUBORDINATE", wantStatus: "PENDING_CERTIFICATE"},
	}

	for _, tt := range tests {
		t.Run(tt.caType, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{
				"CertificateAuthorityConfiguration": map[string]any{
					"Subject":          map[string]any{"CommonName": "Test CA"},
					"KeyAlgorithm":     "EC_prime256v1",
					"SigningAlgorithm": "SHA256WITHECDSA",
				},
				"CertificateAuthorityType": tt.caType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseACMPCAResponse(t, rec)
			caARN, _ := resp["CertificateAuthorityArn"].(string)
			require.NotEmpty(t, caARN)

			// Describe to verify status.
			descRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
				"CertificateAuthorityArn": caARN,
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			descResp := parseACMPCAResponse(t, descRec)
			ca, _ := descResp["CertificateAuthority"].(map[string]any)
			assert.Equal(t, tt.wantStatus, ca["Status"])
		})
	}
}

// TestACMPCA_Accuracy_GetCertificateAuthorityCertificate_NoCert verifies that
// a SUBORDINATE CA with no imported certificate returns ResourceNotFoundException,
// matching AWS behaviour.
func TestACMPCA_Accuracy_GetCertificateAuthorityCertificate_NoCert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		caType   string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "subordinate CA with no cert returns not found",
			caType:   "SUBORDINATE",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
		{
			name:     "root CA auto-signed has cert",
			caType:   "ROOT",
			wantCode: http.StatusOK,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			ca, err := h.Backend.CreateCertificateAuthority(tt.caType, acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
			})
			require.NoError(t, err)

			rec := doACMPCARequest(t, h, "GetCertificateAuthorityCertificate", map[string]any{
				"CertificateAuthorityArn": ca.ARN,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErr {
				resp := parseACMPCAResponse(t, rec)
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
			}
		})
	}
}

// TestACMPCA_Accuracy_GetCertificateAuthorityCertificate_AfterImport verifies
// that after importing a certificate the CA cert round-trips correctly.
func TestACMPCA_Accuracy_GetCertificateAuthorityCertificate_AfterImport(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Use a ROOT CA (auto-signed, ACTIVE) to issue a cert for a SUBORDINATE CA.
	rootCA, err := h.Backend.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root CA"},
	})
	require.NoError(t, err)

	subCA, err := h.Backend.CreateCertificateAuthority("SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
	})
	require.NoError(t, err)
	assert.Equal(t, "PENDING_CERTIFICATE", subCA.Status)

	// Subordinate CA has no cert yet.
	noCertRec := doACMPCARequest(t, h, "GetCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": subCA.ARN,
	})
	assert.Equal(t, http.StatusBadRequest, noCertRec.Code)

	// Issue a cert for the subordinate CA from the root CA.
	csrPEM, err := h.Backend.GetCertificateAuthorityCsr(subCA.ARN)
	require.NoError(t, err)

	issuedCert, err := h.Backend.IssueCertificate(rootCA.ARN, csrPEM, 365)
	require.NoError(t, err)

	// Get the cert PEM from the issued cert.
	gotCert, err := h.Backend.GetCertificate(rootCA.ARN, issuedCert.ARN)
	require.NoError(t, err)

	// Import the cert to activate the subordinate CA.
	// Note: the mock does not enforce IsCA=true on imported certs; we import an
	// end-entity cert here to keep the test self-contained.
	importRec := doACMPCARequest(t, h, "ImportCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": subCA.ARN,
		"Certificate":             gotCert.CertBody,
	})
	require.Equal(t, http.StatusOK, importRec.Code)

	// Now GetCertificateAuthorityCertificate should succeed.
	getCertRec := doACMPCARequest(t, h, "GetCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": subCA.ARN,
	})
	require.Equal(t, http.StatusOK, getCertRec.Code)

	getCertResp := parseACMPCAResponse(t, getCertRec)
	assert.NotEmpty(t, getCertResp["Certificate"])
}

// TestACMPCA_Accuracy_RevokeCertificate_DeletedCA verifies that revoking
// a certificate from a DELETED CA returns InvalidStateException.
func TestACMPCA_Accuracy_RevokeCertificate_DeletedCA(t *testing.T) {
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

			ca, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
			})
			require.NoError(t, err)

			subCA, err := b.CreateCertificateAuthority("SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
			})
			require.NoError(t, err)

			csrPEM, err := b.GetCertificateAuthorityCsr(subCA.ARN)
			require.NoError(t, err)

			issuedCert, err := b.IssueCertificate(ca.ARN, csrPEM, 365)
			require.NoError(t, err)

			if tt.deleted {
				require.NoError(t, b.UpdateCertificateAuthority(ca.ARN, "DISABLED"))
				require.NoError(t, b.DeleteCertificateAuthority(ca.ARN, 0))
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

// TestACMPCA_Accuracy_CreateAuditReport_RequiresActiveCA verifies that
// CreateCertificateAuthorityAuditReport requires an ACTIVE CA.
func TestACMPCA_Accuracy_CreateAuditReport_RequiresActiveCA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		caType   string
		wantType string
		wantCode int
	}{
		{
			name:     "root CA (ACTIVE) succeeds",
			caType:   "ROOT",
			wantCode: http.StatusOK,
		},
		{
			name:     "subordinate CA (PENDING_CERTIFICATE) fails",
			caType:   "SUBORDINATE",
			wantCode: http.StatusBadRequest,
			wantType: "InvalidStateException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			ca, err := h.Backend.CreateCertificateAuthority(tt.caType, acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
			})
			require.NoError(t, err)

			rec := doACMPCARequest(t, h, "CreateCertificateAuthorityAuditReport", map[string]any{
				"AuditReportResponseFormat": "JSON",
				"CertificateAuthorityArn":   ca.ARN,
				"S3BucketName":              "audit-bucket",
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				resp := parseACMPCAResponse(t, rec)
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

// TestACMPCA_Accuracy_IssueCertificate_ValidityTypes verifies all supported
// validity types: DAYS, MONTHS, YEARS.
func TestACMPCA_Accuracy_IssueCertificate_ValidityTypes(t *testing.T) {
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

			rootCA, err := b.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root CA"},
			})
			require.NoError(t, err)

			subCA, err := b.CreateCertificateAuthority("SUBORDINATE", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
			})
			require.NoError(t, err)

			csrPEM, err := b.GetCertificateAuthorityCsr(subCA.ARN)
			require.NoError(t, err)

			rec := doACMPCARequest(t, h, "IssueCertificate", map[string]any{
				"CertificateAuthorityArn": rootCA.ARN,
				"Csr":                     csrPEM,
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

// TestACMPCA_Accuracy_DeleteCA_StateMachine verifies the CA deletion state
// machine: only DISABLED and PENDING_CERTIFICATE CAs can be deleted.
func TestACMPCA_Accuracy_DeleteCA_StateMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		caType      string
		wantErrType string
		wantCode    int
		disableCA   bool
	}{
		{
			name:        "ACTIVE CA cannot be deleted",
			caType:      "ROOT",
			disableCA:   false,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidStateException",
		},
		{
			name:      "DISABLED CA can be deleted",
			caType:    "ROOT",
			disableCA: true,
			wantCode:  http.StatusOK,
		},
		{
			name:     "PENDING_CERTIFICATE CA can be deleted",
			caType:   "SUBORDINATE",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			ca, err := h.Backend.CreateCertificateAuthority(tt.caType, acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
			})
			require.NoError(t, err)

			if tt.disableCA {
				require.NoError(t, h.Backend.UpdateCertificateAuthority(ca.ARN, "DISABLED"))
			}

			rec := doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
				"CertificateAuthorityArn": ca.ARN,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				resp := parseACMPCAResponse(t, rec)
				assert.Equal(t, tt.wantErrType, resp["__type"])
			}
		})
	}
}

// TestACMPCA_Accuracy_RestoreCA_AfterDelete verifies that RestoreCertificateAuthority
// on a deleted CA restores it to DISABLED state, not ACTIVE.
func TestACMPCA_Accuracy_RestoreCA_AfterDelete(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	ca, err := h.Backend.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Restore CA"},
	})
	require.NoError(t, err)

	require.NoError(t, h.Backend.UpdateCertificateAuthority(ca.ARN, "DISABLED"))
	require.NoError(t, h.Backend.DeleteCertificateAuthority(ca.ARN, 0))

	rec := doACMPCARequest(t, h, "RestoreCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	descResp := parseACMPCAResponse(t, descRec)
	caOut, _ := descResp["CertificateAuthority"].(map[string]any)
	assert.Equal(t, "DISABLED", caOut["Status"])
}

// TestACMPCA_Accuracy_PermanentDeletionTimeInDays verifies the 7-30 day range
// enforcement for PermanentDeletionTimeInDays.
func TestACMPCA_Accuracy_PermanentDeletionTimeInDays(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantType string
		days     int
		wantCode int
	}{
		{name: "0 days (omitted) allowed", days: 0, wantCode: http.StatusOK},
		{name: "7 days (min) allowed", days: 7, wantCode: http.StatusOK},
		{name: "30 days (max) allowed", days: 30, wantCode: http.StatusOK},
		{
			name:     "5 days (below min) rejected",
			days:     5,
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name:     "31 days (above max) rejected",
			days:     31,
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			ca, err := h.Backend.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
			})
			require.NoError(t, err)

			require.NoError(t, h.Backend.UpdateCertificateAuthority(ca.ARN, "DISABLED"))

			rec := doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
				"CertificateAuthorityArn":     ca.ARN,
				"PermanentDeletionTimeInDays": tt.days,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				resp := parseACMPCAResponse(t, rec)
				assert.Equal(t, tt.wantType, resp["__type"])
			}
		})
	}
}

// TestACMPCA_Accuracy_ListCertificateAuthorities_Pagination verifies that
// MaxResults and NextToken pagination works correctly.
func TestACMPCA_Accuracy_ListCertificateAuthorities_Pagination(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Create 3 CAs.
	for range 3 {
		_, err := h.Backend.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
		})
		require.NoError(t, err)
	}

	// Page 1: MaxResults=2.
	rec := doACMPCARequest(t, h, "ListCertificateAuthorities", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	cas, _ := resp["CertificateAuthorities"].([]any)
	assert.Len(t, cas, 2)
	nextToken, _ := resp["NextToken"].(string)
	require.NotEmpty(t, nextToken, "NextToken should be present when there are more results")

	// Page 2: use NextToken.
	rec2 := doACMPCARequest(t, h, "ListCertificateAuthorities", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	resp2 := parseACMPCAResponse(t, rec2)
	cas2, _ := resp2["CertificateAuthorities"].([]any)
	assert.Len(t, cas2, 1)
	assert.Empty(t, resp2["NextToken"], "NextToken should be absent on last page")
}

// TestACMPCA_Accuracy_CreateCertificateAuthority_WithTags verifies that tags
// provided at creation time are stored and retrievable via ListTags.
func TestACMPCA_Accuracy_CreateCertificateAuthority_WithTags(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityConfiguration": map[string]any{
			"Subject":          map[string]any{"CommonName": "Tagged CA"},
			"KeyAlgorithm":     "EC_prime256v1",
			"SigningAlgorithm": "SHA256WITHECDSA",
		},
		"CertificateAuthorityType": "ROOT",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "security"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	caARN, _ := resp["CertificateAuthorityArn"].(string)
	require.NotEmpty(t, caARN)

	tagsRec := doACMPCARequest(t, h, "ListTags", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, tagsRec.Code)

	tagsResp := parseACMPCAResponse(t, tagsRec)
	tags, _ := tagsResp["Tags"].([]any)
	assert.Len(t, tags, 2)
}

// TestACMPCA_Accuracy_ListTagsForCertificateAuthority verifies that the
// ListTagsForCertificateAuthority alias works identically to ListTags.
func TestACMPCA_Accuracy_ListTagsForCertificateAuthority(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	h.SetTagsForTest(caARN, map[string]string{"k1": "v1"})

	for _, op := range []string{"ListTags", "ListTagsForCertificateAuthority"} {
		rec := doACMPCARequest(t, h, op, map[string]any{
			"CertificateAuthorityArn": caARN,
		})
		require.Equal(t, http.StatusOK, rec.Code, "op=%s", op)
		resp := parseACMPCAResponse(t, rec)
		tags, _ := resp["Tags"].([]any)
		assert.Len(t, tags, 1, "op=%s", op)
	}
}

// TestACMPCA_Accuracy_DescribeCA_ARNFormat verifies that created CA ARNs use
// the correct acm-pca service prefix and region/account structure.
func TestACMPCA_Accuracy_DescribeCA_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	assert.Contains(t, caARN, "arn:aws:acm-pca:")
	assert.Contains(t, caARN, testRegion)
	assert.Contains(t, caARN, testAccountID)
	assert.Contains(t, caARN, "certificate-authority/")
}

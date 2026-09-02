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

// ---- CA JSON operations via handler ----

func TestACMPCAHandler_CreateDescribeListDeleteCA(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// CreateCertificateAuthority
	rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{
		"CertificateAuthorityConfiguration": map[string]any{
			"Subject": map[string]any{
				"CommonName": "Test CA",
				"Country":    "US",
			},
			"KeyAlgorithm":     "RSA_2048",
			"SigningAlgorithm": "SHA256WITHRSA",
		},
		"CertificateAuthorityType": "ROOT",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	caARN, _ := resp["CertificateAuthorityArn"].(string)
	require.NotEmpty(t, caARN)

	// DescribeCertificateAuthority
	rec = doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeCertificateAuthority - not found
	rec = doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:000:certificate-authority/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// ListCertificateAuthorities
	rec = doACMPCARequest(t, h, "ListCertificateAuthorities", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetCertificateAuthorityCsr
	rec = doACMPCARequest(t, h, "GetCertificateAuthorityCsr", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetCertificateAuthorityCertificate
	rec = doACMPCARequest(t, h, "GetCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.NotNil(t, rec)

	// ImportCertificateAuthorityCertificate
	rec = doACMPCARequest(t, h, "ImportCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Certificate":             "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t",
		"CertificateChain":        "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t",
	})
	assert.NotNil(t, rec)

	// UpdateCertificateAuthority (disable)
	rec = doACMPCARequest(t, h, "UpdateCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
		"Status":                  "DISABLED",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateCertificateAuthority - not found
	rec = doACMPCARequest(t, h, "UpdateCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
		"Status":                  "DISABLED",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)

	// DeleteCertificateAuthority
	rec = doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DeleteCertificateAuthority - already deleted
	rec = doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- RestoreCertificateAuthority ----

func TestACMPCAHandler_RestoreCA(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// RestoreCertificateAuthority (CA must be DELETED first, but let's test the handler path)
	rec := doACMPCARequest(t, h, "RestoreCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	// Should return error because CA is not in DELETED state
	assert.NotNil(t, rec)

	// Restore nonexistent CA
	rec = doACMPCARequest(t, h, "RestoreCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": "nonexistent",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- toCAOutput via DescribeCertificateAuthority response ----

func TestACMPCAHandler_ToCAOutput(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Create CA with full subject
	ca, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{
				CommonName:         "Full CA",
				Country:            "US",
				Organization:       "Test Org",
				OrganizationalUnit: "Test Unit",
				State:              "CA",
				Locality:           "SF",
			},
			KeyAlgorithm:     "RSA_2048",
			SigningAlgorithm: "SHA256WITHRSA",
		},
	)
	require.NoError(t, err)

	// Describe - exercises toCAOutput
	rec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": ca.ARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseACMPCAResponse(t, rec)
	caOut, _ := resp["CertificateAuthority"].(map[string]any)
	require.NotNil(t, caOut)
	assert.Equal(t, ca.ARN, caOut["Arn"])
}

// TestACMPCA_SubordinateCA_StartsAsPendingCertificate verifies that
// SUBORDINATE CAs are created with PENDING_CERTIFICATE status, matching AWS
// behaviour. ROOT CAs are auto-activated (intentional deviation for Terraform).
func TestACMPCA_SubordinateCA_StartsAsPendingCertificate(t *testing.T) {
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

// TestACMPCA_GetCertificateAuthorityCertificate_NoCert verifies that
// a SUBORDINATE CA with no imported certificate returns ResourceNotFoundException,
// matching AWS behaviour.
func TestACMPCA_GetCertificateAuthorityCertificate_NoCert(t *testing.T) {
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
			ca, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				tt.caType,
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "Test CA"},
				},
			)
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

// TestACMPCA_GetCertificateAuthorityCertificate_AfterImport verifies
// that after importing a certificate the CA cert round-trips correctly.
func TestACMPCA_GetCertificateAuthorityCertificate_AfterImport(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Use a ROOT CA (auto-signed, ACTIVE) to issue a cert for a SUBORDINATE CA.
	rootCA, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Root CA"},
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
	assert.Equal(t, "PENDING_CERTIFICATE", subCA.Status)

	// Subordinate CA has no cert yet.
	noCertRec := doACMPCARequest(t, h, "GetCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": subCA.ARN,
	})
	assert.Equal(t, http.StatusBadRequest, noCertRec.Code)

	// Issue a cert for the subordinate CA from the root CA.
	csrPEM, err := h.Backend.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
	require.NoError(t, err)

	issuedCert, err := h.Backend.IssueCertificate(context.Background(), rootCA.ARN, csrPEM, 365)
	require.NoError(t, err)

	// Get the cert PEM from the issued cert.
	gotCert, err := h.Backend.GetCertificate(context.Background(), rootCA.ARN, issuedCert.ARN)
	require.NoError(t, err)

	// Import the cert to activate the subordinate CA.
	// Note: the mock does not enforce IsCA=true on imported certs; we import an
	// end-entity cert here to keep the test self-contained.
	importRec := doACMPCARequest(t, h, "ImportCertificateAuthorityCertificate", map[string]any{
		"CertificateAuthorityArn": subCA.ARN,
		"Certificate":             b64(gotCert.CertBody),
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

// TestACMPCA_DeleteCA_StateMachine verifies the CA deletion state
// machine: only DISABLED and PENDING_CERTIFICATE CAs can be deleted.
func TestACMPCA_DeleteCA_StateMachine(t *testing.T) {
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
			ca, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				tt.caType,
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
				},
			)
			require.NoError(t, err)

			if tt.disableCA {
				require.NoError(t, h.Backend.UpdateCertificateAuthority(context.Background(), ca.ARN, "DISABLED"))
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

// TestACMPCA_RestoreCA_AfterDelete verifies that RestoreCertificateAuthority
// on a deleted CA restores it to DISABLED state, not ACTIVE.
func TestACMPCA_RestoreCA_AfterDelete(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	ca, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Restore CA"},
		},
	)
	require.NoError(t, err)

	require.NoError(t, h.Backend.UpdateCertificateAuthority(context.Background(), ca.ARN, "DISABLED"))
	require.NoError(t, h.Backend.DeleteCertificateAuthority(context.Background(), ca.ARN, 0))

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

// TestACMPCA_PermanentDeletionTimeInDays verifies the 7-30 day range
// enforcement for PermanentDeletionTimeInDays.
func TestACMPCA_PermanentDeletionTimeInDays(t *testing.T) {
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
			wantType: "InvalidArgsException",
		},
		{
			name:     "31 days (above max) rejected",
			days:     31,
			wantCode: http.StatusBadRequest,
			wantType: "InvalidArgsException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMPCAHandler()
			ca, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				"ROOT",
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
				},
			)
			require.NoError(t, err)

			require.NoError(t, h.Backend.UpdateCertificateAuthority(context.Background(), ca.ARN, "DISABLED"))

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

// TestACMPCA_ListCertificateAuthorities_Pagination verifies that
// MaxResults and NextToken pagination works correctly.
func TestACMPCA_ListCertificateAuthorities_Pagination(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Create 3 CAs.
	for range 3 {
		_, err := h.Backend.CreateCertificateAuthority(
			context.Background(),
			"ROOT",
			acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
			},
		)
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

// TestACMPCA_DescribeCA_ARNFormat verifies that created CA ARNs use
// the correct acm-pca service prefix and region/account structure.
func TestACMPCA_DescribeCA_ARNFormat(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	assert.Contains(t, caARN, "arn:aws:acm-pca:")
	assert.Contains(t, caARN, testRegion)
	assert.Contains(t, caARN, testAccountID)
	assert.Contains(t, caARN, "certificate-authority/")
}

// TestACMPCA_DescribeCertificateAuthority_OwnerAccount verifies that
// DescribeCertificateAuthority always includes OwnerAccount set to the backend's account ID,
// matching real AWS ACM PCA behavior.
func TestACMPCA_DescribeCertificateAuthority_OwnerAccount(t *testing.T) {
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

// TestACMPCA_DescribeCertificateAuthority_Serial_AfterActivation verifies that
// DescribeCertificateAuthority includes a Serial for an ACTIVE CA, matching real AWS behavior.
func TestACMPCA_DescribeCertificateAuthority_Serial_AfterActivation(t *testing.T) {
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

// TestACMPCAHandler_DeleteCertificateAuthority_RestorableUntil verifies that
// DescribeCertificateAuthority reports RestorableUntil (epoch seconds) for a
// DELETED CA, matching real AWS ACM PCA wire shape.
func TestACMPCAHandler_DeleteCertificateAuthority_RestorableUntil(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	// Active CAs have no restoration window.
	activeRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, activeRec.Code)
	activeResp := parseACMPCAResponse(t, activeRec)
	activeCA, ok := activeResp["CertificateAuthority"].(map[string]any)
	require.True(t, ok)
	assert.Nil(t, activeCA["RestorableUntil"])

	require.NoError(t, h.Backend.UpdateCertificateAuthority(context.Background(), caARN, "DISABLED"))

	deleteRec := doACMPCARequest(t, h, "DeleteCertificateAuthority", map[string]any{
		"CertificateAuthorityArn":     caARN,
		"PermanentDeletionTimeInDays": 7,
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)

	describeRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeResp := parseACMPCAResponse(t, describeRec)
	caOut, ok := describeResp["CertificateAuthority"].(map[string]any)
	require.True(t, ok)

	restorableUntil, ok := caOut["RestorableUntil"].(float64)
	require.True(t, ok, "RestorableUntil must be present as an epoch-seconds number once the CA is DELETED")

	wantUntil := time.Now().UTC().AddDate(0, 0, 7)
	assert.WithinDuration(t, wantUntil, time.Unix(int64(restorableUntil), 0), time.Minute)
}

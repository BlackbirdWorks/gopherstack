package acm_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// ── Batch-2 Issue #1: ErrNotEligible mapped to "RequestError" instead of "RequestInProgressException" ───
//
// ExportCertificate on an AMAZON_ISSUED cert and RenewCertificate on IMPORTED/PRIVATE certs
// previously returned __type="RequestError", which is not a valid ACM error code.
// AWS returns RequestInProgressException for these cases.

func TestBatch2_ExportCertificate_AmazonIssued_Returns_RequestInProgressException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "amazon_issued_not_exportable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"export-noteligible.example.com"}`)
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			body, _ := json.Marshal(map[string]string{
				"CertificateArn": reqOut.CertificateArn,
				"Passphrase":     "dGVzdA==",
			})
			rec := postACMJSON(t, h, "ExportCertificate", string(body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "RequestInProgressException", errResp.Type,
				"ExportCertificate on AMAZON_ISSUED cert must return RequestInProgressException, not RequestError")
		})
	}
}

func TestBatch2_RenewCertificate_Imported_Returns_RequestInProgressException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		certType string
	}{
		{name: "imported_cert_not_renewable", certType: "imported"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			src, err := b.RequestCertificate("renew-noteligible.example.com", "", "", "", "", "", "", nil)
			require.NoError(t, err)

			imported, err := b.ImportCertificate(src.CertificateBody, src.PrivateKey, "", "")
			require.NoError(t, err)

			h := acm.NewHandler(b)
			body, _ := json.Marshal(map[string]string{"CertificateArn": imported.ARN})
			rec := postACMJSON(t, h, "RenewCertificate", string(body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "RequestInProgressException", errResp.Type,
				"RenewCertificate on IMPORTED cert must return RequestInProgressException, not RequestError")
		})
	}
}

// ── Batch-2 Issue #2: GetCertificate on PENDING/FAILED/TIMED_OUT returns InvalidStateException ───
//
// AWS GetCertificate returns RequestInProgressException when the certificate is not yet issued
// (PENDING_VALIDATION) or is in a terminal non-ready state (VALIDATION_TIMED_OUT, FAILED).
// Previously GetCertificate returned InvalidStateException for these states.

func TestBatch2_GetCertificate_NonIssuedStates_Returns_RequestInProgressException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *acm.InMemoryBackend) string
		name  string
	}{
		{
			name: "pending_validation",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("get-pending.example.com", "", "DNS", "", "", "", "", nil)
				require.NoError(t, err)
				require.Equal(t, "PENDING_VALIDATION", cert.Status)

				return cert.ARN
			},
		},
		{
			name: "validation_timed_out",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("get-timedout.example.com", "", "DNS", "", "", "", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.TimeoutPendingValidation(cert.ARN))

				return cert.ARN
			},
		},
		{
			name: "failed",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("get-failed.example.com", "", "EMAIL", "", "", "", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.FailCertificate(cert.ARN, "CAA_ERROR"))

				return cert.ARN
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			certARN := tt.setup(t, b)

			h := acm.NewHandler(b)
			body, _ := json.Marshal(map[string]string{"CertificateArn": certARN})
			rec := postACMJSON(t, h, "GetCertificate", string(body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "RequestInProgressException", errResp.Type,
				"GetCertificate on non-issued cert (%s) must return RequestInProgressException, not InvalidStateException",
				tt.name)
		})
	}
}

// ── Batch-2 Issue #3: RevokeCertificate on PENDING_VALIDATION returns ValidationException ───
//
// AWS RevokeCertificate returns InvalidStateException when the certificate is not in
// a revocable state. Previously PENDING_VALIDATION certs returned ValidationException.

func TestBatch2_RevokeCertificate_PendingValidation_Returns_InvalidStateException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dns_validation_pending"},
		{name: "email_validation_pending"},
	}

	methods := map[string]string{
		"dns_validation_pending":   "DNS",
		"email_validation_pending": "EMAIL",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			method := methods[tt.name]
			body, _ := json.Marshal(map[string]string{
				"DomainName":       "revoke-pending.example.com",
				"ValidationMethod": method,
			})
			reqRec := postACMJSON(t, h, "RequestCertificate", string(body))
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			// Describe to check status; skip if auto-validated.
			descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
			descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				Certificate struct {
					Status string `json:"Status"`
				} `json:"Certificate"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

			if descOut.Certificate.Status != "PENDING_VALIDATION" {
				t.Skip("cert auto-validated before test could run")
			}

			revokeBody, _ := json.Marshal(map[string]string{
				"CertificateArn":   reqOut.CertificateArn,
				"RevocationReason": "UNSPECIFIED",
			})
			rec := postACMJSON(t, h, "RevokeCertificate", string(revokeBody))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidStateException", errResp.Type,
				"RevokeCertificate on PENDING_VALIDATION cert must return InvalidStateException, not ValidationException")
		})
	}
}

// ── Batch-2 Issue #4: UpdateCertificateOptions on non-ISSUED cert returns ValidationException ───
//
// AWS UpdateCertificateOptions returns InvalidStateException when the certificate is not in
// ISSUED state. Previously all non-ISSUED states returned ValidationException.

func TestBatch2_UpdateCertificateOptions_NonIssued_Returns_InvalidStateException(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *acm.InMemoryBackend) string
		name  string
	}{
		{
			name: "revoked_cert",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("update-opts-revoked.example.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)

				err = b.RevokeCertificate(cert.ARN, "UNSPECIFIED")
				require.NoError(t, err)

				return cert.ARN
			},
		},
		{
			name: "expired_cert",
			setup: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("update-opts-expired.example.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)

				err = b.ExpireCertificate(cert.ARN)
				require.NoError(t, err)

				return cert.ARN
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			certARN := tt.setup(t, b)

			h := acm.NewHandler(b)
			body, _ := json.Marshal(map[string]any{
				"CertificateArn": certARN,
				"Options": map[string]string{
					"CertificateTransparencyLoggingPreference": "ENABLED",
				},
			})
			rec := postACMJSON(t, h, "UpdateCertificateOptions", string(body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidStateException", errResp.Type,
				"UpdateCertificateOptions on non-ISSUED cert (%s) must return InvalidStateException, not ValidationException",
				tt.name)
		})
	}
}

// TestBatch2_GetCertificate_Issued_Succeeds verifies the success path still works after fixes.
func TestBatch2_GetCertificate_Issued_Succeeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "amazon_issued_cert"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"get-issued.example.com"}`)
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			// Wait for ISSUED status (immediate for no-validation certs)
			require.Eventually(t, func() bool {
				rec := postACMJSON(t, h, "DescribeCertificate",
					`{"CertificateArn":"`+reqOut.CertificateArn+`"}`)
				var out struct {
					Certificate struct{ Status string } `json:"Certificate"`
				}
				_ = json.Unmarshal(rec.Body.Bytes(), &out)

				return out.Certificate.Status == "ISSUED"
			}, 2*time.Second, 20*time.Millisecond)

			body, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
			rec := postACMJSON(t, h, "GetCertificate", string(body))
			assert.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Certificate string `json:"Certificate"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out.Certificate, "BEGIN CERTIFICATE")
		})
	}
}

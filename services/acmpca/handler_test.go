package acmpca_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// b64 base64-encodes s the way aws-sdk-go-v2 encodes []byte-typed blob
// fields (IssueCertificateInput.Csr, ImportCertificateAuthorityCertificateInput.
// Certificate/CertificateChain) before putting them on the wire.
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func newACMPCAHandler() *acmpca.Handler {
	return acmpca.NewHandler(acmpca.NewInMemoryBackend(testAccountID, testRegion))
}

func doACMPCARequest(t *testing.T, h *acmpca.Handler, op string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ACMPrivateCA."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseACMPCAResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func createHandlerCA(t *testing.T, h *acmpca.Handler) string {
	t.Helper()

	ca, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Handler CA"},
		},
	)
	require.NoError(t, err)

	return ca.ARN
}

func TestACMPCAHandler_MissingOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		assert func(t *testing.T, h *acmpca.Handler)
		name   string
	}{
		{
			name: "permission_lifecycle",
			assert: func(t *testing.T, h *acmpca.Handler) {
				t.Helper()

				caARN := createHandlerCA(t, h)

				createRec := doACMPCARequest(t, h, "CreatePermission", map[string]any{
					"Actions":                 []string{"IssueCertificate"},
					"CertificateAuthorityArn": caARN,
					"Principal":               "acm.amazonaws.com",
					"SourceAccount":           testAccountID,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				listRec := doACMPCARequest(t, h, "ListPermissions", map[string]any{
					"CertificateAuthorityArn": caARN,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseACMPCAResponse(t, listRec)
				perms, ok := listResp["Permissions"].([]any)
				require.True(t, ok)
				require.Len(t, perms, 1)

				deleteRec := doACMPCARequest(t, h, "DeletePermission", map[string]any{
					"CertificateAuthorityArn": caARN,
					"Principal":               "acm.amazonaws.com",
					"SourceAccount":           testAccountID,
				})
				require.Equal(t, http.StatusOK, deleteRec.Code)
			},
		},
		{
			name: "policy_lifecycle",
			assert: func(t *testing.T, h *acmpca.Handler) {
				t.Helper()

				caARN := createHandlerCA(t, h)
				policy := `{"Version":"2012-10-17","Statement":[]}`

				putRec := doACMPCARequest(t, h, "PutPolicy", map[string]any{
					"Policy":      policy,
					"ResourceArn": caARN,
				})
				require.Equal(t, http.StatusOK, putRec.Code)

				getRec := doACMPCARequest(t, h, "GetPolicy", map[string]any{
					"ResourceArn": caARN,
				})
				require.Equal(t, http.StatusOK, getRec.Code)
				getResp := parseACMPCAResponse(t, getRec)
				assert.Equal(t, policy, getResp["Policy"])

				deleteRec := doACMPCARequest(t, h, "DeletePolicy", map[string]any{
					"ResourceArn": caARN,
				})
				require.Equal(t, http.StatusOK, deleteRec.Code)
			},
		},
		{
			name: "audit_report_and_restore",
			assert: func(t *testing.T, h *acmpca.Handler) {
				t.Helper()

				caARN := createHandlerCA(t, h)

				createAuditRec := doACMPCARequest(t, h, "CreateCertificateAuthorityAuditReport", map[string]any{
					"AuditReportResponseFormat": "JSON",
					"CertificateAuthorityArn":   caARN,
					"S3BucketName":              "audit-bucket",
				})
				require.Equal(t, http.StatusOK, createAuditRec.Code)
				auditResp := parseACMPCAResponse(t, createAuditRec)
				reportID, ok := auditResp["AuditReportId"].(string)
				require.True(t, ok)
				require.NotEmpty(t, reportID)

				describeAuditRec := doACMPCARequest(t, h, "DescribeCertificateAuthorityAuditReport", map[string]any{
					"AuditReportId":           reportID,
					"CertificateAuthorityArn": caARN,
				})
				require.Equal(t, http.StatusOK, describeAuditRec.Code)
				describeResp := parseACMPCAResponse(t, describeAuditRec)
				assert.Equal(t, "SUCCESS", describeResp["AuditReportStatus"])

				require.NoError(t, h.Backend.UpdateCertificateAuthority(context.Background(), caARN, "DISABLED"))
				require.NoError(t, h.Backend.DeleteCertificateAuthority(context.Background(), caARN, 0))

				restoreRec := doACMPCARequest(t, h, "RestoreCertificateAuthority", map[string]any{
					"CertificateAuthorityArn": caARN,
				})
				require.Equal(t, http.StatusOK, restoreRec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.assert(t, newACMPCAHandler())
		})
	}
}

func TestACMPCAHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want string
	}{
		{
			name: "certificate authority arn",
			body: map[string]any{
				"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-1",
			},
			want: "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-1",
		},
		{
			name: "resource arn",
			body: map[string]any{"ResourceArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-2"},
			want: "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-2",
		},
		{
			name: "no supported resource field",
			body: map[string]any{"AuditReportId": "report-1"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, newACMPCAHandler().ExtractResource(c))
		})
	}
}

func TestACMPCAHandler_NewOperationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantType string
		wantCode int
	}{
		{
			name:     "list permissions requires certificate authority arn",
			op:       "ListPermissions",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name:     "get policy requires resource arn",
			op:       "GetPolicy",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
		{
			name: "describe audit report requires report id",
			op:   "DescribeCertificateAuthorityAuditReport",
			body: map[string]any{
				"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-1",
			},
			wantCode: http.StatusBadRequest,
			wantType: "InvalidParameterException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doACMPCARequest(t, newACMPCAHandler(), tt.op, tt.body)
			require.Equal(t, tt.wantCode, rec.Code)
			resp := parseACMPCAResponse(t, rec)
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}

func TestACMPCAHandler_TagValidationAndCertificateChain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *acmpca.Handler)
		name string
	}{
		{
			name: "tagCA on non-existent CA returns error",
			run: func(t *testing.T, h *acmpca.Handler) {
				t.Helper()

				rec := doACMPCARequest(t, h, "TagCertificateAuthority", map[string]any{
					"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:000000000000:certificate-authority/nonexistent",
					"Tags":                    []map[string]string{{"Key": "env", "Value": "test"}},
				})
				require.Equal(t, http.StatusBadRequest, rec.Code)
				resp := parseACMPCAResponse(t, rec)
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
			},
		},
		{
			name: "issueCert with MONTHS validity",
			run: func(t *testing.T, h *acmpca.Handler) {
				t.Helper()

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
			},
		},
		{
			name: "getCert returns chain",
			run: func(t *testing.T, h *acmpca.Handler) {
				t.Helper()

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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newACMPCAHandler())
		})
	}
}

// TestACMPCAHandler_Base64BlobFields verifies that IssueCertificate.Csr and
// ImportCertificateAuthorityCertificate.Certificate/CertificateChain are decoded
// as base64, matching the wire format aws-sdk-go-v2 produces for their []byte
// ([]byte) Go types (see serializers.go Base64EncodeBytes calls upstream). A raw
// (non-base64) PEM string must be rejected, and a base64-encoded PEM must be
// accepted.
func TestACMPCAHandler_Base64BlobFields(t *testing.T) {
	t.Parallel()

	t.Run("IssueCertificate rejects raw (non-base64) Csr", func(t *testing.T) {
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

	t.Run("IssueCertificate accepts base64-encoded Csr", func(t *testing.T) {
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

	t.Run("ImportCertificateAuthorityCertificate rejects raw (non-base64) Certificate", func(t *testing.T) {
		t.Parallel()

		h := newACMPCAHandler()

		subCA, err := h.Backend.CreateCertificateAuthority(
			context.Background(),
			"SUBORDINATE",
			acmpca.CertificateAuthorityConfiguration{
				Subject: acmpca.CertificateAuthoritySubject{CommonName: "Sub CA"},
			},
		)
		require.NoError(t, err)

		rec := doACMPCARequest(t, h, "ImportCertificateAuthorityCertificate", map[string]any{
			"CertificateAuthorityArn": subCA.ARN,
			"Certificate":             "-----BEGIN CERTIFICATE-----\nnotbase64\n-----END CERTIFICATE-----",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)
		resp := parseACMPCAResponse(t, rec)
		assert.Equal(t, "InvalidParameterException", resp["__type"])
	})

	t.Run("ImportCertificateAuthorityCertificate accepts base64-encoded Certificate", func(t *testing.T) {
		t.Parallel()

		h := newACMPCAHandler()

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

		csr, err := h.Backend.GetCertificateAuthorityCsr(context.Background(), subCA.ARN)
		require.NoError(t, err)

		cert, err := h.Backend.IssueCertificate(context.Background(), rootCA.ARN, csr, 365)
		require.NoError(t, err)

		issued, err := h.Backend.GetCertificate(context.Background(), rootCA.ARN, cert.ARN)
		require.NoError(t, err)

		rec := doACMPCARequest(t, h, "ImportCertificateAuthorityCertificate", map[string]any{
			"CertificateAuthorityArn": subCA.ARN,
			"Certificate":             b64(issued.CertBody),
		})
		require.Equal(t, http.StatusOK, rec.Code)

		describeRec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
			"CertificateAuthorityArn": subCA.ARN,
		})
		require.Equal(t, http.StatusOK, describeRec.Code)
		describeResp := parseACMPCAResponse(t, describeRec)
		caOut, ok := describeResp["CertificateAuthority"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "ACTIVE", caOut["Status"])
	})
}

// TestACMPCAHandler_CreatePermission_Duplicate verifies that granting the same
// principal/source-account permission twice returns PermissionAlreadyExistsException
// on the wire, matching real AWS ACM PCA behavior.
func TestACMPCAHandler_CreatePermission_Duplicate(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
	caARN := createHandlerCA(t, h)

	body := map[string]any{
		"Actions":                 []string{"IssueCertificate"},
		"CertificateAuthorityArn": caARN,
		"Principal":               "acm.amazonaws.com",
		"SourceAccount":           testAccountID,
	}

	firstRec := doACMPCARequest(t, h, "CreatePermission", body)
	require.Equal(t, http.StatusOK, firstRec.Code)

	secondRec := doACMPCARequest(t, h, "CreatePermission", body)
	require.Equal(t, http.StatusBadRequest, secondRec.Code)
	resp := parseACMPCAResponse(t, secondRec)
	assert.Equal(t, "PermissionAlreadyExistsException", resp["__type"])
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

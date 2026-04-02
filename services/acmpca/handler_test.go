package acmpca_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

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

	ca, err := h.Backend.CreateCertificateAuthority("ROOT", acmpca.CertificateAuthorityConfiguration{
		Subject: acmpca.CertificateAuthoritySubject{CommonName: "Handler CA"},
	})
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

				require.NoError(t, h.Backend.UpdateCertificateAuthority(caARN, "DISABLED"))
				require.NoError(t, h.Backend.DeleteCertificateAuthority(caARN))

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

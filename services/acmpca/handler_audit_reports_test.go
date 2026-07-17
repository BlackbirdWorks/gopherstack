package acmpca_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestACMPCA_CreateAuditReport_RequiresActiveCA verifies that
// CreateCertificateAuthorityAuditReport requires an ACTIVE CA.
func TestACMPCA_CreateAuditReport_RequiresActiveCA(t *testing.T) {
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
			ca, err := h.Backend.CreateCertificateAuthority(
				context.Background(),
				tt.caType,
				acmpca.CertificateAuthorityConfiguration{
					Subject: acmpca.CertificateAuthoritySubject{CommonName: "CA"},
				},
			)
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

// TestACMPCAHandler_AuditReportAndRestore verifies the create/describe audit
// report flow followed by a CA delete+restore, via the handler dispatch path.
func TestACMPCAHandler_AuditReportAndRestore(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()
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
}

// TestACMPCAHandler_DescribeAuditReport_RequiresReportID verifies that
// DescribeCertificateAuthorityAuditReport without an AuditReportId returns
// InvalidParameterException.
func TestACMPCAHandler_DescribeAuditReport_RequiresReportID(t *testing.T) {
	t.Parallel()

	rec := doACMPCARequest(t, newACMPCAHandler(), "DescribeCertificateAuthorityAuditReport", map[string]any{
		"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-1",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	resp := parseACMPCAResponse(t, rec)
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

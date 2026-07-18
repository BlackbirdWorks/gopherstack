package acmpca_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// TestACMPCAHandler_ImportCertificateBase64 verifies that
// ImportCertificateAuthorityCertificate.Certificate is decoded as base64,
// matching the wire format aws-sdk-go-v2 produces for its []byte Go type.
func TestACMPCAHandler_ImportCertificateBase64(t *testing.T) {
	t.Parallel()

	t.Run("rejects raw (non-base64) Certificate", func(t *testing.T) {
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

	t.Run("accepts base64-encoded Certificate", func(t *testing.T) {
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

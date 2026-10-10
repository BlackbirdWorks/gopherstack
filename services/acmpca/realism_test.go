package acmpca_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func TestCreateCertificateAuthority_AlgorithmValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     acmpcatypes.KeyAlgorithm
		sign    acmpcatypes.SigningAlgorithm
		wantErr bool
	}{
		{name: "rsa with rsa", key: "RSA_2048", sign: "SHA256WITHRSA"},
		{name: "rsa with pss", key: "RSA_4096", sign: "SHA512WITHRSA_PSS"},
		{name: "ec with ecdsa", key: "EC_secp384r1", sign: "SHA384WITHECDSA"},
		{name: "rsa with ecdsa", key: "RSA_2048", sign: "SHA256WITHECDSA", wantErr: true},
		{name: "ec with rsa", key: "EC_prime256v1", sign: "SHA256WITHRSA", wantErr: true},
		{name: "unknown key", key: "BOGUS", sign: "SHA256WITHRSA", wantErr: true},
		{name: "unknown signing", key: "RSA_2048", sign: "BOGUS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestACMPCASDKClient(t, acmpca.NewHandler(newTestBackend()))

			_, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
				CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
				CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
					KeyAlgorithm:     tt.key,
					SigningAlgorithm: tt.sign,
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("alg")},
				},
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			var ia *acmpcatypes.InvalidArgsException
			require.ErrorAs(t, err, &ia)
			assert.False(t, strings.HasPrefix(aws.ToString(ia.Message), "InvalidArgsException"), ia.Message)
		})
	}
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	client := newTestACMPCASDKClient(t, acmpca.NewHandler(newTestBackend()))

	_, err := client.DescribeCertificateAuthority(t.Context(), &acmpcasdk.DescribeCertificateAuthorityInput{
		CertificateAuthorityArn: aws.String("arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/missing"),
	})

	var nf *acmpcatypes.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.False(t, strings.HasPrefix(aws.ToString(nf.Message), "ResourceNotFoundException"), nf.Message)
}

func TestListPagination_InvalidNextToken(t *testing.T) {
	t.Parallel()

	client := newTestACMPCASDKClient(t, acmpca.NewHandler(newTestBackend()))

	_, err := client.ListCertificateAuthorities(t.Context(), &acmpcasdk.ListCertificateAuthoritiesInput{
		NextToken: aws.String("%%not-a-token%%"),
	})

	var nt *acmpcatypes.InvalidNextTokenException
	require.ErrorAs(t, err, &nt)
}

func TestAuditReport_CreatingUntilDelayElapses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status acmpcatypes.AuditReportStatus
		delay  time.Duration
	}{
		{name: "default", delay: 0, status: acmpcatypes.AuditReportStatusSuccess},
		{name: "delayed", delay: time.Hour, status: acmpcatypes.AuditReportStatusCreating},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := newTestBackend()
			backend.SetAuditReportDelay(tt.delay)
			client := newTestACMPCASDKClient(t, acmpca.NewHandler(backend))

			ca, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
				CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
				CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
					KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
					SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
					Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("audit")},
				},
			})
			require.NoError(t, err)

			rep, err := client.CreateCertificateAuthorityAuditReport(t.Context(),
				&acmpcasdk.CreateCertificateAuthorityAuditReportInput{
					CertificateAuthorityArn:   ca.CertificateAuthorityArn,
					S3BucketName:              aws.String("audit-bucket"),
					AuditReportResponseFormat: acmpcatypes.AuditReportResponseFormatJson,
				})
			require.NoError(t, err)

			got, err := client.DescribeCertificateAuthorityAuditReport(t.Context(),
				&acmpcasdk.DescribeCertificateAuthorityAuditReportInput{
					CertificateAuthorityArn: ca.CertificateAuthorityArn,
					AuditReportId:           rep.AuditReportId,
				})
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.AuditReportStatus)
		})
	}
}

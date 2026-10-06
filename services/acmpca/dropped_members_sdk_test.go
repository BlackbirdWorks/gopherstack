package acmpca_test

import (
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

func newDroppedMembersClient(t *testing.T) *acmpcasdk.Client {
	t.Helper()

	return newTestACMPCASDKClient(t, acmpca.NewHandler(acmpca.NewInMemoryBackend(testAccountID, testRegion)))
}

func TestCreateCertificateAuthority_FullSubjectAndCsrExtensions(t *testing.T) {
	t.Parallel()

	oidSIA := asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 11}
	oidKeyUsage := asn1.ObjectIdentifier{2, 5, 29, 15}

	tests := []struct {
		name    string
		sia     []acmpcatypes.AccessDescription
		custom  []acmpcatypes.CustomAttribute
		wantErr bool
	}{
		{
			name: "full",
			sia: []acmpcatypes.AccessDescription{{
				AccessMethod: &acmpcatypes.AccessMethod{AccessMethodType: acmpcatypes.AccessMethodTypeCaRepository},
				AccessLocation: &acmpcatypes.GeneralName{
					UniformResourceIdentifier: aws.String("https://example.com/repo"),
				},
			}},
			custom: []acmpcatypes.CustomAttribute{
				{ObjectIdentifier: aws.String("1.2.3.4.5"), Value: aws.String("custom")},
			},
		},
		{
			name: "bad_custom_oid",
			custom: []acmpcatypes.CustomAttribute{
				{ObjectIdentifier: aws.String("not-an-oid"), Value: aws.String("x")},
			},
			wantErr: true,
		},
		{
			name: "access_method_missing",
			sia: []acmpcatypes.AccessDescription{{
				AccessMethod:   &acmpcatypes.AccessMethod{},
				AccessLocation: &acmpcatypes.GeneralName{DnsName: aws.String("example.com")},
			}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			subject := &acmpcatypes.ASN1Subject{
				CommonName:                 aws.String("Full Subject CA"),
				SerialNumber:               aws.String("SN-42"),
				GivenName:                  aws.String("Ada"),
				Surname:                    aws.String("Lovelace"),
				Title:                      aws.String("Dr"),
				Initials:                   aws.String("AL"),
				Pseudonym:                  aws.String("ada"),
				GenerationQualifier:        aws.String("III"),
				DistinguishedNameQualifier: aws.String("dnq"),
				CustomAttributes:           tt.custom,
			}

			created, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
				CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeSubordinate,
				CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
					KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
					SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
					Subject:          subject,
					CsrExtensions: &acmpcatypes.CsrExtensions{
						KeyUsage:                 &acmpcatypes.KeyUsage{KeyCertSign: true, CRLSign: true},
						SubjectInformationAccess: tt.sia,
					},
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			desc, err := client.DescribeCertificateAuthority(t.Context(), &acmpcasdk.DescribeCertificateAuthorityInput{
				CertificateAuthorityArn: created.CertificateAuthorityArn,
			})
			require.NoError(t, err)

			cfg := desc.CertificateAuthority.CertificateAuthorityConfiguration
			assert.Equal(t, "SN-42", aws.ToString(cfg.Subject.SerialNumber))
			assert.Equal(t, "Ada", aws.ToString(cfg.Subject.GivenName))
			assert.Equal(t, "Lovelace", aws.ToString(cfg.Subject.Surname))
			assert.Equal(t, "dnq", aws.ToString(cfg.Subject.DistinguishedNameQualifier))
			require.Len(t, cfg.Subject.CustomAttributes, 1)
			assert.Equal(t, "1.2.3.4.5", aws.ToString(cfg.Subject.CustomAttributes[0].ObjectIdentifier))
			require.NotNil(t, cfg.CsrExtensions)
			assert.True(t, cfg.CsrExtensions.KeyUsage.KeyCertSign)
			assert.False(t, cfg.CsrExtensions.KeyUsage.DigitalSignature)
			require.Len(t, cfg.CsrExtensions.SubjectInformationAccess, 1)
			assert.Equal(t, acmpcatypes.AccessMethodTypeCaRepository,
				cfg.CsrExtensions.SubjectInformationAccess[0].AccessMethod.AccessMethodType)
			assert.Equal(t, "https://example.com/repo",
				aws.ToString(cfg.CsrExtensions.SubjectInformationAccess[0].AccessLocation.UniformResourceIdentifier))

			csrOut, err := client.GetCertificateAuthorityCsr(t.Context(), &acmpcasdk.GetCertificateAuthorityCsrInput{
				CertificateAuthorityArn: created.CertificateAuthorityArn,
			})
			require.NoError(t, err)

			block, _ := pem.Decode([]byte(aws.ToString(csrOut.Csr)))
			require.NotNil(t, block)

			csr, err := x509.ParseCertificateRequest(block.Bytes)
			require.NoError(t, err)
			assert.Equal(t, "SN-42", csr.Subject.SerialNumber)
			assert.Len(t, csr.Subject.Names, 10)

			found := map[string]bool{}
			for _, e := range csr.Extensions {
				found[e.Id.String()] = true
			}

			assert.True(t, found[oidKeyUsage.String()], "keyUsage extension in CSR")
			assert.True(t, found[oidSIA.String()], "subjectInformationAccess extension in CSR")
		})
	}
}

func TestIssueCertificate_EndDateFormats(t *testing.T) {
	t.Parallel()

	future := time.Now().UTC().AddDate(2, 0, 0)

	tests := []struct {
		name    string
		value   int64
		wantErr bool
	}{
		{name: "utctime", value: mustInt(t, future.Format("060102150405"))},
		{name: "generalizedtime", value: mustInt(t, future.Format("20060102150405"))},
		{name: "epoch_seconds_rejected", value: future.Unix(), wantErr: true},
		{name: "invalid_month", value: 301301000000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			rootArn := createTestRootCA(t, client, "Root")
			subArn := createSubordinateForEndDate(t, client)

			csr, err := client.GetCertificateAuthorityCsr(t.Context(), &acmpcasdk.GetCertificateAuthorityCsrInput{
				CertificateAuthorityArn: aws.String(subArn),
			})
			require.NoError(t, err)

			issued, err := client.IssueCertificate(t.Context(), &acmpcasdk.IssueCertificateInput{
				CertificateAuthorityArn: aws.String(rootArn),
				Csr:                     []byte(aws.ToString(csr.Csr)),
				SigningAlgorithm:        acmpcatypes.SigningAlgorithmSha256withecdsa,
				Validity: &acmpcatypes.Validity{
					Type:  acmpcatypes.ValidityPeriodTypeEndDate,
					Value: aws.Int64(tt.value),
				},
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got, err := client.GetCertificate(t.Context(), &acmpcasdk.GetCertificateInput{
				CertificateAuthorityArn: aws.String(rootArn),
				CertificateArn:          issued.CertificateArn,
			})
			require.NoError(t, err)

			block, _ := pem.Decode([]byte(aws.ToString(got.Certificate)))
			require.NotNil(t, block)

			cert, err := x509.ParseCertificate(block.Bytes)
			require.NoError(t, err)
			assert.WithinDuration(t, future, cert.NotAfter, 48*time.Hour)
		})
	}
}

func mustInt(t *testing.T, s string) int64 {
	t.Helper()

	v, err := strconv.ParseInt(s, 10, 64)
	require.NoError(t, err)

	return v
}

func createSubordinateForEndDate(t *testing.T, client *acmpcasdk.Client) string {
	t.Helper()

	out, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeSubordinate,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject:          &acmpcatypes.ASN1Subject{CommonName: aws.String("Sub")},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.CertificateAuthorityArn)
}

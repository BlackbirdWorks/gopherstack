package acm_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

func vsMakeCert(t *testing.T, domain string, sans []string) (string, string) {
	t.Helper()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate(context.Background(), domain, "", "", "", "", "", "", sans)
	require.NoError(t, err)

	return cert.CertificateBody, cert.PrivateKey
}

func TestImportCertificate_ReimportReplacesSANsKeepsTags(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		firstSANs  []string
		secondSANs []string
	}{
		{
			name:       "sans follow the certificate",
			firstSANs:  []string{"b.example.com"},
			secondSANs: []string{"d.example.com", "e.example.com"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestACMClient(t, acm.NewHandler(acm.NewInMemoryBackend("000000000000", "us-east-1")))

			bodyA, keyA := vsMakeCert(t, "a.example.com", tc.firstSANs)
			bodyB, keyB := vsMakeCert(t, "c.example.com", tc.secondSANs)

			imp, err := client.ImportCertificate(ctx, &acmsdk.ImportCertificateInput{
				Certificate: []byte(bodyA), PrivateKey: []byte(keyA),
				Tags: []acmtypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)

			got, err := client.DescribeCertificate(
				ctx,
				&acmsdk.DescribeCertificateInput{CertificateArn: imp.CertificateArn},
			)
			require.NoError(t, err)
			assert.Contains(t, got.Certificate.SubjectAlternativeNames, "b.example.com")

			_, err = client.ImportCertificate(ctx, &acmsdk.ImportCertificateInput{
				CertificateArn: imp.CertificateArn, Certificate: []byte(bodyB), PrivateKey: []byte(keyB),
			})
			require.NoError(t, err)

			got, err = client.DescribeCertificate(
				ctx,
				&acmsdk.DescribeCertificateInput{CertificateArn: imp.CertificateArn},
			)
			require.NoError(t, err)
			assert.Equal(t, "c.example.com", aws.ToString(got.Certificate.DomainName))
			assert.ElementsMatch(
				t,
				append([]string{"c.example.com"}, tc.secondSANs...),
				got.Certificate.SubjectAlternativeNames,
			)

			tags, err := client.ListTagsForCertificate(
				ctx,
				&acmsdk.ListTagsForCertificateInput{CertificateArn: imp.CertificateArn},
			)
			require.NoError(t, err)
			require.Len(t, tags.Tags, 1)
		})
	}
}

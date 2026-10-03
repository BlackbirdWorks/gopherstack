package acm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListCertificates_KeyTypesDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		includes *types.Filters
		want     []string
	}{
		{name: "omitted_returns_rsa_only", want: []string{"rsa.example.com"}},
		{
			name:     "ec_requested_returns_ec_only",
			includes: &types.Filters{KeyTypes: []types.KeyAlgorithm{types.KeyAlgorithmEcPrime256v1}},
			want:     []string{"ec.example.com"},
		},
		{
			name: "both_requested_returns_both",
			includes: &types.Filters{
				KeyTypes: []types.KeyAlgorithm{types.KeyAlgorithmEcPrime256v1, types.KeyAlgorithmRsa2048},
			},
			want: []string{"ec.example.com", "rsa.example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestACMClient(t, newACMHandler())

			def, err := client.RequestCertificate(t.Context(), &acmsdk.RequestCertificateInput{
				DomainName: aws.String("rsa.example.com"),
			})
			require.NoError(t, err)

			desc, err := client.DescribeCertificate(t.Context(), &acmsdk.DescribeCertificateInput{
				CertificateArn: def.CertificateArn,
			})
			require.NoError(t, err)
			assert.Equal(t, types.KeyAlgorithmRsa2048, desc.Certificate.KeyAlgorithm)

			_, err = client.RequestCertificate(t.Context(), &acmsdk.RequestCertificateInput{
				DomainName: aws.String("ec.example.com"), KeyAlgorithm: types.KeyAlgorithmEcPrime256v1,
			})
			require.NoError(t, err)

			out, err := client.ListCertificates(t.Context(), &acmsdk.ListCertificatesInput{Includes: tt.includes})
			require.NoError(t, err)

			var got []string
			for _, c := range out.CertificateSummaryList {
				got = append(got, aws.ToString(c.DomainName))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

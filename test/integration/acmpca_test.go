package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/acmpca"
	"github.com/stretchr/testify/require"
)

func createACMPCAClient(t *testing.T) *acmpca.Client {
	t.Helper()
	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return acmpca.NewFromConfig(cfg, func(o *acmpca.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func TestIntegration_ACMPCA_ListCertificateAuthorities(t *testing.T) {
	t.Parallel()
	client := createACMPCAClient(t)

	res, err := client.ListCertificateAuthorities(t.Context(), &acmpca.ListCertificateAuthoritiesInput{})
	require.NoError(t, err)
	require.NotNil(t, res)
}

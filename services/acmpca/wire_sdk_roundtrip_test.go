package acmpca_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	acmpcasdk "github.com/aws/aws-sdk-go-v2/service/acmpca"
	acmpcatypes "github.com/aws/aws-sdk-go-v2/service/acmpca/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// newTestACMPCASDKClient stands up the real aws-sdk-go-v2 acmpca client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- so a shape is
// verified by the real client's own deserializer, not gopherstack's own JSON
// tags.
func newTestACMPCASDKClient(t *testing.T, h *acmpca.Handler) *acmpcasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return acmpcasdk.NewFromConfig(cfg, func(o *acmpcasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDescribeCertificateAuthority_SDKRoundTrip proves the deeply nested
// CertificateAuthorityConfiguration.Subject and RevocationConfiguration
// (CrlConfiguration/OcspConfiguration) shapes survive the real SDK client's
// deserializer (aws-sdk-go-v2/service/acmpca@v1.50.0's
// awsAwsjson11_deserializeOpDocumentDescribeCertificateAuthorityOutput,
// which nests CertificateAuthority under the "CertificateAuthority" key). A
// wrong wrapper key, wrong nesting level, or wrong JSON type anywhere in this
// chain would silently zero out the corresponding typed field, or fail the
// whole call, rather than surface as a JSON-map assertion mismatch.
func TestDescribeCertificateAuthority_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := acmpca.NewInMemoryBackend(testAccountID, testRegion)
	h := acmpca.NewHandler(backend)
	client := newTestACMPCASDKClient(t, h)

	created, err := client.CreateCertificateAuthority(t.Context(), &acmpcasdk.CreateCertificateAuthorityInput{
		CertificateAuthorityType: acmpcatypes.CertificateAuthorityTypeRoot,
		CertificateAuthorityConfiguration: &acmpcatypes.CertificateAuthorityConfiguration{
			KeyAlgorithm:     acmpcatypes.KeyAlgorithmEcPrime256v1,
			SigningAlgorithm: acmpcatypes.SigningAlgorithmSha256withecdsa,
			Subject: &acmpcatypes.ASN1Subject{
				CommonName:         aws.String("Test Root CA"),
				Country:            aws.String("US"),
				Organization:       aws.String("Gopherstack"),
				OrganizationalUnit: aws.String("Eng"),
				State:              aws.String("WA"),
				Locality:           aws.String("Seattle"),
			},
		},
		KeyStorageSecurityStandard: acmpcatypes.KeyStorageSecurityStandardFips1402Level3OrHigher,
		UsageMode:                  acmpcatypes.CertificateAuthorityUsageModeGeneralPurpose,
		RevocationConfiguration: &acmpcatypes.RevocationConfiguration{
			CrlConfiguration: &acmpcatypes.CrlConfiguration{
				Enabled:      aws.Bool(true),
				S3BucketName: aws.String("my-crl-bucket"),
				CrlType:      acmpcatypes.CrlTypeComplete,
			},
			OcspConfiguration: &acmpcatypes.OcspConfiguration{
				Enabled: aws.Bool(true),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.CertificateAuthorityArn)

	out, err := client.DescribeCertificateAuthority(t.Context(), &acmpcasdk.DescribeCertificateAuthorityInput{
		CertificateAuthorityArn: created.CertificateAuthorityArn,
	})
	require.NoError(t, err)
	require.NotNil(t, out.CertificateAuthority)

	ca := out.CertificateAuthority
	assert.Equal(t, *created.CertificateAuthorityArn, aws.ToString(ca.Arn))
	assert.Equal(t, acmpcatypes.CertificateAuthorityTypeRoot, ca.Type)
	assert.Equal(t, acmpcatypes.CertificateAuthorityUsageModeGeneralPurpose, ca.UsageMode)
	assert.Equal(
		t,
		acmpcatypes.KeyStorageSecurityStandardFips1402Level3OrHigher,
		ca.KeyStorageSecurityStandard,
	)

	require.NotNil(t, ca.CertificateAuthorityConfiguration)
	require.NotNil(t, ca.CertificateAuthorityConfiguration.Subject)
	assert.Equal(t, "Test Root CA", aws.ToString(ca.CertificateAuthorityConfiguration.Subject.CommonName))
	assert.Equal(t, "US", aws.ToString(ca.CertificateAuthorityConfiguration.Subject.Country))
	assert.Equal(t, "Gopherstack", aws.ToString(ca.CertificateAuthorityConfiguration.Subject.Organization))
	assert.Equal(
		t,
		acmpcatypes.KeyAlgorithmEcPrime256v1,
		ca.CertificateAuthorityConfiguration.KeyAlgorithm,
	)
	assert.Equal(
		t,
		acmpcatypes.SigningAlgorithmSha256withecdsa,
		ca.CertificateAuthorityConfiguration.SigningAlgorithm,
	)

	require.NotNil(t, ca.RevocationConfiguration)
	require.NotNil(t, ca.RevocationConfiguration.CrlConfiguration)
	assert.True(t, aws.ToBool(ca.RevocationConfiguration.CrlConfiguration.Enabled))
	assert.Equal(t, "my-crl-bucket", aws.ToString(ca.RevocationConfiguration.CrlConfiguration.S3BucketName))
	assert.Equal(t, acmpcatypes.CrlTypeComplete, ca.RevocationConfiguration.CrlConfiguration.CrlType)
	require.NotNil(t, ca.RevocationConfiguration.OcspConfiguration)
	assert.True(t, aws.ToBool(ca.RevocationConfiguration.OcspConfiguration.Enabled))
}

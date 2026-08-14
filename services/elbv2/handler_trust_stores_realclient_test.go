package elbv2_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// newTestELBv2Client stands up the real aws-sdk-go-v2 ELBv2 client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestELBv2Client(t *testing.T, h *elbv2.Handler) *elbv2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return elbv2sdk.NewFromConfig(cfg, func(o *elbv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestDescribeTrustStores_RealClient drives DescribeTrustStores through the real
// aws-sdk-go-v2 ELBv2 client. The real deserializer
// (awsAwsquery_deserializeDocumentTrustStore, elasticloadbalancingv2@v1.58.5
// deserializers.go:17481) reads the CA-certificate count under
// "NumberOfCaCertificates". gopherstack emitted "NumberOfCaCerts" (confirmed by
// hand-reverting), a name close enough to look right on skim but not
// case-fold-equal to the real one, so the typed client decoded the field to nil
// on every trust store even though the wrapper key and item count were correct.
func TestDescribeTrustStores_RealClient(t *testing.T) {
	t.Parallel()

	h := elbv2.NewHandler(elbv2.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestELBv2Client(t, h)

	_, err := client.CreateTrustStore(t.Context(), &elbv2sdk.CreateTrustStoreInput{
		Name:                         aws.String("real-client-ts"),
		CaCertificatesBundleS3Bucket: aws.String("test-bucket"),
		CaCertificatesBundleS3Key:    aws.String("test-key.pem"),
	})
	require.NoError(t, err)

	out, err := client.DescribeTrustStores(t.Context(), &elbv2sdk.DescribeTrustStoresInput{})
	require.NoError(t, err)
	require.Len(t, out.TrustStores, 1)
	require.NotNil(
		t, out.TrustStores[0].NumberOfCaCertificates,
		"NumberOfCaCertificates decoded to nil - wire tag name mismatch",
	)
}

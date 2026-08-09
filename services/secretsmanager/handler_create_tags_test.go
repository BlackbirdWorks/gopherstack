package secretsmanager_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const smTagsRTRegion = "us-east-1"

// newTestSecretsManagerClient stands up the real aws-sdk-go-v2
// secretsmanager client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestSecretsManagerClient(t *testing.T, h *secretsmanager.Handler) *secretsmanagersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(smTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return secretsmanagersdk.NewFromConfig(cfg, func(o *secretsmanagersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateSecret_TagsRoundTrip drives CreateSecret, whose real Input
// struct accepts Tags (secretsmanager@v1.44.4 api_op_CreateSecret.go:213),
// through the real SDK client and asserts ListTagsForResource-equivalent
// (DescribeSecret.Tags) sees what was supplied at creation (gopherstack-2mwl).
func TestCreateSecret_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	client := newTestSecretsManagerClient(t, secretsmanager.NewHandler(backend))

	out, err := client.CreateSecret(t.Context(), &secretsmanagersdk.CreateSecretInput{
		Name:         aws.String("tagged-secret"),
		SecretString: aws.String("shh"),
		Tags:         []smtypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	got, err := client.DescribeSecret(t.Context(), &secretsmanagersdk.DescribeSecretInput{
		SecretId: out.ARN,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))
}

package secretsmanager_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	secretsmanagersdk "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const wireFixesRegion = "us-west-2"

// newTestSMClientWithRegion mirrors newTestSecretsManagerClient in
// handler_create_tags_test.go but lets the caller choose the region, since
// this file's fix concerns the PrimaryRegion wire field.
func newTestSMClientWithRegion(t *testing.T, h *secretsmanager.Handler, region string) *secretsmanagersdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return secretsmanagersdk.NewFromConfig(cfg, func(o *secretsmanagersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListSecrets_PrimaryRegion_RealClient covers a layer-3 bug (gopherstack-g8k9):
// every Secret already carries its creation region on the unexported
// Secret.region field (secrets.go's CreateSecret), and DescribeSecret already
// surfaces it as KeyMetadata... err, DescribeSecretOutput.PrimaryRegion
// (secrets.go:586), but ListSecrets' SecretListEntry never carried the same
// field despite secretToListEntry building from the identical *Secret.
// Real field name and presence on SecretListEntry confirmed against
// secretsmanager@v1.44.4 deserializers.go's
// awsAwsjson11_deserializeDocumentSecretListEntry (case "PrimaryRegion":).
// Pre-fix, a real client's ListSecrets always showed a nil PrimaryRegion for
// every secret regardless of which region it was created in.
func TestListSecrets_PrimaryRegion_RealClient(t *testing.T) {
	t.Parallel()

	backend := secretsmanager.NewInMemoryBackend()
	client := newTestSMClientWithRegion(t, secretsmanager.NewHandler(backend), wireFixesRegion)
	ctx := t.Context()

	_, err := client.CreateSecret(ctx, &secretsmanagersdk.CreateSecretInput{
		Name:         aws.String("region-tagged-secret"),
		SecretString: aws.String("shh"),
	})
	require.NoError(t, err)

	out, err := client.ListSecrets(ctx, &secretsmanagersdk.ListSecretsInput{})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)

	entry := out.SecretList[0]
	require.NotNil(t, entry.PrimaryRegion,
		"SecretListEntry.PrimaryRegion must round-trip from the secret's creation region; pre-fix it was always nil")
	assert.Equal(t, wireFixesRegion, aws.ToString(entry.PrimaryRegion))
}

package backup_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/backup"
	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// newTestBackupResourceGroupsRegistryServer wires up Backup's and Resource
// Groups' real Handlers into one shared registry/router, the way cli.go
// does. Both claim a "/resources/" path prefix: Resource Groups' generic
// tagging API lives at /resources/{Arn}/tags (resourcegroups@v1.36.4
// serializers.go) and Backup's DescribeProtectedResource is GET
// /resources/{ResourceArn} (backup@v1.64.0 serializers.go:2828) -- a bare
// resource ARN with no "/tags" suffix. cmd/routecollisions flags this as
// UNGUARDED-WINNER/unguarded because it only sees resourcegroups'
// strings.HasPrefix(path, "/resources/") half of isResourceTagsPath; it
// misses the compound HasPrefix-AND-HasSuffix(path, "/tags") check
// (gopherstack-op3e census sweep 2026-09-19).
func newTestBackupResourceGroupsRegistryServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()

	require.NoError(t, registry.Register(
		resourcegroups.NewHandler(resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	require.NoError(t, registry.Register(
		backup.NewHandler(backup.NewInMemoryBackend("000000000000", "us-east-1")),
	))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

func newResourcesRoutingBackupClient(t *testing.T, baseURL string) *backupsdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return backupsdk.NewFromConfig(cfg, func(o *backupsdk.Options) {
		o.BaseEndpoint = aws.String(baseURL)
	})
}

// TestResourcesRouting_BackupNotShadowedByResourceGroups proves Backup's
// bare /resources/{ResourceArn} still reaches Backup's own handler
// (ResourceNotFoundException for an unknown ARN) when Resource Groups
// (registered first, MatchPriority 100 vs Backup's 85) is in the same
// router: resourcegroups only claims that space when the path also ends in
// "/tags", which DescribeProtectedResource's path never does.
func TestResourcesRouting_BackupNotShadowedByResourceGroups(t *testing.T) {
	t.Parallel()

	srv := newTestBackupResourceGroupsRegistryServer(t)
	client := newResourcesRoutingBackupClient(t, srv.URL)

	_, err := client.DescribeProtectedResource(t.Context(), &backupsdk.DescribeProtectedResourceInput{
		ResourceArn: aws.String("arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"),
	})
	require.Error(t, err, "describing an unknown protected resource must fail")

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(
		t, "ResourceNotFoundException", apiErr.ErrorCode(),
		"must be backup's own not-found error, not resourcegroups swallowing the path",
	)
}

package grafana_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanatypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/grafana"
)

const tagsRTRegion = "us-east-1"

// newTestGrafanaClient stands up the real aws-sdk-go-v2 grafana client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestGrafanaClient(t *testing.T, h *grafana.Handler) *grafanasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(tagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return grafanasdk.NewFromConfig(cfg, func(o *grafanasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateWorkspace_TagsRoundTrip drives CreateWorkspace, the only real
// grafana op whose Input struct accepts Tags (grafana@v1.38.4:
// api_op_CreateWorkspace.go -- "Currently, the only resource that can be
// tagged is workspaces", ListTagsForResourceInput doc comment), through the
// real SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl). WorkspaceDescription carries no Arn field in
// the real SDK, so the ARN is built by convention from the account/region and
// the returned Id, same as terraform-provider-aws does (see store.go's
// WorkspaceARN doc comment).
func TestCreateWorkspace_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	const accountID = "000000000000"

	backend := grafana.NewInMemoryBackend(t.Context(), accountID, tagsRTRegion)
	client := newTestGrafanaClient(t, grafana.NewHandler(backend))

	out, err := client.CreateWorkspace(t.Context(), &grafanasdk.CreateWorkspaceInput{
		AccountAccessType: grafanatypes.AccountAccessTypeCurrentAccount,
		AuthenticationProviders: []grafanatypes.AuthenticationProviderTypes{
			grafanatypes.AuthenticationProviderTypesAwsSso,
		},
		PermissionType: grafanatypes.PermissionTypeServiceManaged,
		Tags:           map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	id := aws.ToString(out.Workspace.Id)
	require.NotEmpty(t, id)
	resourceArn := backend.WorkspaceARN(id)

	got, err := client.ListTagsForResource(t.Context(), &grafanasdk.ListTagsForResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "test", got.Tags["env"])
}
